// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

using System.Globalization;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using StackExchange.Redis;

namespace Boutique.Stateless;

public sealed record AggregateKeys(string State, string Version, string Inbox)
{
    public static AggregateKeys For(string prefix, string aggregateId, string inputMessageId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(inputMessageId);
        var (state, version, baseKey) = ForAggregate(prefix, aggregateId);
        return new AggregateKeys(
            state,
            version,
            $"{baseKey}:inbox:{Digest(inputMessageId)}");
    }

    public static (string State, string Version, string Base) ForAggregate(
        string prefix,
        string aggregateId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        ArgumentException.ThrowIfNullOrWhiteSpace(aggregateId);
        var normalizedPrefix = prefix.Trim().Trim(':');
        var baseKey = $"{normalizedPrefix}:{{{Digest(aggregateId)}}}";
        return ($"{baseKey}:state", $"{baseKey}:version", baseKey);
    }

    private static string Digest(string value) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
}

public sealed record AtomicAggregateSnapshot(
    ulong Version,
    ReadOnlyMemory<byte> State);

public sealed record AtomicCommitRequest(
    string AggregateId,
    string InputMessageId,
    ulong ExpectedVersion,
    ReadOnlyMemory<byte> NextState,
    ReadOnlyMemory<byte> Journal,
    TimeSpan JournalRetention,
    bool AdvanceVersion = true);

public sealed record AtomicCommitOutcome(
    ulong Version,
    ReadOnlyMemory<byte> Journal,
    bool Duplicate);

public sealed class AggregateConflictException(ulong expectedVersion, ulong actualVersion)
    : Exception($"Aggregate version conflict: expected {expectedVersion}, got {actualVersion}.")
{
    public ulong ExpectedVersion { get; } = expectedVersion;
    public ulong ActualVersion { get; } = actualVersion;
}

public interface IAtomicAggregateStore
{
    Task<AtomicAggregateSnapshot> LoadAsync(
        string aggregateId,
        CancellationToken cancellationToken = default);

    Task<AtomicCommitOutcome> CommitAsync(
        AtomicCommitRequest request,
        CancellationToken cancellationToken = default);

    Task<ReadOnlyMemory<byte>?> LoadResultAsync(
        string aggregateId,
        string inputMessageId,
        CancellationToken cancellationToken = default);
}

public static class RedisRecordCompression
{
    public static byte[] Compress(ReadOnlySpan<byte> value)
    {
        if (value.IsEmpty)
        {
            throw new ArgumentException("Redis record data is required.", nameof(value));
        }
        using var output = new MemoryStream();
        using (var compressor = new GZipStream(
            output,
            CompressionLevel.Fastest,
            leaveOpen: true))
        {
            compressor.Write(value);
        }
        return output.ToArray();
    }

    public static byte[] Decompress(ReadOnlySpan<byte> value)
    {
        if (value.IsEmpty)
        {
            throw new InvalidDataException("Compressed Redis record is empty.");
        }
        using var input = new MemoryStream(value.ToArray(), writable: false);
        using var decompressor = new GZipStream(
            input,
            CompressionMode.Decompress,
            leaveOpen: false);
        using var output = new MemoryStream();
        decompressor.CopyTo(output);
        return output.ToArray();
    }
}

// RedisAtomicAggregateStore stores gzip-compressed aggregate state and result
// journals. Uncompressed records are intentionally unsupported. It uses the
// same key layout and Lua transaction as the Go helper; StackExchange.Redis
// routes the operation to the hash slot encoded in all three keys and follows
// MOVED/ASK responses for the caller.
public sealed class RedisAtomicAggregateStore : IAtomicAggregateStore
{
    private const string LoadScript = """
        local state = redis.call("GET", KEYS[1]) or ""
        local current = tonumber(redis.call("GET", KEYS[2]) or "0")
        return {current, state}
        """;

    private const string CommitScript = """
        local existing = redis.call("GET", KEYS[3])
        local current = tonumber(redis.call("GET", KEYS[2]) or "0")
        if existing then
          return {1, current, existing}
        end
        local expected = tonumber(ARGV[1])
        if current ~= expected then
          return {2, current, ""}
        end
        local next_version = current
        if ARGV[5] == "1" then
          next_version = current + 1
          redis.call("SET", KEYS[1], ARGV[2])
          redis.call("SET", KEYS[2], tostring(next_version))
        end
        redis.call("SET", KEYS[3], ARGV[3], "PX", ARGV[4])
        return {0, next_version, ARGV[3]}
        """;

    private readonly IDatabase _database;
    private readonly string _prefix;

    public RedisAtomicAggregateStore(IDatabase database, string prefix)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        _prefix = prefix.Trim().Trim(':');
    }

    public async Task<AtomicAggregateSnapshot> LoadAsync(
        string aggregateId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var keys = AggregateKeys.ForAggregate(_prefix, aggregateId);
        var raw = await _database.ScriptEvaluateAsync(
            LoadScript,
            [(RedisKey)keys.State, (RedisKey)keys.Version],
            Array.Empty<RedisValue>()).WaitAsync(cancellationToken);
        var values = (RedisResult[])raw!;
        if (values.Length != 2)
        {
            throw new RedisServerException("Unexpected aggregate load response.");
        }
        var version = checked((ulong)(long)values[0]);
        var compressedState = (byte[]?)values[1] ?? [];
        var state = compressedState.Length == 0
            ? []
            : RedisRecordCompression.Decompress(compressedState);
        return new AtomicAggregateSnapshot(version, state);
    }

    public async Task<AtomicCommitOutcome> CommitAsync(
        AtomicCommitRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        if (request.NextState.IsEmpty || request.Journal.IsEmpty ||
            request.JournalRetention <= TimeSpan.Zero)
        {
            throw new ArgumentException("State, complete result journal, and positive retention are required.", nameof(request));
        }
        var retentionMilliseconds = checked((long)request.JournalRetention.TotalMilliseconds);
        if (retentionMilliseconds <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(request), "Journal retention is below one millisecond.");
        }

        var keys = AggregateKeys.For(_prefix, request.AggregateId, request.InputMessageId);
        var compressedState = RedisRecordCompression.Compress(request.NextState.Span);
        var compressedJournal = RedisRecordCompression.Compress(request.Journal.Span);
        var raw = await _database.ScriptEvaluateAsync(
            CommitScript,
            [(RedisKey)keys.State, (RedisKey)keys.Version, (RedisKey)keys.Inbox],
            [
                request.ExpectedVersion.ToString(CultureInfo.InvariantCulture),
                compressedState,
                compressedJournal,
                retentionMilliseconds,
                request.AdvanceVersion ? 1 : 0
            ]).WaitAsync(cancellationToken);
        var values = (RedisResult[])raw!;
        if (values.Length != 3)
        {
            throw new RedisServerException("Unexpected aggregate commit response.");
        }
        var status = (long)values[0];
        var version = checked((ulong)(long)values[1]);
        return status switch
        {
            0 => new AtomicCommitOutcome(
                version,
                RedisRecordCompression.Decompress((byte[]?)values[2] ?? []),
                false),
            1 => new AtomicCommitOutcome(
                version,
                RedisRecordCompression.Decompress((byte[]?)values[2] ?? []),
                true),
            2 => throw new AggregateConflictException(request.ExpectedVersion, version),
            _ => throw new RedisServerException($"Unknown aggregate commit status {status}.")
        };
    }

    public async Task<ReadOnlyMemory<byte>?> LoadResultAsync(
        string aggregateId,
        string inputMessageId,
        CancellationToken cancellationToken = default)
    {
        var keys = AggregateKeys.For(_prefix, aggregateId, inputMessageId);
        var value = await _database.StringGetAsync(keys.Inbox).WaitAsync(cancellationToken);
        if (!value.HasValue)
        {
            return null;
        }
        return new ReadOnlyMemory<byte>(
            RedisRecordCompression.Decompress((byte[])value!));
    }
}
