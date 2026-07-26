// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using StackExchange.Redis;

namespace Boutique.Stateless;

public sealed record AggregateKeys(string State, string Version, string Inbox)
{
    public static AggregateKeys For(string prefix, string aggregateId, string inputMessageId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        ArgumentException.ThrowIfNullOrWhiteSpace(aggregateId);
        ArgumentException.ThrowIfNullOrWhiteSpace(inputMessageId);
        var normalizedPrefix = prefix.Trim().Trim(':');
        var baseKey = $"{normalizedPrefix}:{{{Digest(aggregateId)}}}";
        return new AggregateKeys(
            $"{baseKey}:state",
            $"{baseKey}:version",
            $"{baseKey}:inbox:{Digest(inputMessageId)}");
    }

    private static string Digest(string value) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
}

public sealed record AtomicCommitRequest(
    string AggregateId,
    string InputMessageId,
    ulong ExpectedVersion,
    ReadOnlyMemory<byte> NextState,
    ReadOnlyMemory<byte> Journal,
    TimeSpan JournalRetention);

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
    Task<AtomicCommitOutcome> CommitAsync(
        AtomicCommitRequest request,
        CancellationToken cancellationToken = default);

    Task<ReadOnlyMemory<byte>?> LoadResultAsync(
        string aggregateId,
        string inputMessageId,
        CancellationToken cancellationToken = default);
}

// RedisAtomicAggregateStore uses the same key layout and Lua transaction as
// the Go helper. StackExchange.Redis routes the operation to the hash slot
// encoded in all three keys and follows MOVED/ASK responses for the caller.
public sealed class RedisAtomicAggregateStore : IAtomicAggregateStore
{
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
        local next_version = current + 1
        redis.call("SET", KEYS[1], ARGV[2])
        redis.call("SET", KEYS[2], tostring(next_version))
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
        var raw = await _database.ScriptEvaluateAsync(
            CommitScript,
            [(RedisKey)keys.State, (RedisKey)keys.Version, (RedisKey)keys.Inbox],
            [
                request.ExpectedVersion.ToString(CultureInfo.InvariantCulture),
                request.NextState.ToArray(),
                request.Journal.ToArray(),
                retentionMilliseconds
            ]).WaitAsync(cancellationToken);
        var values = (RedisResult[])raw!;
        if (values.Length != 3)
        {
            throw new RedisServerException("Unexpected aggregate commit response.");
        }
        var status = (long)values[0];
        var version = checked((ulong)(long)values[1]);
        var journal = (byte[]?)values[2] ?? [];
        return status switch
        {
            0 => new AtomicCommitOutcome(version, journal, false),
            1 => new AtomicCommitOutcome(version, journal, true),
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
        return value.HasValue ? (byte[]?)value : null;
    }
}
