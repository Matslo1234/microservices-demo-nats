// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Boutique.Common.V1;
using Boutique.Events.V1;
using Google.Protobuf;
using StackExchange.Redis;

namespace cartservice.cartstore;

public interface IProductCatalog
{
    Task<bool> ContainsAsync(
        string productId,
        CancellationToken cancellationToken = default);
}

public interface ICatalogProjection : IProductCatalog
{
    Task ApplyAsync(
        string subject,
        MessageEnvelope envelope,
        CancellationToken cancellationToken = default);

    Task WaitUntilReadyAsync(CancellationToken cancellationToken = default);
}

// Catalog events are consumed by one shared durable and projected into the
// cart Redis cluster. All cart replicas therefore validate against the same
// disposable, replayable product view without querying the storefront.
public sealed class RedisCatalogProjection : ICatalogProjection
{
    private const string Prefix = "cart:catalog:v1";
    private static readonly TimeSpan ReadyPollInterval = TimeSpan.FromMilliseconds(100);

    private const string ApplyProductScript = """
        local current = tonumber(redis.call("HGET", KEYS[1], "version") or "0")
        local incoming = tonumber(ARGV[1])
        if incoming <= current then
          return 0
        end
        redis.call("HSET", KEYS[1],
          "version", ARGV[1],
          "active", ARGV[2],
          "source_event_id", ARGV[3],
          "catalog_revision", ARGV[4])
        return 1
        """;

    private const string ApplySnapshotScript = """
        local current = tonumber(redis.call("GET", KEYS[1]) or "0")
        local incoming = tonumber(ARGV[1])
        if incoming <= current then
          return 0
        end
        redis.call("SET", KEYS[1], ARGV[1])
        return 1
        """;

    private readonly IDatabase _database;

    public RedisCatalogProjection(IConnectionMultiplexer connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        _database = connection.GetDatabase();
    }

    public async Task<bool> ContainsAsync(
        string productId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(productId);
        cancellationToken.ThrowIfCancellationRequested();
        var active = await _database.HashGetAsync(
            ProductKey(productId),
            "active").WaitAsync(cancellationToken);
        return active.HasValue && string.Equals(
            active.ToString(),
            "1",
            StringComparison.Ordinal);
    }

    public async Task ApplyAsync(
        string subject,
        MessageEnvelope envelope,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);
        ArgumentNullException.ThrowIfNull(envelope);
        cancellationToken.ThrowIfCancellationRequested();
        if (envelope.SchemaVersion != 1 ||
            string.IsNullOrWhiteSpace(envelope.MessageId) ||
            envelope.Data == null)
        {
            throw new InvalidOperationException(
                "Catalog event envelope is incomplete or unsupported.");
        }

        switch (subject)
        {
            case "boutique.evt.catalog.product-upserted.v1":
                if (!envelope.Data.TryUnpack<CatalogProductUpsertedEvent>(out var upserted) ||
                    upserted.Product == null ||
                    string.IsNullOrWhiteSpace(upserted.Product.ProductId) ||
                    upserted.Product.ProductVersion == 0)
                {
                    throw new InvalidOperationException(
                        "Catalog product-upserted payload is invalid.");
                }
                await ApplyProductAsync(
                    upserted.Product.ProductId,
                    upserted.Product.ProductVersion,
                    upserted.CatalogRevision,
                    true,
                    envelope.MessageId,
                    cancellationToken);
                break;

            case "boutique.evt.catalog.product-removed.v1":
                if (!envelope.Data.TryUnpack<CatalogProductRemovedEvent>(out var removed) ||
                    string.IsNullOrWhiteSpace(removed.ProductId) ||
                    removed.ProductVersion == 0)
                {
                    throw new InvalidOperationException(
                        "Catalog product-removed payload is invalid.");
                }
                await ApplyProductAsync(
                    removed.ProductId,
                    removed.ProductVersion,
                    removed.CatalogRevision,
                    false,
                    envelope.MessageId,
                    cancellationToken);
                break;

            case "boutique.evt.catalog.snapshot-completed.v1":
                if (!envelope.Data.TryUnpack<CatalogSnapshotCompletedEvent>(out var snapshot) ||
                    snapshot.CatalogRevision == 0)
                {
                    throw new InvalidOperationException(
                        "Catalog snapshot-completed payload is invalid.");
                }
                await _database.ScriptEvaluateAsync(
                    ApplySnapshotScript,
                    [(RedisKey)SnapshotKey],
                    [snapshot.CatalogRevision.ToString(CultureInfo.InvariantCulture)])
                    .WaitAsync(cancellationToken);
                break;

            default:
                throw new InvalidOperationException(
                    $"Unsupported catalog event subject {subject}.");
        }
    }

    public async Task WaitUntilReadyAsync(
        CancellationToken cancellationToken = default)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var revision = await _database.StringGetAsync(SnapshotKey)
                .WaitAsync(cancellationToken);
            if (revision.HasValue && ulong.TryParse(
                revision.ToString(),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var parsed) && parsed > 0)
            {
                return;
            }
            await Task.Delay(ReadyPollInterval, cancellationToken);
        }
    }

    private async Task ApplyProductAsync(
        string productId,
        ulong productVersion,
        ulong catalogRevision,
        bool active,
        string sourceEventId,
        CancellationToken cancellationToken)
    {
        await _database.ScriptEvaluateAsync(
            ApplyProductScript,
            [(RedisKey)ProductKey(productId)],
            [
                productVersion.ToString(CultureInfo.InvariantCulture),
                active ? 1 : 0,
                sourceEventId,
                catalogRevision.ToString(CultureInfo.InvariantCulture)
            ]).WaitAsync(cancellationToken);
    }

    private static string SnapshotKey => $"{Prefix}:snapshot";

    private static string ProductKey(string productId) =>
        $"{Prefix}:product:{Convert.ToHexStringLower(
            SHA256.HashData(Encoding.UTF8.GetBytes(productId)))}";
}
