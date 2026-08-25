// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

using Xunit;

namespace Boutique.Stateless.Tests;

public sealed class PrimitivesTests
{
    [Fact]
    public void RedisRecordsRequireCompressedData()
    {
        var plain = Enumerable.Repeat("checkout-cart-state", 100)
            .SelectMany(value => System.Text.Encoding.UTF8.GetBytes(value))
            .ToArray();

        var compressed = RedisRecordCompression.Compress(plain);

        Assert.Equal((byte)0x1f, compressed[0]);
        Assert.Equal((byte)0x8b, compressed[1]);
        Assert.True(compressed.Length < plain.Length);
        Assert.Equal(plain, RedisRecordCompression.Decompress(compressed));
        Assert.Throws<InvalidDataException>(() =>
            RedisRecordCompression.Decompress(plain));
    }

    [Theory]
    [InlineData(
        "01J0INPUT000000000000000001",
        "cart.mutation",
        "br1_cfipIWfz73yXKiJIc0nF-uV6vnx5kWbMUG2_o-ukd50")]
    [InlineData(
        "event-order-completed-42",
        "notification.order-confirmation",
        "br1_BipmFE_ifI2JqRb67NFrgisjZYeejPTlkKhojRP1Mz8")]
    public void ResultIdsMatchTheCrossLanguageContract(
        string inputMessageId,
        string slot,
        string expected)
    {
        Assert.Equal(expected, ResultMessageIds.Derive(inputMessageId, slot));
    }

    [Fact]
    public void AggregateKeysShareOneOpaqueRedisClusterHashTag()
    {
        var keys = AggregateKeys.For("cart:v1", "user}7", "command-1");
        var aggregateKeys = AggregateKeys.ForAggregate("cart:v1", "user}7");

        var tags = new[] { keys.State, keys.Version, keys.Inbox }
            .Select(key => key[(key.IndexOf('{') + 1)..key.IndexOf('}')])
            .Distinct()
            .ToArray();
        Assert.Single(tags);
        Assert.DoesNotContain("user", tags[0], StringComparison.OrdinalIgnoreCase);
        Assert.Equal(64, tags[0].Length);
        Assert.Equal(keys.State, aggregateKeys.State);
        Assert.Equal(keys.Version, aggregateKeys.Version);
    }

    [Fact]
    public void ResultMetadataIsStableAndCausal()
    {
        var input = new InputEnvelopeIdentity(
            "command-1",
            "operation-1",
            "00-trace",
            "vendor=value");
        var occurredAt = DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123);

        var first = ResultEnvelopes.CreateMetadata(
            input,
            "cart.mutation",
            "boutique.cart.ItemAdded.v1",
            occurredAt,
            "cartservice",
            "cart",
            "user-1",
            4);
        var second = ResultEnvelopes.CreateMetadata(
            input,
            "cart.mutation",
            "boutique.cart.ItemAdded.v1",
            occurredAt,
            "cartservice",
            "cart",
            "user-1",
            4);

        Assert.Equal(first, second);
        Assert.Equal(input.MessageId, first.CausationId);
        Assert.Equal(input.CorrelationId, first.CorrelationId);
        Assert.Equal(1U, first.SchemaVersion);
    }

    [Fact]
    public void ResultMetadataSupportsARejectionBeforeTheFirstCartVersion()
    {
        var metadata = ResultEnvelopes.CreateMetadata(
            new InputEnvelopeIdentity("command-1", "operation-1"),
            "cart.mutation",
            "boutique.cart.CommandRejected.v1",
            DateTimeOffset.UnixEpoch,
            "cartservice/phase4",
            "cart",
            "user-1",
            0);

        Assert.Equal(0UL, metadata.AggregateVersion);
    }

    [Theory]
    [InlineData("TRYAGAIN slot is migrating", RetryClass.Dependency)]
    [InlineData("wrong last sequence: 4", RetryClass.Conflict)]
    [InlineData("payload is invalid", RetryClass.Permanent)]
    public void RetryClassificationIsShared(string message, RetryClass expected)
    {
        Assert.Equal(expected, Retry.Classify(new InvalidOperationException(message)));
    }
}
