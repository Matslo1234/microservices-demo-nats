// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace Boutique.Stateless;

public static partial class ResultMessageIds
{
    private const string Domain = "boutique.result.v1";

    [GeneratedRegex("^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$")]
    private static partial Regex SlotPattern();

    public static string Derive(string inputMessageId, string resultSlot)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(inputMessageId);
        if (!SlotPattern().IsMatch(resultSlot))
        {
            throw new ArgumentException("Result slot does not match the v1 contract.", nameof(resultSlot));
        }

        var input = Encoding.UTF8.GetBytes(inputMessageId);
        var slot = Encoding.UTF8.GetBytes(resultSlot);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(Encoding.UTF8.GetBytes(Domain));
        hash.AppendData([0]);
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(length, checked((uint)input.Length));
        hash.AppendData(length);
        hash.AppendData(input);
        BinaryPrimitives.WriteUInt32BigEndian(length, checked((uint)slot.Length));
        hash.AppendData(length);
        hash.AppendData(slot);
        return $"br1_{Convert.ToBase64String(hash.GetHashAndReset()).TrimEnd('=').Replace('+', '-').Replace('/', '_')}";
    }
}

public sealed record InputEnvelopeIdentity(
    string MessageId,
    string CorrelationId,
    string Traceparent = "",
    string Tracestate = "");

public sealed record ResultEnvelopeMetadata(
    string MessageId,
    string MessageType,
    uint SchemaVersion,
    DateTimeOffset OccurredAt,
    string Producer,
    string AggregateType,
    string AggregateId,
    ulong AggregateVersion,
    string CorrelationId,
    string CausationId,
    string Traceparent,
    string Tracestate);

public static class ResultEnvelopes
{
    // Payload serialization remains service-owned so the shared library does
    // not generate a second copy of the repository protobuf CLR types.
    public static ResultEnvelopeMetadata CreateMetadata(
        InputEnvelopeIdentity input,
        string resultSlot,
        string messageType,
        DateTimeOffset occurredAt,
        string producer,
        string aggregateType,
        string aggregateId,
        ulong aggregateVersion)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentException.ThrowIfNullOrWhiteSpace(input.MessageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(input.CorrelationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageType);
        ArgumentException.ThrowIfNullOrWhiteSpace(producer);
        ArgumentException.ThrowIfNullOrWhiteSpace(aggregateType);
        ArgumentException.ThrowIfNullOrWhiteSpace(aggregateId);
        if (aggregateVersion == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(aggregateVersion));
        }

        return new ResultEnvelopeMetadata(
            ResultMessageIds.Derive(input.MessageId, resultSlot),
            messageType,
            1,
            occurredAt.ToUniversalTime(),
            producer,
            aggregateType,
            aggregateId,
            aggregateVersion,
            input.CorrelationId,
            input.MessageId,
            input.Traceparent,
            input.Tracestate);
    }
}
