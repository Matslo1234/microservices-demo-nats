// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

using System.Diagnostics;
using Boutique.Common.V1;

namespace cartservice;

internal static class CartTelemetry
{
    internal const string SourceName = "online-boutique/messaging";
    internal static readonly ActivitySource Source = new(SourceName);

    internal static Activity? StartConsumer(
        MessageEnvelope envelope,
        string subject,
        string kind)
    {
        var parent = default(ActivityContext);
        if (!string.IsNullOrWhiteSpace(envelope.Traceparent))
        {
            ActivityContext.TryParse(
                envelope.Traceparent,
                envelope.Tracestate,
                isRemote: true,
                out parent);
        }
        var tags = MessageTags(
            subject,
            kind,
            envelope.MessageId,
            envelope.CorrelationId);
        return Source.StartActivity(
            subject + " process",
            ActivityKind.Consumer,
            parent,
            tags);
    }

    internal static Activity? StartProducer(
        string subject,
        string messageId,
        string correlationId)
    {
        return Source.StartActivity(
            subject + " publish",
            ActivityKind.Producer,
            Activity.Current?.Context ?? default,
            MessageTags(subject, "event", messageId, correlationId));
    }

    internal static void Inject(MessageEnvelope envelope)
    {
        var activity = Activity.Current;
        if (activity?.Id is not null)
        {
            envelope.Traceparent = activity.Id;
            envelope.Tracestate = activity.TraceStateString ?? string.Empty;
        }
    }

    private static ActivityTagsCollection MessageTags(
        string subject,
        string kind,
        string messageId,
        string correlationId) =>
        new()
        {
            { "messaging.system", "nats" },
            { "messaging.destination.name", subject },
            { "messaging.operation.type", kind },
            { "messaging.message.id", Normalized(messageId) },
            { "correlation.id", Normalized(correlationId) }
        };

    private static string Normalized(string value) =>
        string.IsNullOrWhiteSpace(value) ? "unknown" : value;
}
