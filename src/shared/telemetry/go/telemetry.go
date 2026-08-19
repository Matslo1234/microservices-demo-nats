// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

// Package telemetry configures OTLP tracing and defines the cross-language
// conventions used for NATS spans in the demo.
package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	// CorrelationIDKey is deliberately separate from the OpenTelemetry trace
	// ID. Correlation IDs are stable business/request identifiers and may not
	// satisfy the W3C trace-ID format.
	CorrelationIDKey = "correlation.id"
	instrumentation  = "online-boutique/messaging"
)

// Init installs W3C propagation and, when ENABLE_TRACING=1, an OTLP/gRPC
// exporter. The returned shutdown function is always safe to call.
func Init(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	if os.Getenv("ENABLE_TRACING") != "1" {
		return func(context.Context) error { return nil }, nil
	}
	endpoint := os.Getenv("COLLECTOR_SERVICE_ADDR")
	if endpoint == "" {
		endpoint = strings.TrimPrefix(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "http://")
		endpoint = strings.TrimPrefix(endpoint, "https://")
	}
	if endpoint == "" {
		return nil, fmt.Errorf("COLLECTOR_SERVICE_ADDR or OTEL_EXPORTER_OTLP_ENDPOINT is required when tracing is enabled")
	}
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		"", attribute.String("service.name", serviceName)))
	if err != nil {
		return nil, fmt.Errorf("create OpenTelemetry resource: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

// SetCorrelationID adds the request/business correlation ID to the current
// span. Empty values are represented consistently with the logs.
func SetCorrelationID(ctx context.Context, correlationID string) {
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String(CorrelationIDKey, normalized(correlationID)))
}

// StartConsumerSpan extracts W3C context from a message envelope and starts a
// NATS consumer span carrying the same correlation ID used by logs.
func StartConsumerSpan(ctx context.Context, subject, kind, messageID, correlationID, traceparent, tracestate string) (context.Context, trace.Span) {
	carrier := propagation.MapCarrier{}
	if traceparent != "" {
		carrier.Set("traceparent", traceparent)
	}
	if tracestate != "" {
		carrier.Set("tracestate", tracestate)
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	return otel.Tracer(instrumentation).Start(ctx, subject+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(messageAttributes(subject, kind, messageID, correlationID)...))
}

// StartProducerSpan starts a NATS producer span for an outgoing envelope.
func StartProducerSpan(ctx context.Context, subject, kind, messageID, correlationID string) (context.Context, trace.Span) {
	return otel.Tracer(instrumentation).Start(ctx, subject+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(messageAttributes(subject, kind, messageID, correlationID)...))
}

// Inject writes the current W3C context into an envelope's propagation fields.
func Inject(ctx context.Context, traceparent, tracestate *string) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	*traceparent = carrier.Get("traceparent")
	*tracestate = carrier.Get("tracestate")
}

// RecordError marks a span failed without exposing error details as attributes.
func RecordError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, "operation failed")
}

func messageAttributes(subject, kind, messageID, correlationID string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", "nats"),
		attribute.String("messaging.destination.name", subject),
		attribute.String("messaging.operation.type", kind),
		attribute.String("messaging.message.id", normalized(messageID)),
		attribute.String(CorrelationIDKey, normalized(correlationID)),
	}
}

func normalized(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}
