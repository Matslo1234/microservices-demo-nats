// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestConsumerSpanUsesCorrelationIDAndRemoteParent(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(previous)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(previousPropagator)

	ctx, span := StartConsumerSpan(
		context.Background(),
		"boutique.cmd.cart.add-item.v1",
		"command",
		"message-1",
		"correlation-1",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"",
	)
	var traceparent, tracestate string
	Inject(ctx, &traceparent, &tracestate)
	span.End()

	ended := recorder.Ended()
	if len(ended) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(ended))
	}
	if got := ended[0].Parent().SpanID().String(); got != "00f067aa0ba902b7" {
		t.Fatalf("parent span ID = %q", got)
	}
	attributes := make(map[string]string)
	for _, value := range ended[0].Attributes() {
		attributes[string(value.Key)] = value.Value.AsString()
	}
	if got := attributes[CorrelationIDKey]; got != "correlation-1" {
		t.Fatalf("correlation.id = %q", got)
	}
	if traceparent == "" {
		t.Fatal("current span context was not injected")
	}
}
