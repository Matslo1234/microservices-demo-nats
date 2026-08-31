// Copyright 2018 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"regexp"
	"testing"
	"time"

	commandsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/commands/v1"
	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	stateless "github.com/GoogleCloudPlatform/microservices-demo/src/shared/stateless/go"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testShippingSecret      = "shipping-provider-secret-aaaaaaaaaaaaaaaaaaaaaaaa"
	otherTestShippingSecret = "shipping-provider-secret-bbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestShippingConsumerTerminalErrorsRestartWorker(t *testing.T) {
	for _, err := range []error{nats.ErrBadSubscription, nats.ErrSubscriptionClosed, nats.ErrConsumerDeleted, nats.ErrNoResponders} {
		if !shippingConsumerTerminal(err) {
			t.Fatalf("%v was not classified as terminal", err)
		}
	}
	if shippingConsumerTerminal(nats.ErrTimeout) {
		t.Fatal("fetch timeout was classified as terminal")
	}
}

type shippingJetStreamStub struct {
	nats.JetStreamContext
	info          *nats.ConsumerInfo
	infoErr       error
	addedStream   string
	addedConfig   *nats.ConsumerConfig
	updatedStream string
	updatedConfig *nats.ConsumerConfig
}

func (stub *shippingJetStreamStub) ConsumerInfo(string, string, ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	return stub.info, stub.infoErr
}

func (stub *shippingJetStreamStub) AddConsumer(stream string, config *nats.ConsumerConfig, _ ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	copy := *config
	stub.addedStream = stream
	stub.addedConfig = &copy
	return &nats.ConsumerInfo{Config: copy}, nil
}

func (stub *shippingJetStreamStub) UpdateConsumer(stream string, config *nats.ConsumerConfig, _ ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	copy := *config
	stub.updatedStream = stream
	stub.updatedConfig = &copy
	return &nats.ConsumerInfo{Config: copy}, nil
}

func TestShippingCreatesConsumersBeforeBinding(t *testing.T) {
	tests := []shippingConsumerDefinition{
		shippingCartConsumerDefinition(),
	}
	tests = append(tests, shippingCommandConsumerDefinitions()...)
	for _, definition := range tests {
		t.Run(definition.durable, func(t *testing.T) {
			stub := &shippingJetStreamStub{infoErr: nats.ErrConsumerNotFound}
			worker := &shippingEventWorker{js: stub}
			if err := worker.ensureConsumer(definition); err != nil {
				t.Fatal(err)
			}
			config := stub.addedConfig
			if stub.addedStream != definition.stream || config == nil {
				t.Fatalf("consumer was not added to %s", definition.stream)
			}
			if config.Durable != definition.durable ||
				config.FilterSubject != definition.filterSubject ||
				config.DeliverPolicy != nats.DeliverAllPolicy ||
				config.AckPolicy != nats.AckExplicitPolicy ||
				config.AckWait != 30*time.Second ||
				config.MaxDeliver != 10 ||
				config.MaxAckPending != definition.maxPending {
				t.Fatalf("unexpected consumer config: %+v", *config)
			}
		})
	}
}

func TestShippingProcessingTime(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "unset", value: "", want: 0},
		{name: "not a number", value: "invalid", want: 0},
		{name: "zero", value: "0", want: 0},
		{name: "negative", value: "-10", want: 0},
		{name: "infinite", value: "Inf", want: 0},
		{name: "milliseconds", value: "12", want: 12 * time.Millisecond},
		{name: "fractional milliseconds", value: "12.5", want: 12*time.Millisecond + 500*time.Microsecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shippingProcessingTime(test.value); got != test.want {
				t.Fatalf("shippingProcessingTime(%q) = %s, want %s", test.value, got, test.want)
			}
		})
	}

	startedAt := time.Now()
	if err := waitForProcessing(context.Background(), 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(startedAt); elapsed < 8*time.Millisecond {
		t.Fatalf("configured shipping processing time was not observed: waited %s", elapsed)
	}
}

func shippingTestEnvelope(t *testing.T, messageID, aggregateID string, version uint64, occurredAt time.Time, payload proto.Message) *commonv1.MessageEnvelope {
	t.Helper()
	wrapped, err := anypb.New(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &commonv1.MessageEnvelope{
		MessageId:        messageID,
		MessageType:      "test.command",
		SchemaVersion:    1,
		OccurredAt:       timestamppb.New(occurredAt),
		Producer:         "test",
		AggregateType:    "order",
		AggregateId:      aggregateID,
		AggregateVersion: version,
		CorrelationId:    aggregateID,
		Data:             wrapped,
	}
}

func shippingTestProvider(t *testing.T, secret string) *shippingProvider {
	t.Helper()
	provider, err := newShippingProvider(secret)
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func decodeShippingResult(t *testing.T, outcome shippingOutcome) *commonv1.MessageEnvelope {
	t.Helper()
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(outcome.Data, envelope); err != nil {
		t.Fatal(err)
	}
	return envelope
}

func TestShippingCommandsAreDeterministicAcrossReplicasAndRetries(t *testing.T) {
	inputTime := time.Date(2026, 7, 27, 9, 30, 15, 123_000_000, time.UTC)
	replicaA := shippingTestProvider(t, testShippingSecret)
	replicaB := shippingTestProvider(t, testShippingSecret)

	tests := []struct {
		name    string
		subject string
		slot    string
		command proto.Message
	}{
		{
			name:    "quote",
			subject: "boutique.cmd.shipping.calculate-order-quote.v1",
			slot:    shippingOrderQuoteSlot,
			command: &commandsv1.ShippingCalculateOrderQuoteCommand{
				CommandId: "quote-command",
				OrderId:   "order-1",
				Cart: &commonv1.CartSnapshot{
					UserId: "user-1",
					Items: []*commonv1.CartLine{
						{ProductId: "product-1", Quantity: 2},
					},
					CartVersion: 3,
				},
			},
		},
		{
			name:    "create",
			subject: "boutique.cmd.shipping.create-shipment.v1",
			slot:    shippingCreateShipmentSlot,
			command: &commandsv1.ShippingCreateShipmentCommand{
				CommandId:      "shipment-command",
				OrderId:        "order-1",
				IdempotencyKey: "order-1/shipment",
			},
		},
		{
			name:    "cancel",
			subject: "boutique.cmd.shipping.cancel-shipment.v1",
			slot:    shippingCancelShipmentSlot,
			command: &commandsv1.ShippingCancelShipmentCommand{
				CommandId:      "cancel-command",
				OrderId:        "order-1",
				ShipmentId:     "shipment-1",
				TrackingId:     "tracking-1",
				IdempotencyKey: "order-1/cancel-shipment",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sourceID := "source-" + test.name
			envelope := shippingTestEnvelope(t, sourceID, "order-1", 4, inputTime, test.command)
			first, err := buildShippingOutcome(test.subject, envelope, replicaA, "")
			if err != nil {
				t.Fatal(err)
			}
			retry, err := buildShippingOutcome(test.subject, envelope, replicaA, "")
			if err != nil {
				t.Fatal(err)
			}
			otherReplica, err := buildShippingOutcome(test.subject, envelope, replicaB, "")
			if err != nil {
				t.Fatal(err)
			}
			if first.MessageID != retry.MessageID || first.MessageID != otherReplica.MessageID ||
				string(first.Data) != string(retry.Data) || string(first.Data) != string(otherReplica.Data) {
				t.Fatal("a retry or another replica changed the shipping outcome")
			}
			expectedID, err := stateless.DeriveResultMessageID(sourceID, test.slot)
			if err != nil {
				t.Fatal(err)
			}
			if first.MessageID != expectedID {
				t.Fatalf("message ID = %s, want %s", first.MessageID, expectedID)
			}
			result := decodeShippingResult(t, first)
			if !result.OccurredAt.AsTime().Equal(inputTime) {
				t.Fatalf("result occurrence = %s, want input occurrence %s", result.OccurredAt.AsTime(), inputTime)
			}
		})
	}
}

func TestShippingProviderSecretControlsStableProviderReferences(t *testing.T) {
	inputTime := time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC)
	command := &commandsv1.ShippingCreateShipmentCommand{
		CommandId:      "shipment-command",
		OrderId:        "order-1",
		IdempotencyKey: "order-1/shipment",
	}
	envelope := shippingTestEnvelope(t, "source-shipment", "order-1", 4, inputTime, command)
	first, err := buildShippingOutcome(
		"boutique.cmd.shipping.create-shipment.v1",
		envelope,
		shippingTestProvider(t, testShippingSecret),
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildShippingOutcome(
		"boutique.cmd.shipping.create-shipment.v1",
		envelope,
		shippingTestProvider(t, otherTestShippingSecret),
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	firstPayload := &eventsv1.ShippingShipmentCreatedEvent{}
	if err := decodeShippingResult(t, first).Data.UnmarshalTo(firstPayload); err != nil {
		t.Fatal(err)
	}
	secondPayload := &eventsv1.ShippingShipmentCreatedEvent{}
	if err := decodeShippingResult(t, second).Data.UnmarshalTo(secondPayload); err != nil {
		t.Fatal(err)
	}
	if firstPayload.ShipmentId == secondPayload.ShipmentId ||
		firstPayload.TrackingId == secondPayload.TrackingId {
		t.Fatal("different provider secrets produced the same provider references")
	}
	if !regexp.MustCompile(`^PH-\d{6}-\d{7}$`).MatchString(firstPayload.TrackingId) {
		t.Fatalf("tracking ID %q has an invalid format", firstPayload.TrackingId)
	}
}

func TestShippingQuoteUsesInputEventTime(t *testing.T) {
	inputTime := time.Date(2026, 7, 27, 11, 0, 0, 0, time.UTC)
	command := &commandsv1.ShippingCalculateOrderQuoteCommand{
		CommandId: "quote-command",
		OrderId:   "order-1",
		Cart:      &commonv1.CartSnapshot{UserId: "user-1", CartVersion: 2},
	}
	outcome, err := buildShippingOutcome(
		"boutique.cmd.shipping.calculate-order-quote.v1",
		shippingTestEnvelope(t, "source-quote", "order-1", 3, inputTime, command),
		shippingTestProvider(t, testShippingSecret),
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	payload := &eventsv1.ShippingOrderQuoteCalculatedEvent{}
	if err := decodeShippingResult(t, outcome).Data.UnmarshalTo(payload); err != nil {
		t.Fatal(err)
	}
	if !payload.ExpiresAt.AsTime().Equal(inputTime.Add(15 * time.Minute)) {
		t.Fatalf("quote expiry = %s, want %s", payload.ExpiresAt.AsTime(), inputTime.Add(15*time.Minute))
	}
}

func TestShippingRefreshesCartQuoteFromCommand(t *testing.T) {
	inputTime := time.Date(2026, 7, 27, 11, 30, 0, 0, time.UTC)
	cart := &commonv1.CartSnapshot{
		UserId: "user-1", CartVersion: 6,
		Items: []*commonv1.CartLine{{ProductId: "product-1", Quantity: 2}},
	}
	command := &commandsv1.ShippingCalculateCartQuoteCommand{
		CommandId: "cart-quote-command", UserId: cart.UserId, Cart: cart,
	}
	outcome, err := buildShippingOutcome(
		shippingCartQuoteCommandSubject,
		shippingTestEnvelope(t, command.CommandId, cart.UserId, cart.CartVersion, inputTime, command),
		shippingTestProvider(t, testShippingSecret),
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Subject != shippingQuoteSubject {
		t.Fatalf("cart quote command published %q", outcome.Subject)
	}
	payload := &eventsv1.ShippingCartQuoteUpdatedEvent{}
	if err := decodeShippingResult(t, outcome).Data.UnmarshalTo(payload); err != nil {
		t.Fatal(err)
	}
	if payload.UserId != cart.UserId || payload.CartVersion != cart.CartVersion ||
		!payload.ExpiresAt.AsTime().Equal(inputTime.Add(15*time.Minute)) {
		t.Fatalf("unexpected refreshed cart quote: %+v", payload)
	}
}

func TestShippingCartQuoteIsDeterministicAcrossReplicas(t *testing.T) {
	inputTime := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	cart := &commonv1.CartSnapshot{
		UserId: "user-1",
		Items: []*commonv1.CartLine{
			{ProductId: "product-1", Quantity: 1},
		},
		CartVersion: 5,
	}
	source := &eventsv1.CartItemAddedEvent{UserId: cart.UserId, Cart: cart}
	envelope := shippingTestEnvelope(t, "cart-event-1", cart.UserId, cart.CartVersion, inputTime, source)
	first, err := buildShippingCartOutcome(envelope, cart)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildShippingCartOutcome(envelope, cart)
	if err != nil {
		t.Fatal(err)
	}
	if first.MessageID != second.MessageID || string(first.Data) != string(second.Data) {
		t.Fatal("cart quote changed when rebuilt by another replica")
	}
	payload := &eventsv1.ShippingCartQuoteUpdatedEvent{}
	if err := decodeShippingResult(t, first).Data.UnmarshalTo(payload); err != nil {
		t.Fatal(err)
	}
	if !payload.ExpiresAt.AsTime().Equal(inputTime.Add(15 * time.Minute)) {
		t.Fatal("cart quote expiry did not derive from the source event")
	}
}

func TestShippingProcessesStreamBeforeFetchBatchCloses(t *testing.T) {
	messages := make(chan *nats.Msg)
	processed := make(chan struct{})
	finished := make(chan struct{})

	go func() {
		processShippingStream(
			messages,
			32,
			1,
			func(*nats.Msg) { close(processed) },
		)
		close(finished)
	}()

	messages <- &nats.Msg{}
	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatal("message was not processed until the stream closed")
	}
	select {
	case <-finished:
		t.Fatal("stream processor stopped before the batch stream closed")
	default:
	}
	close(messages)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("stream processor did not stop after the batch stream closed")
	}
}

func TestShippingCartLaneUsesAggregateIdentity(t *testing.T) {
	first := &commonv1.MessageEnvelope{AggregateId: "user-1", CorrelationId: "request-1"}
	second := &commonv1.MessageEnvelope{AggregateId: "user-1", CorrelationId: "request-2"}
	other := &commonv1.MessageEnvelope{AggregateId: "user-2", CorrelationId: "request-1"}
	firstData, err := proto.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondData, err := proto.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	otherData, err := proto.Marshal(other)
	if err != nil {
		t.Fatal(err)
	}
	if shippingCartMessageLane(firstData, 32) != shippingCartMessageLane(secondData, 32) {
		t.Fatal("cart events for one user were assigned to different lanes")
	}
	if shippingCartMessageLane(firstData, 32) == shippingCartMessageLane(otherData, 32) {
		t.Fatal("test identities unexpectedly hashed to the same lane")
	}
}

func TestShippingFailureInjectionIsDeterministic(t *testing.T) {
	inputTime := time.Date(2026, 7, 27, 13, 0, 0, 0, time.UTC)
	provider := shippingTestProvider(t, testShippingSecret)
	tests := []struct {
		mode, subject, expected string
		command                 proto.Message
	}{
		{"quote", "boutique.cmd.shipping.calculate-order-quote.v1", "boutique.evt.shipping.order-quote-failed.v1", &commandsv1.ShippingCalculateOrderQuoteCommand{CommandId: "quote", OrderId: "order-1", Cart: &commonv1.CartSnapshot{}}},
		{"shipment", "boutique.cmd.shipping.create-shipment.v1", "boutique.evt.shipping.shipment-creation-failed.v1", &commandsv1.ShippingCreateShipmentCommand{CommandId: "shipment", OrderId: "order-1", IdempotencyKey: "order-1/shipment"}},
		{"cancel", "boutique.cmd.shipping.cancel-shipment.v1", "boutique.evt.shipping.shipment-cancellation-failed.v1", &commandsv1.ShippingCancelShipmentCommand{CommandId: "cancel", OrderId: "order-1", ShipmentId: "shipment-1", IdempotencyKey: "order-1/cancel"}},
	}
	for _, test := range tests {
		t.Run(test.mode, func(t *testing.T) {
			envelope := shippingTestEnvelope(t, "command-"+test.mode, "order-1", 2, inputTime, test.command)
			first, err := buildShippingOutcome(test.subject, envelope, provider, test.mode)
			if err != nil {
				t.Fatal(err)
			}
			second, err := buildShippingOutcome(test.subject, envelope, provider, test.mode)
			if err != nil {
				t.Fatal(err)
			}
			if first.Subject != test.expected || string(first.Data) != string(second.Data) {
				t.Fatal("failure retry changed its deterministic outcome")
			}
		})
	}
}

func TestShippingRejectsMissingBusinessIdempotencyKey(t *testing.T) {
	command := &commandsv1.ShippingCreateShipmentCommand{
		CommandId: "shipment-command",
		OrderId:   "order-1",
	}
	_, err := buildShippingOutcome(
		"boutique.cmd.shipping.create-shipment.v1",
		shippingTestEnvelope(t, "source-shipment", "order-1", 4, time.Now().UTC(), command),
		shippingTestProvider(t, testShippingSecret),
		"",
	)
	if err == nil {
		t.Fatal("shipping accepted a provider command without an idempotency key")
	}
}

func TestShippingProviderRequiresStrongSharedSecret(t *testing.T) {
	if _, err := newShippingProvider("short"); err == nil {
		t.Fatal("shipping accepted a short provider secret")
	}
}

func TestCreateQuoteFromFloat(t *testing.T) {
	tests := []struct {
		name    string
		value   float64
		dollars uint32
		cents   uint32
	}{
		{"zero", 0.0, 0, 0},
		{"whole dollars", 10.0, 10, 0},
		{"with cents", 8.99, 8, 99},
		{"small value", 0.50, 0, 50},
		{"large value", 100.01, 100, 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			quote := CreateQuoteFromFloat(test.value)
			if quote.Dollars != test.dollars || quote.Cents != test.cents {
				t.Errorf(
					"CreateQuoteFromFloat(%v) = $%d.%d, want $%d.%d",
					test.value,
					quote.Dollars,
					quote.Cents,
					test.dollars,
					test.cents,
				)
			}
		})
	}
}

func TestCreateQuoteFromCount(t *testing.T) {
	zeroQuote := CreateQuoteFromCount(0)
	if zeroQuote.Dollars != 0 || zeroQuote.Cents != 0 {
		t.Errorf("CreateQuoteFromCount(0) = %s, want $0.0", zeroQuote)
	}
	nonZeroQuote := CreateQuoteFromCount(5)
	if nonZeroQuote.Dollars == 0 && nonZeroQuote.Cents == 0 {
		t.Error("CreateQuoteFromCount(5) returned zero, expected a non-zero quote")
	}
}

func TestQuoteString(t *testing.T) {
	quote := Quote{Dollars: 8, Cents: 99}
	if quote.String() != "$8.99" {
		t.Errorf("Quote.String() = %q, want %q", quote.String(), "$8.99")
	}
}
