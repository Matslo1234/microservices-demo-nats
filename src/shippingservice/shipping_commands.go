// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	commandsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/commands/v1"
	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type shippingOutcome struct {
	MessageID string `json:"message_id"`
	Subject   string `json:"subject"`
	Data      []byte `json:"data"`
}
type shippingProviderStore struct {
	mu       sync.Mutex
	path     string
	Outcomes map[string]shippingOutcome `json:"outcomes"`
}

func openShippingProviderStore(path string) (*shippingProviderStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	store := &shippingProviderStore{path: path, Outcomes: map[string]shippingOutcome{}}
	encoded, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(encoded, store); err != nil {
			return nil, err
		}
		store.path = path
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if store.Outcomes == nil {
		store.Outcomes = map[string]shippingOutcome{}
	}
	return store, nil
}

func (store *shippingProviderStore) outcome(commandID string) (shippingOutcome, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	value, ok := store.Outcomes[commandID]
	return value, ok
}
func (store *shippingProviderStore) record(commandID string, outcome shippingOutcome) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, ok := store.Outcomes[commandID]; ok {
		return nil
	}
	next := make(map[string]shippingOutcome, len(store.Outcomes)+1)
	for key, value := range store.Outcomes {
		next[key] = value
	}
	next[commandID] = outcome
	encoded, err := json.Marshal(struct {
		Outcomes map[string]shippingOutcome `json:"outcomes"`
	}{Outcomes: next})
	if err != nil {
		return err
	}
	temporary := store.path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(encoded); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(temporary, store.path); err != nil {
		return err
	}
	store.Outcomes = next
	return nil
}

func (worker *shippingEventWorker) handleCommand(message *nats.Msg) error {
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(message.Data, envelope); err != nil {
		return err
	}
	if envelope.MessageId == "" || envelope.Data == nil {
		return errors.New("shipping command envelope is incomplete")
	}
	if outcome, ok := worker.provider.outcome(envelope.MessageId); ok {
		return worker.publishOutcome(outcome)
	}
	outcome, err := buildShippingOutcome(message.Subject, envelope)
	if err != nil {
		return err
	}
	if err := worker.provider.record(envelope.MessageId, outcome); err != nil {
		return err
	}
	return worker.publishOutcome(outcome)
}

func buildShippingOutcome(subject string, envelope *commonv1.MessageEnvelope) (shippingOutcome, error) {
	failureMode := os.Getenv("SHIPPING_FAILURE_MODE")
	switch subject {
	case "boutique.cmd.shipping.calculate-order-quote.v1":
		command := &commandsv1.ShippingCalculateOrderQuoteCommand{}
		if err := envelope.Data.UnmarshalTo(command); err != nil {
			return shippingOutcome{}, err
		}
		if failureMode == "quote" {
			return newShippingOutcome("boutique.evt.shipping.order-quote-failed.v1", "boutique.shipping.OrderQuoteFailed.v1", command.OrderId, envelope,
				&eventsv1.ShippingOrderQuoteFailedEvent{OrderId: command.OrderId, Failure: &commonv1.Failure{Code: "QUOTE_PROVIDER_UNAVAILABLE", Retryable: true, SafeMessage: "Shipping quote is unavailable."}})
		}
		count := 0
		for _, line := range command.Cart.GetItems() {
			count += int(line.Quantity)
		}
		quote := CreateQuoteFromCount(count)
		return newShippingOutcome("boutique.evt.shipping.order-quote-calculated.v1", "boutique.shipping.OrderQuoteCalculated.v1", command.OrderId, envelope,
			&eventsv1.ShippingOrderQuoteCalculatedEvent{OrderId: command.OrderId, CostUsd: &commonv1.Money{CurrencyCode: "USD", Units: int64(quote.Dollars), Nanos: int32(quote.Cents * 10_000_000)},
				QuoteId: shippingStableID("quote", command.OrderId), ExpiresAt: timestamppb.New(time.Now().UTC().Add(15 * time.Minute))})
	case "boutique.cmd.shipping.create-shipment.v1":
		command := &commandsv1.ShippingCreateShipmentCommand{}
		if err := envelope.Data.UnmarshalTo(command); err != nil {
			return shippingOutcome{}, err
		}
		if failureMode == "shipment" {
			return newShippingOutcome("boutique.evt.shipping.shipment-creation-failed.v1", "boutique.shipping.ShipmentCreationFailed.v1", command.OrderId, envelope,
				&eventsv1.ShippingShipmentCreationFailedEvent{OrderId: command.OrderId, Failure: &commonv1.Failure{Code: "CARRIER_UNAVAILABLE", Retryable: true, SafeMessage: "Shipment creation failed."}})
		}
		return newShippingOutcome("boutique.evt.shipping.shipment-created.v1", "boutique.shipping.ShipmentCreated.v1", command.OrderId, envelope,
			&eventsv1.ShippingShipmentCreatedEvent{OrderId: command.OrderId, ShipmentId: shippingStableID("shipment", command.IdempotencyKey), TrackingId: shippingTrackingID(command.OrderId)})
	case "boutique.cmd.shipping.cancel-shipment.v1":
		command := &commandsv1.ShippingCancelShipmentCommand{}
		if err := envelope.Data.UnmarshalTo(command); err != nil {
			return shippingOutcome{}, err
		}
		if failureMode == "cancel" {
			return newShippingOutcome("boutique.evt.shipping.shipment-cancellation-failed.v1", "boutique.shipping.ShipmentCancellationFailed.v1", command.OrderId, envelope,
				&eventsv1.ShippingShipmentCancellationFailedEvent{OrderId: command.OrderId, ShipmentId: command.ShipmentId, Failure: &commonv1.Failure{Code: "CARRIER_CANCELLATION_FAILED", SafeMessage: "Shipment cancellation requires review."}})
		}
		return newShippingOutcome("boutique.evt.shipping.shipment-cancelled.v1", "boutique.shipping.ShipmentCancelled.v1", command.OrderId, envelope,
			&eventsv1.ShippingShipmentCancelledEvent{OrderId: command.OrderId, ShipmentId: command.ShipmentId})
	default:
		return shippingOutcome{}, fmt.Errorf("unsupported shipping command %s", subject)
	}
}

func newShippingOutcome(subject, messageType, orderID string, cause *commonv1.MessageEnvelope, payload proto.Message) (shippingOutcome, error) {
	wrapper, err := anypb.New(payload)
	if err != nil {
		return shippingOutcome{}, err
	}
	messageID := shippingStableID(subject, cause.MessageId)
	envelope := &commonv1.MessageEnvelope{MessageId: messageID, MessageType: messageType, SchemaVersion: 1, OccurredAt: timestamppb.Now(),
		Producer: "shippingservice/phase5", AggregateType: "order", AggregateId: orderID, AggregateVersion: cause.AggregateVersion,
		CorrelationId: cause.CorrelationId, CausationId: cause.MessageId, Traceparent: cause.Traceparent, Tracestate: cause.Tracestate, Data: wrapper}
	encoded, err := proto.Marshal(envelope)
	if err != nil {
		return shippingOutcome{}, err
	}
	return shippingOutcome{MessageID: messageID, Subject: subject, Data: encoded}, nil
}

func (worker *shippingEventWorker) publishOutcome(outcome shippingOutcome) error {
	ctx, cancel := context.WithTimeout(context.Background(), worker.publishTimeout)
	defer cancel()
	message := &nats.Msg{Subject: outcome.Subject, Data: outcome.Data, Header: nats.Header{}}
	message.Header.Set("Nats-Msg-Id", outcome.MessageID)
	_, err := worker.js.PublishMsg(message, nats.Context(ctx), nats.MsgId(outcome.MessageID))
	return err
}

func shippingStableID(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	id := hash.Sum(nil)[:16]
	id[6] = (id[6] & 0xf) | 0x50
	id[8] = (id[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
}

func shippingTrackingID(orderID string) string {
	sum := sha256.Sum256([]byte(orderID))
	return fmt.Sprintf("PH-%06d-%07d", uint32(sum[0])<<16|uint32(sum[1])<<8|uint32(sum[2]), uint32(sum[3])<<24|uint32(sum[4])<<16|uint32(sum[5])<<8|uint32(sum[6]))
}
