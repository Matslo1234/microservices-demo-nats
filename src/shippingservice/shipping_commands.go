// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	commandsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/commands/v1"
	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	stateless "github.com/GoogleCloudPlatform/microservices-demo/src/shared/stateless/go"
	telemetry "github.com/GoogleCloudPlatform/microservices-demo/src/shared/telemetry/go"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	shippingOrderQuoteSlot     = "shipping.order-quote"
	shippingCreateShipmentSlot = "shipping.create-shipment"
	shippingCancelShipmentSlot = "shipping.cancel-shipment"
)

type shippingOutcome struct {
	MessageID string
	Subject   string
	Data      []byte
}

// shippingProvider is a deterministic demo carrier. It does not retain
// outcomes: every value which looks provider-generated is an HMAC of the
// business idempotency identity under a replica-shared provider secret.
type shippingProvider struct {
	key []byte
}

func newShippingProvider(secret string) (*shippingProvider, error) {
	if len(secret) < 32 {
		return nil, errors.New("shipping provider secret must contain at least 32 characters")
	}
	key := sha256.Sum256([]byte("boutique/shipping-provider/v1\x00" + secret))
	return &shippingProvider{key: key[:]}, nil
}

func (provider *shippingProvider) stableID(kind string, parts ...string) string {
	hash := hmac.New(sha256.New, provider.key)
	_, _ = hash.Write([]byte(kind))
	for _, part := range parts {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(part))
	}
	id := hash.Sum(nil)[:16]
	id[6] = (id[6] & 0x0f) | 0x50
	id[8] = (id[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
}

func (provider *shippingProvider) trackingID(idempotencyKey string) string {
	hash := hmac.New(sha256.New, provider.key)
	_, _ = hash.Write([]byte("tracking\x00" + idempotencyKey))
	sum := hash.Sum(nil)
	left := (uint32(sum[0])<<16 | uint32(sum[1])<<8 | uint32(sum[2])) % 1_000_000
	right := (uint32(sum[3])<<24 | uint32(sum[4])<<16 | uint32(sum[5])<<8 | uint32(sum[6])) % 10_000_000
	return fmt.Sprintf("PH-%06d-%07d", left, right)
}

func (worker *shippingEventWorker) handleCommand(message *nats.Msg) error {
	return worker.handleCommandWithContext(context.Background(), message)
}

func (worker *shippingEventWorker) handleCommandWithContext(ctx context.Context, message *nats.Msg) error {
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(message.Data, envelope); err != nil {
		return err
	}
	telemetry.Inject(ctx, &envelope.Traceparent, &envelope.Tracestate)
	outcome, err := buildShippingOutcome(message.Subject, envelope, worker.provider, worker.failureMode)
	if err != nil {
		return err
	}
	return worker.publishOutcome(ctx, outcome)
}

func (worker *shippingEventWorker) handleCommandBatch(messages []*nats.Msg) []error {
	outcomes := make([]shippingOutcome, len(messages))
	results := make([]error, len(messages))

	for index, message := range messages {
		envelope := &commonv1.MessageEnvelope{}
		if err := proto.Unmarshal(message.Data, envelope); err != nil {
			results[index] = err
			continue
		}
		outcome, err := buildShippingOutcome(
			message.Subject,
			envelope,
			worker.provider,
			worker.failureMode,
		)
		if err != nil {
			results[index] = err
			continue
		}
		outcomes[index] = outcome
	}

	var publish sync.WaitGroup
	for index := range messages {
		if results[index] != nil {
			continue
		}
		publish.Add(1)
		go func(index int) {
			defer publish.Done()
			results[index] = worker.publishOutcome(context.Background(), outcomes[index])
		}(index)
	}
	publish.Wait()
	return results
}

func buildShippingOutcome(
	subject string,
	envelope *commonv1.MessageEnvelope,
	provider *shippingProvider,
	failureMode string,
) (shippingOutcome, error) {
	inputTime, err := validateShippingInput(envelope)
	if err != nil {
		return shippingOutcome{}, err
	}
	if provider == nil {
		return shippingOutcome{}, errors.New("shipping provider is required")
	}

	switch subject {
	case "boutique.cmd.shipping.calculate-order-quote.v1":
		command := &commandsv1.ShippingCalculateOrderQuoteCommand{}
		if err := envelope.Data.UnmarshalTo(command); err != nil {
			return shippingOutcome{}, err
		}
		if command.CommandId == "" || command.OrderId == "" || command.Cart == nil {
			return shippingOutcome{}, errors.New("shipping quote command is incomplete")
		}
		if command.OrderId != envelope.AggregateId {
			return shippingOutcome{}, errors.New("shipping quote order does not match the envelope aggregate")
		}
		if failureMode == "quote" {
			return newShippingOutcome(
				shippingOrderQuoteSlot,
				"boutique.evt.shipping.order-quote-failed.v1",
				"boutique.shipping.OrderQuoteFailed.v1",
				command.OrderId,
				envelope,
				inputTime,
				&eventsv1.ShippingOrderQuoteFailedEvent{
					OrderId: command.OrderId,
					Failure: &commonv1.Failure{
						Code:        "QUOTE_PROVIDER_UNAVAILABLE",
						Retryable:   true,
						SafeMessage: "Shipping quote is unavailable.",
					},
				},
			)
		}
		count := 0
		for _, line := range command.Cart.Items {
			count += int(line.Quantity)
		}
		quote := CreateQuoteFromCount(count)
		return newShippingOutcome(
			shippingOrderQuoteSlot,
			"boutique.evt.shipping.order-quote-calculated.v1",
			"boutique.shipping.OrderQuoteCalculated.v1",
			command.OrderId,
			envelope,
			inputTime,
			&eventsv1.ShippingOrderQuoteCalculatedEvent{
				OrderId: command.OrderId,
				CostUsd: &commonv1.Money{
					CurrencyCode: "USD",
					Units:        int64(quote.Dollars),
					Nanos:        int32(quote.Cents * 10_000_000),
				},
				QuoteId:   provider.stableID("quote", command.CommandId, command.OrderId),
				ExpiresAt: timestamppb.New(inputTime.Add(15 * time.Minute)),
			},
		)
	case "boutique.cmd.shipping.create-shipment.v1":
		command := &commandsv1.ShippingCreateShipmentCommand{}
		if err := envelope.Data.UnmarshalTo(command); err != nil {
			return shippingOutcome{}, err
		}
		if command.CommandId == "" || command.OrderId == "" || command.IdempotencyKey == "" {
			return shippingOutcome{}, errors.New("shipping create command is incomplete")
		}
		if command.OrderId != envelope.AggregateId {
			return shippingOutcome{}, errors.New("shipping create order does not match the envelope aggregate")
		}
		if failureMode == "shipment" {
			return newShippingOutcome(
				shippingCreateShipmentSlot,
				"boutique.evt.shipping.shipment-creation-failed.v1",
				"boutique.shipping.ShipmentCreationFailed.v1",
				command.OrderId,
				envelope,
				inputTime,
				&eventsv1.ShippingShipmentCreationFailedEvent{
					OrderId: command.OrderId,
					Failure: &commonv1.Failure{
						Code:        "CARRIER_UNAVAILABLE",
						Retryable:   true,
						SafeMessage: "Shipment creation failed.",
					},
				},
			)
		}
		return newShippingOutcome(
			shippingCreateShipmentSlot,
			"boutique.evt.shipping.shipment-created.v1",
			"boutique.shipping.ShipmentCreated.v1",
			command.OrderId,
			envelope,
			inputTime,
			&eventsv1.ShippingShipmentCreatedEvent{
				OrderId:    command.OrderId,
				ShipmentId: provider.stableID("shipment", command.IdempotencyKey),
				TrackingId: provider.trackingID(command.IdempotencyKey),
			},
		)
	case "boutique.cmd.shipping.cancel-shipment.v1":
		command := &commandsv1.ShippingCancelShipmentCommand{}
		if err := envelope.Data.UnmarshalTo(command); err != nil {
			return shippingOutcome{}, err
		}
		if command.CommandId == "" || command.OrderId == "" || command.ShipmentId == "" || command.IdempotencyKey == "" {
			return shippingOutcome{}, errors.New("shipping cancellation command is incomplete")
		}
		if command.OrderId != envelope.AggregateId {
			return shippingOutcome{}, errors.New("shipping cancellation order does not match the envelope aggregate")
		}
		if failureMode == "cancel" {
			return newShippingOutcome(
				shippingCancelShipmentSlot,
				"boutique.evt.shipping.shipment-cancellation-failed.v1",
				"boutique.shipping.ShipmentCancellationFailed.v1",
				command.OrderId,
				envelope,
				inputTime,
				&eventsv1.ShippingShipmentCancellationFailedEvent{
					OrderId:    command.OrderId,
					ShipmentId: command.ShipmentId,
					Failure: &commonv1.Failure{
						Code:        "CARRIER_CANCELLATION_FAILED",
						SafeMessage: "Shipment cancellation requires review.",
					},
				},
			)
		}
		return newShippingOutcome(
			shippingCancelShipmentSlot,
			"boutique.evt.shipping.shipment-cancelled.v1",
			"boutique.shipping.ShipmentCancelled.v1",
			command.OrderId,
			envelope,
			inputTime,
			&eventsv1.ShippingShipmentCancelledEvent{
				OrderId:    command.OrderId,
				ShipmentId: command.ShipmentId,
			},
		)
	default:
		return shippingOutcome{}, fmt.Errorf("unsupported shipping command %s", subject)
	}
}

func validateShippingInput(envelope *commonv1.MessageEnvelope) (time.Time, error) {
	if envelope == nil || envelope.MessageId == "" || envelope.CorrelationId == "" ||
		envelope.AggregateId == "" || envelope.AggregateVersion == 0 || envelope.Data == nil ||
		envelope.OccurredAt == nil {
		return time.Time{}, errors.New("shipping input envelope is incomplete")
	}
	if err := envelope.OccurredAt.CheckValid(); err != nil {
		return time.Time{}, fmt.Errorf("shipping input timestamp is invalid: %w", err)
	}
	return envelope.OccurredAt.AsTime().UTC(), nil
}

func newShippingOutcome(
	slot string,
	subject string,
	messageType string,
	orderID string,
	cause *commonv1.MessageEnvelope,
	occurredAt time.Time,
	payload proto.Message,
) (shippingOutcome, error) {
	envelope, err := stateless.NewResultEnvelope(cause, stateless.ResultSpec{
		Slot:             slot,
		MessageType:      messageType,
		Producer:         "shippingservice/phase3",
		AggregateType:    "order",
		AggregateID:      orderID,
		AggregateVersion: cause.AggregateVersion,
		OccurredAt:       occurredAt,
		Payload:          payload,
	})
	if err != nil {
		return shippingOutcome{}, err
	}
	encoded, err := stateless.MarshalEnvelope(envelope)
	if err != nil {
		return shippingOutcome{}, err
	}
	return shippingOutcome{MessageID: envelope.MessageId, Subject: subject, Data: encoded}, nil
}

func (worker *shippingEventWorker) publishOutcome(ctx context.Context, outcome shippingOutcome) error {
	correlationID := shippingOutcomeCorrelationID(outcome)
	ctx, span := telemetry.StartProducerSpan(ctx, outcome.Subject, "event", outcome.MessageID, correlationID)
	defer span.End()
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(outcome.Data, envelope); err == nil {
		telemetry.Inject(ctx, &envelope.Traceparent, &envelope.Tracestate)
		if encoded, marshalErr := proto.Marshal(envelope); marshalErr == nil {
			outcome.Data = encoded
		}
	}
	publishContext, cancel := context.WithTimeout(ctx, worker.publishTimeout)
	defer cancel()
	message := &nats.Msg{Subject: outcome.Subject, Data: outcome.Data, Header: nats.Header{}}
	message.Header.Set("Nats-Msg-Id", outcome.MessageID)
	_, err := worker.js.PublishMsg(message, nats.Context(publishContext), nats.MsgId(outcome.MessageID))
	if err != nil {
		telemetry.RecordError(span, err)
		return err
	}
	log.WithFields(logrus.Fields{
		"topic":          outcome.Subject,
		"message_kind":   "event",
		"message_id":     outcome.MessageID,
		"correlation_id": correlationID,
	}).Debug("NATS event sent")
	return nil
}

func shippingOutcomeCorrelationID(outcome shippingOutcome) string {
	correlationID, _ := shippingEnvelopeContext(outcome.Data)
	return correlationID
}
