// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"time"

	commandsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/commands/v1"
	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	stageWaitingQuote     = "WAITING_FOR_QUOTE"
	stageWaitingAuthorize = "WAITING_FOR_AUTHORIZATION"
	stageWaitingShipment  = "WAITING_FOR_SHIPMENT"
	stageWaitingCapture   = "WAITING_FOR_CAPTURE"
	stageCompensating     = "COMPENSATING"
	stageCompleted        = "COMPLETED"
	stageCancelled        = "CANCELLED"
	stageRejected         = "REJECTED"
	stageManualReview     = "MANUAL_REVIEW"
)

type orderSaga struct {
	OrderID               string                           `json:"order_id"`
	OperationID           string                           `json:"operation_id"`
	CommandID             string                           `json:"command_id"`
	UserID                string                           `json:"user_id"`
	Email                 string                           `json:"email"`
	Address               *commonv1.PostalAddress          `json:"address"`
	CurrencyCode          string                           `json:"currency_code"`
	PaymentToken          string                           `json:"payment_token,omitempty"`
	CartVersion           uint64                           `json:"cart_version"`
	CatalogRevision       uint64                           `json:"catalog_revision"`
	RateRevision          uint64                           `json:"rate_revision"`
	Version               uint64                           `json:"version"`
	Stage                 string                           `json:"stage"`
	Deadline              time.Time                        `json:"deadline,omitempty"`
	Snapshot              *commonv1.SanitizedOrderSnapshot `json:"snapshot"`
	AuthorizationID       string                           `json:"authorization_id,omitempty"`
	ShipmentID            string                           `json:"shipment_id,omitempty"`
	TrackingID            string                           `json:"tracking_id,omitempty"`
	CancelReason          *commonv1.Failure                `json:"cancel_reason,omitempty"`
	AuthorizationReleased bool                             `json:"authorization_released,omitempty"`
	ShipmentCancelled     bool                             `json:"shipment_cancelled,omitempty"`
	NeedRelease           bool                             `json:"need_release,omitempty"`
	NeedShipmentCancel    bool                             `json:"need_shipment_cancel,omitempty"`
}

func (worker *checkoutWorker) handleOrderCommand(envelope *commonv1.MessageEnvelope) error {
	payload := &commandsv1.OrderSubmitCommand{}
	if err := envelope.Data.UnmarshalTo(payload); err != nil {
		return err
	}
	if payload.OrderId == "" || payload.OperationId == "" || payload.UserId == "" || payload.PaymentToken == "" {
		return errors.New("order command identity or payment token is missing")
	}
	return worker.store.Update(func(state *persistedState) error {
		if _, processed := state.Inbox[envelope.MessageId]; processed {
			return nil
		}
		if existing := state.Orders[payload.OrderId]; existing != nil {
			state.Inbox[envelope.MessageId] = time.Now().UTC()
			return nil
		}
		cart := state.Carts[payload.UserId]
		failure := validateOrderCommand(state, cart, payload)
		if failure != nil {
			saga := &orderSaga{OrderID: payload.OrderId, OperationID: payload.OperationId, CommandID: payload.CommandId,
				UserID: payload.UserId, Version: 1, Stage: stageRejected}
			state.Orders[payload.OrderId] = saga
			if err := queueEnvelope(state, "boutique.evt.order.rejected.v1", "boutique.order.Rejected.v1",
				"order", payload.OrderId, saga.Version, payload.OrderId, envelope.MessageId,
				&eventsv1.OrderRejectedEvent{OperationId: payload.OperationId, OrderId: payload.OrderId, Failure: failure}); err != nil {
				return err
			}
			state.Inbox[envelope.MessageId] = time.Now().UTC()
			return nil
		}
		snapshot, err := buildOrderSnapshot(state, cart, payload)
		if err != nil {
			return err
		}
		saga := &orderSaga{
			OrderID: payload.OrderId, OperationID: payload.OperationId, CommandID: payload.CommandId,
			UserID: payload.UserId, Email: payload.Email, Address: payload.ShippingAddress,
			CurrencyCode: payload.CurrencyCode, PaymentToken: payload.PaymentToken,
			CartVersion: payload.ExpectedCartVersion, CatalogRevision: payload.ExpectedCatalogRevision,
			RateRevision: payload.ExpectedRateRevision, Version: 1, Stage: stageWaitingQuote,
			Deadline: time.Now().UTC().Add(worker.stepTimeout), Snapshot: snapshot,
		}
		state.Orders[payload.OrderId] = saga
		if err := queueEnvelope(state, "boutique.evt.order.submitted.v1", "boutique.order.Submitted.v1", "order",
			payload.OrderId, saga.Version, payload.OrderId, envelope.MessageId, &eventsv1.OrderSubmittedEvent{
				OperationId: payload.OperationId, Order: snapshot, AcceptedCartVersion: saga.CartVersion,
				AcceptedCatalogRevision: saga.CatalogRevision, AcceptedRateRevision: saga.RateRevision,
			}); err != nil {
			return err
		}
		quoteCommand := &commandsv1.ShippingCalculateOrderQuoteCommand{
			CommandId: stableID("quote", payload.OrderId), OrderId: payload.OrderId,
			ShippingAddress: payload.ShippingAddress, Cart: cart,
		}
		if err := queueEnvelope(state, "boutique.cmd.shipping.calculate-order-quote.v1", "boutique.shipping.CalculateOrderQuote.v1",
			"order", payload.OrderId, saga.Version, payload.OrderId, envelope.MessageId, quoteCommand); err != nil {
			return err
		}
		if err := queueStage(state, saga, envelope.MessageId); err != nil {
			return err
		}
		state.Inbox[envelope.MessageId] = time.Now().UTC()
		return nil
	})
}

func validateOrderCommand(state *persistedState, cart *commonv1.CartSnapshot, command *commandsv1.OrderSubmitCommand) *commonv1.Failure {
	failure := func(code, message string) *commonv1.Failure {
		return &commonv1.Failure{Code: code, SafeMessage: message}
	}
	if cart == nil || len(cart.Items) == 0 {
		return failure("EMPTY_CART", "The cart is empty.")
	}
	if cart.CartVersion != command.ExpectedCartVersion {
		return failure("CART_VERSION_CONFLICT", "The cart changed before checkout.")
	}
	if state.CatalogRevision != command.ExpectedCatalogRevision {
		return failure("CATALOG_VERSION_CONFLICT", "The catalog changed before checkout.")
	}
	if state.Rates == nil || state.Rates.RateRevision != command.ExpectedRateRevision {
		return failure("RATE_VERSION_CONFLICT", "Currency rates changed before checkout.")
	}
	if command.ShippingAddress == nil || command.Email == "" {
		return failure("INVALID_ORDER", "Order contact details are incomplete.")
	}
	for _, line := range cart.Items {
		if line.Quantity <= 0 || state.Products[line.ProductId] == nil {
			return failure("PRODUCT_UNAVAILABLE", "A cart product is unavailable.")
		}
	}
	if convertMoney(state.Rates, &commonv1.Money{CurrencyCode: state.Rates.BaseCurrencyCode}, command.CurrencyCode) == nil {
		return failure("INVALID_CURRENCY", "The selected currency is unavailable.")
	}
	return nil
}

func buildOrderSnapshot(state *persistedState, cart *commonv1.CartSnapshot, command *commandsv1.OrderSubmitCommand) (*commonv1.SanitizedOrderSnapshot, error) {
	snapshot := &commonv1.SanitizedOrderSnapshot{OrderId: command.OrderId, UserId: command.UserId, Email: command.Email,
		ShippingAddress: command.ShippingAddress, ShippingCost: &commonv1.Money{CurrencyCode: command.CurrencyCode},
		Total: &commonv1.Money{CurrencyCode: command.CurrencyCode}}
	for _, line := range cart.Items {
		product := state.Products[line.ProductId]
		cost := convertMoney(state.Rates, product.PriceUsd, command.CurrencyCode)
		if cost == nil {
			return nil, fmt.Errorf("cannot convert %s to %s", line.ProductId, command.CurrencyCode)
		}
		snapshot.Items = append(snapshot.Items, &commonv1.OrderLine{Item: line, UnitCost: cost})
		snapshot.Total = addMoney(snapshot.Total, multiplyMoney(cost, line.Quantity))
	}
	return snapshot, nil
}

func (worker *checkoutWorker) handleSagaEvent(subject string, envelope *commonv1.MessageEnvelope) error {
	return worker.store.Update(func(state *persistedState) error {
		if _, processed := state.Inbox[envelope.MessageId]; processed {
			return nil
		}
		state.Inbox[envelope.MessageId] = time.Now().UTC()
		saga := state.Orders[envelope.AggregateId]
		if saga == nil {
			return nil
		}
		if saga.Stage == stageCompleted || saga.Stage == stageCancelled || saga.Stage == stageRejected || saga.Stage == stageManualReview {
			return nil
		}
		cause := envelope.MessageId
		switch subject {
		case "boutique.evt.shipping.order-quote-calculated.v1":
			payload := &eventsv1.ShippingOrderQuoteCalculatedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if saga.Stage != stageWaitingQuote {
				break
			}
			shipping := convertMoney(state.Rates, payload.CostUsd, saga.CurrencyCode)
			if shipping == nil {
				return worker.cancel(state, saga, cause, &commonv1.Failure{Code: "INVALID_CURRENCY", SafeMessage: "Shipping could not be converted."})
			}
			saga.Snapshot.ShippingCost = shipping
			saga.Snapshot.Total = addMoney(saga.Snapshot.Total, shipping)
			saga.Version++
			saga.Stage = stageWaitingAuthorize
			saga.Deadline = time.Now().UTC().Add(worker.stepTimeout)
			command := &commandsv1.PaymentAuthorizeCommand{CommandId: stableID("authorize", saga.OrderID), OrderId: saga.OrderID,
				Amount: saga.Snapshot.Total, PaymentToken: saga.PaymentToken, IdempotencyKey: saga.OrderID + "/authorize"}
			if err := queueEnvelope(state, "boutique.cmd.payment.authorize.v1", "boutique.payment.Authorize.v1", "order", saga.OrderID, saga.Version, saga.OrderID, cause, command); err != nil {
				return err
			}
			return queueStage(state, saga, cause)
		case "boutique.evt.shipping.order-quote-failed.v1":
			payload := &eventsv1.ShippingOrderQuoteFailedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if saga.Stage != stageWaitingQuote {
				break
			}
			return worker.cancel(state, saga, cause, safeFailure(payload.Failure, "SHIPPING_QUOTE_FAILED"))
		case "boutique.evt.payment.authorized.v1":
			payload := &eventsv1.PaymentAuthorizedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if saga.Stage != stageWaitingAuthorize {
				break
			}
			saga.AuthorizationID = payload.AuthorizationId
			saga.PaymentToken = ""
			saga.Version++
			saga.Stage = stageWaitingShipment
			saga.Deadline = time.Now().UTC().Add(worker.stepTimeout)
			command := &commandsv1.ShippingCreateShipmentCommand{CommandId: stableID("shipment", saga.OrderID), OrderId: saga.OrderID,
				ShippingAddress: saga.Address, IdempotencyKey: saga.OrderID + "/shipment"}
			for _, line := range saga.Snapshot.Items {
				command.Items = append(command.Items, line.Item)
			}
			if err := queueEnvelope(state, "boutique.cmd.shipping.create-shipment.v1", "boutique.shipping.CreateShipment.v1", "order", saga.OrderID, saga.Version, saga.OrderID, cause, command); err != nil {
				return err
			}
			return queueStage(state, saga, cause)
		case "boutique.evt.payment.authorization-declined.v1":
			payload := &eventsv1.PaymentAuthorizationDeclinedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if saga.Stage != stageWaitingAuthorize {
				break
			}
			return worker.cancel(state, saga, cause, &commonv1.Failure{Code: "PAYMENT_DECLINED", SafeMessage: "Payment authorization was declined."})
		case "boutique.evt.shipping.shipment-created.v1":
			payload := &eventsv1.ShippingShipmentCreatedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if saga.Stage != stageWaitingShipment {
				break
			}
			saga.ShipmentID = payload.ShipmentId
			saga.TrackingID = payload.TrackingId
			saga.Snapshot.TrackingId = payload.TrackingId
			saga.Version++
			saga.Stage = stageWaitingCapture
			saga.Deadline = time.Now().UTC().Add(worker.stepTimeout)
			command := &commandsv1.PaymentCaptureCommand{CommandId: stableID("capture", saga.OrderID), OrderId: saga.OrderID,
				AuthorizationId: saga.AuthorizationID, Amount: saga.Snapshot.Total, IdempotencyKey: saga.OrderID + "/capture"}
			if err := queueEnvelope(state, "boutique.cmd.payment.capture.v1", "boutique.payment.Capture.v1", "order", saga.OrderID, saga.Version, saga.OrderID, cause, command); err != nil {
				return err
			}
			return queueStage(state, saga, cause)
		case "boutique.evt.shipping.shipment-creation-failed.v1":
			payload := &eventsv1.ShippingShipmentCreationFailedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if saga.Stage != stageWaitingShipment {
				break
			}
			return worker.startCompensation(state, saga, cause, safeFailure(payload.Failure, "SHIPMENT_FAILED"), true, false)
		case "boutique.evt.payment.captured.v1":
			payload := &eventsv1.PaymentCapturedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if saga.Stage != stageWaitingCapture {
				break
			}
			saga.Version++
			saga.Stage = stageCompleted
			saga.Deadline = time.Time{}
			if err := queueEnvelope(state, "boutique.evt.order.completed.v1", "boutique.order.Completed.v1", "order", saga.OrderID, saga.Version, saga.OrderID, cause,
				&eventsv1.OrderCompletedEvent{Order: saga.Snapshot}); err != nil {
				return err
			}
			clear := &commandsv1.CartClearCommand{CommandId: envelopeMessageID("boutique.cmd.cart.clear.v1", cause, saga.CartVersion), UserId: saga.UserID,
				ExpectedCartVersion: saga.CartVersion, Reason: "order-completed", OrderId: saga.OrderID}
			if err := queueEnvelope(state, "boutique.cmd.cart.clear.v1", "boutique.cart.Clear.v1", "cart", saga.UserID, saga.CartVersion, saga.OrderID, cause, clear); err != nil {
				return err
			}
			return queueStage(state, saga, cause)
		case "boutique.evt.payment.capture-failed.v1":
			payload := &eventsv1.PaymentCaptureFailedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if saga.Stage != stageWaitingCapture {
				break
			}
			return worker.startCompensation(state, saga, cause, safeFailure(payload.Failure, "CAPTURE_FAILED"), true, true)
		case "boutique.evt.payment.authorization-released.v1":
			payload := &eventsv1.PaymentAuthorizationReleasedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if saga.Stage != stageCompensating {
				break
			}
			saga.AuthorizationReleased = true
			return worker.finishCompensation(state, saga, cause)
		case "boutique.evt.shipping.shipment-cancelled.v1":
			payload := &eventsv1.ShippingShipmentCancelledEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if saga.Stage != stageCompensating {
				break
			}
			saga.ShipmentCancelled = true
			return worker.finishCompensation(state, saga, cause)
		case "boutique.evt.payment.authorization-release-failed.v1":
			if saga.Stage != stageCompensating {
				break
			}
			return worker.manualReview(state, saga, cause, "payment.authorization-release", "PAYMENT_RELEASE_FAILED")
		case "boutique.evt.shipping.shipment-cancellation-failed.v1":
			if saga.Stage != stageCompensating {
				break
			}
			return worker.manualReview(state, saga, cause, "shipping.cancel-shipment", "SHIPMENT_CANCELLATION_FAILED")
		}
		return nil
	})
}

func (worker *checkoutWorker) cancel(state *persistedState, saga *orderSaga, cause string, failure *commonv1.Failure) error {
	saga.Version++
	saga.Stage = stageCancelled
	saga.Deadline = time.Time{}
	saga.CancelReason = failure
	return queueEnvelope(state, "boutique.evt.order.cancelled.v1", "boutique.order.Cancelled.v1", "order", saga.OrderID,
		saga.Version, saga.OrderID, cause, &eventsv1.OrderCancelledEvent{OrderId: saga.OrderID, Failure: failure})
}

func (worker *checkoutWorker) startCompensation(state *persistedState, saga *orderSaga, cause string, failure *commonv1.Failure, release, cancelShipment bool) error {
	saga.Version++
	saga.Stage = stageCompensating
	saga.Deadline = time.Now().UTC().Add(worker.stepTimeout)
	saga.CancelReason = failure
	saga.NeedRelease = release
	saga.NeedShipmentCancel = cancelShipment
	if release {
		command := &commandsv1.PaymentReleaseAuthorizationCommand{CommandId: stableID("release", saga.OrderID), OrderId: saga.OrderID,
			AuthorizationId: saga.AuthorizationID, Reason: failure.Code, IdempotencyKey: saga.OrderID + "/release"}
		if err := queueEnvelope(state, "boutique.cmd.payment.release-authorization.v1", "boutique.payment.ReleaseAuthorization.v1", "order", saga.OrderID, saga.Version, saga.OrderID, cause, command); err != nil {
			return err
		}
	}
	if cancelShipment {
		command := &commandsv1.ShippingCancelShipmentCommand{CommandId: stableID("cancel-shipment", saga.OrderID), OrderId: saga.OrderID,
			ShipmentId: saga.ShipmentID, TrackingId: saga.TrackingID, Reason: failure.Code, IdempotencyKey: saga.OrderID + "/cancel-shipment"}
		if err := queueEnvelope(state, "boutique.cmd.shipping.cancel-shipment.v1", "boutique.shipping.CancelShipment.v1", "order", saga.OrderID, saga.Version, saga.OrderID, cause, command); err != nil {
			return err
		}
	}
	return queueStage(state, saga, cause)
}

func (worker *checkoutWorker) finishCompensation(state *persistedState, saga *orderSaga, cause string) error {
	if saga.Stage != stageCompensating {
		return nil
	}
	if saga.NeedRelease && !saga.AuthorizationReleased {
		return nil
	}
	if saga.NeedShipmentCancel && !saga.ShipmentCancelled {
		return nil
	}
	saga.Version++
	saga.Stage = stageCancelled
	saga.Deadline = time.Time{}
	completed := []string{}
	if saga.AuthorizationReleased {
		completed = append(completed, "payment.authorization-released")
	}
	if saga.ShipmentCancelled {
		completed = append(completed, "shipping.shipment-cancelled")
	}
	return queueEnvelope(state, "boutique.evt.order.cancelled.v1", "boutique.order.Cancelled.v1", "order", saga.OrderID, saga.Version,
		saga.OrderID, cause, &eventsv1.OrderCancelledEvent{OrderId: saga.OrderID, Failure: saga.CancelReason, CompletedCompensations: completed})
}

func (worker *checkoutWorker) manualReview(state *persistedState, saga *orderSaga, cause, failedCompensation, reference string) error {
	saga.Version++
	saga.Stage = stageManualReview
	saga.Deadline = time.Time{}
	return queueEnvelope(state, "boutique.evt.order.manual-review-required.v1", "boutique.order.ManualReviewRequired.v1", "order", saga.OrderID,
		saga.Version, saga.OrderID, cause, &eventsv1.OrderManualReviewRequiredEvent{OrderId: saga.OrderID,
			FailedStep: saga.CancelReason.GetCode(), FailedCompensation: failedCompensation, RestrictedProviderReferences: []string{reference}})
}

func queueStage(state *persistedState, saga *orderSaga, cause string) error {
	return queueEnvelope(state, "boutique.evt.order.processing-stage-changed.v1", "boutique.order.ProcessingStageChanged.v1", "order",
		saga.OrderID, saga.Version, saga.OrderID, cause, &eventsv1.OrderProcessingStageChangedEvent{OrderId: saga.OrderID,
			Stage: saga.Stage, ChangedAt: timestamppb.Now()})
}

func queueEnvelope(state *persistedState, subject, messageType, aggregateType, aggregateID string, aggregateVersion uint64,
	correlationID, causationID string, payload proto.Message) error {
	wrapper, err := anypb.New(payload)
	if err != nil {
		return err
	}
	messageID := envelopeMessageID(subject, causationID, aggregateVersion)
	envelope := &commonv1.MessageEnvelope{MessageId: messageID, MessageType: messageType, SchemaVersion: 1,
		OccurredAt: timestamppb.Now(), Producer: "checkoutservice/phase5", AggregateType: aggregateType, AggregateId: aggregateID,
		AggregateVersion: aggregateVersion, CorrelationId: correlationID, CausationId: causationID, Data: wrapper}
	encoded, err := proto.Marshal(envelope)
	if err != nil {
		return err
	}
	state.Outbox[messageID] = outboxMessage{MessageID: messageID, Subject: subject, Data: encoded}
	return nil
}

func envelopeMessageID(subject, causationID string, aggregateVersion uint64) string {
	return stableID(subject, causationID, fmt.Sprint(aggregateVersion))
}

func safeFailure(failure *commonv1.Failure, fallback string) *commonv1.Failure {
	if failure != nil {
		return failure
	}
	return &commonv1.Failure{Code: fallback, SafeMessage: "The order could not be completed."}
}

func stableID(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	id := hash.Sum(nil)[:16]
	id[6] = (id[6] & 0x0f) | 0x50
	id[8] = (id[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
}

func convertMoney(rates *eventsv1.CurrencyRatesUpdatedEvent, from *commonv1.Money, to string) *commonv1.Money {
	if rates == nil || from == nil {
		return nil
	}
	values := map[string]float64{}
	for _, rate := range rates.Rates {
		values[rate.CurrencyCode] = rate.UnitsPerBase
	}
	fromRate, fromOK := values[from.CurrencyCode]
	toRate, toOK := values[to]
	if !fromOK || !toOK || fromRate == 0 {
		return nil
	}
	amount := (float64(from.Units) + float64(from.Nanos)/1e9) / fromRate * toRate
	units := math.Floor(amount)
	nanos := math.Round((amount - units) * 1e9)
	if nanos >= 1e9 {
		units++
		nanos -= 1e9
	}
	return &commonv1.Money{CurrencyCode: to, Units: int64(units), Nanos: int32(nanos)}
}

func addMoney(left, right *commonv1.Money) *commonv1.Money {
	if left == nil {
		return proto.Clone(right).(*commonv1.Money)
	}
	result := &commonv1.Money{CurrencyCode: left.CurrencyCode, Units: left.Units + right.Units, Nanos: left.Nanos + right.Nanos}
	if result.Nanos >= 1e9 {
		result.Units++
		result.Nanos -= 1e9
	}
	return result
}

func multiplyMoney(value *commonv1.Money, quantity int32) *commonv1.Money {
	result := &commonv1.Money{CurrencyCode: value.CurrencyCode, Units: value.Units * int64(quantity), Nanos: value.Nanos * quantity}
	result.Units += int64(result.Nanos / 1e9)
	result.Nanos %= 1e9
	return result
}
