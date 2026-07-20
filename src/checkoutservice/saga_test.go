// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"path/filepath"
	"testing"
	"time"

	commandsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/commands/v1"
	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testWorker(t *testing.T) *checkoutWorker {
	t.Helper()
	store, err := openStateStore(filepath.Join(t.TempDir(), "sagas.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(state *persistedState) error {
		state.CatalogRevision = 7
		state.Rates = &eventsv1.CurrencyRatesUpdatedEvent{BaseCurrencyCode: "USD", RateRevision: 9,
			Rates: []*eventsv1.CurrencyRate{{CurrencyCode: "USD", UnitsPerBase: 1}}}
		state.Products["product-1"] = &commonv1.ProductSnapshot{ProductId: "product-1", ProductVersion: 1,
			PriceUsd: &commonv1.Money{CurrencyCode: "USD", Units: 10}}
		state.Carts["user-1"] = &commonv1.CartSnapshot{UserId: "user-1", CartVersion: 3,
			Items: []*commonv1.CartLine{{ProductId: "product-1", Quantity: 2}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return &checkoutWorker{store: store, stepTimeout: time.Minute}
}

func testEnvelope(t *testing.T, messageID, orderID string, version uint64, payload proto.Message) *commonv1.MessageEnvelope {
	t.Helper()
	wrapped, err := anypb.New(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &commonv1.MessageEnvelope{MessageId: messageID, SchemaVersion: 1, OccurredAt: timestamppb.Now(),
		AggregateType: "order", AggregateId: orderID, AggregateVersion: version, CorrelationId: orderID, Data: wrapped}
}

func submitTestOrder(t *testing.T, worker *checkoutWorker, orderID string) {
	t.Helper()
	command := &commandsv1.OrderSubmitCommand{CommandId: orderID, OperationId: orderID, OrderId: orderID, UserId: "user-1",
		ExpectedCartVersion: 3, ExpectedCatalogRevision: 7, ExpectedRateRevision: 9, CurrencyCode: "USD",
		ShippingAddress: &commonv1.PostalAddress{StreetAddress: "1 Main", City: "Test", Country: "SI"}, Email: "buyer@example.com", PaymentToken: "ptok_test"}
	if err := worker.handleOrderCommand(testEnvelope(t, "submit-"+orderID, orderID, 0, command)); err != nil {
		t.Fatal(err)
	}
}

func progressToCapture(t *testing.T, worker *checkoutWorker, orderID string) {
	t.Helper()
	submitTestOrder(t, worker, orderID)
	steps := []struct {
		subject, id string
		payload     proto.Message
	}{
		{"boutique.evt.shipping.order-quote-calculated.v1", "quote", &eventsv1.ShippingOrderQuoteCalculatedEvent{OrderId: orderID, CostUsd: &commonv1.Money{CurrencyCode: "USD", Units: 5}}},
		{"boutique.evt.payment.authorized.v1", "authorized", &eventsv1.PaymentAuthorizedEvent{OrderId: orderID, AuthorizationId: "auth-1", Amount: &commonv1.Money{CurrencyCode: "USD", Units: 25}}},
		{"boutique.evt.shipping.shipment-created.v1", "shipment", &eventsv1.ShippingShipmentCreatedEvent{OrderId: orderID, ShipmentId: "ship-1", TrackingId: "track-1"}},
	}
	for index, step := range steps {
		if err := worker.handleSagaEvent(step.subject, testEnvelope(t, step.id+orderID, orderID, uint64(index+1), step.payload)); err != nil {
			t.Fatal(err)
		}
	}
}

func applyQuote(t *testing.T, worker *checkoutWorker, orderID string) {
	t.Helper()
	if err := worker.handleSagaEvent("boutique.evt.shipping.order-quote-calculated.v1", testEnvelope(t, "quote-"+orderID, orderID, 1,
		&eventsv1.ShippingOrderQuoteCalculatedEvent{OrderId: orderID, CostUsd: &commonv1.Money{CurrencyCode: "USD", Units: 5}})); err != nil {
		t.Fatal(err)
	}
}

func applyAuthorization(t *testing.T, worker *checkoutWorker, orderID string) {
	t.Helper()
	if err := worker.handleSagaEvent("boutique.evt.payment.authorized.v1", testEnvelope(t, "authorized-"+orderID, orderID, 2,
		&eventsv1.PaymentAuthorizedEvent{OrderId: orderID, AuthorizationId: "auth-1", Amount: &commonv1.Money{CurrencyCode: "USD", Units: 25}})); err != nil {
		t.Fatal(err)
	}
}

func TestSagaCompletesOnceAndQueuesIndependentSideEffects(t *testing.T) {
	worker := testWorker(t)
	orderID := "order-complete"
	progressToCapture(t, worker, orderID)
	captured := testEnvelope(t, "captured", orderID, 4, &eventsv1.PaymentCapturedEvent{OrderId: orderID, TransactionId: "tx-1", Amount: &commonv1.Money{CurrencyCode: "USD", Units: 25}})
	if err := worker.handleSagaEvent("boutique.evt.payment.captured.v1", captured); err != nil {
		t.Fatal(err)
	}
	before, _ := worker.store.Snapshot()
	version := before.Orders[orderID].Version
	if before.Orders[orderID].Stage != stageCompleted {
		t.Fatalf("stage = %s", before.Orders[orderID].Stage)
	}
	seenCompleted, seenCartClear := false, false
	for _, output := range before.Outbox {
		seenCompleted = seenCompleted || output.Subject == "boutique.evt.order.completed.v1"
		if output.Subject == "boutique.cmd.cart.clear.v1" {
			seenCartClear = true
			envelope, err := decodeEnvelope(output.Data)
			if err != nil {
				t.Fatal(err)
			}
			clear := &commandsv1.CartClearCommand{}
			if err := envelope.Data.UnmarshalTo(clear); err != nil {
				t.Fatal(err)
			}
			if clear.CommandId != envelope.MessageId {
				t.Fatalf("cart clear command ID %q does not match envelope message ID %q", clear.CommandId, envelope.MessageId)
			}
		}
	}
	if !seenCompleted || !seenCartClear {
		t.Fatalf("completed=%t cart_clear=%t", seenCompleted, seenCartClear)
	}
	if err := worker.handleSagaEvent("boutique.evt.payment.captured.v1", captured); err != nil {
		t.Fatal(err)
	}
	after, _ := worker.store.Snapshot()
	if after.Orders[orderID].Version != version {
		t.Fatal("duplicate capture advanced the saga")
	}
}

func TestCaptureFailureCompensatesBothSideEffects(t *testing.T) {
	worker := testWorker(t)
	orderID := "order-compensate"
	progressToCapture(t, worker, orderID)
	failure := &commonv1.Failure{Code: "CAPTURE_PROVIDER_ERROR", Retryable: false, SafeMessage: "Capture failed."}
	if err := worker.handleSagaEvent("boutique.evt.payment.capture-failed.v1", testEnvelope(t, "capture-failed", orderID, 4,
		&eventsv1.PaymentCaptureFailedEvent{OrderId: orderID, AuthorizationId: "auth-1", Failure: failure})); err != nil {
		t.Fatal(err)
	}
	state, _ := worker.store.Snapshot()
	if state.Orders[orderID].Stage != stageCompensating {
		t.Fatalf("stage = %s", state.Orders[orderID].Stage)
	}
	if err := worker.handleSagaEvent("boutique.evt.payment.authorization-released.v1", testEnvelope(t, "released", orderID, 5,
		&eventsv1.PaymentAuthorizationReleasedEvent{OrderId: orderID, AuthorizationId: "auth-1"})); err != nil {
		t.Fatal(err)
	}
	if err := worker.handleSagaEvent("boutique.evt.shipping.shipment-cancelled.v1", testEnvelope(t, "cancelled", orderID, 5,
		&eventsv1.ShippingShipmentCancelledEvent{OrderId: orderID, ShipmentId: "ship-1"})); err != nil {
		t.Fatal(err)
	}
	state, _ = worker.store.Snapshot()
	if state.Orders[orderID].Stage != stageCancelled {
		t.Fatalf("stage = %s", state.Orders[orderID].Stage)
	}
}

func TestCompensationFailureRequiresManualReview(t *testing.T) {
	worker := testWorker(t)
	orderID := "order-review"
	progressToCapture(t, worker, orderID)
	if err := worker.handleSagaEvent("boutique.evt.payment.capture-failed.v1", testEnvelope(t, "capture-failed-review", orderID, 4,
		&eventsv1.PaymentCaptureFailedEvent{OrderId: orderID, AuthorizationId: "auth-1", Failure: &commonv1.Failure{Code: "CAPTURE_FAILED"}})); err != nil {
		t.Fatal(err)
	}
	if err := worker.handleSagaEvent("boutique.evt.payment.authorization-release-failed.v1", testEnvelope(t, "release-failed", orderID, 5,
		&eventsv1.PaymentAuthorizationReleaseFailedEvent{OrderId: orderID, AuthorizationId: "auth-1", Failure: &commonv1.Failure{Code: "RELEASE_FAILED"}})); err != nil {
		t.Fatal(err)
	}
	state, _ := worker.store.Snapshot()
	if state.Orders[orderID].Stage != stageManualReview {
		t.Fatalf("stage = %s", state.Orders[orderID].Stage)
	}
}

func TestOrderRejectsStaleCartWithoutPaymentCommand(t *testing.T) {
	worker := testWorker(t)
	command := &commandsv1.OrderSubmitCommand{CommandId: "stale", OperationId: "stale", OrderId: "stale", UserId: "user-1",
		ExpectedCartVersion: 2, ExpectedCatalogRevision: 7, ExpectedRateRevision: 9, CurrencyCode: "USD", PaymentToken: "ptok",
		ShippingAddress: &commonv1.PostalAddress{StreetAddress: "1 Main"}, Email: "buyer@example.com"}
	if err := worker.handleOrderCommand(testEnvelope(t, "stale-submit", "stale", 0, command)); err != nil {
		t.Fatal(err)
	}
	state, _ := worker.store.Snapshot()
	if state.Orders["stale"].Stage != stageRejected {
		t.Fatal("stale order was not rejected")
	}
	for _, output := range state.Outbox {
		if output.Subject == "boutique.cmd.payment.authorize.v1" {
			t.Fatal("rejected order queued payment")
		}
	}
}

func TestFailureInjectionAtQuoteAuthorizationAndShipment(t *testing.T) {
	t.Run("quote", func(t *testing.T) {
		worker := testWorker(t)
		orderID := "order-quote-failure"
		submitTestOrder(t, worker, orderID)
		if err := worker.handleSagaEvent("boutique.evt.shipping.order-quote-failed.v1", testEnvelope(t, "quote-failure", orderID, 1,
			&eventsv1.ShippingOrderQuoteFailedEvent{OrderId: orderID, Failure: &commonv1.Failure{Code: "QUOTE_FAILED"}})); err != nil {
			t.Fatal(err)
		}
		state, _ := worker.store.Snapshot()
		if state.Orders[orderID].Stage != stageCancelled {
			t.Fatalf("stage = %s", state.Orders[orderID].Stage)
		}
	})
	t.Run("authorization", func(t *testing.T) {
		worker := testWorker(t)
		orderID := "order-declined"
		submitTestOrder(t, worker, orderID)
		applyQuote(t, worker, orderID)
		if err := worker.handleSagaEvent("boutique.evt.payment.authorization-declined.v1", testEnvelope(t, "declined", orderID, 2,
			&eventsv1.PaymentAuthorizationDeclinedEvent{OrderId: orderID, DeclineCategory: "TEST"})); err != nil {
			t.Fatal(err)
		}
		state, _ := worker.store.Snapshot()
		if state.Orders[orderID].Stage != stageCancelled {
			t.Fatalf("stage = %s", state.Orders[orderID].Stage)
		}
	})
	t.Run("shipment", func(t *testing.T) {
		worker := testWorker(t)
		orderID := "order-shipment-failure"
		submitTestOrder(t, worker, orderID)
		applyQuote(t, worker, orderID)
		applyAuthorization(t, worker, orderID)
		if err := worker.handleSagaEvent("boutique.evt.shipping.shipment-creation-failed.v1", testEnvelope(t, "shipment-failed", orderID, 3,
			&eventsv1.ShippingShipmentCreationFailedEvent{OrderId: orderID, Failure: &commonv1.Failure{Code: "SHIPMENT_FAILED"}})); err != nil {
			t.Fatal(err)
		}
		state, _ := worker.store.Snapshot()
		if state.Orders[orderID].Stage != stageCompensating || !state.Orders[orderID].NeedRelease || state.Orders[orderID].NeedShipmentCancel {
			t.Fatalf("unexpected compensation state: %#v", state.Orders[orderID])
		}
	})
}
