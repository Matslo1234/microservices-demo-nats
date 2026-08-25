// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	commandsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/commands/v1"
	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	stateless "github.com/GoogleCloudPlatform/microservices-demo/src/shared/stateless/go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var testTime = time.Date(2026, 7, 27, 10, 0, 0, 123000000, time.UTC)

func newTestWorker(t *testing.T, store *stateStore) *checkoutWorker {
	t.Helper()
	leases, err := stateless.NewRedisLeaseStore(store.client, store.prefix+":deadline-lease", resultJournalRetention)
	if err != nil {
		t.Fatal(err)
	}
	return &checkoutWorker{
		store: store, stepTimeout: time.Minute, leaseDuration: 10 * time.Second,
		leaseStore: leases, workerID: "test-worker", publishTimeout: time.Second,
	}
}

func testWorker(t *testing.T) *checkoutWorker {
	t.Helper()
	store, _ := newTestStateStore(t)
	seedTestCheckoutState(t, store)
	return newTestWorker(t, store)
}

func seedTestCheckoutState(t *testing.T, store *stateStore) {
	t.Helper()
	product := &commonv1.ProductSnapshot{
		ProductId: "product-1", ProductVersion: 1,
		PriceUsd: &commonv1.Money{CurrencyCode: "USD", Units: 10},
	}
	projections := []struct {
		subject string
		value   proto.Message
		id      string
		agg     string
		version uint64
	}{
		{"boutique.evt.catalog.product-upserted.v1", &eventsv1.CatalogProductUpsertedEvent{Product: product, CatalogRevision: 7}, "seed-product", "product-1", 1},
		{"boutique.evt.catalog.snapshot-completed.v1", &eventsv1.CatalogSnapshotCompletedEvent{CatalogRevision: 7}, "seed-catalog", "catalog", 7},
		{"boutique.evt.currency.rates-updated.v1", &eventsv1.CurrencyRatesUpdatedEvent{
			BaseCurrencyCode: "USD", RateRevision: 9,
			Rates: []*eventsv1.CurrencyRate{{CurrencyCode: "USD", UnitsPerBase: 1}},
		}, "seed-rates", "rates", 9},
		{"boutique.evt.cart.item-added.v1", &eventsv1.CartItemAddedEvent{Cart: &commonv1.CartSnapshot{
			UserId: "user-1", CartVersion: 3,
			Items: []*commonv1.CartLine{{ProductId: "product-1", Quantity: 2}},
		}}, "seed-cart", "user-1", 3},
	}
	for _, projection := range projections {
		envelope := testEnvelope(t, projection.id, projection.agg, projection.version, testTime, projection.value)
		if err := store.ApplyProjection(projection.subject, envelope); err != nil {
			t.Fatal(err)
		}
	}
}

func testEnvelope(t *testing.T, messageID, aggregateID string, version uint64, occurred time.Time, payload proto.Message) *commonv1.MessageEnvelope {
	t.Helper()
	wrapped, err := anypb.New(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &commonv1.MessageEnvelope{
		MessageId: messageID, MessageType: "test.v1", SchemaVersion: 1,
		OccurredAt: timestamppb.New(occurred), Producer: "test",
		AggregateType: "order", AggregateId: aggregateID, AggregateVersion: version,
		CorrelationId: aggregateID, Data: wrapped,
	}
}

func orderSubmitEnvelope(t *testing.T, orderID string, occurred time.Time) *commonv1.MessageEnvelope {
	t.Helper()
	command := &commandsv1.OrderSubmitCommand{
		CommandId: orderID, OperationId: orderID, OrderId: orderID, UserId: "user-1",
		ExpectedCartVersion: 3, ExpectedCatalogRevision: 7, ExpectedRateRevision: 9,
		CurrencyCode: "USD", PaymentToken: "ptok_test",
		ShippingAddress: &commonv1.PostalAddress{StreetAddress: "1 Main", City: "Test", Country: "SI"},
		Email:           "buyer@example.com",
	}
	return testEnvelope(t, "submit-"+orderID, orderID, 1, occurred, command)
}

func submitTestOrder(t *testing.T, worker *checkoutWorker, orderID string, occurred time.Time) transitionOutcome {
	t.Helper()
	outcome, err := worker.processOrderCommand(orderSubmitEnvelope(t, orderID, occurred))
	if err != nil {
		t.Fatal(err)
	}
	return outcome
}

func sagaEvent(t *testing.T, worker *checkoutWorker, subject, id, orderID string, version uint64, at time.Time, payload proto.Message) transitionOutcome {
	t.Helper()
	outcome, err := worker.processSagaEvent(subject, testEnvelope(t, id, orderID, version, at, payload))
	if err != nil {
		t.Fatal(err)
	}
	return outcome
}

func progressToCapture(t *testing.T, worker *checkoutWorker, orderID string) {
	t.Helper()
	submitTestOrder(t, worker, orderID, testTime)
	sagaEvent(t, worker, "boutique.evt.shipping.order-quote-calculated.v1", "quote-"+orderID, orderID, 1,
		testTime.Add(time.Second), &eventsv1.ShippingOrderQuoteCalculatedEvent{
			OrderId: orderID, CostUsd: &commonv1.Money{CurrencyCode: "USD", Units: 5},
		})
	sagaEvent(t, worker, "boutique.evt.payment.authorized.v1", "authorized-"+orderID, orderID, 2,
		testTime.Add(2*time.Second), &eventsv1.PaymentAuthorizedEvent{
			OrderId: orderID, AuthorizationId: "auth-1",
		})
	sagaEvent(t, worker, "boutique.evt.shipping.shipment-created.v1", "shipment-"+orderID, orderID, 3,
		testTime.Add(3*time.Second), &eventsv1.ShippingShipmentCreatedEvent{
			OrderId: orderID, ShipmentId: "ship-1", TrackingId: "track-1",
		})
}

func TestOrderTotalDoesNotOverflowFractionalNanos(t *testing.T) {
	state := newPersistedState(testTime)
	state.Rates = &eventsv1.CurrencyRatesUpdatedEvent{
		BaseCurrencyCode: "EUR",
		Rates: []*eventsv1.CurrencyRate{
			{CurrencyCode: "EUR", UnitsPerBase: 1},
			{CurrencyCode: "USD", UnitsPerBase: 1.1305},
		},
	}
	state.Products["1YMWWN1N4O"] = &commonv1.ProductSnapshot{
		ProductId: "1YMWWN1N4O", PriceUsd: &commonv1.Money{CurrencyCode: "USD", Units: 109, Nanos: 990_000_000},
	}
	state.Products["9SIQT8TOJO"] = &commonv1.ProductSnapshot{
		ProductId: "9SIQT8TOJO", PriceUsd: &commonv1.Money{CurrencyCode: "USD", Units: 5, Nanos: 490_000_000},
	}
	cart := &commonv1.CartSnapshot{Items: []*commonv1.CartLine{
		{ProductId: "1YMWWN1N4O", Quantity: 3}, {ProductId: "9SIQT8TOJO", Quantity: 3},
	}}
	snapshot, err := buildOrderSnapshot(state, cart, &commandsv1.OrderSubmitCommand{OrderId: "order", CurrencyCode: "EUR"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Total = addMoney(snapshot.Total, convertMoney(state.Rates,
		&commonv1.Money{CurrencyCode: "USD", Units: 8, Nanos: 990_000_000}, "EUR"))
	want := &commonv1.Money{CurrencyCode: "EUR", Units: 314, Nanos: 400_707_653}
	if !proto.Equal(snapshot.Total, want) {
		t.Fatalf("order total = %v, want %v", snapshot.Total, want)
	}
}

func TestSagaStepDeadlinesStartWhenEventsAreProcessed(t *testing.T) {
	store, _ := newTestStateStore(t)
	seedTestCheckoutState(t, store)
	processingTime := testTime.Add(10 * time.Minute)
	store.now = func() time.Time { return processingTime }
	worker := newTestWorker(t, store)
	orderID := "delayed-events"

	assertDeadline := func(stage string) {
		t.Helper()
		saga, err := store.LoadOrder(orderID)
		if err != nil {
			t.Fatal(err)
		}
		want := processingTime.Add(worker.stepTimeout)
		if saga == nil || saga.Stage != stage || !saga.Deadline.Equal(want) {
			t.Fatalf("saga = %#v, want stage %s and deadline %s", saga, stage, want)
		}
		due, err := store.DueDeadlines(processingTime, 16)
		if err != nil {
			t.Fatal(err)
		}
		if len(due) != 0 {
			t.Fatalf("stale input occurrence time made deadline immediately due: %#v", due)
		}
	}

	submitted := submitTestOrder(t, worker, orderID, testTime)
	assertDeadline(stageWaitingQuote)
	for _, result := range submitted.Results {
		envelope := &commonv1.MessageEnvelope{}
		if err := proto.Unmarshal(result.Data, envelope); err != nil {
			t.Fatal(err)
		}
		if got := envelope.GetOccurredAt().AsTime(); !got.Equal(testTime) {
			t.Fatalf("result occurrence time = %s, want deterministic input time %s", got, testTime)
		}
	}

	processingTime = processingTime.Add(20 * time.Second)
	sagaEvent(t, worker, "boutique.evt.shipping.order-quote-calculated.v1", "delayed-quote", orderID, 1,
		testTime.Add(time.Second), &eventsv1.ShippingOrderQuoteCalculatedEvent{
			OrderId: orderID, CostUsd: &commonv1.Money{CurrencyCode: "USD", Units: 5},
		})
	assertDeadline(stageWaitingAuthorize)

	processingTime = processingTime.Add(20 * time.Second)
	sagaEvent(t, worker, "boutique.evt.payment.authorized.v1", "delayed-authorization", orderID, 2,
		testTime.Add(2*time.Second), &eventsv1.PaymentAuthorizedEvent{
			OrderId: orderID, AuthorizationId: "auth-delayed",
		})
	assertDeadline(stageWaitingShipment)

	processingTime = processingTime.Add(20 * time.Second)
	sagaEvent(t, worker, "boutique.evt.shipping.shipment-created.v1", "delayed-shipment", orderID, 3,
		testTime.Add(3*time.Second), &eventsv1.ShippingShipmentCreatedEvent{
			OrderId: orderID, ShipmentId: "ship-delayed", TrackingId: "track-delayed",
		})
	assertDeadline(stageWaitingCapture)

	processingTime = processingTime.Add(20 * time.Second)
	sagaEvent(t, worker, "boutique.evt.payment.capture-failed.v1", "delayed-capture-failure", orderID, 4,
		testTime.Add(4*time.Second), &eventsv1.PaymentCaptureFailedEvent{
			OrderId: orderID, Failure: &commonv1.Failure{Code: "CAPTURE_FAILED"},
		})
	assertDeadline(stageCompensating)
}

func TestSagaTransitionsAcrossRandomReplicasAtScale(t *testing.T) {
	for _, replicas := range []int{1, 3, 10} {
		t.Run(fmt.Sprintf("%d-replicas", replicas), func(t *testing.T) {
			first, server := newTestStateStore(t)
			seedTestCheckoutState(t, first)
			workers := make([]*checkoutWorker, replicas)
			workers[0] = newTestWorker(t, first)
			for index := 1; index < replicas; index++ {
				workers[index] = newTestWorker(t, openSharedTestStateStore(t, server))
			}
			const orders = 24
			var wait sync.WaitGroup
			for index := 0; index < orders; index++ {
				index := index
				wait.Add(1)
				go func() {
					defer wait.Done()
					orderID := fmt.Sprintf("scale-%d-%02d", replicas, index)
					submitTestOrder(t, workers[index%replicas], orderID, testTime)
					sagaEvent(t, workers[(index+1)%replicas], "boutique.evt.shipping.order-quote-calculated.v1",
						"quote-"+orderID, orderID, 1, testTime.Add(time.Second),
						&eventsv1.ShippingOrderQuoteCalculatedEvent{OrderId: orderID, CostUsd: &commonv1.Money{CurrencyCode: "USD", Units: 5}})
					sagaEvent(t, workers[(index+2)%replicas], "boutique.evt.payment.authorized.v1",
						"auth-"+orderID, orderID, 2, testTime.Add(2*time.Second),
						&eventsv1.PaymentAuthorizedEvent{OrderId: orderID, AuthorizationId: "auth-" + orderID})
					sagaEvent(t, workers[(index+3)%replicas], "boutique.evt.shipping.shipment-created.v1",
						"shipment-"+orderID, orderID, 3, testTime.Add(3*time.Second),
						&eventsv1.ShippingShipmentCreatedEvent{OrderId: orderID, ShipmentId: "ship-" + orderID, TrackingId: "track"})
					sagaEvent(t, workers[(index+4)%replicas], "boutique.evt.payment.captured.v1",
						"capture-"+orderID, orderID, 4, testTime.Add(4*time.Second),
						&eventsv1.PaymentCapturedEvent{OrderId: orderID, TransactionId: "tx-" + orderID})
				}()
			}
			wait.Wait()
			for index := 0; index < orders; index++ {
				orderID := fmt.Sprintf("scale-%d-%02d", replicas, index)
				saga, err := first.LoadOrder(orderID)
				if err != nil || saga == nil || saga.Stage != stageCompleted {
					t.Fatalf("%s did not complete: saga=%#v err=%v", orderID, saga, err)
				}
			}
		})
	}
}

func TestFullFailureAndCompensationMatrix(t *testing.T) {
	t.Run("quote", func(t *testing.T) {
		worker := testWorker(t)
		submitTestOrder(t, worker, "quote-fail", testTime)
		sagaEvent(t, worker, "boutique.evt.shipping.order-quote-failed.v1", "quote-failed", "quote-fail", 1,
			testTime.Add(time.Second), &eventsv1.ShippingOrderQuoteFailedEvent{OrderId: "quote-fail", Failure: &commonv1.Failure{Code: "QUOTE"}})
		assertStage(t, worker, "quote-fail", stageCancelled)
	})
	t.Run("authorization", func(t *testing.T) {
		worker := testWorker(t)
		submitTestOrder(t, worker, "auth-fail", testTime)
		sagaEvent(t, worker, "boutique.evt.shipping.order-quote-calculated.v1", "quote-auth", "auth-fail", 1,
			testTime.Add(time.Second), &eventsv1.ShippingOrderQuoteCalculatedEvent{OrderId: "auth-fail", CostUsd: &commonv1.Money{CurrencyCode: "USD"}})
		sagaEvent(t, worker, "boutique.evt.payment.authorization-declined.v1", "auth-declined", "auth-fail", 2,
			testTime.Add(2*time.Second), &eventsv1.PaymentAuthorizationDeclinedEvent{OrderId: "auth-fail"})
		assertStage(t, worker, "auth-fail", stageCancelled)
	})
	t.Run("shipment-release", func(t *testing.T) {
		worker := testWorker(t)
		orderID := "shipment-fail"
		submitTestOrder(t, worker, orderID, testTime)
		sagaEvent(t, worker, "boutique.evt.shipping.order-quote-calculated.v1", "quote-shipment", orderID, 1,
			testTime.Add(time.Second), &eventsv1.ShippingOrderQuoteCalculatedEvent{OrderId: orderID, CostUsd: &commonv1.Money{CurrencyCode: "USD"}})
		sagaEvent(t, worker, "boutique.evt.payment.authorized.v1", "auth-shipment", orderID, 2,
			testTime.Add(2*time.Second), &eventsv1.PaymentAuthorizedEvent{OrderId: orderID, AuthorizationId: "auth"})
		sagaEvent(t, worker, "boutique.evt.shipping.shipment-creation-failed.v1", "shipment-failed", orderID, 3,
			testTime.Add(3*time.Second), &eventsv1.ShippingShipmentCreationFailedEvent{OrderId: orderID, Failure: &commonv1.Failure{Code: "SHIP"}})
		assertStage(t, worker, orderID, stageCompensating)
		sagaEvent(t, worker, "boutique.evt.payment.authorization-released.v1", "released", orderID, 4,
			testTime.Add(4*time.Second), &eventsv1.PaymentAuthorizationReleasedEvent{OrderId: orderID})
		assertStage(t, worker, orderID, stageCancelled)
	})
	t.Run("capture-both-compensations", func(t *testing.T) {
		worker := testWorker(t)
		orderID := "capture-fail"
		progressToCapture(t, worker, orderID)
		sagaEvent(t, worker, "boutique.evt.payment.capture-failed.v1", "capture-failed", orderID, 4,
			testTime.Add(4*time.Second), &eventsv1.PaymentCaptureFailedEvent{OrderId: orderID, Failure: &commonv1.Failure{Code: "CAPTURE"}})
		sagaEvent(t, worker, "boutique.evt.payment.authorization-released.v1", "capture-released", orderID, 5,
			testTime.Add(5*time.Second), &eventsv1.PaymentAuthorizationReleasedEvent{OrderId: orderID})
		assertStage(t, worker, orderID, stageCompensating)
		sagaEvent(t, worker, "boutique.evt.shipping.shipment-cancelled.v1", "capture-cancelled", orderID, 5,
			testTime.Add(6*time.Second), &eventsv1.ShippingShipmentCancelledEvent{OrderId: orderID})
		assertStage(t, worker, orderID, stageCancelled)
	})
	t.Run("compensation-failure", func(t *testing.T) {
		worker := testWorker(t)
		orderID := "manual-review"
		progressToCapture(t, worker, orderID)
		sagaEvent(t, worker, "boutique.evt.payment.capture-failed.v1", "review-capture", orderID, 4,
			testTime.Add(4*time.Second), &eventsv1.PaymentCaptureFailedEvent{OrderId: orderID, Failure: &commonv1.Failure{Code: "CAPTURE"}})
		sagaEvent(t, worker, "boutique.evt.payment.authorization-release-failed.v1", "review-release", orderID, 5,
			testTime.Add(5*time.Second), &eventsv1.PaymentAuthorizationReleaseFailedEvent{OrderId: orderID})
		assertStage(t, worker, orderID, stageManualReview)
	})
}

func TestCrashBoundariesReplayStoredResults(t *testing.T) {
	worker := testWorker(t)
	input := orderSubmitEnvelope(t, "crash-order", testTime)
	committed, err := worker.processOrderCommand(input)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Duplicate || len(committed.Results) != 3 {
		t.Fatalf("initial commit = %#v", committed)
	}

	ambiguous := []resultMessage{}
	worker.publishHook = func(result resultMessage) error {
		ambiguous = append(ambiguous, result)
		return errors.New("injected publish acknowledgement loss")
	}
	if err := worker.finishTransition(committed); err == nil {
		t.Fatal("ambiguous publication did not fail")
	}

	replayed, err := worker.processOrderCommand(input)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Duplicate {
		t.Fatal("redelivery did not load the stored journal")
	}
	published := []resultMessage{}
	worker.publishHook = func(result resultMessage) error {
		published = append(published, result)
		return nil
	}
	if err := worker.finishTransition(replayed); err != nil {
		t.Fatal(err)
	}
	if len(published) != len(committed.Results) {
		t.Fatalf("replayed %d results, want %d", len(published), len(committed.Results))
	}
	for index := range published {
		if published[index].MessageID != committed.Results[index].MessageID ||
			!bytes.Equal(published[index].Data, committed.Results[index].Data) {
			t.Fatalf("result %d changed at crash boundary", index)
		}
	}
	saga, err := worker.store.LoadOrder("crash-order")
	if err != nil || saga.Version != 1 {
		t.Fatalf("republication repeated business transition: saga=%#v err=%v", saga, err)
	}
}

func TestOrderWaitsForRequiredProjection(t *testing.T) {
	worker := testWorker(t)
	input := orderSubmitEnvelope(t, "projection-lag", testTime)
	command := &commandsv1.OrderSubmitCommand{}
	if err := input.Data.UnmarshalTo(command); err != nil {
		t.Fatal(err)
	}
	command.ExpectedCartVersion = 4
	input.Data, _ = anypb.New(command)
	_, err := worker.processOrderCommand(input)
	if !errors.Is(err, errCheckoutProjectionLag) {
		t.Fatalf("error = %v, want projection lag", err)
	}
	saga, loadErr := worker.store.LoadOrder("projection-lag")
	if loadErr != nil || saga != nil {
		t.Fatalf("lagging projection persisted saga: saga=%#v err=%v", saga, loadErr)
	}
}

func assertStage(t *testing.T, worker *checkoutWorker, orderID, want string) {
	t.Helper()
	saga, err := worker.store.LoadOrder(orderID)
	if err != nil {
		t.Fatal(err)
	}
	if saga == nil || saga.Stage != want {
		t.Fatalf("%s stage = %#v, want %s", orderID, saga, want)
	}
}
