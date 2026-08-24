// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"testing"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestCheckoutConsumerTerminalErrorsRestartWorker(t *testing.T) {
	for _, err := range []error{nats.ErrBadSubscription, nats.ErrSubscriptionClosed, nats.ErrConsumerDeleted, nats.ErrNoResponders} {
		if !checkoutConsumerTerminal(err) {
			t.Fatalf("%v was not classified as terminal", err)
		}
	}
	if checkoutConsumerTerminal(nats.ErrTimeout) {
		t.Fatal("fetch timeout was classified as terminal")
	}
}

func TestCheckoutWorkflowConsumerFilters(t *testing.T) {
	if !checkoutConsumerFiltersMatch("", checkoutShippingSagaSubjects, checkoutShippingSagaSubjects) {
		t.Fatal("shipping saga filters do not match themselves")
	}
	if checkoutConsumerFiltersMatch("boutique.evt.shipping.>", nil, checkoutShippingSagaSubjects) {
		t.Fatal("legacy broad shipping filter unexpectedly matches optimized filters")
	}
	if !checkoutConsumerFiltersMatch(
		checkoutPaymentSagaSubjects[0],
		nil,
		[]string{checkoutPaymentSagaSubjects[0]},
	) {
		t.Fatal("single-subject consumer filter did not match")
	}
	for _, subject := range append(
		append([]string(nil), checkoutShippingSagaSubjects...),
		checkoutPaymentSagaSubjects...,
	) {
		if !isCheckoutSagaEvent(subject) {
			t.Errorf("configured workflow subject %q is not handled", subject)
		}
	}
	if isCheckoutSagaEvent("boutique.evt.shipping.cart-quote-updated.v1") {
		t.Fatal("cart quote event unexpectedly enters the order saga")
	}
}

func TestCheckoutMessageGroupUsesAggregateIdentity(t *testing.T) {
	envelope := &commonv1.MessageEnvelope{
		MessageId:     "message-1",
		CorrelationId: "correlation-1",
		AggregateType: "order",
		AggregateId:   "order-1",
	}
	if got, want := checkoutMessageGroup(envelope), "order\x00order-1"; got != want {
		t.Fatalf("message group = %q, want %q", got, want)
	}
}

func TestCheckoutProcessesStreamBeforeFetchBatchCloses(t *testing.T) {
	messages := make(chan *nats.Msg)
	processed := make(chan struct{})
	finished := make(chan struct{})
	worker := &checkoutWorker{}

	go func() {
		worker.processStream(
			messages,
			func(*nats.Msg, *commonv1.MessageEnvelope) error {
				close(processed)
				return nil
			},
			256,
			1,
		)
		close(finished)
	}()

	encoded, err := proto.Marshal(testEnvelope(t, "message-1", "order-1", 1, testTime, &emptypb.Empty{}))
	if err != nil {
		t.Fatal(err)
	}
	messages <- &nats.Msg{Data: encoded}
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

func TestCheckoutProcessesIndependentProjectionsConcurrentlyInAggregateOrder(t *testing.T) {
	messages := make(chan *nats.Msg)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	sameAggregateProcessed := make(chan struct{})
	secondProcessed := make(chan struct{})
	finished := make(chan struct{})
	worker := &checkoutWorker{}

	go func() {
		worker.processStream(
			messages,
			func(_ *nats.Msg, envelope *commonv1.MessageEnvelope) error {
				switch envelope.MessageId {
				case "message-1":
					close(firstStarted)
					<-releaseFirst
				case "message-1-next":
					close(sameAggregateProcessed)
				case "message-2":
					close(secondProcessed)
				}
				return nil
			},
			checkoutProjectionFetchBatchSize,
			checkoutProjectionParallelism,
		)
		close(finished)
	}()

	encode := func(messageID, orderID string) []byte {
		t.Helper()
		encoded, err := proto.Marshal(testEnvelope(t, messageID, orderID, 1, testTime, &emptypb.Empty{}))
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	messages <- &nats.Msg{Data: encode("message-1", "order-1")}
	<-firstStarted
	messages <- &nats.Msg{Data: encode("message-1-next", "order-1")}
	messages <- &nats.Msg{Data: encode("message-2", "order-2")}
	select {
	case <-secondProcessed:
	case <-time.After(time.Second):
		t.Fatal("independent projection waited for the blocked aggregate")
	}
	select {
	case <-sameAggregateProcessed:
		t.Fatal("later projection overtook a blocked projection for the same aggregate")
	default:
	}
	close(releaseFirst)
	select {
	case <-sameAggregateProcessed:
	case <-time.After(time.Second):
		t.Fatal("same-aggregate projection remained blocked after its predecessor finished")
	}
	close(messages)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("parallel stream processor did not stop")
	}
}
