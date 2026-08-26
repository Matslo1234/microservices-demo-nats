// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type testPubAckFuture struct {
	message *nats.Msg
	ok      chan *nats.PubAck
	err     chan error
}

func newTestPubAckFuture() *testPubAckFuture {
	return &testPubAckFuture{
		message: &nats.Msg{},
		ok:      make(chan *nats.PubAck, 1),
		err:     make(chan error, 1),
	}
}

func (future *testPubAckFuture) Ok() <-chan *nats.PubAck { return future.ok }

func (future *testPubAckFuture) Err() <-chan error { return future.err }

func (future *testPubAckFuture) Msg() *nats.Msg { return future.message }

type checkoutJetStreamStub struct {
	nats.JetStreamContext
	consumers map[string]*nats.ConsumerInfo
	added     *nats.ConsumerConfig
}

func (stub *checkoutJetStreamStub) ConsumerInfo(_ string, consumer string, _ ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	info := stub.consumers[consumer]
	if info == nil {
		return nil, nats.ErrConsumerNotFound
	}
	return info, nil
}

func (stub *checkoutJetStreamStub) AddConsumer(_ string, config *nats.ConsumerConfig, _ ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	copy := *config
	copy.FilterSubjects = append([]string(nil), config.FilterSubjects...)
	stub.added = &copy
	return &nats.ConsumerInfo{Config: copy}, nil
}

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

func TestCheckoutDeadlineScanInterval(t *testing.T) {
	if got, want := checkoutDeadlineScanInterval, 5*time.Second; got != want {
		t.Fatalf("deadline scan interval = %s, want %s", got, want)
	}
}

func TestCheckoutWorkflowConsumerFilters(t *testing.T) {
	if !checkoutConsumerFiltersMatch("", checkoutShippingQuoteSagaSubjects, checkoutShippingQuoteSagaSubjects) {
		t.Fatal("shipping quote saga filters do not match themselves")
	}
	if !checkoutConsumerFiltersMatch("", checkoutShipmentSagaSubjects, checkoutShipmentSagaSubjects) {
		t.Fatal("shipment saga filters do not match themselves")
	}
	if checkoutConsumerFiltersMatch("boutique.evt.shipping.>", nil, checkoutShippingQuoteSagaSubjects) {
		t.Fatal("legacy broad shipping filter unexpectedly matches quote filters")
	}
	if checkoutConsumerFiltersMatch("boutique.evt.shipping.>", nil, checkoutShipmentSagaSubjects) {
		t.Fatal("legacy broad shipping filter unexpectedly matches shipment filters")
	}
	if !checkoutConsumerFiltersMatch(
		checkoutPaymentAuthorizationSagaSubjects[0],
		nil,
		[]string{checkoutPaymentAuthorizationSagaSubjects[0]},
	) {
		t.Fatal("single-subject consumer filter did not match")
	}
	for _, quoteSubject := range checkoutShippingQuoteSagaSubjects {
		if isCheckoutShipmentEvent(quoteSubject) {
			t.Errorf("quote subject %q also enters the shipment handler", quoteSubject)
		}
	}
	for _, shipmentSubject := range checkoutShipmentSagaSubjects {
		if isCheckoutShippingQuoteEvent(shipmentSubject) {
			t.Errorf("shipment subject %q also enters the quote handler", shipmentSubject)
		}
	}
	for _, authorizationSubject := range checkoutPaymentAuthorizationSagaSubjects {
		if isCheckoutPaymentLateStageEvent(authorizationSubject) {
			t.Errorf("authorization subject %q also enters the late-stage payment handler", authorizationSubject)
		}
	}
	for _, lateStageSubject := range checkoutPaymentLateStageSagaSubjects {
		if isCheckoutPaymentAuthorizationEvent(lateStageSubject) {
			t.Errorf("late-stage payment subject %q also enters the authorization handler", lateStageSubject)
		}
	}
	for _, subject := range append(
		append(
			append(append([]string(nil), checkoutShippingQuoteSagaSubjects...), checkoutShipmentSagaSubjects...),
			checkoutPaymentAuthorizationSagaSubjects...,
		),
		checkoutPaymentLateStageSagaSubjects...,
	) {
		if !isCheckoutSagaEvent(subject) {
			t.Errorf("configured workflow subject %q is not handled", subject)
		}
	}
	if isCheckoutSagaEvent("boutique.evt.shipping.cart-quote-updated.v1") {
		t.Fatal("cart quote event unexpectedly enters the order saga")
	}
}

func TestCheckoutSplitConsumerStartsAfterLegacyAcknowledgementFloor(t *testing.T) {
	jetStream := &checkoutJetStreamStub{consumers: map[string]*nats.ConsumerInfo{
		"checkout-saga-shipping-v1": {AckFloor: nats.SequenceInfo{Stream: 4321}},
	}}
	worker := &checkoutWorker{js: jetStream}
	definition := checkoutConsumerDefinition{
		filters: checkoutShippingQuoteSagaSubjects, durable: "checkout-saga-shipping-quote-v1",
		stream: "BOUTIQUE_EVENTS", maxPending: checkoutWorkflowMaxPending,
		migrationSource: "checkout-saga-shipping-v1",
	}

	if err := worker.ensureConsumer(definition); err != nil {
		t.Fatal(err)
	}
	if jetStream.added == nil {
		t.Fatal("split consumer was not created")
	}
	if got, want := jetStream.added.DeliverPolicy, nats.DeliverByStartSequencePolicy; got != want {
		t.Fatalf("delivery policy = %v, want %v", got, want)
	}
	if got, want := jetStream.added.OptStartSeq, uint64(4322); got != want {
		t.Fatalf("start sequence = %d, want %d", got, want)
	}
}

func TestCheckoutSplitConsumerReplaysWhenLegacyConsumerIsAbsent(t *testing.T) {
	jetStream := &checkoutJetStreamStub{consumers: map[string]*nats.ConsumerInfo{}}
	worker := &checkoutWorker{js: jetStream}
	definition := checkoutConsumerDefinition{
		filters: checkoutShippingQuoteSagaSubjects, durable: "checkout-saga-shipping-quote-v1",
		stream: "BOUTIQUE_EVENTS", maxPending: checkoutWorkflowMaxPending,
		migrationSource: "checkout-saga-shipping-v1",
	}

	if err := worker.ensureConsumer(definition); err != nil {
		t.Fatal(err)
	}
	if jetStream.added == nil {
		t.Fatal("split consumer was not created")
	}
	if got, want := jetStream.added.DeliverPolicy, nats.DeliverAllPolicy; got != want {
		t.Fatalf("delivery policy = %v, want %v", got, want)
	}
}

func TestCheckoutPaymentAuthorizationConsumerStartsAfterLegacyAcknowledgementFloor(t *testing.T) {
	jetStream := &checkoutJetStreamStub{consumers: map[string]*nats.ConsumerInfo{
		"checkout-saga-payment-v1": {AckFloor: nats.SequenceInfo{Stream: 9876}},
	}}
	worker := &checkoutWorker{js: jetStream}
	definition := checkoutConsumerDefinition{
		filters: checkoutPaymentAuthorizationSagaSubjects, durable: "checkout-saga-payment-authorization-v1",
		stream: "BOUTIQUE_EVENTS", maxPending: checkoutWorkflowMaxPending,
		migrationSource: "checkout-saga-payment-v1",
	}

	if err := worker.ensureConsumer(definition); err != nil {
		t.Fatal(err)
	}
	if jetStream.added == nil {
		t.Fatal("payment authorization consumer was not created")
	}
	if got, want := jetStream.added.DeliverPolicy, nats.DeliverByStartSequencePolicy; got != want {
		t.Fatalf("delivery policy = %v, want %v", got, want)
	}
	if got, want := jetStream.added.OptStartSeq, uint64(9877); got != want {
		t.Fatalf("start sequence = %d, want %d", got, want)
	}
	if !checkoutConsumerFiltersMatch("", jetStream.added.FilterSubjects, checkoutPaymentAuthorizationSagaSubjects) {
		t.Fatalf("authorization filters = %v, want %v", jetStream.added.FilterSubjects, checkoutPaymentAuthorizationSagaSubjects)
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

func TestCheckoutPipelinesResultPublishesBeforeWaitingForAcknowledgements(t *testing.T) {
	worker := &checkoutWorker{publishTimeout: time.Second}
	results := []resultMessage{
		{MessageID: "result-1", Subject: "boutique.test.1"},
		{MessageID: "result-2", Subject: "boutique.test.2"},
		{MessageID: "result-3", Subject: "boutique.test.3"},
	}
	futures := make([]*testPubAckFuture, 0, len(results))
	queued := make(chan struct{}, len(results))
	worker.publishAsyncHook = func(result resultMessage) (nats.PubAckFuture, error) {
		future := newTestPubAckFuture()
		future.message.Subject = result.Subject
		futures = append(futures, future)
		queued <- struct{}{}
		return future, nil
	}

	finished := make(chan error, 1)
	go func() { finished <- worker.publishResults(results) }()
	for range results {
		select {
		case <-queued:
		case <-time.After(time.Second):
			t.Fatal("checkout waited for an acknowledgement before queuing every result")
		}
	}
	for _, future := range futures {
		future.ok <- &nats.PubAck{Stream: "BOUTIQUE_EVENTS"}
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("checkout did not finish after all publish acknowledgements arrived")
	}
	if attempts := worker.metrics.resultPublishAttempts.Load(); attempts != 3 {
		t.Fatalf("publish attempts = %d, want 3", attempts)
	}
	if successes := worker.metrics.resultPublishSuccesses.Load(); successes != 3 {
		t.Fatalf("publish successes = %d, want 3", successes)
	}
	if failures := worker.metrics.resultPublishFailures.Load(); failures != 0 {
		t.Fatalf("publish failures = %d, want 0", failures)
	}
}

func TestCheckoutSettlesPipelinedPublishesAndReturnsFirstError(t *testing.T) {
	worker := &checkoutWorker{publishTimeout: time.Second}
	results := []resultMessage{
		{MessageID: "result-1", Subject: "boutique.test.1"},
		{MessageID: "result-2", Subject: "boutique.test.2"},
		{MessageID: "result-3", Subject: "boutique.test.3"},
	}
	futures := make([]*testPubAckFuture, 0, len(results))
	queued := make(chan struct{}, len(results))
	worker.publishAsyncHook = func(result resultMessage) (nats.PubAckFuture, error) {
		future := newTestPubAckFuture()
		futures = append(futures, future)
		queued <- struct{}{}
		return future, nil
	}

	finished := make(chan error, 1)
	go func() { finished <- worker.publishResults(results) }()
	for range results {
		select {
		case <-queued:
		case <-time.After(time.Second):
			t.Fatal("checkout did not queue every result")
		}
	}
	futures[0].ok <- &nats.PubAck{Stream: "BOUTIQUE_EVENTS"}
	futures[1].err <- errors.New("injected acknowledgement loss")
	futures[2].ok <- &nats.PubAck{Stream: "BOUTIQUE_EVENTS"}

	var err error
	select {
	case err = <-finished:
	case <-time.After(time.Second):
		t.Fatal("checkout did not settle every pipelined publish")
	}
	if err == nil || !strings.Contains(err.Error(), "result-2") {
		t.Fatalf("publish error = %v, want result-2 acknowledgement failure", err)
	}
	if successes := worker.metrics.resultPublishSuccesses.Load(); successes != 2 {
		t.Fatalf("publish successes = %d, want 2", successes)
	}
	if failures := worker.metrics.resultPublishFailures.Load(); failures != 1 {
		t.Fatalf("publish failures = %d, want 1", failures)
	}
}
