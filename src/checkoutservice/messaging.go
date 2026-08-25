// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	stateless "github.com/GoogleCloudPlatform/microservices-demo/src/shared/stateless/go"
	telemetry "github.com/GoogleCloudPlatform/microservices-demo/src/shared/telemetry/go"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	checkoutProjectionFetchBatchSize = 32
	checkoutWorkflowFetchBatchSize   = 256
	checkoutProjectionMaxPending     = 256
	checkoutWorkflowMaxPending       = 1024
	checkoutProjectionParallelism    = 16
	checkoutWorkflowParallelism      = 32
	checkoutAsyncPublishMaxPending   = 2048
)

var checkoutShippingSagaSubjects = []string{
	// Cart quote events share the shipping namespace but never advance an
	// order. Keeping them out of this durable prevents cart activity from
	// head-of-line blocking checkout sagas.
	"boutique.evt.shipping.order-quote-calculated.v1",
	"boutique.evt.shipping.order-quote-failed.v1",
	"boutique.evt.shipping.shipment-created.v1",
	"boutique.evt.shipping.shipment-creation-failed.v1",
	"boutique.evt.shipping.shipment-cancelled.v1",
	"boutique.evt.shipping.shipment-cancellation-failed.v1",
}

var checkoutPaymentSagaSubjects = []string{
	"boutique.evt.payment.authorized.v1",
	"boutique.evt.payment.authorization-declined.v1",
	"boutique.evt.payment.captured.v1",
	"boutique.evt.payment.capture-failed.v1",
	"boutique.evt.payment.authorization-released.v1",
	"boutique.evt.payment.authorization-release-failed.v1",
}

type checkoutConsumerDefinition struct {
	filters     []string
	durable     string
	stream      string
	handler     checkoutMessageHandler
	fetchSize   int
	maxPending  int
	parallelism int
}

type checkoutMessageHandler func(*nats.Msg, *commonv1.MessageEnvelope) error

type checkoutStreamMessage struct {
	message  *nats.Msg
	envelope *commonv1.MessageEnvelope
	err      error
}

type checkoutMetrics struct {
	transitions             atomic.Uint64
	duplicates              atomic.Uint64
	projectionUpdates       atomic.Uint64
	resultPublishAttempts   atomic.Uint64
	resultPublishSuccesses  atomic.Uint64
	resultPublishFailures   atomic.Uint64
	deadlineClaims          atomic.Uint64
	deadlineLeaseRecoveries atomic.Uint64
	deadlineAgeMillis       atomic.Int64
}

type checkoutWorker struct {
	store            *stateStore
	nc               *nats.Conn
	js               nats.JetStreamContext
	subscriptions    []*nats.Subscription
	publishTimeout   time.Duration
	stepTimeout      time.Duration
	leaseDuration    time.Duration
	leaseStore       *stateless.RedisLeaseStore
	workerID         string
	publishHook      func(resultMessage) error
	publishAsyncHook func(resultMessage) (nats.PubAckFuture, error)
	metrics          checkoutMetrics
	stop             chan struct{}
	failed           chan error
	wg               sync.WaitGroup
	ready            atomic.Bool
	lifecycleFailed  atomic.Bool
	failureOnce      sync.Once
	closeOnce        sync.Once
}

func startCheckoutWorker(store *stateStore) (*checkoutWorker, error) {
	url, user, password, caFile := os.Getenv("NATS_URL"), os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD"), os.Getenv("NATS_CA_FILE")
	if url == "" || user == "" || password == "" || caFile == "" || os.Getenv("REGION_ID") == "" || os.Getenv("K8S_CLUSTER_NAME") == "" {
		return nil, errors.New("NATS_URL, NATS_USER, NATS_PASSWORD, NATS_CA_FILE, REGION_ID, and K8S_CLUSTER_NAME are required")
	}
	connectTimeout, err := durationEnv("NATS_CONNECT_TIMEOUT", 2*time.Second)
	if err != nil {
		return nil, err
	}
	reconnectWait, err := durationEnv("NATS_RECONNECT_WAIT", 2*time.Second)
	if err != nil {
		return nil, err
	}
	publishTimeout, err := durationEnv("NATS_PUBLISH_TIMEOUT", 5*time.Second)
	if err != nil {
		return nil, err
	}
	stepTimeout, err := durationEnv("CHECKOUT_SAGA_STEP_TIMEOUT", 30*time.Second)
	if err != nil {
		return nil, err
	}
	leaseDuration, err := durationEnv("CHECKOUT_DEADLINE_LEASE", 15*time.Second)
	if err != nil {
		return nil, err
	}
	projectionCatchupTimeout, err := durationEnv("CHECKOUT_PROJECTION_CATCHUP_TIMEOUT", 10*time.Minute)
	if err != nil {
		return nil, err
	}
	pingInterval, err := durationEnv("NATS_PING_INTERVAL", 20*time.Second)
	if err != nil {
		return nil, err
	}
	maxReconnects, err := integerEnv("NATS_MAX_RECONNECTS", -1)
	if err != nil {
		return nil, err
	}
	maxPings, err := integerEnv("NATS_MAX_PINGS_OUT", 2)
	if err != nil {
		return nil, err
	}
	workerID := strings.TrimSpace(os.Getenv("POD_NAME"))
	if workerID == "" {
		hostname, _ := os.Hostname()
		workerID = hostname + "/" + strings.TrimPrefix(nats.NewInbox(), "_INBOX.")
	}
	leaseStore, err := stateless.NewRedisLeaseStore(store.client, store.prefix+":deadline-lease", store.retention)
	if err != nil {
		return nil, err
	}
	worker := &checkoutWorker{
		store: store, publishTimeout: publishTimeout, stepTimeout: stepTimeout,
		leaseDuration: leaseDuration, leaseStore: leaseStore, workerID: workerID,
		stop: make(chan struct{}), failed: make(chan error, 1),
	}
	connectionName := fmt.Sprintf("checkoutservice/stateless-phase5/%s/%s", os.Getenv("REGION_ID"), os.Getenv("K8S_CLUSTER_NAME"))
	nc, err := nats.Connect(url, nats.Name(connectionName),
		nats.UserInfo(user, password), nats.RootCAs(caFile),
		nats.Timeout(connectTimeout), nats.ReconnectWait(reconnectWait), nats.MaxReconnects(maxReconnects),
		nats.PingInterval(pingInterval), nats.MaxPingsOutstanding(maxPings),
		nats.DisconnectErrHandler(func(_ *nats.Conn, disconnectErr error) {
			worker.ready.Store(false)
			log.WithError(disconnectErr).Warn("NATS disconnected")
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			if !worker.lifecycleFailed.Load() {
				worker.ready.Store(true)
			}
		}))
	if err != nil {
		return nil, fmt.Errorf("connect checkoutservice to NATS: %w", err)
	}
	worker.nc = nc
	worker.js, err = nc.JetStream(
		nats.PublishAsyncMaxPending(checkoutAsyncPublishMaxPending),
		nats.PublishAsyncTimeout(publishTimeout),
	)
	if err != nil {
		nc.Close()
		return nil, err
	}

	projections := []checkoutConsumerDefinition{
		{[]string{"boutique.evt.catalog.>"}, "checkout-catalog-v1", "BOUTIQUE_EVENTS",
			worker.handleProjectionMessage, checkoutProjectionFetchBatchSize, checkoutProjectionMaxPending, checkoutProjectionParallelism},
		{[]string{"boutique.evt.currency.>"}, "checkout-currency-v1", "BOUTIQUE_EVENTS",
			worker.handleProjectionMessage, checkoutProjectionFetchBatchSize, checkoutProjectionMaxPending, checkoutProjectionParallelism},
		{[]string{"boutique.evt.cart.>"}, "checkout-cart-v1", "BOUTIQUE_EVENTS",
			worker.handleProjectionMessage, checkoutProjectionFetchBatchSize, checkoutProjectionMaxPending, checkoutProjectionParallelism},
	}
	workflows := []checkoutConsumerDefinition{
		{[]string{"boutique.cmd.order.submit.v1"}, "checkout-order-commands-v1", "BOUTIQUE_COMMANDS",
			worker.handleCommandMessage, checkoutWorkflowFetchBatchSize, checkoutWorkflowMaxPending, checkoutWorkflowParallelism},
		{checkoutShippingSagaSubjects, "checkout-saga-shipping-v1", "BOUTIQUE_EVENTS",
			worker.handleEventMessage, checkoutWorkflowFetchBatchSize, checkoutWorkflowMaxPending, checkoutWorkflowParallelism},
		{checkoutPaymentSagaSubjects, "checkout-saga-payment-v1", "BOUTIQUE_EVENTS",
			worker.handleEventMessage, checkoutWorkflowFetchBatchSize, checkoutWorkflowMaxPending, checkoutWorkflowParallelism},
	}
	add := func(definition checkoutConsumerDefinition) error {
		if ensureErr := worker.ensureConsumer(definition); ensureErr != nil {
			return ensureErr
		}
		subject := ""
		if len(definition.filters) == 1 {
			subject = definition.filters[0]
		}
		subscription, subscribeErr := worker.js.PullSubscribe(
			subject,
			definition.durable,
			nats.Bind(definition.stream, definition.durable),
		)
		if subscribeErr != nil {
			return fmt.Errorf("bind %s: %w", definition.durable, subscribeErr)
		}
		worker.subscriptions = append(worker.subscriptions, subscription)
		worker.wg.Add(1)
		go func() {
			defer worker.wg.Done()
			if consumeErr := worker.consume(
				subscription,
				definition.handler,
				definition.fetchSize,
				definition.parallelism,
			); consumeErr != nil {
				worker.reportFailure(fmt.Errorf("consumer %s: %w", definition.durable, consumeErr))
			}
		}()
		return nil
	}
	for _, definition := range projections {
		if err := add(definition); err != nil {
			_ = worker.Close()
			return nil, err
		}
	}
	catchupDeadline := time.Now().Add(projectionCatchupTimeout)
	for _, subscription := range worker.subscriptions {
		for {
			info, infoErr := subscription.ConsumerInfo()
			if infoErr != nil {
				_ = worker.Close()
				return nil, fmt.Errorf("inspect checkout projection consumer: %w", infoErr)
			}
			if info.NumPending == 0 && info.NumAckPending == 0 {
				break
			}
			if time.Now().After(catchupDeadline) {
				_ = worker.Close()
				return nil, errors.New("checkout projections did not catch up before startup deadline")
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	for _, definition := range workflows {
		if err := add(definition); err != nil {
			_ = worker.Close()
			return nil, err
		}
	}
	worker.ready.Store(true)
	worker.wg.Add(1)
	go func() {
		defer worker.wg.Done()
		worker.scanDeadlines()
	}()
	return worker, nil
}

func (worker *checkoutWorker) ensureConsumer(definition checkoutConsumerDefinition) error {
	config := &nats.ConsumerConfig{
		Durable:       definition.durable,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    10,
		MaxAckPending: definition.maxPending,
	}
	if len(definition.filters) == 1 {
		config.FilterSubject = definition.filters[0]
	} else {
		config.FilterSubjects = append([]string(nil), definition.filters...)
	}
	for attempt := 0; attempt < 20; attempt++ {
		info, err := worker.js.ConsumerInfo(definition.stream, definition.durable)
		if errors.Is(err, nats.ErrConsumerNotFound) {
			if _, addErr := worker.js.AddConsumer(definition.stream, config); addErr == nil {
				return nil
			} else if checkoutConsumerSetupRace(addErr) {
				time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
				continue
			} else {
				return fmt.Errorf("create %s: %w", definition.durable, addErr)
			}
		}
		if err != nil {
			return fmt.Errorf("inspect %s: %w", definition.durable, err)
		}
		if checkoutConsumerFiltersMatch(
			info.Config.FilterSubject,
			info.Config.FilterSubjects,
			definition.filters,
		) &&
			info.Config.MaxAckPending == definition.maxPending &&
			info.Config.AckPolicy == nats.AckExplicitPolicy &&
			info.Config.AckWait == 30*time.Second &&
			info.Config.MaxDeliver == 10 &&
			info.Config.DeliverPolicy == nats.DeliverAllPolicy {
			return nil
		}
		next := info.Config
		next.FilterSubject = ""
		next.FilterSubjects = nil
		if len(definition.filters) == 1 {
			next.FilterSubject = definition.filters[0]
		} else {
			next.FilterSubjects = append([]string(nil), definition.filters...)
		}
		next.MaxAckPending = definition.maxPending
		next.AckPolicy = nats.AckExplicitPolicy
		next.AckWait = 30 * time.Second
		next.MaxDeliver = 10
		if _, updateErr := worker.js.UpdateConsumer(definition.stream, &next); updateErr == nil {
			return nil
		} else if checkoutConsumerSetupRace(updateErr) {
			time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
			continue
		} else {
			return fmt.Errorf("update %s: %w", definition.durable, updateErr)
		}
	}
	return fmt.Errorf("consumer %s setup conflicted too many times", definition.durable)
}

func checkoutConsumerSetupRace(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return errors.Is(err, nats.ErrConsumerNotFound) ||
		strings.Contains(message, "consumer already exists") ||
		strings.Contains(message, "consumer name already in use") ||
		strings.Contains(message, "stream sequence")
}

func checkoutConsumerFiltersMatch(single string, multiple, wanted []string) bool {
	configured := append([]string(nil), multiple...)
	if single != "" {
		configured = append(configured, single)
	}
	if len(configured) != len(wanted) {
		return false
	}
	expected := make(map[string]struct{}, len(wanted))
	for _, subject := range wanted {
		expected[subject] = struct{}{}
	}
	for _, subject := range configured {
		if _, ok := expected[subject]; !ok {
			return false
		}
	}
	return true
}

func (worker *checkoutWorker) consume(
	subscription *nats.Subscription,
	handler checkoutMessageHandler,
	fetchSize int,
	parallelism int,
) error {
	for {
		select {
		case <-worker.stop:
			return nil
		default:
		}
		batch, err := subscription.FetchBatch(fetchSize, nats.MaxWait(time.Second))
		if err != nil {
			if checkoutConsumerTerminal(err) {
				return err
			}
			continue
		}
		worker.processStream(batch.Messages(), handler, fetchSize, parallelism)
		if err := batch.Error(); err != nil {
			if checkoutConsumerTerminal(err) {
				return err
			}
		}
	}
}

func checkoutConsumerTerminal(err error) bool {
	return err != nil && !errors.Is(err, nats.ErrTimeout)
}

func (worker *checkoutWorker) reportFailure(err error) {
	worker.failureOnce.Do(func() {
		worker.lifecycleFailed.Store(true)
		worker.ready.Store(false)
		worker.failed <- err
	})
}

func (worker *checkoutWorker) processStream(
	messages <-chan *nats.Msg,
	handler checkoutMessageHandler,
	fetchSize int,
	parallelism int,
) {
	if parallelism <= 1 {
		for message := range messages {
			worker.processMessage(decodeCheckoutStreamMessage(message), handler)
		}
		return
	}

	// Replicated Redis and JetStream commits spend most of their time waiting
	// for I/O. Dispatch each message as soon as FetchBatch yields it while
	// retaining stream order for every individual aggregate.
	lanes := make([]chan checkoutStreamMessage, parallelism)
	var running sync.WaitGroup
	for index := range lanes {
		lane := make(chan checkoutStreamMessage, fetchSize)
		lanes[index] = lane
		running.Add(1)
		go func(messages <-chan checkoutStreamMessage) {
			defer running.Done()
			for message := range messages {
				worker.processMessage(message, handler)
			}
		}(lane)
	}
	for message := range messages {
		decoded := decodeCheckoutStreamMessage(message)
		lane := checkoutMessageLane(decoded.envelope, len(lanes))
		lanes[lane] <- decoded
	}
	for _, lane := range lanes {
		close(lane)
	}
	running.Wait()
}

func decodeCheckoutStreamMessage(message *nats.Msg) checkoutStreamMessage {
	envelope, err := decodeEnvelope(message.Data)
	return checkoutStreamMessage{message: message, envelope: envelope, err: err}
}

func checkoutMessageLane(envelope *commonv1.MessageEnvelope, lanes int) int {
	group := checkoutMessageGroup(envelope)
	hash := uint32(2166136261)
	for index := 0; index < len(group); index++ {
		hash ^= uint32(group[index])
		hash *= 16777619
	}
	return int(hash % uint32(lanes))
}

func (worker *checkoutWorker) processMessage(message checkoutStreamMessage, handler checkoutMessageHandler) {
	correlationID, messageID, traceparent, tracestate := "unknown", "unknown", "", ""
	if message.envelope != nil {
		correlationID = message.envelope.CorrelationId
		messageID = message.envelope.MessageId
		traceparent = message.envelope.Traceparent
		tracestate = message.envelope.Tracestate
	}
	ctx, span := telemetry.StartConsumerSpan(context.Background(), message.message.Subject,
		checkoutMessageKind(message.message.Subject), messageID, correlationID, traceparent, tracestate)
	defer span.End()
	if message.envelope != nil {
		// Result envelopes copy these fields, so downstream consumers become
		// children of this processing span instead of siblings of it.
		telemetry.Inject(ctx, &message.envelope.Traceparent, &message.envelope.Tracestate)
	}
	err := message.err
	if err == nil {
		err = handler(message.message, message.envelope)
	}
	if err != nil {
		telemetry.RecordError(span, err)
		entry := checkoutMessageLog(message.message, message.envelope)
		if errors.Is(err, errCheckoutProjectionLag) {
			entry.WithError(err).Debug("checkout command is waiting for its projections")
		} else {
			entry.WithError(err).Error("checkout message processing failed")
		}
		_ = message.message.NakWithDelay(time.Second)
		return
	}
	if err := message.message.Ack(); err != nil {
		checkoutMessageLog(message.message, message.envelope).
			WithError(err).Error("checkout message acknowledgement failed")
	}
}

func checkoutMessageLog(message *nats.Msg, envelope *commonv1.MessageEnvelope) *logrus.Entry {
	correlationID, messageID := "unknown", "unknown"
	if envelope != nil {
		if envelope.CorrelationId != "" {
			correlationID = envelope.CorrelationId
		}
		if envelope.MessageId != "" {
			messageID = envelope.MessageId
		}
	}
	return log.WithFields(logrus.Fields{
		"topic": message.Subject, "message_kind": checkoutMessageKind(message.Subject),
		"message_id": messageID, "correlation_id": correlationID,
	})
}

func (worker *checkoutWorker) handleProjectionMessage(message *nats.Msg, envelope *commonv1.MessageEnvelope) error {
	if err := worker.store.ApplyProjection(message.Subject, envelope); err != nil {
		return err
	}
	worker.metrics.projectionUpdates.Add(1)
	return nil
}

func (worker *checkoutWorker) handleCommandMessage(_ *nats.Msg, envelope *commonv1.MessageEnvelope) error {
	outcome, err := worker.processOrderCommand(envelope)
	if err != nil {
		return err
	}
	return worker.finishTransition(outcome)
}

func (worker *checkoutWorker) handleEventMessage(message *nats.Msg, envelope *commonv1.MessageEnvelope) error {
	if !isCheckoutSagaEvent(message.Subject) {
		return nil
	}
	outcome, err := worker.processSagaEvent(message.Subject, envelope)
	if err != nil {
		return err
	}
	return worker.finishTransition(outcome)
}

func (worker *checkoutWorker) finishTransition(outcome transitionOutcome) error {
	worker.metrics.transitions.Add(1)
	if outcome.Duplicate {
		worker.metrics.duplicates.Add(1)
	}
	return worker.publishResults(outcome.Results)
}

func (worker *checkoutWorker) publishResults(results []resultMessage) error {
	if worker.publishHook != nil {
		for _, result := range results {
			worker.metrics.resultPublishAttempts.Add(1)
			if err := worker.publishHook(result); err != nil {
				worker.metrics.resultPublishFailures.Add(1)
				return err
			}
			worker.metrics.resultPublishSuccesses.Add(1)
		}
		return nil
	}

	type pendingPublish struct {
		result resultMessage
		future nats.PubAckFuture
	}
	pending := make([]pendingPublish, 0, len(results))
	var firstError error
	for _, result := range results {
		worker.metrics.resultPublishAttempts.Add(1)
		future, err := worker.publishResultAsync(result)
		if err != nil {
			worker.metrics.resultPublishFailures.Add(1)
			firstError = fmt.Errorf("publish checkout result %s: %w", result.MessageID, err)
			break
		}
		pending = append(pending, pendingPublish{result: result, future: future})
	}
	for _, publish := range pending {
		select {
		case acknowledgement := <-publish.future.Ok():
			if acknowledgement == nil {
				worker.metrics.resultPublishFailures.Add(1)
				if firstError == nil {
					firstError = fmt.Errorf("publish checkout result %s: empty JetStream acknowledgement", publish.result.MessageID)
				}
				continue
			}
			worker.metrics.resultPublishSuccesses.Add(1)
		case err := <-publish.future.Err():
			worker.metrics.resultPublishFailures.Add(1)
			if firstError == nil {
				firstError = fmt.Errorf("publish checkout result %s: %w", publish.result.MessageID, err)
			}
		}
	}
	return firstError
}

func (worker *checkoutWorker) publishResultAsync(result resultMessage) (nats.PubAckFuture, error) {
	if worker.publishAsyncHook != nil {
		return worker.publishAsyncHook(result)
	}
	message := &nats.Msg{Subject: result.Subject, Data: result.Data, Header: nats.Header{}}
	message.Header.Set("Nats-Msg-Id", result.MessageID)
	message.Header.Set("Content-Type", "application/protobuf")
	return worker.js.PublishMsgAsync(
		message,
		nats.MsgId(result.MessageID),
		nats.StallWait(worker.publishTimeout),
	)
}

func isCheckoutSagaEvent(subject string) bool {
	switch subject {
	case "boutique.evt.shipping.order-quote-calculated.v1",
		"boutique.evt.shipping.order-quote-failed.v1",
		"boutique.evt.payment.authorized.v1",
		"boutique.evt.payment.authorization-declined.v1",
		"boutique.evt.shipping.shipment-created.v1",
		"boutique.evt.shipping.shipment-creation-failed.v1",
		"boutique.evt.payment.captured.v1",
		"boutique.evt.payment.capture-failed.v1",
		"boutique.evt.payment.authorization-released.v1",
		"boutique.evt.shipping.shipment-cancelled.v1",
		"boutique.evt.payment.authorization-release-failed.v1",
		"boutique.evt.shipping.shipment-cancellation-failed.v1":
		return true
	default:
		return false
	}
}

func decodeEnvelope(data []byte) (*commonv1.MessageEnvelope, error) {
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	if envelope.SchemaVersion != 1 || envelope.MessageId == "" || envelope.CorrelationId == "" ||
		envelope.AggregateId == "" || envelope.Data == nil || envelope.OccurredAt == nil {
		return nil, errors.New("unsupported or incomplete envelope")
	}
	if err := envelope.OccurredAt.CheckValid(); err != nil {
		return nil, fmt.Errorf("invalid envelope occurrence time: %w", err)
	}
	return envelope, nil
}

func checkoutMessageKind(topic string) string {
	if strings.HasPrefix(topic, "boutique.cmd.") {
		return "command"
	}
	if strings.HasPrefix(topic, "boutique.qry.") {
		return "query"
	}
	return "event"
}

func checkoutMessageGroup(envelope *commonv1.MessageEnvelope) string {
	if envelope == nil {
		return "unknown"
	}
	if envelope.AggregateId != "" {
		return envelope.AggregateType + "\x00" + envelope.AggregateId
	}
	if envelope.CorrelationId != "" {
		return "correlation\x00" + envelope.CorrelationId
	}
	if envelope.MessageId != "" {
		return "message\x00" + envelope.MessageId
	}
	return "unknown"
}

func (worker *checkoutWorker) scanDeadlines() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-worker.stop:
			return
		case now := <-ticker.C:
			deadlines, err := worker.store.DueDeadlines(now, 16)
			if err != nil {
				log.WithError(err).Warn("checkout deadline scan failed")
				continue
			}
			for _, deadline := range deadlines {
				if err := worker.claimDeadline(deadline.OrderID, now.UTC()); err != nil &&
					!errors.Is(err, stateless.ErrLeaseHeld) && !errors.Is(err, stateless.ErrLeaseComplete) {
					log.WithError(err).WithField("order_id", deadline.OrderID).Warn("checkout deadline dispatch failed")
				}
			}
		}
	}
}

func (worker *checkoutWorker) claimDeadline(orderID string, now time.Time) error {
	record, encodedRecord, err := worker.store.LoadDeadline(orderID)
	if err != nil {
		return err
	}
	if record == nil {
		return worker.store.RemoveOrphanDeadline(orderID)
	}
	lease, err := worker.leaseStore.Acquire(context.Background(), record.WorkID, worker.workerID, now, worker.leaseDuration)
	if errors.Is(err, stateless.ErrLeaseComplete) {
		return worker.store.CompleteDeadline(orderID, encodedRecord)
	}
	if err != nil {
		return err
	}
	worker.metrics.deadlineClaims.Add(1)
	if lease.Attempts > 1 {
		worker.metrics.deadlineLeaseRecoveries.Add(1)
	}
	age := now.Sub(record.Deadline).Milliseconds()
	if age < 0 {
		age = 0
	}
	for current := worker.metrics.deadlineAgeMillis.Load(); age > current; current = worker.metrics.deadlineAgeMillis.Load() {
		if worker.metrics.deadlineAgeMillis.CompareAndSwap(current, age) {
			break
		}
	}
	outcome, err := worker.processDeadline(orderID, record.Version, record.Deadline, record.WorkID)
	if err != nil {
		return err
	}
	if err := worker.finishTransition(outcome); err != nil {
		return err
	}
	if err := worker.leaseStore.Complete(context.Background(), lease, time.Now().UTC()); err != nil &&
		!errors.Is(err, stateless.ErrLeaseComplete) {
		return err
	}
	return worker.store.CompleteDeadline(orderID, encodedRecord)
}

func (worker *checkoutWorker) processDeadline(orderID string, version uint64, deadline time.Time, inputID string) (transitionOutcome, error) {
	payload, err := anypb.New(&emptypb.Empty{})
	if err != nil {
		return transitionOutcome{}, err
	}
	input := &commonv1.MessageEnvelope{
		MessageId: inputID, MessageType: "boutique.checkout.Deadline.v1", SchemaVersion: 1,
		OccurredAt: timestamppb.New(deadline), Producer: "checkoutservice/deadline",
		AggregateType: "order", AggregateId: orderID, AggregateVersion: version,
		CorrelationId: orderID, Data: payload,
	}
	return worker.store.ApplyOrder(orderID, input, nil, func(state *persistedState) error {
		saga := state.Orders[orderID]
		if saga == nil || saga.Version != version || !saga.Deadline.Equal(deadline) {
			return nil
		}
		previousStage := saga.Stage
		saga.Version++
		if err := queueEnvelope(state, "boutique.evt.order.step-timed-out.v1", "boutique.order.StepTimedOut.v1",
			"order", orderID, saga.Version, orderID, inputID, &eventsv1.OrderStepTimedOutEvent{
				OrderId: orderID, WaitingStage: previousStage, Deadline: googleTimestamp(deadline),
				LastCommandId: inputID, ChosenAction: "manual-review",
			}); err != nil {
			return err
		}
		return worker.manualReview(state, saga, inputID, previousStage, "STEP_TIMEOUT")
	})
}

func googleTimestamp(value time.Time) *timestamppb.Timestamp { return timestamppb.New(value.UTC()) }

func (worker *checkoutWorker) Ready() bool {
	return worker != nil && worker.ready.Load() && worker.nc != nil && worker.nc.IsConnected() && worker.store.Ready()
}

func (worker *checkoutWorker) Metrics() string {
	conflicts := worker.store.conflicts.Load()
	return fmt.Sprintf(
		"boutique_checkout_transitions_total %d\n"+
			"boutique_checkout_result_republishes_total %d\n"+
			"boutique_checkout_projection_updates_total %d\n"+
			"boutique_checkout_transition_conflicts_total %d\n"+
			"boutique_checkout_result_publications_total{outcome=\"attempt\"} %d\n"+
			"boutique_checkout_result_publications_total{outcome=\"success\"} %d\n"+
			"boutique_checkout_result_publications_total{outcome=\"failure\"} %d\n"+
			"boutique_checkout_deadline_claims_total %d\n"+
			"boutique_checkout_deadline_lease_recoveries_total %d\n"+
			"boutique_checkout_deadline_oldest_age_seconds %g\n",
		worker.metrics.transitions.Load(), worker.metrics.duplicates.Load(),
		worker.metrics.projectionUpdates.Load(), conflicts,
		worker.metrics.resultPublishAttempts.Load(), worker.metrics.resultPublishSuccesses.Load(),
		worker.metrics.resultPublishFailures.Load(), worker.metrics.deadlineClaims.Load(),
		worker.metrics.deadlineLeaseRecoveries.Load(),
		float64(worker.metrics.deadlineAgeMillis.Load())/1000,
	)
}

func (worker *checkoutWorker) Close() error {
	var result error
	worker.closeOnce.Do(func() {
		worker.ready.Store(false)
		close(worker.stop)
		for _, subscription := range worker.subscriptions {
			_ = subscription.Unsubscribe()
		}
		worker.wg.Wait()
		if worker.nc != nil {
			result = worker.nc.Drain()
		}
		result = errors.Join(result, worker.store.Close())
	})
	return result
}

func integerEnv(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return parsed, nil
}
