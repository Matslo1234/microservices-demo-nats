// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	shippingCartConsumer           = "shipping-cart-quotes-v1"
	shippingQuoteSubject           = "boutique.evt.shipping.cart-quote-updated.v1"
	shippingCartBatchSize          = 32
	shippingCartWorkers            = 32
	shippingCartMaxPending         = 1000
	legacyShippingCommandConsumer  = "shipping-commands-v1"
	shippingOrderQuoteConsumer     = "shipping-order-quotes-v1"
	shippingCreateShipmentConsumer = "shipping-create-shipments-v1"
	shippingCancelShipmentConsumer = "shipping-cancel-shipments-v1"
)

type shippingConsumerDefinition struct {
	stream        string
	durable       string
	filterSubject string
	maxPending    int
	batchSize     int
	workers       int
	queueDepth    int
}

func shippingCartConsumerDefinition() shippingConsumerDefinition {
	return shippingConsumerDefinition{
		stream: "BOUTIQUE_EVENTS", durable: shippingCartConsumer,
		filterSubject: "boutique.evt.cart.>", maxPending: shippingCartMaxPending,
	}
}

func shippingCommandConsumerDefinitions() []shippingConsumerDefinition {
	return []shippingConsumerDefinition{
		{
			stream: "BOUTIQUE_COMMANDS", durable: shippingOrderQuoteConsumer,
			filterSubject: shippingOrderQuoteCommandSubject,
			maxPending:    1024, batchSize: 512, workers: 32, queueDepth: 512,
		},
		{
			stream: "BOUTIQUE_COMMANDS", durable: shippingCreateShipmentConsumer,
			filterSubject: shippingCreateShipmentSubject,
			maxPending:    2048, batchSize: 512, workers: 96, queueDepth: 1024,
		},
		{
			stream: "BOUTIQUE_COMMANDS", durable: shippingCancelShipmentConsumer,
			filterSubject: shippingCancelShipmentSubject,
			maxPending:    256, batchSize: 128, workers: 16, queueDepth: 128,
		},
	}
}

type shippingEventWorker struct {
	nc                   *nats.Conn
	js                   nats.JetStreamContext
	subscription         *nats.Subscription
	commandSubscriptions []*nats.Subscription
	provider             *shippingProvider
	failureMode          string
	processingTime       time.Duration
	publishTimeout       time.Duration
	stop                 chan struct{}
	failed               chan error
	ready                atomic.Bool
	lifecycleFailed      atomic.Bool
	wg                   sync.WaitGroup
	failureOnce          sync.Once
	closeOnce            sync.Once
}

func startShippingEvents() (*shippingEventWorker, error) {
	required, _ := strconv.ParseBool(os.Getenv("NATS_REQUIRED"))
	if !required {
		return nil, nil
	}
	url, user, password, caFile := os.Getenv("NATS_URL"), os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD"), os.Getenv("NATS_CA_FILE")
	if url == "" || user == "" || password == "" || caFile == "" || os.Getenv("REGION_ID") == "" || os.Getenv("K8S_CLUSTER_NAME") == "" {
		return nil, fmt.Errorf("NATS_URL, NATS_USER, NATS_PASSWORD, NATS_CA_FILE, REGION_ID, and K8S_CLUSTER_NAME are required")
	}
	connectTimeout, err := shippingDuration("NATS_CONNECT_TIMEOUT", 2*time.Second)
	if err != nil {
		return nil, err
	}
	reconnectWait, err := shippingDuration("NATS_RECONNECT_WAIT", 2*time.Second)
	if err != nil {
		return nil, err
	}
	pingInterval, err := shippingDuration("NATS_PING_INTERVAL", 20*time.Second)
	if err != nil {
		return nil, err
	}
	publishTimeout, err := shippingDuration("NATS_PUBLISH_TIMEOUT", 5*time.Second)
	if err != nil {
		return nil, err
	}
	maxReconnects, err := shippingInt("NATS_MAX_RECONNECTS", -1)
	if err != nil {
		return nil, err
	}
	maxPings, err := shippingInt("NATS_MAX_PINGS_OUT", 2)
	if err != nil {
		return nil, err
	}
	worker := &shippingEventWorker{
		publishTimeout: publishTimeout,
		failureMode:    os.Getenv("SHIPPING_FAILURE_MODE"),
		processingTime: shippingProcessingTime(os.Getenv("PROCESSING_TIME_MS")),
		stop:           make(chan struct{}),
		failed:         make(chan error, 1),
	}
	providerSecret := os.Getenv("SHIPPING_PROVIDER_SECRET")
	worker.provider, err = newShippingProvider(providerSecret)
	if err != nil {
		return nil, err
	}
	providerFingerprint := fmt.Sprintf("%x", sha256.Sum256([]byte(providerSecret)))[:16]
	log.WithFields(logrus.Fields{
		"region": os.Getenv("REGION_ID"), "k8s_cluster": os.Getenv("K8S_CLUSTER_NAME"),
		"shipping_provider_key_fingerprint": providerFingerprint,
	}).Info("shipping regional configuration loaded")
	nc, err := nats.Connect(url,
		nats.Name(fmt.Sprintf("shippingservice/phase3/%s/%s", os.Getenv("REGION_ID"), os.Getenv("K8S_CLUSTER_NAME"))),
		nats.UserInfo(user, password), nats.RootCAs(caFile),
		nats.Timeout(connectTimeout), nats.ReconnectWait(reconnectWait), nats.MaxReconnects(maxReconnects),
		nats.PingInterval(pingInterval), nats.MaxPingsOutstanding(maxPings),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			worker.ready.Store(false)
			log.Warnf("NATS disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			if !worker.lifecycleFailed.Load() {
				worker.ready.Store(true)
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connect shippingservice to NATS: %w", err)
	}
	worker.nc = nc
	worker.js, err = nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}
	cartConsumer := shippingCartConsumerDefinition()
	if err := worker.ensureConsumer(cartConsumer); err != nil {
		nc.Close()
		return nil, fmt.Errorf("ensure shipping cart consumer: %w", err)
	}
	worker.subscription, err = worker.js.PullSubscribe(
		cartConsumer.filterSubject,
		cartConsumer.durable,
		nats.Bind(cartConsumer.stream, cartConsumer.durable),
	)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("bind shipping cart consumer: %w", err)
	}
	commandConsumers := shippingCommandConsumerDefinitions()
	if deleteErr := worker.js.DeleteConsumer("BOUTIQUE_COMMANDS", legacyShippingCommandConsumer); deleteErr != nil &&
		!errors.Is(deleteErr, nats.ErrConsumerNotFound) {
		nc.Close()
		return nil, fmt.Errorf("delete legacy shipping command consumer: %w", deleteErr)
	}
	for _, commandConsumer := range commandConsumers {
		if err := worker.ensureConsumer(commandConsumer); err != nil {
			nc.Close()
			return nil, fmt.Errorf("ensure shipping command consumer %s: %w", commandConsumer.durable, err)
		}
		subscription, subscribeErr := worker.js.PullSubscribe(
			commandConsumer.filterSubject,
			commandConsumer.durable,
			nats.Bind(commandConsumer.stream, commandConsumer.durable),
		)
		if subscribeErr != nil {
			nc.Close()
			return nil, fmt.Errorf("bind shipping command consumer %s: %w", commandConsumer.durable, subscribeErr)
		}
		worker.commandSubscriptions = append(worker.commandSubscriptions, subscription)
	}
	worker.ready.Store(true)
	worker.wg.Add(1 + len(commandConsumers))
	go func() {
		defer worker.wg.Done()
		if runErr := worker.run(); runErr != nil {
			worker.reportFailure(fmt.Errorf("shipping cart consumer: %w", runErr))
		}
	}()
	for index, commandConsumer := range commandConsumers {
		subscription := worker.commandSubscriptions[index]
		go func() {
			defer worker.wg.Done()
			if runErr := worker.runCommands(subscription, commandConsumer); runErr != nil {
				worker.reportFailure(fmt.Errorf("shipping command consumer %s: %w", commandConsumer.durable, runErr))
			}
		}()
	}
	return worker, nil
}

func (worker *shippingEventWorker) ensureConsumer(definition shippingConsumerDefinition) error {
	// A durable created by PullSubscribe is deleted by nats.go when its owning
	// connection drains. Manage it separately so scale-down preserves its cursor.
	config := &nats.ConsumerConfig{
		Durable:       definition.durable,
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    10,
		MaxAckPending: definition.maxPending,
		FilterSubject: definition.filterSubject,
	}
	for attempt := 0; attempt < 20; attempt++ {
		info, err := worker.js.ConsumerInfo(definition.stream, definition.durable)
		if errors.Is(err, nats.ErrConsumerNotFound) {
			if _, addErr := worker.js.AddConsumer(definition.stream, config); addErr == nil {
				return nil
			} else if shippingConsumerSetupRace(addErr) {
				time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
				continue
			} else {
				return fmt.Errorf("create %s: %w", definition.durable, addErr)
			}
		}
		if err != nil {
			return fmt.Errorf("inspect %s: %w", definition.durable, err)
		}
		if info.Config.FilterSubject == definition.filterSubject &&
			info.Config.MaxAckPending == definition.maxPending &&
			info.Config.AckPolicy == nats.AckExplicitPolicy &&
			info.Config.AckWait == 30*time.Second &&
			info.Config.MaxDeliver == 10 &&
			info.Config.DeliverPolicy == nats.DeliverAllPolicy {
			return nil
		}
		next := info.Config
		next.FilterSubject = definition.filterSubject
		next.FilterSubjects = nil
		next.MaxAckPending = definition.maxPending
		next.AckPolicy = nats.AckExplicitPolicy
		next.AckWait = 30 * time.Second
		next.MaxDeliver = 10
		next.DeliverPolicy = nats.DeliverAllPolicy
		if _, updateErr := worker.js.UpdateConsumer(definition.stream, &next); updateErr == nil {
			return nil
		} else if shippingConsumerSetupRace(updateErr) {
			time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
			continue
		} else {
			return fmt.Errorf("update %s: %w", definition.durable, updateErr)
		}
	}
	return fmt.Errorf("consumer %s setup conflicted too many times", definition.durable)
}

func shippingConsumerSetupRace(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return errors.Is(err, nats.ErrConsumerNotFound) ||
		strings.Contains(message, "consumer already exists") ||
		strings.Contains(message, "consumer name already in use") ||
		strings.Contains(message, "stream sequence")
}

func (worker *shippingEventWorker) runCommands(subscription *nats.Subscription, definition shippingConsumerDefinition) error {
	jobs := make(chan *nats.Msg, definition.queueDepth)
	var processors sync.WaitGroup
	processors.Add(definition.workers)
	for range definition.workers {
		go func() {
			defer processors.Done()
			for message := range jobs {
				worker.processCommandMessage(message)
			}
		}()
	}
	defer func() {
		close(jobs)
		processors.Wait()
	}()

	for {
		select {
		case <-worker.stop:
			return nil
		default:
		}
		batch, err := subscription.FetchBatch(
			definition.batchSize,
			nats.MaxWait(time.Second),
		)
		if err != nil {
			if shippingConsumerTerminal(err) {
				return err
			}
			continue
		}
		for message := range batch.Messages() {
			select {
			case jobs <- message:
			case <-worker.stop:
				return nil
			}
		}
		if err := batch.Error(); err != nil {
			if shippingConsumerTerminal(err) {
				return err
			}
		}
	}
}

func (worker *shippingEventWorker) processCommandMessage(message *nats.Msg) {
	entry := shippingMessageLog(message, "command")
	entry.Debug("NATS command received")
	correlationID, messageID, traceparent, tracestate := shippingEnvelopeTelemetry(message.Data)
	ctx, span := telemetry.StartConsumerSpan(context.Background(), message.Subject, "command",
		messageID, correlationID, traceparent, tracestate)
	defer span.End()
	if err := worker.handleCommandWithContext(ctx, message); err != nil {
		telemetry.RecordError(span, err)
		entry.WithError(err).Error("shipping command processing failed")
		_ = message.NakWithDelay(time.Second)
		return
	}
	if err := message.Ack(); err != nil {
		entry.WithError(err).Error("shipping command acknowledgement failed")
	}
}

func processShippingStream(
	messages <-chan *nats.Msg,
	fetchSize int,
	parallelism int,
	process func(*nats.Msg),
) {
	processShippingStreamByLane(
		messages, fetchSize, parallelism, shippingMessageLane, process,
	)
}

func processShippingStreamByLane(
	messages <-chan *nats.Msg,
	fetchSize int,
	parallelism int,
	laneFor func([]byte, int) int,
	process func(*nats.Msg),
) {
	if parallelism <= 1 {
		for message := range messages {
			process(message)
		}
		return
	}
	lanes := make([]chan *nats.Msg, parallelism)
	var running sync.WaitGroup
	for index := range lanes {
		lane := make(chan *nats.Msg, fetchSize)
		lanes[index] = lane
		running.Add(1)
		go func(messages <-chan *nats.Msg) {
			defer running.Done()
			for message := range messages {
				process(message)
			}
		}(lane)
	}
	for message := range messages {
		lane := laneFor(message.Data, len(lanes))
		lanes[lane] <- message
	}
	for _, lane := range lanes {
		close(lane)
	}
	running.Wait()
}

func shippingMessageLane(data []byte, lanes int) int {
	correlationID, _ := shippingEnvelopeContext(data)
	return shippingLane(correlationID, lanes)
}

func shippingLane(identity string, lanes int) int {
	hash := uint32(2166136261)
	for index := 0; index < len(identity); index++ {
		hash ^= uint32(identity[index])
		hash *= 16777619
	}
	return int(hash % uint32(lanes))
}

func (worker *shippingEventWorker) run() error {
	for {
		select {
		case <-worker.stop:
			return nil
		default:
		}
		batch, err := worker.subscription.FetchBatch(shippingCartBatchSize, nats.MaxWait(time.Second))
		if err != nil {
			if shippingConsumerTerminal(err) {
				return err
			}
			continue
		}
		processShippingStreamByLane(
			batch.Messages(), shippingCartBatchSize, shippingCartWorkers,
			shippingCartMessageLane, worker.processCartMessage,
		)
		if err := batch.Error(); err != nil {
			if shippingConsumerTerminal(err) {
				return err
			}
		}
	}
}

func (worker *shippingEventWorker) processCartMessage(message *nats.Msg) {
	entry := shippingMessageLog(message, "event")
	entry.Debug("NATS event received")
	correlationID, messageID, traceparent, tracestate := shippingEnvelopeTelemetry(message.Data)
	ctx, span := telemetry.StartConsumerSpan(context.Background(), message.Subject, "event",
		messageID, correlationID, traceparent, tracestate)
	defer span.End()
	if err := worker.handle(ctx, message); err != nil {
		telemetry.RecordError(span, err)
		entry.WithError(err).Error("shipping cart event processing failed")
		_ = message.NakWithDelay(time.Second)
		return
	}
	if err := message.Ack(); err != nil {
		telemetry.RecordError(span, err)
		entry.WithError(err).Error("shipping cart event acknowledgement failed")
	}
}

func shippingConsumerTerminal(err error) bool {
	return err != nil && !errors.Is(err, nats.ErrTimeout)
}

func (worker *shippingEventWorker) reportFailure(err error) {
	worker.failureOnce.Do(func() {
		worker.lifecycleFailed.Store(true)
		worker.ready.Store(false)
		worker.failed <- err
	})
}

func shippingMessageLog(message *nats.Msg, kind string) *logrus.Entry {
	correlationID, messageID := shippingEnvelopeContext(message.Data)
	return log.WithFields(logrus.Fields{
		"message_kind":   kind,
		"topic":          message.Subject,
		"message_id":     messageID,
		"correlation_id": correlationID,
	})
}

func shippingEnvelopeContext(data []byte) (string, string) {
	correlationID, messageID, _, _ := shippingEnvelopeTelemetry(data)
	return correlationID, messageID
}

func shippingCartMessageLane(data []byte, lanes int) int {
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(data, envelope); err == nil && envelope.AggregateId != "" {
		return shippingLane(envelope.AggregateId, lanes)
	}
	return shippingMessageLane(data, lanes)
}

func shippingEnvelopeTelemetry(data []byte) (string, string, string, string) {
	correlationID, messageID := "unknown", "unknown"
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(data, envelope); err == nil {
		if envelope.CorrelationId != "" {
			correlationID = envelope.CorrelationId
		}
		if envelope.MessageId != "" {
			messageID = envelope.MessageId
		}
		return correlationID, messageID, envelope.Traceparent, envelope.Tracestate
	}
	return correlationID, messageID, "", ""
}

func (worker *shippingEventWorker) handle(ctx context.Context, message *nats.Msg) error {
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(message.Data, envelope); err != nil {
		return fmt.Errorf("decode cart envelope: %w", err)
	}
	telemetry.Inject(ctx, &envelope.Traceparent, &envelope.Tracestate)
	var cart *commonv1.CartSnapshot
	switch message.Subject {
	case "boutique.evt.cart.item-added.v1":
		payload := &eventsv1.CartItemAddedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		cart = payload.Cart
	case "boutique.evt.cart.cleared.v1":
		payload := &eventsv1.CartClearedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		cart = payload.Cart
	default:
		return nil
	}
	if cart == nil || cart.UserId == "" {
		return fmt.Errorf("cart snapshot is missing")
	}
	outcome, err := buildShippingCartOutcome(envelope, cart)
	if err != nil {
		return err
	}
	return worker.publishOutcome(ctx, outcome)
}

func buildShippingCartOutcome(
	envelope *commonv1.MessageEnvelope,
	cart *commonv1.CartSnapshot,
) (shippingOutcome, error) {
	inputTime, err := validateShippingInput(envelope)
	if err != nil {
		return shippingOutcome{}, err
	}
	if cart == nil || cart.UserId == "" || cart.CartVersion == 0 {
		return shippingOutcome{}, errors.New("cart snapshot is incomplete")
	}
	if envelope.AggregateId != cart.UserId || envelope.AggregateVersion != cart.CartVersion {
		return shippingOutcome{}, errors.New("cart snapshot does not match the input aggregate")
	}
	count := 0
	for _, line := range cart.Items {
		count += int(line.Quantity)
	}
	quote := CreateQuoteFromCount(count)
	payload := &eventsv1.ShippingCartQuoteUpdatedEvent{
		UserId: cart.UserId, CartVersion: cart.CartVersion,
		CostUsd:   &commonv1.Money{CurrencyCode: "USD", Units: int64(quote.Dollars), Nanos: int32(quote.Cents * 10_000_000)},
		ExpiresAt: timestamppb.New(inputTime.Add(15 * time.Minute)),
	}
	result, err := stateless.NewResultEnvelope(envelope, stateless.ResultSpec{
		Slot:             "shipping.cart-quote",
		MessageType:      "boutique.shipping.CartQuoteUpdated.v1",
		Producer:         "shippingservice/phase3",
		AggregateType:    "cart",
		AggregateID:      cart.UserId,
		AggregateVersion: cart.CartVersion,
		OccurredAt:       inputTime,
		Payload:          payload,
	})
	if err != nil {
		return shippingOutcome{}, err
	}
	encoded, err := stateless.MarshalEnvelope(result)
	if err != nil {
		return shippingOutcome{}, err
	}
	return shippingOutcome{MessageID: result.MessageId, Subject: shippingQuoteSubject, Data: encoded}, nil
}

func (worker *shippingEventWorker) Ready() bool {
	return worker == nil || (worker.ready.Load() && worker.nc.IsConnected())
}

func (worker *shippingEventWorker) Close() {
	if worker == nil {
		return
	}
	worker.closeOnce.Do(func() {
		worker.ready.Store(false)
		close(worker.stop)
		if worker.subscription != nil {
			_ = worker.subscription.Unsubscribe()
		}
		for _, subscription := range worker.commandSubscriptions {
			_ = subscription.Unsubscribe()
		}
		worker.wg.Wait()
		_ = worker.nc.Drain()
	})
}

func shippingDuration(name string, fallback time.Duration) (time.Duration, error) {
	if value := os.Getenv(name); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return 0, fmt.Errorf("invalid %s: %w", name, err)
		}
		return parsed, nil
	}
	return fallback, nil
}

func shippingInt(name string, fallback int) (int, error) {
	if value := os.Getenv(name); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf("invalid %s: %w", name, err)
		}
		return parsed, nil
	}
	return fallback, nil
}

func shippingProcessingTime(value string) time.Duration {
	milliseconds, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil || math.IsNaN(milliseconds) || math.IsInf(milliseconds, 0) ||
		milliseconds <= 0 || milliseconds > float64(math.MaxInt64)/float64(time.Millisecond) {
		return 0
	}
	return time.Duration(milliseconds * float64(time.Millisecond))
}
