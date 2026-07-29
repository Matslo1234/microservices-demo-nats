// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	stateless "github.com/GoogleCloudPlatform/microservices-demo/src/shared/stateless/go"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	shippingCartConsumer      = "shipping-cart-quotes-v1"
	shippingQuoteSubject      = "boutique.evt.shipping.cart-quote-updated.v1"
	shippingCommandConsumer   = "shipping-commands-v1"
	shippingCommandBatchSize  = 256
	shippingCommandMaxPending = 512
	shippingCommandWorkers    = 32
)

type shippingEventWorker struct {
	nc                  *nats.Conn
	js                  nats.JetStreamContext
	subscription        *nats.Subscription
	commandSubscription *nats.Subscription
	provider            *shippingProvider
	failureMode         string
	publishTimeout      time.Duration
	stop                chan struct{}
	ready               atomic.Bool
}

func startShippingEvents() (*shippingEventWorker, error) {
	required, _ := strconv.ParseBool(os.Getenv("NATS_REQUIRED"))
	if !required {
		return nil, nil
	}
	url, user, password, caFile := os.Getenv("NATS_URL"), os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD"), os.Getenv("NATS_CA_FILE")
	if url == "" || user == "" || password == "" || caFile == "" {
		return nil, fmt.Errorf("NATS_URL, NATS_USER, NATS_PASSWORD, and NATS_CA_FILE are required")
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
		stop:           make(chan struct{}),
	}
	worker.provider, err = newShippingProvider(os.Getenv("SHIPPING_PROVIDER_SECRET"))
	if err != nil {
		return nil, err
	}
	nc, err := nats.Connect(url,
		nats.Name("shippingservice/phase3"), nats.UserInfo(user, password), nats.RootCAs(caFile),
		nats.Timeout(connectTimeout), nats.ReconnectWait(reconnectWait), nats.MaxReconnects(maxReconnects),
		nats.PingInterval(pingInterval), nats.MaxPingsOutstanding(maxPings),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			worker.ready.Store(false)
			log.Warnf("NATS disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) { worker.ready.Store(true) }),
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
	worker.subscription, err = worker.js.PullSubscribe(
		"boutique.evt.cart.>", shippingCartConsumer,
		nats.BindStream("BOUTIQUE_EVENTS"), nats.ManualAck(), nats.AckExplicit(),
		nats.DeliverAll(), nats.AckWait(30*time.Second), nats.MaxDeliver(10),
	)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create shipping cart consumer: %w", err)
	}
	commandInfo, err := worker.js.ConsumerInfo("BOUTIQUE_COMMANDS", shippingCommandConsumer)
	if err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
		nc.Close()
		return nil, fmt.Errorf("inspect shipping command consumer: %w", err)
	}
	if commandInfo != nil && commandInfo.Config.MaxAckPending != shippingCommandMaxPending {
		config := commandInfo.Config
		config.MaxAckPending = shippingCommandMaxPending
		if _, err := worker.js.UpdateConsumer("BOUTIQUE_COMMANDS", &config); err != nil {
			nc.Close()
			return nil, fmt.Errorf("update shipping command consumer: %w", err)
		}
	}
	worker.commandSubscription, err = worker.js.PullSubscribe(
		"boutique.cmd.shipping.>", shippingCommandConsumer,
		nats.BindStream("BOUTIQUE_COMMANDS"), nats.ManualAck(), nats.AckExplicit(),
		nats.DeliverAll(), nats.AckWait(30*time.Second), nats.MaxDeliver(10),
		nats.MaxAckPending(shippingCommandMaxPending),
	)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create shipping command consumer: %w", err)
	}
	worker.ready.Store(true)
	go worker.run()
	go worker.runCommands()
	return worker, nil
}

func (worker *shippingEventWorker) runCommands() {
	for {
		select {
		case <-worker.stop:
			return
		default:
		}
		batch, err := worker.commandSubscription.FetchBatch(
			shippingCommandBatchSize,
			nats.MaxWait(time.Second),
		)
		if err != nil {
			if !shippingConsumerStopped(err) {
				log.Errorf("shipping command fetch failed: %v", err)
				time.Sleep(time.Second)
			}
			continue
		}
		processShippingStream(
			batch.Messages(),
			shippingCommandBatchSize,
			shippingCommandWorkers,
			worker.processCommandMessage,
		)
		if err := batch.Error(); err != nil && !shippingConsumerStopped(err) {
			log.Errorf("shipping command stream failed: %v", err)
			time.Sleep(time.Second)
		}
	}
}

func (worker *shippingEventWorker) processCommandMessage(message *nats.Msg) {
	entry := shippingMessageLog(message, "command")
	entry.Debug("NATS command received")
	if err := worker.handleCommand(message); err != nil {
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
		lane := shippingMessageLane(message.Data, len(lanes))
		lanes[lane] <- message
	}
	for _, lane := range lanes {
		close(lane)
	}
	running.Wait()
}

func shippingMessageLane(data []byte, lanes int) int {
	correlationID, _ := shippingEnvelopeContext(data)
	hash := uint32(2166136261)
	for index := 0; index < len(correlationID); index++ {
		hash ^= uint32(correlationID[index])
		hash *= 16777619
	}
	return int(hash % uint32(lanes))
}

func (worker *shippingEventWorker) run() {
	for {
		select {
		case <-worker.stop:
			return
		default:
		}
		batch, err := worker.subscription.FetchBatch(32, nats.MaxWait(time.Second))
		if err != nil {
			if !shippingConsumerStopped(err) {
				log.Errorf("shipping cart event fetch failed: %v", err)
				time.Sleep(time.Second)
			}
			continue
		}
		for message := range batch.Messages() {
			entry := shippingMessageLog(message, "event")
			entry.Debug("NATS event received")
			if err := worker.handle(message); err != nil {
				entry.WithError(err).Error("shipping cart event processing failed")
				_ = message.NakWithDelay(time.Second)
				continue
			}
			if err := message.Ack(); err != nil {
				entry.WithError(err).Error("shipping cart event acknowledgement failed")
				continue
			}
		}
		if err := batch.Error(); err != nil && !shippingConsumerStopped(err) {
			log.Errorf("shipping cart event stream failed: %v", err)
			time.Sleep(time.Second)
		}
	}
}

func shippingConsumerStopped(err error) bool {
	return errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrConnectionClosed) ||
		errors.Is(err, nats.ErrBadSubscription) ||
		errors.Is(err, nats.ErrSubscriptionClosed)
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
	correlationID, messageID := "unknown", "unknown"
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(data, envelope); err == nil {
		if envelope.CorrelationId != "" {
			correlationID = envelope.CorrelationId
		}
		if envelope.MessageId != "" {
			messageID = envelope.MessageId
		}
	}
	return correlationID, messageID
}

func (worker *shippingEventWorker) handle(message *nats.Msg) error {
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(message.Data, envelope); err != nil {
		return fmt.Errorf("decode cart envelope: %w", err)
	}
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
	return worker.publishOutcome(outcome)
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
	worker.ready.Store(false)
	close(worker.stop)
	_ = worker.nc.Drain()
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
