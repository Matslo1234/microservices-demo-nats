// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commandsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/commands/v1"
	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	checkoutFetchBatchSize = 32
	outboxPublishBatchSize = 256
)

type checkoutWorker struct {
	store          *stateStore
	nc             *nats.Conn
	js             nats.JetStreamContext
	subscriptions  []*nats.Subscription
	publishTimeout time.Duration
	stepTimeout    time.Duration
	stop           chan struct{}
	wg             sync.WaitGroup
	ready          atomic.Bool
	closeOnce      sync.Once
}

func startCheckoutWorker(store *stateStore) (*checkoutWorker, error) {
	url, user, password, caFile := os.Getenv("NATS_URL"), os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD"), os.Getenv("NATS_CA_FILE")
	if url == "" || user == "" || password == "" || caFile == "" {
		return nil, errors.New("NATS_URL, NATS_USER, NATS_PASSWORD, and NATS_CA_FILE are required")
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
	worker := &checkoutWorker{store: store, publishTimeout: publishTimeout, stepTimeout: stepTimeout, stop: make(chan struct{})}
	nc, err := nats.Connect(url, nats.Name("checkoutservice/phase5"), nats.UserInfo(user, password), nats.RootCAs(caFile),
		nats.Timeout(connectTimeout), nats.ReconnectWait(reconnectWait), nats.MaxReconnects(maxReconnects),
		nats.PingInterval(pingInterval), nats.MaxPingsOutstanding(maxPings),
		nats.DisconnectErrHandler(func(_ *nats.Conn, disconnectErr error) {
			worker.ready.Store(false)
			log.WithError(disconnectErr).Warn("NATS disconnected")
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) { worker.ready.Store(true) }))
	if err != nil {
		return nil, fmt.Errorf("connect checkoutservice to NATS: %w", err)
	}
	worker.nc = nc
	worker.js, err = nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}

	projectionDefinitions := []struct {
		subject, durable, stream string
		handler                  func([]*nats.Msg) []error
	}{
		{"boutique.evt.catalog.>", "checkout-catalog-v1", "BOUTIQUE_EVENTS", worker.handleProjectionMessages},
		{"boutique.evt.currency.>", "checkout-currency-v1", "BOUTIQUE_EVENTS", worker.handleProjectionMessages},
		{"boutique.evt.cart.>", "checkout-cart-v1", "BOUTIQUE_EVENTS", worker.handleProjectionMessages},
	}
	workflowDefinitions := []struct {
		subject, durable, stream string
		handler                  func([]*nats.Msg) []error
	}{
		{"boutique.cmd.order.submit.v1", "checkout-order-commands-v1", "BOUTIQUE_COMMANDS", worker.handleCommandMessages},
		{"boutique.evt.shipping.>", "checkout-saga-shipping-v1", "BOUTIQUE_EVENTS", worker.handleEventMessages},
		{"boutique.evt.payment.>", "checkout-saga-payment-v1", "BOUTIQUE_EVENTS", worker.handleEventMessages},
	}
	addSubscription := func(definition struct {
		subject, durable, stream string
		handler                  func([]*nats.Msg) []error
	}) error {
		subscription, subscribeErr := worker.js.PullSubscribe(definition.subject, definition.durable,
			nats.BindStream(definition.stream), nats.ManualAck(), nats.AckExplicit(), nats.DeliverAll(),
			nats.AckWait(30*time.Second), nats.MaxDeliver(10), nats.MaxAckPending(64))
		if subscribeErr != nil {
			return fmt.Errorf("create %s: %w", definition.durable, subscribeErr)
		}
		worker.subscriptions = append(worker.subscriptions, subscription)
		worker.wg.Add(1)
		go func() {
			defer worker.wg.Done()
			worker.consume(subscription, definition.handler)
		}()
		return nil
	}
	for _, definition := range projectionDefinitions {
		if err := addSubscription(definition); err != nil {
			nc.Close()
			return nil, err
		}
	}
	catchupDeadline := time.Now().Add(projectionCatchupTimeout)
	for _, subscription := range worker.subscriptions {
		for {
			info, infoErr := subscription.ConsumerInfo()
			if infoErr != nil {
				nc.Close()
				return nil, fmt.Errorf("inspect checkout projection consumer: %w", infoErr)
			}
			if info.NumPending == 0 && info.NumAckPending == 0 {
				break
			}
			if time.Now().After(catchupDeadline) {
				nc.Close()
				return nil, errors.New("checkout projections did not catch up before startup deadline")
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	for _, definition := range workflowDefinitions {
		if err := addSubscription(definition); err != nil {
			nc.Close()
			return nil, err
		}
	}
	worker.ready.Store(true)
	worker.wg.Add(2)
	go func() {
		defer worker.wg.Done()
		worker.relayOutbox()
	}()
	go func() {
		defer worker.wg.Done()
		worker.scanDeadlines()
	}()
	return worker, nil
}

func (worker *checkoutWorker) consume(subscription *nats.Subscription, handler func([]*nats.Msg) []error) {
	for {
		select {
		case <-worker.stop:
			return
		default:
		}
		messages, err := subscription.Fetch(checkoutFetchBatchSize, nats.MaxWait(time.Second))
		if err != nil && !errors.Is(err, nats.ErrTimeout) {
			log.WithError(err).Error("checkout consumer fetch failed")
			time.Sleep(time.Second)
			continue
		}
		entries := make([]*logrus.Entry, len(messages))
		for index, message := range messages {
			correlationID, messageID := checkoutMessageContext(message.Data)
			kind := checkoutMessageKind(message.Subject)
			entries[index] = log.WithFields(logrus.Fields{
				"topic":          message.Subject,
				"message_kind":   kind,
				"message_id":     messageID,
				"correlation_id": correlationID,
			})
			entries[index].Debug("NATS " + kind + " received")
		}
		results := handler(messages)
		if len(results) != len(messages) {
			results = make([]error, len(messages))
			for index := range results {
				results[index] = errors.New("checkout batch handler returned an invalid result count")
			}
		}
		for index, message := range messages {
			entry := entries[index]
			if err := results[index]; err != nil {
				if errors.Is(err, errCheckoutProjectionLag) {
					entry.WithError(err).Debug("checkout command is waiting for its projections")
				} else {
					entry.WithError(err).Error("checkout message processing failed")
				}
				_ = message.NakWithDelay(time.Second)
				continue
			}
			if err := message.Ack(); err != nil {
				entry.WithError(err).Error("checkout message acknowledgement failed")
				continue
			}
		}
	}
}

func checkoutMessageKind(topic string) string {
	switch {
	case strings.HasPrefix(topic, "boutique.cmd."):
		return "command"
	case strings.HasPrefix(topic, "boutique.qry."):
		return "query"
	default:
		return "event"
	}
}

func checkoutMessageContext(data []byte) (string, string) {
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return "unknown", "unknown"
	}
	correlationID := envelope.CorrelationId
	if correlationID == "" {
		correlationID = "unknown"
	}
	messageID := envelope.MessageId
	if messageID == "" {
		messageID = "unknown"
	}
	return correlationID, messageID
}

func (worker *checkoutWorker) handleProjectionMessage(message *nats.Msg) error {
	return worker.handleProjectionMessages([]*nats.Msg{message})[0]
}

func (worker *checkoutWorker) handleProjectionMessages(messages []*nats.Msg) []error {
	results := make([]error, len(messages))
	envelopes := make([]*commonv1.MessageEnvelope, len(messages))
	for index, message := range messages {
		envelopes[index], results[index] = decodeEnvelope(message.Data)
	}
	commitErr := worker.store.UpdateTracked(func(state *persistedState) error {
		for index, envelope := range envelopes {
			if envelope != nil {
				results[index] = nil
			}
		}
		for index, envelope := range envelopes {
			if envelope == nil {
				continue
			}
			if err := worker.applyProjectionMessage(state, messages[index].Subject, envelope); err != nil {
				results[index] = err
				return err
			}
		}
		return nil
	})
	if commitErr != nil {
		for index, envelope := range envelopes {
			if envelope != nil {
				results[index] = commitErr
			}
		}
	}
	return results
}

func (worker *checkoutWorker) applyProjectionMessage(state *persistedState, subject string,
	envelope *commonv1.MessageEnvelope) error {
	if _, ok := state.Inbox[envelope.MessageId]; ok {
		return nil
	}
	switch subject {
	case "boutique.evt.catalog.product-upserted.v1":
		payload := &eventsv1.CatalogProductUpsertedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if payload.Product != nil {
			current := state.Products[payload.Product.ProductId]
			if current == nil || current.ProductVersion < payload.Product.ProductVersion {
				state.setProduct(payload.Product.ProductId, payload.Product)
			}
		}
		if payload.CatalogRevision > state.CatalogRevision {
			state.setCatalogRevision(payload.CatalogRevision)
		}
	case "boutique.evt.catalog.product-removed.v1":
		payload := &eventsv1.CatalogProductRemovedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		state.deleteProduct(payload.ProductId)
		if payload.CatalogRevision > state.CatalogRevision {
			state.setCatalogRevision(payload.CatalogRevision)
		}
	case "boutique.evt.catalog.snapshot-completed.v1":
		payload := &eventsv1.CatalogSnapshotCompletedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if payload.CatalogRevision > state.CatalogRevision {
			state.setCatalogRevision(payload.CatalogRevision)
		}
	case "boutique.evt.currency.rates-updated.v1":
		payload := &eventsv1.CurrencyRatesUpdatedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if state.Rates == nil || state.Rates.RateRevision < payload.RateRevision {
			state.setRates(payload)
		}
	case "boutique.evt.cart.item-added.v1":
		payload := &eventsv1.CartItemAddedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		updateCheckoutCart(state, payload.Cart)
	case "boutique.evt.cart.cleared.v1":
		payload := &eventsv1.CartClearedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		updateCheckoutCart(state, payload.Cart)
	}
	state.setInbox(envelope.MessageId, time.Now().UTC())
	return nil
}

func updateCheckoutCart(state *persistedState, cart *commonv1.CartSnapshot) {
	if cart == nil || cart.UserId == "" {
		return
	}
	current := state.Carts[cart.UserId]
	if current == nil || current.CartVersion < cart.CartVersion {
		state.setCart(cart.UserId, cart)
	}
}

func (worker *checkoutWorker) handleCommandMessage(message *nats.Msg) error {
	return worker.handleCommandMessages([]*nats.Msg{message})[0]
}

func (worker *checkoutWorker) handleCommandMessages(messages []*nats.Msg) []error {
	results := make([]error, len(messages))
	envelopes := make([]*commonv1.MessageEnvelope, len(messages))
	payloads := make([]*commandsv1.OrderSubmitCommand, len(messages))
	for index, message := range messages {
		envelope, err := decodeEnvelope(message.Data)
		if err != nil {
			results[index] = err
			continue
		}
		payload, err := decodeOrderCommand(envelope)
		if err != nil {
			results[index] = err
			continue
		}
		envelopes[index], payloads[index] = envelope, payload
	}
	commitErr := worker.store.UpdateTracked(func(state *persistedState) error {
		for index, envelope := range envelopes {
			if envelope != nil {
				results[index] = nil
			}
		}
		for index, envelope := range envelopes {
			if envelope == nil {
				continue
			}
			if err := validateOrderProjection(state, envelope, payloads[index]); err != nil {
				results[index] = err
				continue
			}
			if err := worker.applyOrderCommand(state, envelope, payloads[index]); err != nil {
				results[index] = err
				return err
			}
		}
		return nil
	})
	if commitErr != nil {
		for index, envelope := range envelopes {
			if envelope != nil && !errors.Is(results[index], errCheckoutProjectionLag) {
				results[index] = commitErr
			}
		}
	}
	return results
}

func (worker *checkoutWorker) handleEventMessage(message *nats.Msg) error {
	return worker.handleEventMessages([]*nats.Msg{message})[0]
}

func (worker *checkoutWorker) handleEventMessages(messages []*nats.Msg) []error {
	results := make([]error, len(messages))
	envelopes := make([]*commonv1.MessageEnvelope, len(messages))
	for index, message := range messages {
		envelope, err := decodeEnvelope(message.Data)
		if err != nil {
			results[index] = err
			continue
		}
		if isCheckoutSagaEvent(message.Subject) {
			envelopes[index] = envelope
		}
	}
	commitErr := worker.store.UpdateTracked(func(state *persistedState) error {
		for index, envelope := range envelopes {
			if envelope != nil {
				results[index] = nil
			}
		}
		for index, envelope := range envelopes {
			if envelope == nil {
				continue
			}
			if err := worker.applySagaEvent(state, messages[index].Subject, envelope); err != nil {
				results[index] = err
				return err
			}
		}
		return nil
	})
	if commitErr != nil {
		for index, envelope := range envelopes {
			if envelope != nil {
				results[index] = commitErr
			}
		}
	}
	return results
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
	if envelope.SchemaVersion != 1 || envelope.MessageId == "" || envelope.Data == nil {
		return nil, errors.New("unsupported or incomplete envelope")
	}
	return envelope, nil
}

func (worker *checkoutWorker) relayOutbox() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-worker.stop:
			return
		case <-ticker.C:
		}
		type pendingPublish struct {
			message outboxMessage
			future  nats.PubAckFuture
			entry   *logrus.Entry
		}
		pending := make([]pendingPublish, 0, outboxPublishBatchSize)
		for _, message := range worker.store.Outbox() {
			if len(pending) == outboxPublishBatchSize {
				break
			}
			correlationID, _ := checkoutMessageContext(message.Data)
			kind := checkoutMessageKind(message.Subject)
			entry := log.WithFields(logrus.Fields{
				"topic":          message.Subject,
				"message_kind":   kind,
				"message_id":     message.MessageID,
				"correlation_id": correlationID,
			})
			out := &nats.Msg{Subject: message.Subject, Data: message.Data, Header: nats.Header{}}
			out.Header.Set("Nats-Msg-Id", message.MessageID)
			out.Header.Set("Content-Type", "application/protobuf")
			future, err := worker.js.PublishMsgAsync(out, nats.MsgId(message.MessageID))
			if err != nil {
				entry.WithError(err).Warn("checkout outbox publish failed")
				break
			}
			pending = append(pending, pendingPublish{message: message, future: future, entry: entry})
		}
		published := make([]string, 0, len(pending))
		deadline := time.NewTimer(worker.publishTimeout)
		timedOut := false
		for _, publish := range pending {
			select {
			case <-publish.future.Ok():
				publish.entry.Debug("NATS " + checkoutMessageKind(publish.message.Subject) + " sent")
				published = append(published, publish.message.MessageID)
			case err := <-publish.future.Err():
				publish.entry.WithError(err).Warn("checkout outbox publish failed")
			case <-deadline.C:
				publish.entry.Warn("checkout outbox publish acknowledgement timed out")
				timedOut = true
			}
			if timedOut {
				break
			}
		}
		if !deadline.Stop() && !timedOut {
			select {
			case <-deadline.C:
			default:
			}
		}
		if err := worker.store.RemoveOutboxBatch(published); err != nil {
			log.WithError(err).Error("checkout outbox batch removal failed")
		}
	}
}

func (worker *checkoutWorker) scanDeadlines() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-worker.stop:
			return
		case now := <-ticker.C:
			_ = worker.store.UpdateTracked(func(state *persistedState) error {
				for _, saga := range state.Orders {
					if saga.Deadline.IsZero() || now.Before(saga.Deadline) {
						continue
					}
					state.markOrder(saga.OrderID)
					cause := stableID("timeout", saga.OrderID, fmt.Sprint(saga.Version))
					previousStage, deadline := saga.Stage, saga.Deadline
					saga.Version++
					if err := queueEnvelope(state, "boutique.evt.order.step-timed-out.v1", "boutique.order.StepTimedOut.v1", "order", saga.OrderID,
						saga.Version, saga.OrderID, cause, &eventsv1.OrderStepTimedOutEvent{OrderId: saga.OrderID, WaitingStage: previousStage,
							Deadline: googleTimestamp(deadline), LastCommandId: cause, ChosenAction: "manual-review"}); err != nil {
						return err
					}
					if err := worker.manualReview(state, saga, cause, previousStage, "STEP_TIMEOUT"); err != nil {
						return err
					}
				}
				return nil
			})
		}
	}
}

func googleTimestamp(value time.Time) *timestamppb.Timestamp { return timestamppb.New(value) }

func (worker *checkoutWorker) Ready() bool {
	return worker.ready.Load() && worker.nc.IsConnected() && worker.store.Ready()
}

func (worker *checkoutWorker) Close() error {
	var result error
	worker.closeOnce.Do(func() {
		worker.ready.Store(false)
		close(worker.stop)
		result = worker.nc.Drain()
		worker.wg.Wait()
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
