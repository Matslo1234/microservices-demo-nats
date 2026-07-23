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
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type checkoutWorker struct {
	store          *stateStore
	nc             *nats.Conn
	js             nats.JetStreamContext
	subscriptions  []*nats.Subscription
	publishTimeout time.Duration
	stepTimeout    time.Duration
	stop           chan struct{}
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
		handler                  func(*nats.Msg) error
	}{
		{"boutique.evt.catalog.>", "checkout-catalog-v1", "BOUTIQUE_EVENTS", worker.handleProjectionMessage},
		{"boutique.evt.currency.>", "checkout-currency-v1", "BOUTIQUE_EVENTS", worker.handleProjectionMessage},
		{"boutique.evt.cart.>", "checkout-cart-v1", "BOUTIQUE_EVENTS", worker.handleProjectionMessage},
	}
	workflowDefinitions := []struct {
		subject, durable, stream string
		handler                  func(*nats.Msg) error
	}{
		{"boutique.cmd.order.submit.v1", "checkout-order-commands-v1", "BOUTIQUE_COMMANDS", worker.handleCommandMessage},
		{"boutique.evt.shipping.>", "checkout-saga-shipping-v1", "BOUTIQUE_EVENTS", worker.handleEventMessage},
		{"boutique.evt.payment.>", "checkout-saga-payment-v1", "BOUTIQUE_EVENTS", worker.handleEventMessage},
	}
	addSubscription := func(definition struct {
		subject, durable, stream string
		handler                  func(*nats.Msg) error
	}) error {
		subscription, subscribeErr := worker.js.PullSubscribe(definition.subject, definition.durable,
			nats.BindStream(definition.stream), nats.ManualAck(), nats.AckExplicit(), nats.DeliverAll(),
			nats.AckWait(30*time.Second), nats.MaxDeliver(10), nats.MaxAckPending(64))
		if subscribeErr != nil {
			return fmt.Errorf("create %s: %w", definition.durable, subscribeErr)
		}
		worker.subscriptions = append(worker.subscriptions, subscription)
		go worker.consume(subscription, definition.handler)
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
	go worker.relayOutbox()
	go worker.scanDeadlines()
	return worker, nil
}

func (worker *checkoutWorker) consume(subscription *nats.Subscription, handler func(*nats.Msg) error) {
	for {
		select {
		case <-worker.stop:
			return
		default:
		}
		messages, err := subscription.Fetch(32, nats.MaxWait(time.Second))
		if err != nil && !errors.Is(err, nats.ErrTimeout) {
			log.WithError(err).Error("checkout consumer fetch failed")
			time.Sleep(time.Second)
			continue
		}
		for _, message := range messages {
			correlationID, messageID := checkoutMessageContext(message.Data)
			kind := checkoutMessageKind(message.Subject)
			entry := log.WithFields(logrus.Fields{
				"topic":          message.Subject,
				"message_kind":   kind,
				"message_id":     messageID,
				"correlation_id": correlationID,
			})
			entry.Debug("NATS " + kind + " received")
			if err := handler(message); err != nil {
				entry.WithError(err).Error("checkout message processing failed")
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
	envelope, err := decodeEnvelope(message.Data)
	if err != nil {
		return err
	}
	return worker.store.Update(func(state *persistedState) error {
		if _, ok := state.Inbox[envelope.MessageId]; ok {
			return nil
		}
		switch message.Subject {
		case "boutique.evt.catalog.product-upserted.v1":
			payload := &eventsv1.CatalogProductUpsertedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if payload.Product != nil {
				current := state.Products[payload.Product.ProductId]
				if current == nil || current.ProductVersion < payload.Product.ProductVersion {
					state.Products[payload.Product.ProductId] = payload.Product
				}
			}
			if payload.CatalogRevision > state.CatalogRevision {
				state.CatalogRevision = payload.CatalogRevision
			}
		case "boutique.evt.catalog.product-removed.v1":
			payload := &eventsv1.CatalogProductRemovedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			delete(state.Products, payload.ProductId)
			if payload.CatalogRevision > state.CatalogRevision {
				state.CatalogRevision = payload.CatalogRevision
			}
		case "boutique.evt.catalog.snapshot-completed.v1":
			payload := &eventsv1.CatalogSnapshotCompletedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if payload.CatalogRevision > state.CatalogRevision {
				state.CatalogRevision = payload.CatalogRevision
			}
		case "boutique.evt.currency.rates-updated.v1":
			payload := &eventsv1.CurrencyRatesUpdatedEvent{}
			if err := envelope.Data.UnmarshalTo(payload); err != nil {
				return err
			}
			if state.Rates == nil || state.Rates.RateRevision < payload.RateRevision {
				state.Rates = payload
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
		state.Inbox[envelope.MessageId] = time.Now().UTC()
		return nil
	})
}

func updateCheckoutCart(state *persistedState, cart *commonv1.CartSnapshot) {
	if cart == nil || cart.UserId == "" {
		return
	}
	current := state.Carts[cart.UserId]
	if current == nil || current.CartVersion < cart.CartVersion {
		state.Carts[cart.UserId] = cart
	}
}

func (worker *checkoutWorker) handleCommandMessage(message *nats.Msg) error {
	envelope, err := decodeEnvelope(message.Data)
	if err != nil {
		return err
	}
	return worker.handleOrderCommand(envelope)
}

func (worker *checkoutWorker) handleEventMessage(message *nats.Msg) error {
	envelope, err := decodeEnvelope(message.Data)
	if err != nil {
		return err
	}
	return worker.handleSagaEvent(message.Subject, envelope)
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
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-worker.stop:
			return
		case <-ticker.C:
		}
		for _, message := range worker.store.Outbox() {
			correlationID, _ := checkoutMessageContext(message.Data)
			kind := checkoutMessageKind(message.Subject)
			entry := log.WithFields(logrus.Fields{
				"topic":          message.Subject,
				"message_kind":   kind,
				"message_id":     message.MessageID,
				"correlation_id": correlationID,
			})
			ctx, cancel := context.WithTimeout(context.Background(), worker.publishTimeout)
			out := &nats.Msg{Subject: message.Subject, Data: message.Data, Header: nats.Header{}}
			out.Header.Set("Nats-Msg-Id", message.MessageID)
			out.Header.Set("Content-Type", "application/protobuf")
			_, err := worker.js.PublishMsg(out, nats.Context(ctx), nats.MsgId(message.MessageID))
			cancel()
			if err != nil {
				entry.WithError(err).Warn("checkout outbox publish failed")
				break
			}
			entry.Debug("NATS " + kind + " sent")
			if err := worker.store.RemoveOutbox(message.MessageID); err != nil {
				entry.WithError(err).Error("checkout outbox removal failed")
				break
			}
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
			_ = worker.store.Update(func(state *persistedState) error {
				for _, saga := range state.Orders {
					if saga.Deadline.IsZero() || now.Before(saga.Deadline) {
						continue
					}
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

func (worker *checkoutWorker) Ready() bool { return worker.ready.Load() && worker.nc.IsConnected() }

func (worker *checkoutWorker) Close() error {
	var result error
	worker.closeOnce.Do(func() { worker.ready.Store(false); close(worker.stop); result = worker.nc.Drain() })
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
