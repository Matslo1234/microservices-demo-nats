// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	shippingCartConsumer    = "shipping-cart-quotes-v1"
	shippingQuoteSubject    = "boutique.evt.shipping.cart-quote-updated.v1"
	shippingCommandConsumer = "shipping-commands-v1"
)

type shippingEventWorker struct {
	nc                  *nats.Conn
	js                  nats.JetStreamContext
	subscription        *nats.Subscription
	commandSubscription *nats.Subscription
	provider            *shippingProviderStore
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
	worker := &shippingEventWorker{publishTimeout: publishTimeout, stop: make(chan struct{})}
	storePath := os.Getenv("SHIPPING_STORE_PATH")
	if storePath == "" {
		storePath = "/tmp/shipping/provider-state.json"
	}
	worker.provider, err = openShippingProviderStore(storePath)
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
	worker.commandSubscription, err = worker.js.PullSubscribe(
		"boutique.cmd.shipping.>", shippingCommandConsumer,
		nats.BindStream("BOUTIQUE_COMMANDS"), nats.ManualAck(), nats.AckExplicit(),
		nats.DeliverAll(), nats.AckWait(30*time.Second), nats.MaxDeliver(10),
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
		messages, err := worker.commandSubscription.Fetch(16, nats.MaxWait(time.Second))
		if err != nil && err != nats.ErrTimeout {
			log.Errorf("shipping command fetch failed: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, message := range messages {
			if err := worker.handleCommand(message); err != nil {
				log.Errorf("shipping command failed: %v", err)
				_ = message.NakWithDelay(time.Second)
				continue
			}
			_ = message.Ack()
		}
	}
}

func (worker *shippingEventWorker) run() {
	for {
		select {
		case <-worker.stop:
			return
		default:
		}
		messages, err := worker.subscription.Fetch(32, nats.MaxWait(time.Second))
		if err != nil && err != nats.ErrTimeout {
			log.Errorf("shipping cart event fetch failed: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, message := range messages {
			if err := worker.handle(message); err != nil {
				log.Errorf("shipping cart event failed: %v", err)
				_ = message.NakWithDelay(time.Second)
				continue
			}
			_ = message.Ack()
		}
	}
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
	count := 0
	for _, line := range cart.Items {
		count += int(line.Quantity)
	}
	quote := CreateQuoteFromCount(count)
	now := time.Now().UTC()
	payload := &eventsv1.ShippingCartQuoteUpdatedEvent{
		UserId: cart.UserId, CartVersion: cart.CartVersion,
		CostUsd:   &commonv1.Money{CurrencyCode: "USD", Units: int64(quote.Dollars), Nanos: int32(quote.Cents * 10_000_000)},
		ExpiresAt: timestamppb.New(now.Add(15 * time.Minute)),
	}
	wrapped, err := anypb.New(payload)
	if err != nil {
		return err
	}
	messageID := shippingMessageID(shippingQuoteSubject, envelope.MessageId)
	result := &commonv1.MessageEnvelope{
		MessageId: messageID, MessageType: "boutique.shipping.CartQuoteUpdated.v1", SchemaVersion: 1,
		OccurredAt: timestamppb.New(now), Producer: "shippingservice/phase3",
		AggregateType: "cart", AggregateId: cart.UserId, AggregateVersion: cart.CartVersion,
		CorrelationId: envelope.CorrelationId, CausationId: envelope.MessageId,
		Traceparent: envelope.Traceparent, Tracestate: envelope.Tracestate, Data: wrapped,
	}
	encoded, err := proto.Marshal(result)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), worker.publishTimeout)
	defer cancel()
	out := &nats.Msg{Subject: shippingQuoteSubject, Data: encoded, Header: nats.Header{}}
	out.Header.Set("Nats-Msg-Id", messageID)
	_, err = worker.js.PublishMsg(out, nats.Context(ctx), nats.MsgId(messageID))
	return err
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

func shippingMessageID(parts ...string) string {
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
