// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	"github.com/GoogleCloudPlatform/microservices-demo/src/storefrontprojectionservice/internal/storefront"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const projectionDurable = "storefront-projection-v1"

const (
	projectionFetchSize   = 1024
	projectionParallelism = 128
	projectionMaxPending  = 4096
)

var projectionFilterSubjects = []string{
	"boutique.evt.catalog.>",
	"boutique.evt.currency.rates-updated.v1",
	"boutique.evt.cart.>",
	"boutique.evt.storefront.operation-accepted.v1",
	"boutique.evt.recommendation.>",
	"boutique.evt.ad.>",
	"boutique.evt.shipping.cart-quote-updated.v1",
	"boutique.evt.shipping.cart-quote-failed.v1",
	"boutique.evt.order.>",
	"boutique.evt.notification.order-confirmation-sent.v1",
	"boutique.evt.notification.order-confirmation-failed.v1",
}

type projector struct {
	js         nats.JetStreamContext
	products   projectionKV
	carts      projectionKV
	context    projectionKV
	operations projectionKV
	orders     projectionKV

	kvConflictRetries atomic.Uint64
	staleEventSkips   atomic.Uint64
	queryRevision     atomic.Uint64
	lastProjectedUnix atomic.Int64
}

// projectionKV keeps the authoritative query surface deliberately small and
// makes CAS behavior independently testable without an in-process cache.
type projectionKV interface {
	Get(string) (nats.KeyValueEntry, error)
	Create(string, []byte) (uint64, error)
	Update(string, []byte, uint64) (uint64, error)
	Keys(...nats.WatchOpt) ([]string, error)
}

func newProjector(js nats.JetStreamContext) (*projector, error) {
	products, err := js.KeyValue("STOREFRONT_PRODUCTS")
	if err != nil {
		return nil, fmt.Errorf("open product KV: %w", err)
	}
	carts, err := js.KeyValue("STOREFRONT_CARTS")
	if err != nil {
		return nil, fmt.Errorf("open cart KV: %w", err)
	}
	context, err := js.KeyValue("STOREFRONT_CONTEXT")
	if err != nil {
		return nil, fmt.Errorf("open context KV: %w", err)
	}
	operations, err := js.KeyValue("STOREFRONT_OPERATIONS")
	if err != nil {
		return nil, fmt.Errorf("open operations KV: %w", err)
	}
	orders, err := js.KeyValue("STOREFRONT_ORDERS")
	if err != nil {
		return nil, fmt.Errorf("open orders KV: %w", err)
	}
	return &projector{js: js, products: products, carts: carts, context: context, operations: operations, orders: orders}, nil
}

func (p *projector) subscribe() (*nats.Subscription, bool, error) {
	rebuilding := false
	if _, err := p.products.Get(storefront.CatalogKey); errors.Is(err, nats.ErrKeyNotFound) {
		rebuilding = true
	}
	if err := p.ensureProjectionConsumer(); err != nil {
		return nil, rebuilding, err
	}
	subscription, err := p.js.PullSubscribe(
		"",
		projectionDurable,
		nats.Bind("BOUTIQUE_EVENTS", projectionDurable),
	)
	if err != nil {
		return nil, rebuilding, fmt.Errorf("bind projection consumer: %w", err)
	}
	return subscription, rebuilding, nil
}

func (p *projector) ensureProjectionConsumer() error {
	config := &nats.ConsumerConfig{
		Durable:        projectionDurable,
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		MaxDeliver:     10,
		MaxAckPending:  projectionMaxPending,
		FilterSubjects: append([]string(nil), projectionFilterSubjects...),
	}
	for attempt := 0; attempt < 20; attempt++ {
		info, err := p.js.ConsumerInfo("BOUTIQUE_EVENTS", projectionDurable)
		if errors.Is(err, nats.ErrConsumerNotFound) {
			if _, addErr := p.js.AddConsumer("BOUTIQUE_EVENTS", config); addErr == nil {
				return nil
			} else if isConsumerSetupRace(addErr) {
				projectionBackoff(attempt)
				continue
			} else {
				return fmt.Errorf("create projection consumer: %w", addErr)
			}
		}
		if err != nil {
			return fmt.Errorf("inspect projection consumer: %w", err)
		}
		if projectionFiltersMatch(info.Config.FilterSubject, info.Config.FilterSubjects) &&
			info.Config.MaxAckPending == projectionMaxPending &&
			info.Config.AckPolicy == nats.AckExplicitPolicy &&
			info.Config.AckWait == 30*time.Second &&
			info.Config.MaxDeliver == 10 &&
			info.Config.DeliverPolicy == nats.DeliverAllPolicy {
			return nil
		}
		next := info.Config
		next.FilterSubject = ""
		next.FilterSubjects = append([]string(nil), projectionFilterSubjects...)
		next.MaxAckPending = projectionMaxPending
		next.AckPolicy = nats.AckExplicitPolicy
		next.AckWait = 30 * time.Second
		next.MaxDeliver = 10
		if _, updateErr := p.js.UpdateConsumer("BOUTIQUE_EVENTS", &next); updateErr == nil {
			return nil
		} else if isConsumerSetupRace(updateErr) {
			projectionBackoff(attempt)
			continue
		} else {
			return fmt.Errorf("update projection consumer: %w", updateErr)
		}
	}
	return fmt.Errorf("projection consumer setup conflicted too many times")
}

func isConsumerSetupRace(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return errors.Is(err, nats.ErrConsumerNotFound) ||
		strings.Contains(message, "consumer already exists") ||
		strings.Contains(message, "consumer name already in use") ||
		strings.Contains(message, "stream sequence")
}

func (p *projector) run(subscription *nats.Subscription, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		batch, err := subscription.FetchBatch(
			projectionFetchSize,
			nats.MaxWait(time.Second),
		)
		if err != nil {
			if !projectionConsumerStopped(err) {
				log.Printf("projection fetch failed: %v", err)
				time.Sleep(time.Second)
			}
			continue
		}
		p.applyStream(batch.Messages())
		if err := batch.Error(); err != nil && !projectionConsumerStopped(err) {
			log.Printf("projection stream failed: %v", err)
			time.Sleep(time.Second)
		}
	}
}

type projectionMessage struct {
	message       *nats.Msg
	correlationID string
	messageID     string
	publishedAt   time.Time
}

func (p *projector) applyStream(messages <-chan *nats.Msg) {
	queueDepth := projectionFetchSize/projectionParallelism + 1
	lanes := make([]chan projectionMessage, projectionParallelism)
	var running sync.WaitGroup
	for index := range lanes {
		lane := make(chan projectionMessage, queueDepth)
		lanes[index] = lane
		running.Add(1)
		go func(messages <-chan projectionMessage) {
			defer running.Done()
			for message := range messages {
				p.applyMessage(message)
			}
		}(lane)
	}

	received := 0
	groups := make(map[string]struct{})
	for message := range messages {
		if !projectionHandlesSubject(message.Subject) {
			if err := message.Ack(); err != nil {
				log.Printf("ignored projection event acknowledgement failed topic=%q error=%v", message.Subject, err)
			}
			continue
		}
		correlationID, messageID := projectionMessageContext(message.Data)
		// Stateless handlers intentionally retain a causal occurrence time in
		// result envelopes. JetStream's stored timestamp is immutable too, and
		// unlike the causal time includes time spent waiting in upstream queues.
		publishedAt := time.Now().UTC()
		if metadata, err := message.Metadata(); err == nil && !metadata.Timestamp.IsZero() {
			publishedAt = metadata.Timestamp.UTC()
		}
		item := projectionMessage{
			message: message, correlationID: correlationID, messageID: messageID, publishedAt: publishedAt,
		}
		received++
		groups[correlationID] = struct{}{}
		lanes[projectionMessageLane(correlationID, len(lanes))] <- item
	}
	for _, lane := range lanes {
		close(lane)
	}
	running.Wait()
	if received != 0 {
		slog.Debug("NATS projection batch received", "message_kind", "event",
			"messages", received, "correlation_groups", len(groups))
	}
}

func (p *projector) applyMessage(item projectionMessage) {
	if err := p.apply(item.message.Subject, item.message.Data, item.publishedAt); err != nil {
		log.Printf("projection event processing failed topic=%q message_id=%q correlation_id=%q error=%v",
			item.message.Subject, item.messageID, item.correlationID, err)
		if nakErr := item.message.NakWithDelay(time.Second); nakErr != nil {
			log.Printf("projection event NAK failed topic=%q message_id=%q correlation_id=%q error=%v",
				item.message.Subject, item.messageID, item.correlationID, nakErr)
		}
		return
	}
	if err := item.message.Ack(); err != nil {
		log.Printf("projection event acknowledgement failed topic=%q message_id=%q correlation_id=%q error=%v",
			item.message.Subject, item.messageID, item.correlationID, err)
	}
}

func projectionMessageLane(correlationID string, lanes int) int {
	hash := uint32(2166136261)
	for index := 0; index < len(correlationID); index++ {
		hash ^= uint32(correlationID[index])
		hash *= 16777619
	}
	return int(hash % uint32(lanes))
}

func projectionConsumerStopped(err error) bool {
	return errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrConnectionClosed) ||
		errors.Is(err, nats.ErrBadSubscription) ||
		errors.Is(err, nats.ErrSubscriptionClosed)
}

func projectionFiltersMatch(single string, multiple []string) bool {
	if single != "" || len(multiple) != len(projectionFilterSubjects) {
		return false
	}
	wanted := make(map[string]struct{}, len(projectionFilterSubjects))
	for _, subject := range projectionFilterSubjects {
		wanted[subject] = struct{}{}
	}
	for _, subject := range multiple {
		if _, ok := wanted[subject]; !ok {
			return false
		}
	}
	return true
}

func projectionHandlesSubject(subject string) bool {
	switch subject {
	case "boutique.evt.currency.rates-updated.v1",
		"boutique.evt.storefront.operation-accepted.v1",
		"boutique.evt.shipping.cart-quote-updated.v1",
		"boutique.evt.shipping.cart-quote-failed.v1",
		"boutique.evt.notification.order-confirmation-sent.v1",
		"boutique.evt.notification.order-confirmation-failed.v1":
		return true
	}
	return strings.HasPrefix(subject, "boutique.evt.catalog.") ||
		strings.HasPrefix(subject, "boutique.evt.cart.") ||
		strings.HasPrefix(subject, "boutique.evt.recommendation.") ||
		strings.HasPrefix(subject, "boutique.evt.ad.") ||
		strings.HasPrefix(subject, "boutique.evt.order.")
}

func projectionMessageContext(data []byte) (string, string) {
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

func (p *projector) apply(subject string, data []byte, publishedAt time.Time) error {
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return fmt.Errorf("decode envelope: %w", err)
	}
	if envelope.SchemaVersion != 1 || envelope.Data == nil {
		return fmt.Errorf("unsupported or empty envelope")
	}
	if strings.TrimSpace(envelope.MessageId) == "" {
		return fmt.Errorf("envelope message ID is required")
	}
	updatedAt := time.Now().UTC()
	if envelope.OccurredAt != nil && envelope.OccurredAt.IsValid() {
		updatedAt = envelope.OccurredAt.AsTime()
	}
	if publishedAt.IsZero() {
		publishedAt = time.Now().UTC()
	}
	switch subject {
	case "boutique.evt.catalog.product-upserted.v1":
		payload := &eventsv1.CatalogProductUpsertedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if payload.Product == nil {
			return fmt.Errorf("product snapshot is missing")
		}
		return updateJSON(p, p.products, storefront.ProductKey(payload.Product.ProductId), payload.Product.ProductVersion,
			func(current storefront.ProductView) uint64 { return current.Product.GetProductVersion() },
			func(current storefront.ProductView) string { return current.SourceEventID },
			envelope.MessageId,
			updatedAt,
			storefront.ProductView{ProjectionMetadata: projectionMetadata(envelope, payload.Product.ProductVersion),
				Product: payload.Product, CatalogRevision: payload.CatalogRevision, UpdatedAt: updatedAt})
	case "boutique.evt.catalog.product-removed.v1":
		payload := &eventsv1.CatalogProductRemovedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return updateJSON(p, p.products, storefront.ProductKey(payload.ProductId), payload.ProductVersion,
			func(current storefront.ProductView) uint64 { return current.Product.GetProductVersion() },
			func(current storefront.ProductView) string { return current.SourceEventID },
			envelope.MessageId,
			updatedAt,
			storefront.ProductView{
				ProjectionMetadata: projectionMetadata(envelope, payload.ProductVersion),
				Product:            &commonv1.ProductSnapshot{ProductId: payload.ProductId, ProductVersion: payload.ProductVersion},
				CatalogRevision:    payload.CatalogRevision,
				Removed:            true,
				UpdatedAt:          updatedAt,
			})
	case "boutique.evt.catalog.snapshot-completed.v1":
		payload := &eventsv1.CatalogSnapshotCompletedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return updateJSON(p, p.products, storefront.CatalogKey, payload.CatalogRevision,
			func(current storefront.CatalogView) uint64 { return current.CatalogRevision },
			func(current storefront.CatalogView) string { return current.SourceEventID },
			envelope.MessageId,
			updatedAt,
			storefront.CatalogView{ProjectionMetadata: projectionMetadata(envelope, payload.CatalogRevision),
				CatalogRevision: payload.CatalogRevision, ProductCount: payload.ProductCount, Checksum: payload.Checksum, UpdatedAt: updatedAt})
	case "boutique.evt.currency.rates-updated.v1":
		payload := &eventsv1.CurrencyRatesUpdatedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.CurrencyView{ProjectionMetadata: projectionMetadata(envelope, payload.RateRevision),
			BaseCurrencyCode: payload.BaseCurrencyCode, RateRevision: payload.RateRevision, UpdatedAt: updatedAt}
		if payload.EffectiveAt != nil {
			view.EffectiveSeconds, view.EffectiveNanos = payload.EffectiveAt.Seconds, payload.EffectiveAt.Nanos
		}
		for _, rate := range payload.Rates {
			view.Rates = append(view.Rates, storefront.Rate{CurrencyCode: rate.CurrencyCode, UnitsPerBase: rate.UnitsPerBase})
		}
		return updateJSON(p, p.products, storefront.CurrencyKey, payload.RateRevision,
			func(current storefront.CurrencyView) uint64 { return current.RateRevision },
			func(current storefront.CurrencyView) string { return current.SourceEventID },
			envelope.MessageId, updatedAt, view)
	case "boutique.evt.cart.item-added.v1":
		payload := &eventsv1.CartItemAddedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if err := p.updateCart(payload.Cart, updatedAt, projectionMetadata(envelope, payload.Cart.GetCartVersion())); err != nil {
			return err
		}
		return p.updateOperation(storefront.OperationView{
			ProjectionMetadata: projectionMetadata(envelope, payload.Cart.GetCartVersion()),
			OperationID:        payload.CommandId, CommandID: payload.CommandId, Kind: "cart.add-item",
			Status: "SUCCEEDED", UserID: payload.UserId,
			CartVersion: payload.Cart.GetCartVersion(), UpdatedAt: updatedAt,
		})
	case "boutique.evt.cart.cleared.v1":
		payload := &eventsv1.CartClearedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		source := projectionMetadata(envelope, payload.Cart.GetCartVersion())
		if err := p.updateCart(payload.Cart, updatedAt, source); err != nil {
			return err
		}
		if err := p.updateOperation(storefront.OperationView{
			ProjectionMetadata: source,
			OperationID:        payload.CommandId, CommandID: payload.CommandId, Kind: "cart.clear",
			Status: "SUCCEEDED", UserID: payload.UserId,
			CartVersion: payload.Cart.GetCartVersion(), UpdatedAt: updatedAt,
		}); err != nil {
			return err
		}
		if payload.OrderId != "" {
			return p.updateOrderSettlement(payload.OrderId, "SUCCEEDED", "", updatedAt, source)
		}
		return nil
	case "boutique.evt.cart.command-rejected.v1":
		payload := &eventsv1.CartCommandRejectedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.OperationView{
			ProjectionMetadata: projectionMetadata(envelope, payload.CurrentCartVersion),
			OperationID:        payload.CommandId, CommandID: payload.CommandId, Status: "REJECTED",
			UserID: payload.UserId, CartVersion: payload.CurrentCartVersion, UpdatedAt: updatedAt,
		}
		if payload.Failure != nil {
			view.FailureCode = payload.Failure.Code
			view.Retryable = payload.Failure.Retryable
			view.SafeMessage = payload.Failure.SafeMessage
		}
		if err := p.updateOperation(view); err != nil {
			return err
		}
		// Checkout uses the order ID as the correlation ID for its independent
		// cart-clear command. User-initiated cart operations correlate to their
		// own command ID and must not create an order settlement record.
		if envelope.CorrelationId != "" && envelope.CorrelationId != payload.CommandId {
			return p.updateOrderSettlement(envelope.CorrelationId, "REJECTED", view.FailureCode, updatedAt, view.ProjectionMetadata)
		}
		return nil
	case "boutique.evt.storefront.operation-accepted.v1":
		payload := &eventsv1.StorefrontOperationAcceptedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if err := p.updateOperation(storefront.OperationView{
			ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
			OperationID:        payload.OperationId, CommandID: payload.CommandId, Kind: payload.OperationKind,
			Status: payload.Status, UserID: payload.UserOrSessionId, UpdatedAt: updatedAt,
		}); err != nil {
			return err
		}
		if payload.OperationKind == "order.submit" {
			return p.updateOrder(storefront.OrderView{ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
				OrderID: payload.OperationId, UserID: payload.UserOrSessionId,
				Status: "QUEUED", Stage: "QUEUED", UpdatedAt: updatedAt})
		}
		return nil
	case "boutique.evt.recommendation.generated.v1":
		payload := &eventsv1.RecommendationGeneratedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.RecommendationView{
			ProjectionMetadata: projectionMetadata(envelope, payload.TriggeringContextVersion),
			SessionID:          payload.SessionId, ContextVersion: payload.TriggeringContextVersion,
			ProductIDs: append([]string(nil), payload.ProductIds...), UpdatedAt: updatedAt,
		}
		if payload.ExpiresAt != nil && payload.ExpiresAt.IsValid() {
			view.ExpiresAt = payload.ExpiresAt.AsTime()
		}
		return updateJSON(p, p.context, storefront.RecommendationKey(payload.SessionId), payload.TriggeringContextVersion,
			func(current storefront.RecommendationView) uint64 { return current.ContextVersion },
			func(current storefront.RecommendationView) string { return current.SourceEventID },
			envelope.MessageId, updatedAt, view)
	case "boutique.evt.recommendation.generation-failed.v1":
		payload := &eventsv1.RecommendationGenerationFailedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.RecommendationView{ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
			SessionID: payload.SessionId, ContextVersion: envelope.AggregateVersion, UpdatedAt: updatedAt}
		if payload.Failure != nil {
			view.FailureCode = payload.Failure.Code
		}
		return updateJSON(p, p.context, storefront.RecommendationKey(payload.SessionId), envelope.AggregateVersion,
			func(current storefront.RecommendationView) uint64 { return current.ContextVersion },
			func(current storefront.RecommendationView) string { return current.SourceEventID },
			envelope.MessageId, updatedAt, view)
	case "boutique.evt.ad.selection-generated.v1":
		payload := &eventsv1.AdSelectionGeneratedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.AdView{
			ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
			SessionID:          payload.SessionId, PageType: payload.TriggeringPageType,
			ContextVersion: envelope.AggregateVersion, UpdatedAt: updatedAt,
		}
		for _, ad := range payload.Ads {
			view.Ads = append(view.Ads, storefront.Ad{RedirectURL: ad.RedirectUrl, Text: ad.Text})
		}
		if payload.ExpiresAt != nil && payload.ExpiresAt.IsValid() {
			view.ExpiresAt = payload.ExpiresAt.AsTime()
		}
		return updateJSON(p, p.context, storefront.AdKey(payload.SessionId), envelope.AggregateVersion,
			func(current storefront.AdView) uint64 { return current.ContextVersion },
			func(current storefront.AdView) string { return current.SourceEventID },
			envelope.MessageId, updatedAt, view)
	case "boutique.evt.shipping.cart-quote-updated.v1":
		payload := &eventsv1.ShippingCartQuoteUpdatedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.CartQuoteView{
			ProjectionMetadata: projectionMetadata(envelope, payload.CartVersion),
			UserID:             payload.UserId, CartVersion: payload.CartVersion, CostUSD: payload.CostUsd, UpdatedAt: updatedAt,
		}
		if payload.ExpiresAt != nil && payload.ExpiresAt.IsValid() {
			view.ExpiresAt = payload.ExpiresAt.AsTime()
		}
		return updateJSON(p, p.context, storefront.CartQuoteKey(payload.UserId), payload.CartVersion,
			func(current storefront.CartQuoteView) uint64 { return current.CartVersion },
			func(current storefront.CartQuoteView) string { return current.SourceEventID },
			envelope.MessageId, updatedAt, view)
	case "boutique.evt.shipping.cart-quote-failed.v1":
		payload := &eventsv1.ShippingCartQuoteFailedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.CartQuoteView{ProjectionMetadata: projectionMetadata(envelope, payload.CartVersion),
			UserID: payload.UserId, CartVersion: payload.CartVersion, UpdatedAt: updatedAt}
		if payload.Failure != nil {
			view.FailureCode = payload.Failure.Code
		}
		return updateJSON(p, p.context, storefront.CartQuoteKey(payload.UserId), payload.CartVersion,
			func(current storefront.CartQuoteView) uint64 { return current.CartVersion },
			func(current storefront.CartQuoteView) string { return current.SourceEventID },
			envelope.MessageId, updatedAt, view)
	case "boutique.evt.order.submitted.v1":
		payload := &eventsv1.OrderSubmittedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if payload.Order == nil {
			return fmt.Errorf("submitted order snapshot is missing")
		}
		return p.updateOrder(storefront.OrderView{ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
			OrderID: payload.Order.OrderId, UserID: payload.Order.UserId,
			Status: "PROCESSING", Stage: "WAITING_FOR_QUOTE", Snapshot: payload.Order, AggregateVersion: envelope.AggregateVersion, UpdatedAt: updatedAt})
	case "boutique.evt.order.processing-stage-changed.v1":
		payload := &eventsv1.OrderProcessingStageChangedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		status := "PROCESSING"
		if payload.Stage == "COMPLETED" {
			status = "COMPLETED"
		} else if payload.Stage == "CANCELLED" {
			status = "CANCELLED"
		} else if payload.Stage == "MANUAL_REVIEW" {
			status = "MANUAL_REVIEW"
		}
		return p.updateOrder(storefront.OrderView{ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
			OrderID: payload.OrderId, Status: status, Stage: payload.Stage,
			AggregateVersion: envelope.AggregateVersion, OutcomeAt: terminalOrderOutcomeAt(status, publishedAt), UpdatedAt: updatedAt})
	case "boutique.evt.order.rejected.v1":
		payload := &eventsv1.OrderRejectedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.OrderView{ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
			OrderID: payload.OrderId, UserID: p.operationUser(payload.OperationId), Status: "REJECTED", Stage: "REJECTED",
			AggregateVersion: envelope.AggregateVersion, OutcomeAt: terminalOrderOutcomeAt("REJECTED", publishedAt), UpdatedAt: updatedAt}
		applyOrderFailure(&view, payload.Failure)
		return p.updateOrder(view)
	case "boutique.evt.order.completed.v1":
		payload := &eventsv1.OrderCompletedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if payload.Order == nil {
			return fmt.Errorf("completed order snapshot is missing")
		}
		return p.updateOrder(storefront.OrderView{ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
			OrderID: payload.Order.OrderId, UserID: payload.Order.UserId, Status: "COMPLETED", Stage: "COMPLETED",
			Snapshot: payload.Order, AggregateVersion: envelope.AggregateVersion, OutcomeAt: terminalOrderOutcomeAt("COMPLETED", publishedAt), UpdatedAt: updatedAt})
	case "boutique.evt.order.cancelled.v1":
		payload := &eventsv1.OrderCancelledEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.OrderView{ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
			OrderID: payload.OrderId, Status: "CANCELLED", Stage: "CANCELLED", AggregateVersion: envelope.AggregateVersion,
			OutcomeAt: terminalOrderOutcomeAt("CANCELLED", publishedAt), UpdatedAt: updatedAt}
		applyOrderFailure(&view, payload.Failure)
		return p.updateOrder(view)
	case "boutique.evt.order.manual-review-required.v1":
		payload := &eventsv1.OrderManualReviewRequiredEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return p.updateOrder(storefront.OrderView{ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
			OrderID: payload.OrderId, Status: "MANUAL_REVIEW", Stage: "MANUAL_REVIEW", FailureCode: payload.FailedCompensation,
			SafeMessage: "The order requires manual review.", AggregateVersion: envelope.AggregateVersion,
			OutcomeAt: terminalOrderOutcomeAt("MANUAL_REVIEW", publishedAt), UpdatedAt: updatedAt})
	case "boutique.evt.order.step-timed-out.v1":
		payload := &eventsv1.OrderStepTimedOutEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return p.updateOrder(storefront.OrderView{ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
			OrderID: payload.OrderId, Status: "PROCESSING", Stage: "TIMED_OUT_" + payload.WaitingStage,
			FailureCode: "STEP_TIMEOUT", AggregateVersion: envelope.AggregateVersion, UpdatedAt: updatedAt})
	case "boutique.evt.notification.order-confirmation-sent.v1":
		payload := &eventsv1.NotificationOrderConfirmationSentEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return p.updateOrder(storefront.OrderView{ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
			OrderID: payload.OrderId, NotificationStatus: "SENT", AggregateVersion: envelope.AggregateVersion, UpdatedAt: updatedAt})
	case "boutique.evt.notification.order-confirmation-failed.v1":
		payload := &eventsv1.NotificationOrderConfirmationFailedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return p.updateOrder(storefront.OrderView{ProjectionMetadata: projectionMetadata(envelope, envelope.AggregateVersion),
			OrderID: payload.OrderId, NotificationStatus: "FAILED", AggregateVersion: envelope.AggregateVersion, UpdatedAt: updatedAt})
	default:
		return nil
	}
}

func applyOrderFailure(view *storefront.OrderView, failure *commonv1.Failure) {
	if failure == nil {
		return
	}
	view.FailureCode = failure.Code
	view.Retryable = failure.Retryable
	view.SafeMessage = failure.SafeMessage
}

func (p *projector) operationUser(operationID string) string {
	key := storefront.OperationKey(operationID)
	operation, err := getJSON[storefront.OperationView](p.operations, key)
	if err != nil {
		return ""
	}
	return operation.UserID
}

func (p *projector) updateOrder(incoming storefront.OrderView) error {
	if incoming.OrderID == "" {
		return fmt.Errorf("order identity is incomplete")
	}
	key := storefront.OrderKey(incoming.OrderID)
	for attempt := 0; attempt < 20; attempt++ {
		entry, err := p.orders.Get(key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			encoded, marshalErr := json.Marshal(incoming)
			if marshalErr != nil {
				return marshalErr
			}
			if _, err := p.orders.Create(key, encoded); err == nil {
				p.recordProjected(incoming.UpdatedAt)
				return nil
			} else if !isKVConflict(err) {
				return err
			}
			p.recordConflict(attempt)
			continue
		}
		if err != nil {
			return err
		}
		var current storefront.OrderView
		if err := json.Unmarshal(entry.Value(), &current); err != nil {
			return err
		}
		if projectionApplied(current.ProjectionMetadata, incoming.SourceEventID) {
			p.staleEventSkips.Add(1)
			return nil
		}
		if incoming.AggregateVersion < current.AggregateVersion {
			if current.UserID != "" || incoming.UserID == "" {
				p.staleEventSkips.Add(1)
				return nil
			}
			incoming.AggregateVersion = current.AggregateVersion
			incoming.Status = current.Status
			incoming.Stage = current.Stage
			incoming.Snapshot = current.Snapshot
		}
		next := mergeOrder(current, incoming)
		encoded, err := json.Marshal(next)
		if err != nil {
			return err
		}
		if _, err := p.orders.Update(key, encoded, entry.Revision()); err == nil {
			p.recordProjected(next.UpdatedAt)
			return nil
		} else if !isKVConflict(err) {
			return err
		}
		p.recordConflict(attempt)
	}
	return fmt.Errorf("order KV update conflicted too many times for %s", key)
}

func mergeOrder(current, incoming storefront.OrderView) storefront.OrderView {
	incoming.ProjectionMetadata = mergeProjectionMetadata(
		current.ProjectionMetadata,
		incoming.ProjectionMetadata,
	)
	if incoming.UserID == "" {
		incoming.UserID = current.UserID
	}
	if incoming.Snapshot == nil {
		incoming.Snapshot = current.Snapshot
	}
	if incoming.Status == "" {
		incoming.Status = current.Status
	}
	if incoming.Stage == "" {
		incoming.Stage = current.Stage
	}
	if incoming.FailureCode == "" {
		incoming.FailureCode = current.FailureCode
		incoming.Retryable = current.Retryable
		incoming.SafeMessage = current.SafeMessage
	}
	if incoming.NotificationStatus == "" {
		incoming.NotificationStatus = current.NotificationStatus
	}
	if incoming.CartClearStatus == "" {
		incoming.CartClearStatus = current.CartClearStatus
		incoming.CartClearFailureCode = current.CartClearFailureCode
	}
	if incoming.AggregateVersion < current.AggregateVersion {
		incoming.AggregateVersion = current.AggregateVersion
	}
	if current.OutcomeAt != nil {
		incoming.OutcomeAt = current.OutcomeAt
	}
	if current.UpdatedAt.After(incoming.UpdatedAt) {
		incoming.UpdatedAt = current.UpdatedAt
	}
	if terminalOrderStatus(current.Status) && !terminalOrderStatus(incoming.Status) {
		incoming.Status = current.Status
		incoming.Stage = current.Stage
	}
	return incoming
}

func terminalOrderStatus(status string) bool {
	return status == "COMPLETED" || status == "CANCELLED" || status == "REJECTED" || status == "MANUAL_REVIEW"
}

func terminalOrderOutcomeAt(status string, value time.Time) *time.Time {
	if !terminalOrderStatus(status) {
		return nil
	}
	outcomeAt := value.UTC()
	return &outcomeAt
}

func (p *projector) updateOrderSettlement(
	orderID, status, failureCode string,
	updatedAt time.Time,
	source storefront.ProjectionMetadata,
) error {
	if orderID == "" || status == "" {
		return fmt.Errorf("order settlement identity is incomplete")
	}
	key := storefront.OrderKey(orderID)
	for attempt := 0; attempt < 20; attempt++ {
		entry, err := p.orders.Get(key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			view := storefront.OrderView{
				ProjectionMetadata: source,
				OrderID:            orderID, CartClearStatus: status, CartClearFailureCode: failureCode, UpdatedAt: updatedAt,
			}
			encoded, marshalErr := json.Marshal(view)
			if marshalErr != nil {
				return marshalErr
			}
			if _, err := p.orders.Create(key, encoded); err == nil {
				p.recordProjected(view.UpdatedAt)
				return nil
			} else if !isKVConflict(err) {
				return err
			}
			p.recordConflict(attempt)
			continue
		}
		if err != nil {
			return err
		}
		var current storefront.OrderView
		if err := json.Unmarshal(entry.Value(), &current); err != nil {
			return err
		}
		if projectionApplied(current.ProjectionMetadata, source.SourceEventID) {
			p.staleEventSkips.Add(1)
			return nil
		}
		current.CartClearStatus = status
		current.CartClearFailureCode = failureCode
		current.ProjectionMetadata = mergeProjectionMetadata(
			current.ProjectionMetadata,
			source,
		)
		if updatedAt.After(current.UpdatedAt) {
			current.UpdatedAt = updatedAt
		}
		encoded, err := json.Marshal(current)
		if err != nil {
			return err
		}
		if _, err := p.orders.Update(key, encoded, entry.Revision()); err == nil {
			p.recordProjected(current.UpdatedAt)
			return nil
		} else if !isKVConflict(err) {
			return err
		}
		p.recordConflict(attempt)
	}
	return fmt.Errorf("order settlement KV update conflicted too many times for %s", key)
}

func (p *projector) updateOperation(incoming storefront.OperationView) error {
	if incoming.OperationID == "" {
		incoming.OperationID = incoming.CommandID
	}
	if incoming.OperationID == "" || incoming.CommandID == "" || incoming.UserID == "" {
		return fmt.Errorf("operation identity is incomplete")
	}
	key := storefront.OperationKey(incoming.OperationID)
	for attempt := 0; attempt < 20; attempt++ {
		entry, err := p.operations.Get(key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			encoded, marshalErr := json.Marshal(incoming)
			if marshalErr != nil {
				return marshalErr
			}
			if _, err := p.operations.Create(key, encoded); err == nil {
				p.recordProjected(incoming.UpdatedAt)
				return nil
			} else if !isKVConflict(err) {
				return err
			}
			p.recordConflict(attempt)
			continue
		}
		if err != nil {
			return err
		}
		var current storefront.OperationView
		if err := json.Unmarshal(entry.Value(), &current); err != nil {
			return err
		}
		if projectionApplied(current.ProjectionMetadata, incoming.SourceEventID) {
			p.staleEventSkips.Add(1)
			return nil
		}
		next := mergeOperation(current, incoming)
		encoded, err := json.Marshal(next)
		if err != nil {
			return err
		}
		if _, err := p.operations.Update(key, encoded, entry.Revision()); err == nil {
			p.recordProjected(next.UpdatedAt)
			return nil
		} else if !isKVConflict(err) {
			return err
		}
		p.recordConflict(attempt)
	}
	return fmt.Errorf("operation KV update conflicted too many times for %s", key)
}

func mergeOperation(current, incoming storefront.OperationView) storefront.OperationView {
	incoming.ProjectionMetadata = mergeProjectionMetadata(
		current.ProjectionMetadata,
		incoming.ProjectionMetadata,
	)
	currentTerminal := current.Status == "SUCCEEDED" || current.Status == "REJECTED"
	incomingTerminal := incoming.Status == "SUCCEEDED" || incoming.Status == "REJECTED"
	if currentTerminal && !incomingTerminal {
		if current.Kind == "" {
			current.Kind = incoming.Kind
		}
		if current.CommandID == "" {
			current.CommandID = incoming.CommandID
		}
		current.ProjectionMetadata = incoming.ProjectionMetadata
		if incoming.UpdatedAt.After(current.UpdatedAt) {
			current.UpdatedAt = incoming.UpdatedAt
		}
		return current
	}
	if currentTerminal && incomingTerminal {
		current.ProjectionMetadata = incoming.ProjectionMetadata
		if incoming.UpdatedAt.After(current.UpdatedAt) {
			current.UpdatedAt = incoming.UpdatedAt
		}
		return current
	}
	if incoming.Kind == "" {
		incoming.Kind = current.Kind
	}
	if current.UpdatedAt.After(incoming.UpdatedAt) {
		incoming.UpdatedAt = current.UpdatedAt
	}
	return incoming
}

func (p *projector) updateCart(
	cart *commonv1.CartSnapshot,
	updatedAt time.Time,
	source storefront.ProjectionMetadata,
) error {
	if cart == nil || cart.UserId == "" {
		return fmt.Errorf("cart snapshot is missing user ID")
	}
	key := cart.UserId
	next := storefront.CartView{ProjectionMetadata: source, Cart: cart, UpdatedAt: updatedAt}
	encoded, err := json.Marshal(next)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 20; attempt++ {
		entry, err := p.carts.Get(key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			if _, err := p.carts.Create(key, encoded); err == nil {
				p.recordProjected(next.UpdatedAt)
				return nil
			} else if !isKVConflict(err) {
				return err
			}
			p.recordConflict(attempt)
			continue
		}
		if err != nil {
			return err
		}
		var current storefront.CartView
		if err := json.Unmarshal(entry.Value(), &current); err != nil {
			return err
		}
		if current.SourceEventID == source.SourceEventID {
			p.staleEventSkips.Add(1)
			return nil
		}
		if cart.CartVersion <= current.Cart.GetCartVersion() {
			p.staleEventSkips.Add(1)
			return nil
		}
		if _, err := p.carts.Update(key, encoded, entry.Revision()); err == nil {
			p.recordProjected(next.UpdatedAt)
			return nil
		} else if !isKVConflict(err) {
			return err
		}
		p.recordConflict(attempt)
	}
	return fmt.Errorf("cart KV update conflicted too many times for %s", key)
}

func updateJSON[T any](
	p *projector,
	bucket projectionKV,
	key string,
	incomingVersion uint64,
	currentVersion func(T) uint64,
	currentSourceEventID func(T) string,
	incomingSourceEventID string,
	occurredAt time.Time,
	next T,
) error {
	encoded, err := json.Marshal(next)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 20; attempt++ {
		entry, err := bucket.Get(key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			if _, err := bucket.Create(key, encoded); err == nil {
				p.recordProjected(occurredAt)
				return nil
			} else if !isKVConflict(err) {
				return err
			}
			p.recordConflict(attempt)
			continue
		}
		if err != nil {
			return err
		}
		var current T
		if err := json.Unmarshal(entry.Value(), &current); err != nil {
			return err
		}
		if currentSourceEventID(current) != "" && currentSourceEventID(current) == incomingSourceEventID {
			p.staleEventSkips.Add(1)
			return nil
		}
		if incomingVersion <= currentVersion(current) {
			p.staleEventSkips.Add(1)
			return nil
		}
		if _, err := bucket.Update(key, encoded, entry.Revision()); err == nil {
			p.recordProjected(occurredAt)
			return nil
		} else if !isKVConflict(err) {
			return err
		}
		p.recordConflict(attempt)
	}
	return fmt.Errorf("KV update conflicted too many times for %s", key)
}

func projectionMetadata(
	envelope *commonv1.MessageEnvelope,
	sourceVersion uint64,
) storefront.ProjectionMetadata {
	return storefront.ProjectionMetadata{
		SourceEventID:   envelope.GetMessageId(),
		SourceVersion:   sourceVersion,
		AppliedEventIDs: []string{envelope.GetMessageId()},
	}
}

func projectionApplied(metadata storefront.ProjectionMetadata, eventID string) bool {
	if eventID == "" {
		return false
	}
	if metadata.SourceEventID == eventID {
		return true
	}
	for _, applied := range metadata.AppliedEventIDs {
		if applied == eventID {
			return true
		}
	}
	return false
}

func mergeProjectionMetadata(
	current, incoming storefront.ProjectionMetadata,
) storefront.ProjectionMetadata {
	if incoming.SourceEventID == "" {
		return current
	}
	merged := incoming
	merged.AppliedEventIDs = append([]string(nil), current.AppliedEventIDs...)
	if current.SourceEventID != "" && !projectionApplied(
		storefront.ProjectionMetadata{AppliedEventIDs: merged.AppliedEventIDs},
		current.SourceEventID,
	) {
		merged.AppliedEventIDs = append(merged.AppliedEventIDs, current.SourceEventID)
	}
	if !projectionApplied(
		storefront.ProjectionMetadata{AppliedEventIDs: merged.AppliedEventIDs},
		incoming.SourceEventID,
	) {
		merged.AppliedEventIDs = append(merged.AppliedEventIDs, incoming.SourceEventID)
	}
	return merged
}

func isKVConflict(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, nats.ErrKeyExists) ||
		strings.Contains(strings.ToLower(err.Error()), "wrong last sequence")
}

func projectionBackoff(attempt int) {
	delay := 250 * time.Microsecond
	for step := 0; step < attempt && delay < 10*time.Millisecond; step++ {
		delay *= 2
	}
	if delay > 10*time.Millisecond {
		delay = 10 * time.Millisecond
	}
	jitter := time.Duration(rand.Uint64N(uint64(time.Millisecond)))
	time.Sleep(delay + jitter)
}

func (p *projector) recordConflict(attempt int) {
	p.kvConflictRetries.Add(1)
	projectionBackoff(attempt)
}

func (p *projector) recordProjected(occurredAt time.Time) {
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	candidate := occurredAt.UTC().UnixNano()
	for {
		current := p.lastProjectedUnix.Load()
		if candidate <= current || p.lastProjectedUnix.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func (p *projector) observeQueryRevision(revision uint64) {
	for {
		current := p.queryRevision.Load()
		if revision <= current || p.queryRevision.CompareAndSwap(current, revision) {
			return
		}
	}
}

func (p *projector) projectionAgeSeconds(now time.Time) float64 {
	last := p.lastProjectedUnix.Load()
	if last == 0 {
		return 0
	}
	age := now.UTC().Sub(time.Unix(0, last).UTC()).Seconds()
	if age < 0 {
		return 0
	}
	return age
}
