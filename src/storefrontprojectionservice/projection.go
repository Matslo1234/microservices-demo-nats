// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
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
	telemetry "github.com/GoogleCloudPlatform/microservices-demo/src/shared/telemetry/go"
	"github.com/GoogleCloudPlatform/microservices-demo/src/storefrontprojectionservice/internal/storefront"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	projectionFetchSize                  = 1024
	projectionParallelism                = 128
	projectionMaxPending                 = 4096
	projectionMaxDeliver                 = -1
	projectionEventHistory               = 32
	personalizationProjectionParallelism = 8
	personalizationProjectionMaxPending  = 256
)

var criticalProjectionFilterSubjects = []string{
	"boutique.evt.catalog.product-upserted.v1",
	"boutique.evt.catalog.product-removed.v1",
	"boutique.evt.catalog.snapshot-completed.v1",
	"boutique.evt.currency.rates-updated.v1",
	"boutique.evt.cart.item-added.v1",
	"boutique.evt.cart.cleared.v1",
	"boutique.evt.cart.command-rejected.v1",
	"boutique.evt.storefront.operation-accepted.v1",
	"boutique.evt.shipping.cart-quote-updated.v1",
	"boutique.evt.shipping.cart-quote-failed.v1",
	"boutique.evt.order.submitted.v1",
	"boutique.evt.order.processing-stage-changed.v1",
	"boutique.evt.order.rejected.v1",
	"boutique.evt.order.completed.v1",
	"boutique.evt.order.cancelled.v1",
	"boutique.evt.order.manual-review-required.v1",
	"boutique.evt.order.step-timed-out.v1",
	"boutique.evt.notification.order-confirmation-sent.v1",
	"boutique.evt.notification.order-confirmation-failed.v1",
}

var personalizationProjectionFilterSubjects = []string{
	"boutique.evt.recommendation.generated.v1",
	"boutique.evt.recommendation.generation-failed.v1",
	"boutique.evt.ad.selection-generated.v1",
}

var projectionFilterSubjects = append(
	append([]string(nil), criticalProjectionFilterSubjects...),
	personalizationProjectionFilterSubjects...,
)

type projectionConsumer struct {
	js          nats.JetStreamContext
	stream      string
	durable     string
	filters     []string
	parallelism int
	maxPending  int
	critical    bool
}

func (p *projector) criticalConsumer() projectionConsumer {
	return projectionConsumer{
		js: p.js, stream: p.config.eventStream, durable: p.config.durable,
		filters: criticalProjectionFilterSubjects, parallelism: projectionParallelism,
		maxPending: projectionMaxPending, critical: true,
	}
}

func (p *projector) personalizationConsumer(js nats.JetStreamContext) projectionConsumer {
	return projectionConsumer{
		js: js, stream: p.config.personalizationStream, durable: p.config.personalizationDurable,
		filters:     personalizationProjectionFilterSubjects,
		parallelism: personalizationProjectionParallelism,
		maxPending:  personalizationProjectionMaxPending,
	}
}

type projector struct {
	js                        nats.JetStreamContext
	config                    projectionConfig
	products                  projectionKV
	catalog                   *projectionReadCache
	carts                     projectionKV
	cartCache                 *projectionReadCache
	context                   projectionKV
	contextCache              *projectionReadCache
	operations                projectionKV
	orders                    projectionKV
	productWrites             projectionKV
	cartWrites                projectionKV
	contextWrites             projectionKV
	operationWrites           projectionKV
	orderWrites               projectionKV
	publishLive               func(string, []byte) error
	catalogSnapshotMu         sync.Mutex
	catalogSnapshotGeneration uint64
	catalogSnapshot           []storefront.ProductView

	kvConflictRetries  atomic.Uint64
	staleEventSkips    atomic.Uint64
	queryRevision      atomic.Uint64
	lastProjectedUnix  atomic.Int64
	consumerPending    atomic.Uint64
	consumerAckPending atomic.Uint64
}

// projectionReader keeps read-model queries independently testable.
type projectionReader interface {
	Get(string) (nats.KeyValueEntry, error)
	Keys(...nats.WatchOpt) ([]string, error)
}

// projectionKV adds the authoritative CAS operations used by event handlers.
// The shared catalog query cache deliberately does not implement these writes.
type projectionKV interface {
	projectionReader
	Create(string, []byte) (uint64, error)
	Update(string, []byte, uint64) (uint64, error)
}

func newProjector(js nats.JetStreamContext, config projectionConfig) (*projector, error) {
	products, err := js.KeyValue(config.productsBucket)
	if err != nil {
		return nil, fmt.Errorf("open product KV: %w", err)
	}
	catalog, err := newProjectionReadCache(products)
	if err != nil {
		return nil, fmt.Errorf("initialize product query cache: %w", err)
	}
	carts, err := js.KeyValue(config.cartsBucket)
	if err != nil {
		catalog.Close()
		return nil, fmt.Errorf("open cart KV: %w", err)
	}
	cartCache, err := newExpiringProjectionReadCache(carts, config.cartCacheEntries, cartCacheTTL, nil)
	if err != nil {
		catalog.Close()
		return nil, fmt.Errorf("initialize cart query cache: %w", err)
	}
	context, err := js.KeyValue(config.contextBucket)
	if err != nil {
		catalog.Close()
		cartCache.Close()
		return nil, fmt.Errorf("open context KV: %w", err)
	}
	contextCache, err := newExpiringProjectionReadCache(context, config.contextCacheEntries, 0, projectionExpiresAt)
	if err != nil {
		catalog.Close()
		cartCache.Close()
		return nil, fmt.Errorf("initialize context query cache: %w", err)
	}
	operations, err := js.KeyValue(config.operationsBucket)
	if err != nil {
		catalog.Close()
		cartCache.Close()
		contextCache.Close()
		return nil, fmt.Errorf("open operations KV: %w", err)
	}
	orders, err := js.KeyValue(config.ordersBucket)
	if err != nil {
		catalog.Close()
		cartCache.Close()
		contextCache.Close()
		return nil, fmt.Errorf("open orders KV: %w", err)
	}
	return &projector{
		js: js, config: config, products: products, catalog: catalog, carts: carts,
		cartCache: cartCache, context: context, contextCache: contextCache,
		operations: operations, orders: orders,
		productWrites:   newCachedProjectionKV(products, 4096),
		cartWrites:      newCachedProjectionKV(carts, config.cartCacheEntries),
		contextWrites:   newCachedProjectionKV(context, config.contextCacheEntries),
		operationWrites: newCachedProjectionKV(operations, 65536),
		orderWrites:     newCachedProjectionKV(orders, 65536),
	}, nil
}

func writer(cached, authoritative projectionKV) projectionKV {
	if cached != nil {
		return cached
	}
	return authoritative
}

func (p *projector) catalogReader() projectionReader {
	if p.catalog != nil {
		return p.catalog
	}
	return p.products
}

func (p *projector) close() {
	if p.catalog != nil {
		p.catalog.Close()
	}
	if p.cartCache != nil {
		p.cartCache.Close()
	}
	if p.contextCache != nil {
		p.contextCache.Close()
	}
}

func (p *projector) subscribe(consumer projectionConsumer) (*nats.Subscription, bool, error) {
	rebuilding := false
	if _, err := p.products.Get(storefront.CatalogKey); errors.Is(err, nats.ErrKeyNotFound) {
		rebuilding = true
	}
	if err := p.ensureProjectionConsumer(consumer); err != nil {
		return nil, rebuilding, err
	}
	subscription, err := consumer.js.PullSubscribe(
		"",
		consumer.durable,
		nats.Bind(consumer.stream, consumer.durable),
	)
	if err != nil {
		return nil, rebuilding, fmt.Errorf("bind projection consumer: %w", err)
	}
	return subscription, rebuilding, nil
}

func (p *projector) ensureProjectionConsumer(consumer projectionConsumer) error {
	config := &nats.ConsumerConfig{
		Durable:        consumer.durable,
		DeliverPolicy:  nats.DeliverAllPolicy,
		AckPolicy:      nats.AckExplicitPolicy,
		AckWait:        30 * time.Second,
		MaxDeliver:     projectionMaxDeliver,
		MaxAckPending:  consumer.maxPending,
		FilterSubjects: append([]string(nil), consumer.filters...),
	}
	for attempt := 0; attempt < 20; attempt++ {
		info, err := consumer.js.ConsumerInfo(consumer.stream, consumer.durable)
		if errors.Is(err, nats.ErrConsumerNotFound) {
			if _, addErr := consumer.js.AddConsumer(consumer.stream, config); addErr == nil {
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
		if projectionFiltersMatch(info.Config.FilterSubject, info.Config.FilterSubjects, consumer.filters) &&
			info.Config.MaxAckPending == consumer.maxPending &&
			info.Config.AckPolicy == nats.AckExplicitPolicy &&
			info.Config.AckWait == 30*time.Second &&
			info.Config.MaxDeliver == projectionMaxDeliver &&
			info.Config.DeliverPolicy == nats.DeliverAllPolicy {
			return nil
		}
		next := info.Config
		next.FilterSubject = ""
		next.FilterSubjects = append([]string(nil), consumer.filters...)
		next.MaxAckPending = consumer.maxPending
		next.AckPolicy = nats.AckExplicitPolicy
		next.AckWait = 30 * time.Second
		next.MaxDeliver = projectionMaxDeliver
		if _, updateErr := consumer.js.UpdateConsumer(consumer.stream, &next); updateErr == nil {
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

func (p *projector) run(subscription *nats.Subscription, stop <-chan struct{}, consumer projectionConsumer) error {
	processor := newProjectionProcessorWithParallelism(p, stop, consumer.parallelism)
	defer processor.close()
	for {
		select {
		case <-stop:
			return nil
		default:
		}
		batch, err := subscription.FetchBatch(
			projectionFetchSize,
			nats.MaxWait(time.Second),
		)
		if err != nil {
			if projectionConsumerTerminal(err) {
				return err
			}
			continue
		}
		if !processor.dispatchStream(batch.Messages()) {
			return nil
		}
		if consumer.critical {
			_ = p.refreshConsumerState()
		}
		if err := batch.Error(); err != nil {
			if projectionConsumerTerminal(err) {
				return err
			}
		}
	}
}

type projectionMessage struct {
	message     *nats.Msg
	envelope    *commonv1.MessageEnvelope
	decodeErr   error
	publishedAt time.Time
}

type projectionProcessor struct {
	projector *projector
	stop      <-chan struct{}
	lanes     []chan projectionMessage
	running   sync.WaitGroup
}

func newProjectionProcessor(p *projector, stop <-chan struct{}) *projectionProcessor {
	return newProjectionProcessorWithParallelism(p, stop, projectionParallelism)
}

func newProjectionProcessorWithParallelism(p *projector, stop <-chan struct{}, parallelism int) *projectionProcessor {
	queueDepth := projectionFetchSize/parallelism + 1
	processor := &projectionProcessor{
		projector: p,
		stop:      stop,
		lanes:     make([]chan projectionMessage, parallelism),
	}
	for index := range processor.lanes {
		lane := make(chan projectionMessage, queueDepth)
		processor.lanes[index] = lane
		processor.running.Add(1)
		go func(messages <-chan projectionMessage) {
			defer processor.running.Done()
			for message := range messages {
				processor.projector.applyMessage(message)
			}
		}(lane)
	}
	return processor
}

func (processor *projectionProcessor) dispatchStream(messages <-chan *nats.Msg) bool {
	debug := slog.Default().Enabled(context.Background(), slog.LevelDebug)
	received := 0
	var groups map[string]struct{}
	if debug {
		groups = make(map[string]struct{})
	}
	for message := range messages {
		if !projectionHandlesSubject(message.Subject) {
			if err := message.Ack(); err != nil {
				log.Printf("ignored projection event acknowledgement failed topic=%q error=%v", message.Subject, err)
			}
			continue
		}
		item := decodeProjectionMessage(message)
		group := projectionMessageGroup(item.envelope)
		received++
		if debug {
			groups[group] = struct{}{}
		}
		lane := processor.lanes[projectionMessageLane(group, len(processor.lanes))]
		select {
		case lane <- item:
		case <-processor.stop:
			return false
		}
	}
	if debug && received != 0 {
		slog.Debug("NATS projection batch received", "message_kind", "event",
			"messages", received, "correlation_groups", len(groups))
	}
	return true
}

func (processor *projectionProcessor) close() {
	for _, lane := range processor.lanes {
		close(lane)
	}
	processor.running.Wait()
}

// applyStream retains a bounded helper for unit tests and one-shot callers.
// The runtime uses one persistent processor across all fetched batches.
func (p *projector) applyStream(messages <-chan *nats.Msg) {
	stop := make(chan struct{})
	processor := newProjectionProcessor(p, stop)
	processor.dispatchStream(messages)
	processor.close()
}

func decodeProjectionMessage(message *nats.Msg) projectionMessage {
	envelope := &commonv1.MessageEnvelope{}
	decodeErr := proto.Unmarshal(message.Data, envelope)
	// Stateless handlers intentionally retain a causal occurrence time in
	// result envelopes. JetStream's stored timestamp is immutable too, and
	// unlike the causal time includes time spent waiting in upstream queues.
	publishedAt := time.Now().UTC()
	if metadata, err := message.Metadata(); err == nil && !metadata.Timestamp.IsZero() {
		publishedAt = metadata.Timestamp.UTC()
	}
	return projectionMessage{
		message: message, envelope: envelope, decodeErr: decodeErr, publishedAt: publishedAt,
	}
}

func (p *projector) applyMessage(item projectionMessage) {
	correlationID, messageID := projectionMessageContext(item.envelope)
	traceparent, tracestate := "", ""
	if item.envelope != nil {
		traceparent, tracestate = item.envelope.Traceparent, item.envelope.Tracestate
	}
	_, span := telemetry.StartConsumerSpan(context.Background(), item.message.Subject, "event",
		messageID, correlationID, traceparent, tracestate)
	defer span.End()
	err := item.decodeErr
	if err == nil {
		err = p.applyEnvelope(item.message.Subject, item.envelope, item.publishedAt)
	}
	if err != nil {
		telemetry.RecordError(span, err)
		log.Printf("projection event processing failed topic=%q message_id=%q correlation_id=%q error=%v",
			item.message.Subject, messageID, correlationID, err)
		deliveries := uint64(1)
		if metadata, metadataErr := item.message.Metadata(); metadataErr == nil {
			deliveries = metadata.NumDelivered
		}
		if nakErr := item.message.NakWithDelay(projectionRetryDelay(deliveries)); nakErr != nil {
			log.Printf("projection event NAK failed topic=%q message_id=%q correlation_id=%q error=%v",
				item.message.Subject, messageID, correlationID, nakErr)
		}
		return
	}
	if err := item.message.Ack(); err != nil {
		telemetry.RecordError(span, err)
		log.Printf("projection event acknowledgement failed topic=%q message_id=%q correlation_id=%q error=%v",
			item.message.Subject, messageID, correlationID, err)
	}
}

func projectionRetryDelay(deliveries uint64) time.Duration {
	delay := time.Second
	for attempt := uint64(1); attempt < deliveries && delay < 30*time.Second; attempt++ {
		delay *= 2
	}
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func projectionMessageLane(correlationID string, lanes int) int {
	hash := uint32(2166136261)
	for index := 0; index < len(correlationID); index++ {
		hash ^= uint32(correlationID[index])
		hash *= 16777619
	}
	return int(hash % uint32(lanes))
}

func projectionConsumerTerminal(err error) bool {
	return err != nil && !errors.Is(err, nats.ErrTimeout)
}

func projectionFiltersMatch(single string, multiple, filters []string) bool {
	if single != "" || len(multiple) != len(filters) {
		return false
	}
	wanted := make(map[string]struct{}, len(filters))
	for _, subject := range filters {
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
	for _, candidate := range projectionFilterSubjects {
		if subject == candidate {
			return true
		}
	}
	return false
}

func projectionMessageGroup(envelope *commonv1.MessageEnvelope) string {
	if envelope == nil || envelope.CorrelationId == "" {
		return "unknown"
	}
	return envelope.CorrelationId
}

func projectionMessageContext(envelope *commonv1.MessageEnvelope) (string, string) {
	if envelope == nil {
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
	return p.applyEnvelope(subject, envelope, publishedAt)
}

func (p *projector) applyEnvelope(
	subject string,
	envelope *commonv1.MessageEnvelope,
	publishedAt time.Time,
) error {
	if envelope == nil {
		return fmt.Errorf("decode envelope: empty envelope")
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
		return updateJSON(p, writer(p.productWrites, p.products), storefront.ProductKey(payload.Product.ProductId), payload.Product.ProductVersion,
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
		return updateJSON(p, writer(p.productWrites, p.products), storefront.ProductKey(payload.ProductId), payload.ProductVersion,
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
		return updateJSON(p, writer(p.productWrites, p.products), storefront.CatalogKey, payload.CatalogRevision,
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
		return updateJSON(p, writer(p.productWrites, p.products), storefront.CurrencyKey, payload.RateRevision,
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
		return updateJSON(p, writer(p.contextWrites, p.context), storefront.RecommendationKey(payload.SessionId), payload.TriggeringContextVersion,
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
		return updateJSON(p, writer(p.contextWrites, p.context), storefront.RecommendationKey(payload.SessionId), envelope.AggregateVersion,
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
		return updateJSON(p, writer(p.contextWrites, p.context), storefront.AdKey(payload.SessionId), envelope.AggregateVersion,
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
		return p.updateCartQuote(view)
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
		return p.updateCartQuote(view)
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
	orders := writer(p.orderWrites, p.orders)
	for attempt := 0; attempt < 20; attempt++ {
		entry, err := orders.Get(key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			encoded, marshalErr := json.Marshal(incoming)
			if marshalErr != nil {
				return marshalErr
			}
			if _, err := orders.Create(key, encoded); err == nil {
				p.recordProjected(incoming.UpdatedAt)
				p.publishOrderUpdate(incoming)
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
		if _, err := orders.Update(key, encoded, entry.Revision()); err == nil {
			p.recordProjected(next.UpdatedAt)
			p.publishOrderUpdate(next)
			return nil
		} else if !isKVConflict(err) {
			return err
		}
		p.recordConflict(attempt)
	}
	return fmt.Errorf("order KV update conflicted too many times for %s", key)
}

func (p *projector) publishOrderUpdate(order storefront.OrderView) {
	if p.publishLive == nil {
		return
	}
	subject, err := liveOperationSubject(p.config.livePrefix, order.OrderID)
	if err != nil {
		log.Printf("order live update skipped order_id=%q error=%v", order.OrderID, err)
		return
	}
	encoded, err := json.Marshal(order)
	if err != nil {
		log.Printf("order live update encoding failed order_id=%q error=%v", order.OrderID, err)
		return
	}
	if err := p.publishLive(subject, encoded); err != nil {
		// The KV projection remains authoritative. SSE clients reconnect through
		// the query endpoint, so a best-effort live notification must never make
		// the durable projection event fail or redeliver.
		log.Printf("order live update failed order_id=%q error=%v", order.OrderID, err)
	}
}

func liveOperationSubject(prefix, operationID string) (string, error) {
	if prefix == "" || !strings.HasSuffix(prefix, ".") {
		return "", errors.New("live operation prefix is invalid")
	}
	if operationID == "" {
		return "", errors.New("operation ID is empty")
	}
	for _, character := range operationID {
		if (character < 'a' || character > 'z') &&
			(character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') && character != '-' && character != '_' {
			return "", errors.New("operation ID is not a safe NATS subject token")
		}
	}
	return prefix + operationID, nil
}

func (p *projector) refreshConsumerState() error {
	info, err := p.js.ConsumerInfo(p.config.eventStream, p.config.durable)
	if err != nil {
		return err
	}
	p.consumerPending.Store(info.NumPending)
	p.consumerAckPending.Store(uint64(info.NumAckPending))
	return nil
}

func (p *projector) waitForInitialReplay(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if err := p.refreshConsumerState(); err != nil {
			return fmt.Errorf("inspect regional projection replay: %w", err)
		}
		if p.consumerPending.Load() == 0 && p.consumerAckPending.Load() == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("regional projection replay did not catch up within %s", timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
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
	orders := writer(p.orderWrites, p.orders)
	for attempt := 0; attempt < 20; attempt++ {
		entry, err := orders.Get(key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			view := storefront.OrderView{
				ProjectionMetadata: source,
				OrderID:            orderID, CartClearStatus: status, CartClearFailureCode: failureCode, UpdatedAt: updatedAt,
			}
			encoded, marshalErr := json.Marshal(view)
			if marshalErr != nil {
				return marshalErr
			}
			if _, err := orders.Create(key, encoded); err == nil {
				p.recordProjected(view.UpdatedAt)
				p.publishOrderUpdate(view)
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
		if _, err := orders.Update(key, encoded, entry.Revision()); err == nil {
			p.recordProjected(current.UpdatedAt)
			p.publishOrderUpdate(current)
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
	operations := writer(p.operationWrites, p.operations)
	for attempt := 0; attempt < 20; attempt++ {
		entry, err := operations.Get(key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			encoded, marshalErr := json.Marshal(incoming)
			if marshalErr != nil {
				return marshalErr
			}
			if _, err := operations.Create(key, encoded); err == nil {
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
		if _, err := operations.Update(key, encoded, entry.Revision()); err == nil {
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
	carts := writer(p.cartWrites, p.carts)
	next := storefront.CartView{ProjectionMetadata: source, Cart: cart, UpdatedAt: updatedAt}
	encoded, err := json.Marshal(next)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 20; attempt++ {
		entry, err := carts.Get(key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			if _, err := carts.Create(key, encoded); err == nil {
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
		if _, err := carts.Update(key, encoded, entry.Revision()); err == nil {
			p.recordProjected(next.UpdatedAt)
			return nil
		} else if !isKVConflict(err) {
			return err
		}
		p.recordConflict(attempt)
	}
	return fmt.Errorf("cart KV update conflicted too many times for %s", key)
}

func (p *projector) updateCartQuote(incoming storefront.CartQuoteView) error {
	if incoming.UserID == "" || incoming.CartVersion == 0 {
		return fmt.Errorf("cart quote identity is incomplete")
	}
	key := storefront.CartQuoteKey(incoming.UserID)
	context := writer(p.contextWrites, p.context)
	encoded, err := json.Marshal(incoming)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 20; attempt++ {
		entry, err := context.Get(key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			if _, err := context.Create(key, encoded); err == nil {
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
		var current storefront.CartQuoteView
		if err := json.Unmarshal(entry.Value(), &current); err != nil {
			return err
		}
		if current.SourceEventID == incoming.SourceEventID {
			p.staleEventSkips.Add(1)
			return nil
		}
		if incoming.CartVersion < current.CartVersion ||
			(incoming.CartVersion == current.CartVersion && !incoming.UpdatedAt.After(current.UpdatedAt)) {
			p.staleEventSkips.Add(1)
			return nil
		}
		if _, err := context.Update(key, encoded, entry.Revision()); err == nil {
			p.recordProjected(incoming.UpdatedAt)
			return nil
		} else if !isKVConflict(err) {
			return err
		}
		p.recordConflict(attempt)
	}
	return fmt.Errorf("cart quote KV update conflicted too many times for %s", key)
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
	if len(merged.AppliedEventIDs) > projectionEventHistory {
		start := len(merged.AppliedEventIDs) - projectionEventHistory
		merged.AppliedEventIDs = append([]string(nil), merged.AppliedEventIDs[start:]...)
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
