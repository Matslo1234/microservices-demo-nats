// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	"github.com/GoogleCloudPlatform/microservices-demo/src/storefrontprojectionservice/internal/storefront"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const projectionDurable = "storefront-projection-v1"

type projector struct {
	js         nats.JetStreamContext
	products   nats.KeyValue
	carts      nats.KeyValue
	context    nats.KeyValue
	operations nats.KeyValue
	orders     nats.KeyValue
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
		if err := p.js.DeleteConsumer("BOUTIQUE_EVENTS", projectionDurable); err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
			return nil, false, fmt.Errorf("reset projection consumer: %w", err)
		}
	}
	subscription, err := p.js.PullSubscribe(
		"boutique.evt.>",
		projectionDurable,
		nats.BindStream("BOUTIQUE_EVENTS"),
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.DeliverAll(),
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(10),
	)
	if err != nil {
		return nil, rebuilding, fmt.Errorf("create projection consumer: %w", err)
	}
	return subscription, rebuilding, nil
}

func (p *projector) run(subscription *nats.Subscription, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		messages, err := subscription.Fetch(64, nats.MaxWait(time.Second))
		if err != nil && !errors.Is(err, nats.ErrTimeout) {
			log.Printf("projection fetch failed: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for _, message := range messages {
			correlationID, messageID := projectionMessageContext(message.Data)
			slog.Debug("NATS event received",
				"topic", message.Subject,
				"message_kind", "event",
				"message_id", messageID,
				"correlation_id", correlationID)
			if err := p.apply(message.Subject, message.Data); err != nil {
				log.Printf("projection event processing failed topic=%q message_id=%q correlation_id=%q error=%v",
					message.Subject, messageID, correlationID, err)
				if nakErr := message.NakWithDelay(time.Second); nakErr != nil {
					log.Printf("projection event NAK failed topic=%q message_id=%q correlation_id=%q error=%v",
						message.Subject, messageID, correlationID, nakErr)
				}
				continue
			}
			if err := message.Ack(); err != nil {
				log.Printf("projection event acknowledgement failed topic=%q message_id=%q correlation_id=%q error=%v",
					message.Subject, messageID, correlationID, err)
				continue
			}
		}
	}
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

func (p *projector) apply(subject string, data []byte) error {
	envelope := &commonv1.MessageEnvelope{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return fmt.Errorf("decode envelope: %w", err)
	}
	if envelope.SchemaVersion != 1 || envelope.Data == nil {
		return fmt.Errorf("unsupported or empty envelope")
	}
	updatedAt := time.Now().UTC()
	if envelope.OccurredAt != nil && envelope.OccurredAt.IsValid() {
		updatedAt = envelope.OccurredAt.AsTime()
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
		return updateJSON(p.products, storefront.ProductKey(payload.Product.ProductId), payload.Product.ProductVersion,
			func(current storefront.ProductView) uint64 { return current.Product.GetProductVersion() },
			storefront.ProductView{Product: payload.Product, CatalogRevision: payload.CatalogRevision, UpdatedAt: updatedAt})
	case "boutique.evt.catalog.product-removed.v1":
		payload := &eventsv1.CatalogProductRemovedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return updateJSON(p.products, storefront.ProductKey(payload.ProductId), payload.ProductVersion,
			func(current storefront.ProductView) uint64 { return current.Product.GetProductVersion() },
			storefront.ProductView{
				Product:         &commonv1.ProductSnapshot{ProductId: payload.ProductId, ProductVersion: payload.ProductVersion},
				CatalogRevision: payload.CatalogRevision,
				Removed:         true,
				UpdatedAt:       updatedAt,
			})
	case "boutique.evt.catalog.snapshot-completed.v1":
		payload := &eventsv1.CatalogSnapshotCompletedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return updateJSON(p.products, storefront.CatalogKey, payload.CatalogRevision,
			func(current storefront.CatalogView) uint64 { return current.CatalogRevision },
			storefront.CatalogView{CatalogRevision: payload.CatalogRevision, ProductCount: payload.ProductCount, Checksum: payload.Checksum, UpdatedAt: updatedAt})
	case "boutique.evt.currency.rates-updated.v1":
		payload := &eventsv1.CurrencyRatesUpdatedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.CurrencyView{BaseCurrencyCode: payload.BaseCurrencyCode, RateRevision: payload.RateRevision, UpdatedAt: updatedAt}
		if payload.EffectiveAt != nil {
			view.EffectiveSeconds, view.EffectiveNanos = payload.EffectiveAt.Seconds, payload.EffectiveAt.Nanos
		}
		for _, rate := range payload.Rates {
			view.Rates = append(view.Rates, storefront.Rate{CurrencyCode: rate.CurrencyCode, UnitsPerBase: rate.UnitsPerBase})
		}
		return updateJSON(p.products, storefront.CurrencyKey, payload.RateRevision,
			func(current storefront.CurrencyView) uint64 { return current.RateRevision }, view)
	case "boutique.evt.cart.item-added.v1":
		payload := &eventsv1.CartItemAddedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if err := p.updateCart(payload.Cart, updatedAt); err != nil {
			return err
		}
		return p.updateOperation(storefront.OperationView{
			OperationID: payload.CommandId, CommandID: payload.CommandId, Kind: "cart.add-item",
			Status: "SUCCEEDED", UserID: payload.UserId,
			CartVersion: payload.Cart.GetCartVersion(), UpdatedAt: updatedAt,
		})
	case "boutique.evt.cart.cleared.v1":
		payload := &eventsv1.CartClearedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if err := p.updateCart(payload.Cart, updatedAt); err != nil {
			return err
		}
		return p.updateOperation(storefront.OperationView{
			OperationID: payload.CommandId, CommandID: payload.CommandId, Kind: "cart.clear",
			Status: "SUCCEEDED", UserID: payload.UserId,
			CartVersion: payload.Cart.GetCartVersion(), UpdatedAt: updatedAt,
		})
	case "boutique.evt.cart.command-rejected.v1":
		payload := &eventsv1.CartCommandRejectedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.OperationView{
			OperationID: payload.CommandId, CommandID: payload.CommandId, Status: "REJECTED",
			UserID: payload.UserId, CartVersion: payload.CurrentCartVersion, UpdatedAt: updatedAt,
		}
		if payload.Failure != nil {
			view.FailureCode = payload.Failure.Code
			view.Retryable = payload.Failure.Retryable
			view.SafeMessage = payload.Failure.SafeMessage
		}
		return p.updateOperation(view)
	case "boutique.evt.storefront.operation-accepted.v1":
		payload := &eventsv1.StorefrontOperationAcceptedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if err := p.updateOperation(storefront.OperationView{
			OperationID: payload.OperationId, CommandID: payload.CommandId, Kind: payload.OperationKind,
			Status: payload.Status, UserID: payload.UserOrSessionId, UpdatedAt: updatedAt,
		}); err != nil {
			return err
		}
		if payload.OperationKind == "order.submit" {
			return p.updateOrder(storefront.OrderView{OrderID: payload.OperationId, UserID: payload.UserOrSessionId,
				Status: "QUEUED", Stage: "QUEUED", UpdatedAt: updatedAt})
		}
		return nil
	case "boutique.evt.recommendation.generated.v1":
		payload := &eventsv1.RecommendationGeneratedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.RecommendationView{
			SessionID: payload.SessionId, ContextVersion: payload.TriggeringContextVersion,
			ProductIDs: append([]string(nil), payload.ProductIds...), UpdatedAt: updatedAt,
		}
		if payload.ExpiresAt != nil && payload.ExpiresAt.IsValid() {
			view.ExpiresAt = payload.ExpiresAt.AsTime()
		}
		return updateJSON(p.context, storefront.RecommendationKey(payload.SessionId), payload.TriggeringContextVersion,
			func(current storefront.RecommendationView) uint64 { return current.ContextVersion }, view)
	case "boutique.evt.recommendation.generation-failed.v1":
		payload := &eventsv1.RecommendationGenerationFailedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.RecommendationView{SessionID: payload.SessionId, ContextVersion: envelope.AggregateVersion, UpdatedAt: updatedAt}
		if payload.Failure != nil {
			view.FailureCode = payload.Failure.Code
		}
		return updateJSON(p.context, storefront.RecommendationKey(payload.SessionId), envelope.AggregateVersion,
			func(current storefront.RecommendationView) uint64 { return current.ContextVersion }, view)
	case "boutique.evt.ad.selection-generated.v1":
		payload := &eventsv1.AdSelectionGeneratedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.AdView{
			SessionID: payload.SessionId, PageType: payload.TriggeringPageType,
			ContextVersion: envelope.AggregateVersion, UpdatedAt: updatedAt,
		}
		for _, ad := range payload.Ads {
			view.Ads = append(view.Ads, storefront.Ad{RedirectURL: ad.RedirectUrl, Text: ad.Text})
		}
		if payload.ExpiresAt != nil && payload.ExpiresAt.IsValid() {
			view.ExpiresAt = payload.ExpiresAt.AsTime()
		}
		return updateJSON(p.context, storefront.AdKey(payload.SessionId), envelope.AggregateVersion,
			func(current storefront.AdView) uint64 { return current.ContextVersion }, view)
	case "boutique.evt.shipping.cart-quote-updated.v1":
		payload := &eventsv1.ShippingCartQuoteUpdatedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.CartQuoteView{
			UserID: payload.UserId, CartVersion: payload.CartVersion, CostUSD: payload.CostUsd, UpdatedAt: updatedAt,
		}
		if payload.ExpiresAt != nil && payload.ExpiresAt.IsValid() {
			view.ExpiresAt = payload.ExpiresAt.AsTime()
		}
		return updateJSON(p.context, storefront.CartQuoteKey(payload.UserId), payload.CartVersion,
			func(current storefront.CartQuoteView) uint64 { return current.CartVersion }, view)
	case "boutique.evt.shipping.cart-quote-failed.v1":
		payload := &eventsv1.ShippingCartQuoteFailedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.CartQuoteView{UserID: payload.UserId, CartVersion: payload.CartVersion, UpdatedAt: updatedAt}
		if payload.Failure != nil {
			view.FailureCode = payload.Failure.Code
		}
		return updateJSON(p.context, storefront.CartQuoteKey(payload.UserId), payload.CartVersion,
			func(current storefront.CartQuoteView) uint64 { return current.CartVersion }, view)
	case "boutique.evt.order.submitted.v1":
		payload := &eventsv1.OrderSubmittedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if payload.Order == nil {
			return fmt.Errorf("submitted order snapshot is missing")
		}
		return p.updateOrder(storefront.OrderView{OrderID: payload.Order.OrderId, UserID: payload.Order.UserId,
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
		return p.updateOrder(storefront.OrderView{OrderID: payload.OrderId, Status: status, Stage: payload.Stage,
			AggregateVersion: envelope.AggregateVersion, UpdatedAt: updatedAt})
	case "boutique.evt.order.rejected.v1":
		payload := &eventsv1.OrderRejectedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.OrderView{OrderID: payload.OrderId, UserID: p.operationUser(payload.OperationId), Status: "REJECTED", Stage: "REJECTED",
			AggregateVersion: envelope.AggregateVersion, UpdatedAt: updatedAt}
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
		return p.updateOrder(storefront.OrderView{OrderID: payload.Order.OrderId, UserID: payload.Order.UserId, Status: "COMPLETED", Stage: "COMPLETED",
			Snapshot: payload.Order, AggregateVersion: envelope.AggregateVersion, UpdatedAt: updatedAt})
	case "boutique.evt.order.cancelled.v1":
		payload := &eventsv1.OrderCancelledEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		view := storefront.OrderView{OrderID: payload.OrderId, Status: "CANCELLED", Stage: "CANCELLED", AggregateVersion: envelope.AggregateVersion, UpdatedAt: updatedAt}
		applyOrderFailure(&view, payload.Failure)
		return p.updateOrder(view)
	case "boutique.evt.order.manual-review-required.v1":
		payload := &eventsv1.OrderManualReviewRequiredEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return p.updateOrder(storefront.OrderView{OrderID: payload.OrderId, Status: "MANUAL_REVIEW", Stage: "MANUAL_REVIEW", FailureCode: payload.FailedCompensation,
			SafeMessage: "The order requires manual review.", AggregateVersion: envelope.AggregateVersion, UpdatedAt: updatedAt})
	case "boutique.evt.order.step-timed-out.v1":
		payload := &eventsv1.OrderStepTimedOutEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return p.updateOrder(storefront.OrderView{OrderID: payload.OrderId, Status: "PROCESSING", Stage: "TIMED_OUT_" + payload.WaitingStage,
			FailureCode: "STEP_TIMEOUT", AggregateVersion: envelope.AggregateVersion, UpdatedAt: updatedAt})
	case "boutique.evt.notification.order-confirmation-sent.v1":
		payload := &eventsv1.NotificationOrderConfirmationSentEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return p.updateOrder(storefront.OrderView{OrderID: payload.OrderId, NotificationStatus: "SENT", AggregateVersion: envelope.AggregateVersion, UpdatedAt: updatedAt})
	case "boutique.evt.notification.order-confirmation-failed.v1":
		payload := &eventsv1.NotificationOrderConfirmationFailedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return p.updateOrder(storefront.OrderView{OrderID: payload.OrderId, NotificationStatus: "FAILED", AggregateVersion: envelope.AggregateVersion, UpdatedAt: updatedAt})
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
	operation, err := getJSON[storefront.OperationView](p.operations, storefront.OperationKey(operationID))
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
				return nil
			} else if !errors.Is(err, nats.ErrKeyExists) {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		var current storefront.OrderView
		if err := json.Unmarshal(entry.Value(), &current); err != nil {
			return err
		}
		if incoming.AggregateVersion < current.AggregateVersion {
			if current.UserID != "" || incoming.UserID == "" {
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
			return nil
		} else if !errors.Is(err, nats.ErrKeyExists) {
			return err
		}
	}
	return fmt.Errorf("order KV update conflicted too many times for %s", key)
}

func mergeOrder(current, incoming storefront.OrderView) storefront.OrderView {
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
	if incoming.AggregateVersion < current.AggregateVersion {
		incoming.AggregateVersion = current.AggregateVersion
	}
	terminal := func(status string) bool {
		return status == "COMPLETED" || status == "CANCELLED" || status == "REJECTED" || status == "MANUAL_REVIEW"
	}
	if terminal(current.Status) && !terminal(incoming.Status) {
		incoming.Status = current.Status
		incoming.Stage = current.Stage
	}
	return incoming
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
				return nil
			} else if !errors.Is(err, nats.ErrKeyExists) {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		var current storefront.OperationView
		if err := json.Unmarshal(entry.Value(), &current); err != nil {
			return err
		}
		next := mergeOperation(current, incoming)
		encoded, err := json.Marshal(next)
		if err != nil {
			return err
		}
		if _, err := p.operations.Update(key, encoded, entry.Revision()); err == nil {
			return nil
		} else if !errors.Is(err, nats.ErrKeyExists) {
			return err
		}
	}
	return fmt.Errorf("operation KV update conflicted too many times for %s", key)
}

func mergeOperation(current, incoming storefront.OperationView) storefront.OperationView {
	currentTerminal := current.Status == "SUCCEEDED" || current.Status == "REJECTED"
	incomingTerminal := incoming.Status == "SUCCEEDED" || incoming.Status == "REJECTED"
	if currentTerminal && !incomingTerminal {
		if current.Kind == "" {
			current.Kind = incoming.Kind
		}
		if current.CommandID == "" {
			current.CommandID = incoming.CommandID
		}
		return current
	}
	if currentTerminal && incomingTerminal {
		return current
	}
	if incoming.Kind == "" {
		incoming.Kind = current.Kind
	}
	return incoming
}

func (p *projector) updateCart(cart *commonv1.CartSnapshot, updatedAt time.Time) error {
	if cart == nil || cart.UserId == "" {
		return fmt.Errorf("cart snapshot is missing user ID")
	}
	return updateJSON(p.carts, cart.UserId, cart.CartVersion,
		func(current storefront.CartView) uint64 { return current.Cart.GetCartVersion() },
		storefront.CartView{Cart: cart, UpdatedAt: updatedAt})
}

func updateJSON[T any](bucket nats.KeyValue, key string, incomingVersion uint64,
	currentVersion func(T) uint64, next T) error {
	encoded, err := json.Marshal(next)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 20; attempt++ {
		entry, err := bucket.Get(key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			if _, err := bucket.Create(key, encoded); err == nil {
				return nil
			} else if !errors.Is(err, nats.ErrKeyExists) {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		var current T
		if err := json.Unmarshal(entry.Value(), &current); err != nil {
			return err
		}
		if incomingVersion <= currentVersion(current) {
			return nil
		}
		if _, err := bucket.Update(key, encoded, entry.Revision()); err == nil {
			return nil
		} else if !errors.Is(err, nats.ErrKeyExists) {
			return err
		}
	}
	return fmt.Errorf("KV update conflicted too many times for %s", key)
}
