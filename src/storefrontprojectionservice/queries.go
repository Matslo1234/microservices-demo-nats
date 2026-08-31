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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hipstershop "github.com/GoogleCloudPlatform/microservices-demo/hipstershop"
	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	telemetry "github.com/GoogleCloudPlatform/microservices-demo/src/shared/telemetry/go"
	"github.com/GoogleCloudPlatform/microservices-demo/src/storefrontprojectionservice/internal/storefront"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

var errInvalidCurrency = errors.New("invalid currency")

const (
	browseQueryRole   = "browse"
	trackingQueryRole = "tracking"
)

var queryNamesByRole = map[string][]string{
	browseQueryRole:   {"home", "product", "cart", "checkout", "currencies", "product-meta"},
	trackingQueryRole: {"operation", "order"},
}

type queryRequest struct {
	ProductID      string   `json:"product_id"`
	UserID         string   `json:"user_id"`
	ProductIDs     []string `json:"product_ids"`
	CurrencyCode   string   `json:"currency_code"`
	OperationID    string   `json:"operation_id"`
	OrderID        string   `json:"order_id"`
	MinCartVersion uint64   `json:"min_cart_version"`
	CorrelationID  string   `json:"correlation_id"`
	Traceparent    string   `json:"traceparent,omitempty"`
	Tracestate     string   `json:"tracestate,omitempty"`
}

type localizedProduct struct {
	Item  *hipstershop.Product `json:"item"`
	Price *hipstershop.Money   `json:"price"`
}

type cartItemView struct {
	Item     *hipstershop.Product `json:"item"`
	Quantity int32                `json:"quantity"`
	Price    *hipstershop.Money   `json:"price"`
}

type queryResponse struct {
	Products          []localizedProduct        `json:"products,omitempty"`
	Product           *localizedProduct         `json:"product,omitempty"`
	ProductMeta       []*hipstershop.Product    `json:"product_meta,omitempty"`
	Items             []cartItemView            `json:"items,omitempty"`
	Currencies        []string                  `json:"currencies,omitempty"`
	Recommendations   []*hipstershop.Product    `json:"recommendations,omitempty"`
	Ad                *hipstershop.Ad           `json:"ad,omitempty"`
	ShippingCost      *hipstershop.Money        `json:"shipping_cost,omitempty"`
	ShippingPending   bool                      `json:"shipping_pending,omitempty"`
	CartSize          int                       `json:"cart_size"`
	CartVersion       uint64                    `json:"cart_version"`
	CatalogRevision   uint64                    `json:"catalog_revision"`
	RateRevision      uint64                    `json:"rate_revision"`
	QueryRevision     uint64                    `json:"query_revision"`
	UpdatedAt         time.Time                 `json:"updated_at"`
	Stale             []string                  `json:"stale,omitempty"`
	Operation         *storefront.OperationView `json:"operation,omitempty"`
	Order             *storefront.OrderView     `json:"order,omitempty"`
	Error             string                    `json:"error,omitempty"`
	RetryAfterSeconds int                       `json:"retry_after_seconds,omitempty"`
}

type queryAdmission struct {
	active atomic.Int64
	limit  int64
}

func (admission *queryAdmission) acquire() bool {
	for {
		active := admission.active.Load()
		if active >= admission.limit {
			return false
		}
		if admission.active.CompareAndSwap(active, active+1) {
			return true
		}
	}
}

func (admission *queryAdmission) release() {
	admission.active.Add(-1)
}

type queryHandler func(queryRequest) (queryResponse, error)

type decodedProjectionReader interface {
	decodedValue(string, func([]byte) (any, error)) (any, uint64, error)
}

func (p *projector) registerQueries(nc *nats.Conn, role string, stopped chan<- string) (micro.Service, int, error) {
	endpointPending := make(map[string]*atomic.Int64)
	var endpointPendingMutex sync.RWMutex
	handlers, err := p.queryHandlers(role)
	if err != nil {
		return nil, 0, err
	}
	service, err := micro.AddService(nc, micro.Config{
		Name:        "StorefrontProjection",
		Version:     "1.0.0",
		Description: "Event-built storefront read model",
		Metadata:    map[string]string{"role": role},
		QueueGroup:  "storefront-projection-v1",
		ErrorHandler: func(_ micro.Service, serviceErr *micro.NATSError) {
			log.Printf("NATS query service failed role=%q subject=%q error=%v", role, serviceErr.Subject, serviceErr)
		},
		DoneHandler: func(_ micro.Service) {
			log.Printf("NATS query service stopped role=%q", role)
			select {
			case stopped <- role:
			default:
				log.Printf("NATS query service stop notification already pending role=%q", role)
			}
		},
		StatsHandler: func(endpoint *micro.Endpoint) any {
			endpointPendingMutex.RLock()
			pending, ok := endpointPending[endpoint.Name]
			endpointPendingMutex.RUnlock()
			if !ok {
				return map[string]int64{"pending_requests": 0}
			}
			return map[string]int64{
				"pending_requests": pending.Load(),
			}
		},
	})
	if err != nil {
		return nil, 0, fmt.Errorf("register NATS service: %w", err)
	}
	debugQueries := slog.Default().Enabled(context.Background(), slog.LevelDebug)

	concurrency := p.config.queryConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	endpointCount := 0
	for name, handler := range handlers {
		name, handler := name, handler
		admission := &queryAdmission{limit: int64(p.config.queryMaxInFlight)}
		subject := "boutique.qry.storefront." + name + ".v1"
		requestHandler := micro.HandlerFunc(func(request micro.Request) {
			if !admission.acquire() {
				_ = request.RespondJSON(queryResponse{
					Error: "OVERLOADED", RetryAfterSeconds: 1,
				})
				return
			}
			defer admission.release()
			var decoded queryRequest
			var decodeErr error
			if len(request.Data()) > 0 {
				decodeErr = json.Unmarshal(request.Data(), &decoded)
			}
			correlationID := decoded.CorrelationID
			if correlationID == "" {
				correlationID = "unknown"
			}
			_, span := telemetry.StartConsumerSpan(context.Background(), subject, "query", "",
				correlationID, decoded.Traceparent, decoded.Tracestate)
			defer span.End()
			if debugQueries {
				slog.Debug("NATS query received",
					"topic", subject,
					"message_kind", "query",
					"correlation_id", correlationID)
			}
			if decodeErr != nil {
				telemetry.RecordError(span, decodeErr)
				respondErr := request.RespondJSON(queryResponse{Error: "INVALID_QUERY"})
				log.Printf("storefront query processing failed topic=%q correlation_id=%q error_code=%q error=%v response_error=%v",
					subject, correlationID, "INVALID_QUERY", decodeErr, respondErr)
				return
			}
			response, err := handler(decoded)
			telemetry.RecordError(span, err)
			switch {
			case errors.Is(err, nats.ErrKeyNotFound):
				response.Error = "NOT_FOUND"
			case errors.Is(err, errInvalidCurrency):
				response.Error = "INVALID_CURRENCY"
			case err != nil:
				response.Error = "PROJECTION_UNAVAILABLE"
			}
			respondErr := request.RespondJSON(response)
			switch {
			case respondErr != nil:
				log.Printf("storefront query processing failed topic=%q correlation_id=%q error_code=%q error=%v",
					subject, correlationID, response.Error, respondErr)
			case err != nil:
				log.Printf("storefront query processing failed topic=%q correlation_id=%q error_code=%q error=%v",
					subject, correlationID, response.Error, err)
			}
		})
		for slot := 0; slot < concurrency; slot++ {
			endpointName := name
			if concurrency > 1 {
				endpointName = fmt.Sprintf("%s-%02d", name, slot+1)
			}
			pending := &atomic.Int64{}
			endpointPendingMutex.Lock()
			endpointPending[endpointName] = pending
			endpointPendingMutex.Unlock()
			endpointHandler := micro.HandlerFunc(func(request micro.Request) {
				pending.Add(1)
				defer pending.Add(-1)
				requestHandler.Handle(request)
			})
			if err := service.AddEndpoint(
				endpointName,
				endpointHandler,
				micro.WithEndpointSubject(subject),
				micro.WithEndpointPendingLimits(p.config.queryPendingMessages, p.config.queryPendingBytes),
			); err != nil {
				endpointPendingMutex.Lock()
				delete(endpointPending, endpointName)
				endpointPendingMutex.Unlock()
				_ = service.Stop()
				return nil, 0, fmt.Errorf("register %s slot %d: %w", subject, slot+1, err)
			}
			endpointCount++
		}
	}
	if err := nc.Flush(); err != nil {
		_ = service.Stop()
		return nil, 0, err
	}
	return service, endpointCount, nil
}

func (p *projector) queryHandlers(role string) (map[string]queryHandler, error) {
	all := map[string]queryHandler{
		"home":         p.homeQuery,
		"product":      p.productQuery,
		"cart":         p.cartQuery,
		"checkout":     p.checkoutQuery,
		"currencies":   p.currenciesQuery,
		"product-meta": p.productMetaQuery,
		"operation":    p.operationQuery,
		"order":        p.orderQuery,
	}
	names, ok := queryNamesByRole[role]
	if !ok {
		return nil, fmt.Errorf("unknown query service role %q", role)
	}
	handlers := make(map[string]queryHandler, len(names))
	for _, name := range names {
		handlers[name] = all[name]
	}
	return handlers, nil
}

func (p *projector) orderQuery(request queryRequest) (queryResponse, error) {
	if request.OrderID == "" || request.UserID == "" {
		return queryResponse{}, nats.ErrKeyNotFound
	}
	key := storefront.OrderKey(request.OrderID)
	order, revision, err := getJSONWithRevision[storefront.OrderView](p.orders, key)
	if err != nil || order.UserID != request.UserID {
		if err == nil {
			err = nats.ErrKeyNotFound
		}
		return queryResponse{}, err
	}
	p.observeQueryRevision(revision)
	return queryResponse{Order: order, UpdatedAt: order.UpdatedAt, QueryRevision: revision}, nil
}

func (p *projector) operationQuery(request queryRequest) (queryResponse, error) {
	if request.OperationID == "" || request.UserID == "" {
		return queryResponse{}, nats.ErrKeyNotFound
	}
	key := storefront.OperationKey(request.OperationID)
	operation, revision, err := getJSONWithRevision[storefront.OperationView](p.operations, key)
	if err != nil || operation.UserID != request.UserID {
		if err == nil {
			err = nats.ErrKeyNotFound
		}
		return queryResponse{}, err
	}
	p.observeQueryRevision(revision)
	return queryResponse{Operation: operation, UpdatedAt: operation.UpdatedAt, QueryRevision: revision}, nil
}

func (p *projector) homeQuery(request queryRequest) (queryResponse, error) {
	rates, err := p.currencyView(request.CurrencyCode)
	if err != nil {
		return queryResponse{}, err
	}
	products, err := p.allProducts(nil)
	if err != nil {
		return queryResponse{}, err
	}
	response := queryResponse{
		Currencies: rates.SupportedCurrencies(), RateRevision: rates.RateRevision,
		UpdatedAt: rates.UpdatedAt, Products: make([]localizedProduct, 0, len(products)),
	}
	for _, product := range products {
		localized, err := localizeProduct(product.Product, rates, request.CurrencyCode)
		if err != nil {
			return queryResponse{}, err
		}
		response.Products = append(response.Products, *localized)
		response.CatalogRevision = max(response.CatalogRevision, product.CatalogRevision)
		response.UpdatedAt = latest(response.UpdatedAt, product.UpdatedAt)
	}
	cart, cartErr := p.cartViewCached(request.UserID)
	ad := p.currentAdCached(request.UserID, &response.Stale)
	if cartErr != nil {
		return queryResponse{}, cartErr
	}
	response.CartSize, response.CartVersion = cartSize(cart.Cart), cart.Cart.GetCartVersion()
	response.UpdatedAt = latest(response.UpdatedAt, cart.UpdatedAt)
	response.Ad = ad
	return response, nil
}

func (p *projector) productQuery(request queryRequest) (queryResponse, error) {
	product, productRevision, err := getJSONWithRevision[storefront.ProductView](p.catalogReader(), storefront.ProductKey(request.ProductID))
	if err != nil || product.Removed {
		if err == nil {
			err = nats.ErrKeyNotFound
		}
		return queryResponse{}, err
	}
	p.observeQueryRevision(productRevision)
	rates, err := p.currencyView(request.CurrencyCode)
	if err != nil {
		return queryResponse{}, err
	}
	localized, err := localizeProduct(product.Product, rates, request.CurrencyCode)
	if err != nil {
		return queryResponse{}, err
	}
	var recommendationStale, adStale []string
	cart, cartErr := p.cartViewCached(request.UserID)
	recommendations := p.currentRecommendationsCached(
		request.UserID, request.ProductID, &recommendationStale,
	)
	ad := p.currentAdCached(request.UserID, &adStale)
	if cartErr != nil {
		return queryResponse{}, cartErr
	}
	response := queryResponse{
		Product: localized, Currencies: rates.SupportedCurrencies(),
		CartSize: cartSize(cart.Cart), CartVersion: cart.Cart.GetCartVersion(),
		CatalogRevision: product.CatalogRevision, RateRevision: rates.RateRevision,
		UpdatedAt:       latest(product.UpdatedAt, rates.UpdatedAt, cart.UpdatedAt),
		Recommendations: recommendations, Ad: ad,
	}
	response.Stale = append(response.Stale, recommendationStale...)
	response.Stale = append(response.Stale, adStale...)
	return response, nil
}

func (p *projector) cartQuery(request queryRequest) (queryResponse, error) {
	return p.cartQueryFrom(request, p.cartView)
}

// checkoutQuery uses the watch-fed cart cache on the order-admission path.
// A caller that has already observed a cart version can require at least that
// version; a lagging or cold cache falls back to the authoritative KV bucket.
func (p *projector) checkoutQuery(request queryRequest) (queryResponse, error) {
	return p.cartQueryFrom(request, func(userID string) (*storefront.CartView, error) {
		cart, err := p.cachedCartIfPresent(userID)
		if err == nil && cart.Cart.GetCartVersion() >= request.MinCartVersion {
			return cart, nil
		}
		return p.cartView(userID)
	})
}

func (p *projector) cartQueryFrom(request queryRequest,
	readCart func(string) (*storefront.CartView, error)) (queryResponse, error) {
	rates, err := p.currencyView(request.CurrencyCode)
	if err != nil {
		return queryResponse{}, err
	}
	cart, err := readCart(request.UserID)
	if err != nil {
		return queryResponse{}, err
	}
	response := queryResponse{
		Currencies: rates.SupportedCurrencies(), CartVersion: cart.Cart.GetCartVersion(),
		RateRevision: rates.RateRevision, UpdatedAt: latest(rates.UpdatedAt, cart.UpdatedAt),
		Items: make([]cartItemView, 0, len(cart.Cart.GetItems())),
	}
	for _, line := range cart.Cart.GetItems() {
		product, productRevision, err := getJSONWithRevision[storefront.ProductView](p.catalogReader(), storefront.ProductKey(line.ProductId))
		if err != nil {
			return queryResponse{}, fmt.Errorf("cart product %s is unavailable: %w", line.ProductId, err)
		}
		if product.Removed {
			return queryResponse{}, fmt.Errorf("cart product %s is unavailable", line.ProductId)
		}
		p.observeQueryRevision(productRevision)
		localized, err := localizeProduct(product.Product, rates, request.CurrencyCode)
		if err != nil {
			return queryResponse{}, err
		}
		linePrice := multiplyMoney(localized.Price, uint32(line.Quantity))
		response.Items = append(response.Items, cartItemView{Item: localized.Item, Quantity: line.Quantity, Price: linePrice})
		response.CartSize += int(line.Quantity)
		response.CatalogRevision = max(response.CatalogRevision, product.CatalogRevision)
		response.UpdatedAt = latest(response.UpdatedAt, product.UpdatedAt)
	}
	response.Recommendations = p.currentRecommendationsCached(request.UserID, "", &response.Stale)
	if len(cart.Cart.GetItems()) == 0 {
		response.ShippingCost = &hipstershop.Money{CurrencyCode: request.CurrencyCode}
		return response, nil
	}
	quote, quoteRevision, err := getJSONWithRevision[storefront.CartQuoteView](p.context, storefront.CartQuoteKey(request.UserID))
	if err != nil || quote.CartVersion != response.CartVersion || quote.CostUSD == nil || quote.FailureCode != "" || expired(quote.ExpiresAt) {
		response.ShippingPending = true
		response.Stale = append(response.Stale, "shipping_quote")
		return response, nil
	}
	p.observeQueryRevision(quoteRevision)
	converted := rates.Convert(quote.CostUSD, request.CurrencyCode)
	if converted == nil {
		return queryResponse{}, errInvalidCurrency
	}
	response.ShippingCost = legacyMoney(converted)
	response.UpdatedAt = latest(response.UpdatedAt, quote.UpdatedAt)
	return response, nil
}

func (p *projector) currenciesQuery(request queryRequest) (queryResponse, error) {
	rates, err := p.currencyView(request.CurrencyCode)
	if err != nil {
		return queryResponse{}, err
	}
	return queryResponse{
		Currencies: rates.SupportedCurrencies(), RateRevision: rates.RateRevision, UpdatedAt: rates.UpdatedAt,
	}, nil
}

func (p *projector) productMetaQuery(request queryRequest) (queryResponse, error) {
	products, err := p.allProducts(request.ProductIDs)
	if err != nil {
		return queryResponse{}, err
	}
	if len(products) == 0 {
		return queryResponse{}, nats.ErrKeyNotFound
	}
	response := queryResponse{ProductMeta: make([]*hipstershop.Product, 0, len(products))}
	for _, product := range products {
		response.ProductMeta = append(response.ProductMeta, legacyProduct(product.Product))
		response.CatalogRevision = max(response.CatalogRevision, product.CatalogRevision)
		response.UpdatedAt = latest(response.UpdatedAt, product.UpdatedAt)
	}
	return response, nil
}

func (p *projector) currencyView(currencyCode string) (*storefront.CurrencyView, error) {
	rates, revision, err := getJSONWithRevision[storefront.CurrencyView](p.catalogReader(), storefront.CurrencyKey)
	if err != nil {
		return nil, err
	}
	p.observeQueryRevision(revision)
	if currencyCode == "" {
		currencyCode = "USD"
	}
	for _, code := range rates.SupportedCurrencies() {
		if code == currencyCode {
			return rates, nil
		}
	}
	return nil, errInvalidCurrency
}

func (p *projector) cartView(userID string) (*storefront.CartView, error) {
	return p.cartViewFrom(p.carts, userID)
}

func (p *projector) cartViewCached(userID string) (*storefront.CartView, error) {
	if p.cartCache == nil {
		return p.cartView(userID)
	}
	return p.cartViewFrom(cachedOnlyProjectionReader{cache: p.cartCache}, userID)
}

// cachedCartIfPresent preserves a cache miss so checkout can distinguish it
// from an authoritative missing cart. cartViewCached intentionally retains
// empty-cart semantics for latency-tolerant browse queries.
func (p *projector) cachedCartIfPresent(userID string) (*storefront.CartView, error) {
	if p.cartCache == nil {
		return nil, nats.ErrKeyNotFound
	}
	cart, revision, err := getJSONWithRevision[storefront.CartView](
		cachedOnlyProjectionReader{cache: p.cartCache}, userID,
	)
	if err != nil {
		return nil, err
	}
	p.observeQueryRevision(revision)
	return cart, nil
}

func (p *projector) cartViewFrom(reader projectionReader, userID string) (*storefront.CartView, error) {
	if userID == "" {
		return &storefront.CartView{Cart: &commonv1.CartSnapshot{}}, nil
	}
	cart, revision, err := getJSONWithRevision[storefront.CartView](reader, userID)
	if errors.Is(err, nats.ErrKeyNotFound) {
		return &storefront.CartView{Cart: &commonv1.CartSnapshot{UserId: userID}}, nil
	}
	if err != nil {
		return nil, err
	}
	p.observeQueryRevision(revision)
	return cart, nil
}

func (p *projector) currentRecommendations(sessionID, excludedProductID string, stale *[]string) []*hipstershop.Product {
	return p.currentRecommendationsFrom(p.context, sessionID, excludedProductID, stale)
}

func (p *projector) currentRecommendationsCached(sessionID, excludedProductID string, stale *[]string) []*hipstershop.Product {
	if p.contextCache == nil {
		return p.currentRecommendations(sessionID, excludedProductID, stale)
	}
	return p.currentRecommendationsFrom(
		cachedOnlyProjectionReader{cache: p.contextCache}, sessionID, excludedProductID, stale,
	)
}

func (p *projector) currentRecommendationsFrom(reader projectionReader, sessionID, excludedProductID string, stale *[]string) []*hipstershop.Product {
	view, revision, err := getJSONWithRevision[storefront.RecommendationView](reader, storefront.RecommendationKey(sessionID))
	if err != nil || view.FailureCode != "" || expired(view.ExpiresAt) {
		*stale = append(*stale, "recommendations")
		return nil
	}
	p.observeQueryRevision(revision)
	products := make([]*hipstershop.Product, 0, min(4, len(view.ProductIDs)))
	for _, id := range view.ProductIDs {
		if id == excludedProductID {
			continue
		}
		product, revision, err := getJSONWithRevision[storefront.ProductView](p.catalogReader(), storefront.ProductKey(id))
		if err == nil && !product.Removed {
			p.observeQueryRevision(revision)
			products = append(products, legacyProduct(product.Product))
		}
		if len(products) == 4 {
			break
		}
	}
	return products
}

func (p *projector) currentAd(sessionID string, stale *[]string) *hipstershop.Ad {
	return p.currentAdFrom(p.context, sessionID, stale)
}

func (p *projector) currentAdCached(sessionID string, stale *[]string) *hipstershop.Ad {
	if p.contextCache == nil {
		return p.currentAd(sessionID, stale)
	}
	return p.currentAdFrom(cachedOnlyProjectionReader{cache: p.contextCache}, sessionID, stale)
}

func (p *projector) currentAdFrom(reader projectionReader, sessionID string, stale *[]string) *hipstershop.Ad {
	view, revision, err := getJSONWithRevision[storefront.AdView](reader, storefront.AdKey(sessionID))
	if err != nil || view.FailureCode != "" || expired(view.ExpiresAt) || len(view.Ads) == 0 {
		*stale = append(*stale, "ad")
		return nil
	}
	p.observeQueryRevision(revision)
	return &hipstershop.Ad{RedirectUrl: view.Ads[0].RedirectURL, Text: view.Ads[0].Text}
}

func expired(value time.Time) bool { return !value.IsZero() && time.Now().After(value) }

func localizeProduct(product *commonv1.ProductSnapshot, rates *storefront.CurrencyView, currency string) (*localizedProduct, error) {
	if product == nil || product.PriceUsd == nil {
		return nil, errors.New("product snapshot is incomplete")
	}
	if currency == "" {
		currency = "USD"
	}
	price := rates.Convert(product.PriceUsd, currency)
	if price == nil {
		return nil, errInvalidCurrency
	}
	return &localizedProduct{Item: legacyProduct(product), Price: legacyMoney(price)}, nil
}

func legacyProduct(product *commonv1.ProductSnapshot) *hipstershop.Product {
	if product == nil {
		return nil
	}
	return &hipstershop.Product{
		Id: product.ProductId, Name: product.Name, Description: product.Description,
		Picture: product.Picture, PriceUsd: legacyMoney(product.PriceUsd),
		Categories: append([]string(nil), product.Categories...),
	}
}

func legacyMoney(value *commonv1.Money) *hipstershop.Money {
	if value == nil {
		return nil
	}
	return &hipstershop.Money{CurrencyCode: value.CurrencyCode, Units: value.Units, Nanos: value.Nanos}
}

func multiplyMoney(value *hipstershop.Money, quantity uint32) *hipstershop.Money {
	if value == nil {
		return nil
	}
	nanos := int64(value.Nanos) * int64(quantity)
	return &hipstershop.Money{
		CurrencyCode: value.CurrencyCode,
		Units:        value.Units*int64(quantity) + nanos/1_000_000_000,
		Nanos:        int32(nanos % 1_000_000_000),
	}
}

func cartSize(cart *commonv1.CartSnapshot) int {
	var size int
	for _, line := range cart.GetItems() {
		size += int(line.Quantity)
	}
	return size
}

func latest(values ...time.Time) time.Time {
	var result time.Time
	for _, value := range values {
		if value.After(result) {
			result = value
		}
	}
	return result
}

func (p *projector) allProducts(only []string) ([]storefront.ProductView, error) {
	if len(only) == 0 && p.catalog != nil {
		generation := p.catalog.Generation()
		p.catalogSnapshotMu.Lock()
		defer p.catalogSnapshotMu.Unlock()
		if p.catalogSnapshot != nil && p.catalogSnapshotGeneration == generation {
			return p.catalogSnapshot, nil
		}
		products, err := p.readProducts(nil)
		if err != nil {
			return nil, err
		}
		p.catalogSnapshot = products
		p.catalogSnapshotGeneration = generation
		return products, nil
	}
	return p.readProducts(only)
}

func (p *projector) readProducts(only []string) ([]storefront.ProductView, error) {
	wanted := make(map[string]bool, len(only))
	for _, id := range only {
		wanted[id] = true
	}
	reader := p.catalogReader()
	keys, err := reader.Keys()
	if errors.Is(err, nats.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	products := make([]storefront.ProductView, 0, len(keys))
	for _, key := range keys {
		if !strings.HasPrefix(key, "product.") {
			continue
		}
		id := strings.TrimPrefix(key, "product.")
		if len(wanted) > 0 && !wanted[id] {
			continue
		}
		product, revision, err := getJSONWithRevision[storefront.ProductView](reader, key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		p.observeQueryRevision(revision)
		if !product.Removed {
			products = append(products, *product)
		}
	}
	sort.Slice(products, func(i, j int) bool { return products[i].Product.ProductId < products[j].Product.ProductId })
	return products, nil
}

func getJSON[T any](bucket projectionReader, key string) (*T, error) {
	value, _, err := getJSONWithRevision[T](bucket, key)
	return value, err
}

func getJSONWithRevision[T any](bucket projectionReader, key string) (*T, uint64, error) {
	if decoded, ok := bucket.(decodedProjectionReader); ok {
		value, revision, err := decoded.decodedValue(key, func(data []byte) (any, error) {
			var value T
			if err := json.Unmarshal(data, &value); err != nil {
				return nil, err
			}
			if preparable, ok := any(&value).(interface{ Prepare() }); ok {
				preparable.Prepare()
			}
			return &value, nil
		})
		if err != nil {
			return nil, 0, err
		}
		return value.(*T), revision, nil
	}
	entry, err := bucket.Get(key)
	if err != nil {
		return nil, 0, err
	}
	var value T
	if err := json.Unmarshal(entry.Value(), &value); err != nil {
		return nil, 0, err
	}
	return &value, entry.Revision(), nil
}
