// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	hipstershop "github.com/GoogleCloudPlatform/microservices-demo/hipstershop"
	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	"github.com/GoogleCloudPlatform/microservices-demo/src/storefrontprojectionservice/internal/storefront"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

var errInvalidCurrency = errors.New("invalid currency")

type queryRequest struct {
	ProductID    string   `json:"product_id"`
	UserID       string   `json:"user_id"`
	ProductIDs   []string `json:"product_ids"`
	CurrencyCode string   `json:"currency_code"`
	OperationID  string   `json:"operation_id"`
	OrderID      string   `json:"order_id"`
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
	Products        []localizedProduct        `json:"products,omitempty"`
	Product         *localizedProduct         `json:"product,omitempty"`
	ProductMeta     []*hipstershop.Product    `json:"product_meta,omitempty"`
	Items           []cartItemView            `json:"items,omitempty"`
	Currencies      []string                  `json:"currencies,omitempty"`
	Recommendations []*hipstershop.Product    `json:"recommendations,omitempty"`
	Ad              *hipstershop.Ad           `json:"ad,omitempty"`
	ShippingCost    *hipstershop.Money        `json:"shipping_cost,omitempty"`
	ShippingPending bool                      `json:"shipping_pending,omitempty"`
	CartSize        int                       `json:"cart_size"`
	CartVersion     uint64                    `json:"cart_version"`
	CatalogRevision uint64                    `json:"catalog_revision"`
	RateRevision    uint64                    `json:"rate_revision"`
	UpdatedAt       time.Time                 `json:"updated_at"`
	Stale           []string                  `json:"stale,omitempty"`
	Operation       *storefront.OperationView `json:"operation,omitempty"`
	Order           *storefront.OrderView     `json:"order,omitempty"`
	Error           string                    `json:"error,omitempty"`
}

type queryHandler func(queryRequest) (queryResponse, error)

func (p *projector) registerQueries(nc *nats.Conn) (micro.Service, int, error) {
	service, err := micro.AddService(nc, micro.Config{
		Name:        "StorefrontProjection",
		Version:     "1.0.0",
		Description: "Event-built storefront read model",
		QueueGroup:  "storefront-projection-v1",
	})
	if err != nil {
		return nil, 0, fmt.Errorf("register NATS service: %w", err)
	}

	handlers := map[string]queryHandler{
		"home":         p.homeQuery,
		"product":      p.productQuery,
		"cart":         p.cartQuery,
		"currencies":   p.currenciesQuery,
		"product-meta": p.productMetaQuery,
		"operation":    p.operationQuery,
		"order":        p.orderQuery,
	}
	for name, handler := range handlers {
		name, handler := name, handler
		subject := "boutique.qry.storefront." + name + ".v1"
		if err := service.AddEndpoint(name, micro.HandlerFunc(func(request micro.Request) {
			var decoded queryRequest
			if len(request.Data()) > 0 {
				if err := json.Unmarshal(request.Data(), &decoded); err != nil {
					_ = request.RespondJSON(queryResponse{Error: "INVALID_QUERY"})
					return
				}
			}
			response, err := handler(decoded)
			switch {
			case errors.Is(err, nats.ErrKeyNotFound):
				response.Error = "NOT_FOUND"
			case errors.Is(err, errInvalidCurrency):
				response.Error = "INVALID_CURRENCY"
			case err != nil:
				response.Error = "PROJECTION_UNAVAILABLE"
			}
			_ = request.RespondJSON(response)
		}), micro.WithEndpointSubject(subject)); err != nil {
			_ = service.Stop()
			return nil, 0, fmt.Errorf("register %s: %w", subject, err)
		}
	}
	if err := nc.Flush(); err != nil {
		_ = service.Stop()
		return nil, 0, err
	}
	return service, len(handlers), nil
}

func (p *projector) orderQuery(request queryRequest) (queryResponse, error) {
	if request.OrderID == "" || request.UserID == "" {
		return queryResponse{}, nats.ErrKeyNotFound
	}
	order, err := getJSON[storefront.OrderView](p.orders, storefront.OrderKey(request.OrderID))
	if err != nil || order.UserID != request.UserID {
		if err == nil {
			err = nats.ErrKeyNotFound
		}
		return queryResponse{}, err
	}
	return queryResponse{Order: order, UpdatedAt: order.UpdatedAt}, nil
}

func (p *projector) operationQuery(request queryRequest) (queryResponse, error) {
	if request.OperationID == "" || request.UserID == "" {
		return queryResponse{}, nats.ErrKeyNotFound
	}
	operation, err := getJSON[storefront.OperationView](p.operations, storefront.OperationKey(request.OperationID))
	if err != nil || operation.UserID != request.UserID {
		if err == nil {
			err = nats.ErrKeyNotFound
		}
		return queryResponse{}, err
	}
	return queryResponse{Operation: operation, UpdatedAt: operation.UpdatedAt}, nil
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
		UpdatedAt: rates.UpdatedAt,
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
	cart, err := p.cartView(request.UserID)
	if err != nil {
		return queryResponse{}, err
	}
	response.CartSize, response.CartVersion = cartSize(cart.Cart), cart.Cart.GetCartVersion()
	response.UpdatedAt = latest(response.UpdatedAt, cart.UpdatedAt)
	response.Ad = p.currentAd(request.UserID, &response.Stale)
	return response, nil
}

func (p *projector) productQuery(request queryRequest) (queryResponse, error) {
	product, err := getJSON[storefront.ProductView](p.products, storefront.ProductKey(request.ProductID))
	if err != nil || product.Removed {
		if err == nil {
			err = nats.ErrKeyNotFound
		}
		return queryResponse{}, err
	}
	rates, err := p.currencyView(request.CurrencyCode)
	if err != nil {
		return queryResponse{}, err
	}
	localized, err := localizeProduct(product.Product, rates, request.CurrencyCode)
	if err != nil {
		return queryResponse{}, err
	}
	cart, err := p.cartView(request.UserID)
	if err != nil {
		return queryResponse{}, err
	}
	response := queryResponse{
		Product: localized, Currencies: rates.SupportedCurrencies(),
		CartSize: cartSize(cart.Cart), CartVersion: cart.Cart.GetCartVersion(),
		CatalogRevision: product.CatalogRevision, RateRevision: rates.RateRevision,
		UpdatedAt: latest(product.UpdatedAt, rates.UpdatedAt, cart.UpdatedAt),
	}
	response.Recommendations = p.currentRecommendations(request.UserID, request.ProductID, &response.Stale)
	response.Ad = p.currentAd(request.UserID, &response.Stale)
	return response, nil
}

func (p *projector) cartQuery(request queryRequest) (queryResponse, error) {
	rates, err := p.currencyView(request.CurrencyCode)
	if err != nil {
		return queryResponse{}, err
	}
	cart, err := p.cartView(request.UserID)
	if err != nil {
		return queryResponse{}, err
	}
	response := queryResponse{
		Currencies: rates.SupportedCurrencies(), CartVersion: cart.Cart.GetCartVersion(),
		RateRevision: rates.RateRevision, UpdatedAt: latest(rates.UpdatedAt, cart.UpdatedAt),
	}
	for _, line := range cart.Cart.GetItems() {
		product, err := getJSON[storefront.ProductView](p.products, storefront.ProductKey(line.ProductId))
		if err != nil {
			return queryResponse{}, fmt.Errorf("cart product %s is unavailable: %w", line.ProductId, err)
		}
		if product.Removed {
			return queryResponse{}, fmt.Errorf("cart product %s is unavailable", line.ProductId)
		}
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
	response.Recommendations = p.currentRecommendations(request.UserID, "", &response.Stale)
	if len(cart.Cart.GetItems()) == 0 {
		response.ShippingCost = &hipstershop.Money{CurrencyCode: request.CurrencyCode}
		return response, nil
	}
	quote, err := getJSON[storefront.CartQuoteView](p.context, storefront.CartQuoteKey(request.UserID))
	if err != nil || quote.CartVersion != response.CartVersion || quote.CostUSD == nil || quote.FailureCode != "" || expired(quote.ExpiresAt) {
		response.ShippingPending = true
		response.Stale = append(response.Stale, "shipping_quote")
		return response, nil
	}
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
	response := queryResponse{}
	for _, product := range products {
		response.ProductMeta = append(response.ProductMeta, legacyProduct(product.Product))
		response.CatalogRevision = max(response.CatalogRevision, product.CatalogRevision)
		response.UpdatedAt = latest(response.UpdatedAt, product.UpdatedAt)
	}
	return response, nil
}

func (p *projector) currencyView(currencyCode string) (*storefront.CurrencyView, error) {
	rates, err := getJSON[storefront.CurrencyView](p.products, storefront.CurrencyKey)
	if err != nil {
		return nil, err
	}
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
	if userID == "" {
		return &storefront.CartView{Cart: &commonv1.CartSnapshot{}}, nil
	}
	cart, err := getJSON[storefront.CartView](p.carts, userID)
	if errors.Is(err, nats.ErrKeyNotFound) {
		return &storefront.CartView{Cart: &commonv1.CartSnapshot{UserId: userID}}, nil
	}
	return cart, err
}

func (p *projector) currentRecommendations(sessionID, excludedProductID string, stale *[]string) []*hipstershop.Product {
	view, err := getJSON[storefront.RecommendationView](p.context, storefront.RecommendationKey(sessionID))
	if err != nil || view.FailureCode != "" || expired(view.ExpiresAt) {
		*stale = append(*stale, "recommendations")
		return nil
	}
	var products []*hipstershop.Product
	for _, id := range view.ProductIDs {
		if id == excludedProductID {
			continue
		}
		product, err := getJSON[storefront.ProductView](p.products, storefront.ProductKey(id))
		if err == nil && !product.Removed {
			products = append(products, legacyProduct(product.Product))
		}
		if len(products) == 4 {
			break
		}
	}
	return products
}

func (p *projector) currentAd(sessionID string, stale *[]string) *hipstershop.Ad {
	view, err := getJSON[storefront.AdView](p.context, storefront.AdKey(sessionID))
	if err != nil || view.FailureCode != "" || expired(view.ExpiresAt) || len(view.Ads) == 0 {
		*stale = append(*stale, "ad")
		return nil
	}
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
	wanted := make(map[string]bool, len(only))
	for _, id := range only {
		wanted[id] = true
	}
	keys, err := p.products.Keys()
	if errors.Is(err, nats.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var products []storefront.ProductView
	for _, key := range keys {
		if !strings.HasPrefix(key, "product.") {
			continue
		}
		id := strings.TrimPrefix(key, "product.")
		if len(wanted) > 0 && !wanted[id] {
			continue
		}
		product, err := getJSON[storefront.ProductView](p.products, key)
		if errors.Is(err, nats.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !product.Removed {
			products = append(products, *product)
		}
	}
	sort.Slice(products, func(i, j int) bool { return products[i].Product.ProductId < products[j].Product.ProductId })
	return products, nil
}

func getJSON[T any](bucket nats.KeyValue, key string) (*T, error) {
	entry, err := bucket.Get(key)
	if err != nil {
		return nil, err
	}
	var value T
	if err := json.Unmarshal(entry.Value(), &value); err != nil {
		return nil, err
	}
	return &value, nil
}
