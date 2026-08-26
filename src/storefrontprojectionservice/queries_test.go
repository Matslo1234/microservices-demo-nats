// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"slices"
	"sort"
	"testing"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	"github.com/GoogleCloudPlatform/microservices-demo/src/storefrontprojectionservice/internal/storefront"
)

func TestQueryAdmissionRejectsAboveLimitAndRecovers(t *testing.T) {
	admission := &queryAdmission{limit: 2}
	if !admission.acquire() || !admission.acquire() {
		t.Fatal("admission rejected capacity below its limit")
	}
	if admission.acquire() {
		t.Fatal("admission accepted capacity above its limit")
	}
	admission.release()
	if !admission.acquire() {
		t.Fatal("admission did not recover released capacity")
	}
	admission.release()
	admission.release()
	if active := admission.active.Load(); active != 0 {
		t.Fatalf("active queries = %d, want 0", active)
	}
}

func TestQueryServicesIsolateBrowseFromTracking(t *testing.T) {
	projector := &projector{}
	browse, err := projector.queryHandlers(browseQueryRole)
	if err != nil {
		t.Fatal(err)
	}
	tracking, err := projector.queryHandlers(trackingQueryRole)
	if err != nil {
		t.Fatal(err)
	}
	browseNames := make([]string, 0, len(browse))
	for name := range browse {
		browseNames = append(browseNames, name)
	}
	trackingNames := make([]string, 0, len(tracking))
	for name := range tracking {
		trackingNames = append(trackingNames, name)
	}
	sort.Strings(browseNames)
	sort.Strings(trackingNames)
	if !slices.Equal(browseNames, []string{"cart", "currencies", "home", "product", "product-meta"}) {
		t.Fatalf("unexpected browse handlers: %v", browseNames)
	}
	if !slices.Equal(trackingNames, []string{"operation", "order"}) {
		t.Fatalf("unexpected tracking handlers: %v", trackingNames)
	}
}

func TestLocalizeProductBuildsLegacyView(t *testing.T) {
	rates := &storefront.CurrencyView{Rates: []storefront.Rate{
		{CurrencyCode: "EUR", UnitsPerBase: 1},
		{CurrencyCode: "USD", UnitsPerBase: 2},
	}}
	product := &commonv1.ProductSnapshot{
		ProductId: "sku", Name: "Product", PriceUsd: &commonv1.Money{CurrencyCode: "EUR", Units: 3},
	}
	view, err := localizeProduct(product, rates, "USD")
	if err != nil {
		t.Fatal(err)
	}
	if view.Item.Id != "sku" || view.Price.CurrencyCode != "USD" || view.Price.Units != 6 {
		t.Fatalf("unexpected localized product: %#v", view)
	}
}

func TestMultiplyMoneyCarriesNanos(t *testing.T) {
	result := multiplyMoney(legacyMoney(&commonv1.Money{CurrencyCode: "USD", Units: 1, Nanos: 750_000_000}), 2)
	if result.Units != 3 || result.Nanos != 500_000_000 {
		t.Fatalf("unexpected multiplication result: %#v", result)
	}
}

func TestMergeOperationDoesNotDowngradeTerminalState(t *testing.T) {
	terminal := storefront.OperationView{
		OperationID: "operation-1", CommandID: "operation-1", Status: "SUCCEEDED", UserID: "user-1",
		CartVersion: 4,
	}
	accepted := storefront.OperationView{
		OperationID: "operation-1", CommandID: "operation-1", Kind: "cart.add-item",
		Status: "QUEUED", UserID: "user-1",
	}
	merged := mergeOperation(terminal, accepted)
	if merged.Status != "SUCCEEDED" || merged.CartVersion != 4 || merged.Kind != "cart.add-item" {
		t.Fatalf("accepted event downgraded terminal operation: %#v", merged)
	}
}

func TestMergeOrderCombinesSameVersionTerminalFacts(t *testing.T) {
	current := storefront.OrderView{OrderID: "order-1", UserID: "user-1", Status: "PROCESSING", Stage: "WAITING_FOR_CAPTURE",
		AggregateVersion: 4, Snapshot: &commonv1.SanitizedOrderSnapshot{OrderId: "order-1"}}
	completed := storefront.OrderView{OrderID: "order-1", Status: "COMPLETED", Stage: "COMPLETED", AggregateVersion: 4,
		Snapshot: &commonv1.SanitizedOrderSnapshot{OrderId: "order-1", TrackingId: "track-1"}}
	merged := mergeOrder(current, completed)
	if merged.Status != "COMPLETED" || merged.UserID != "user-1" || merged.Snapshot.GetTrackingId() != "track-1" {
		t.Fatalf("same-version terminal facts did not merge: %#v", merged)
	}
	lateStage := mergeOrder(merged, storefront.OrderView{OrderID: "order-1", Status: "PROCESSING", Stage: "WAITING_FOR_CAPTURE", AggregateVersion: 4})
	if lateStage.Status != "COMPLETED" || lateStage.Stage != "COMPLETED" {
		t.Fatalf("late stage downgraded order: %#v", lateStage)
	}
}

func TestMergeOrderPreservesIndependentSettlementFacts(t *testing.T) {
	current := storefront.OrderView{
		OrderID: "order-1", CartClearStatus: "REJECTED", CartClearFailureCode: "VERSION_CONFLICT",
		NotificationStatus: "SENT",
	}
	completed := storefront.OrderView{
		OrderID: "order-1", UserID: "user-1", Status: "COMPLETED", Stage: "COMPLETED",
		AggregateVersion: 4,
	}
	merged := mergeOrder(current, completed)
	if merged.CartClearStatus != "REJECTED" || merged.CartClearFailureCode != "VERSION_CONFLICT" || merged.NotificationStatus != "SENT" {
		t.Fatalf("order update discarded settlement facts: %#v", merged)
	}
}

func TestMergeOrderPreservesFirstTerminalOutcomeTime(t *testing.T) {
	first := time.Date(2026, time.July, 26, 10, 11, 12, 123, time.UTC)
	later := first.Add(time.Second)
	current := storefront.OrderView{
		OrderID: "order-1", Status: "COMPLETED", Stage: "COMPLETED", OutcomeAt: &first,
	}
	settlement := storefront.OrderView{
		OrderID: "order-1", Status: "COMPLETED", NotificationStatus: "SENT", OutcomeAt: &later, UpdatedAt: later,
	}

	merged := mergeOrder(current, settlement)

	if merged.OutcomeAt == nil || !merged.OutcomeAt.Equal(first) {
		t.Fatalf("terminal outcome time was not preserved: %#v", merged.OutcomeAt)
	}
}

func TestTerminalOrderOutcomeAtOnlyTimestampsTerminalStates(t *testing.T) {
	value := time.Date(2026, time.July, 26, 10, 11, 12, 123, time.FixedZone("CEST", 2*60*60))
	if outcomeAt := terminalOrderOutcomeAt("PROCESSING", value); outcomeAt != nil {
		t.Fatalf("processing order unexpectedly has outcome time %v", outcomeAt)
	}
	outcomeAt := terminalOrderOutcomeAt("COMPLETED", value)
	if outcomeAt == nil || !outcomeAt.Equal(value) || outcomeAt.Location() != time.UTC {
		t.Fatalf("unexpected terminal outcome time: %v", outcomeAt)
	}
}

func TestStatusQueriesReadLatestAuthoritativeKVView(t *testing.T) {
	orders := newMemoryKV()
	operations := newMemoryKV()
	projector := &projector{orders: orders, operations: operations}
	orderKey := storefront.OrderKey("order-1")
	storeJSON(t, orders, orderKey, storefront.OrderView{
		OrderID: "order-1", UserID: "user-1", Status: "PROCESSING",
	})
	orderRevision := storeJSON(t, orders, orderKey, storefront.OrderView{
		OrderID: "order-1", UserID: "user-1", Status: "COMPLETED",
	})
	order, err := projector.orderQuery(queryRequest{OrderID: "order-1", UserID: "user-1"})
	if err != nil || order.Order == nil || order.Order.Status != "COMPLETED" ||
		order.QueryRevision != orderRevision {
		t.Fatalf("order query did not use authoritative KV: response=%#v error=%v", order, err)
	}

	operationKey := storefront.OperationKey("operation-1")
	storeJSON(t, operations, operationKey, storefront.OperationView{
		OperationID: "operation-1", CommandID: "operation-1", UserID: "user-1", Status: "QUEUED",
	})
	operationRevision := storeJSON(t, operations, operationKey, storefront.OperationView{
		OperationID: "operation-1", CommandID: "operation-1", UserID: "user-1", Status: "SUCCEEDED",
	})
	operation, err := projector.operationQuery(queryRequest{OperationID: "operation-1", UserID: "user-1"})
	if err != nil || operation.Operation == nil || operation.Operation.Status != "SUCCEEDED" ||
		operation.QueryRevision != operationRevision {
		t.Fatalf("operation query did not use authoritative KV: response=%#v error=%v", operation, err)
	}
}

func TestCartQueryReadsLatestAuthoritativeKVView(t *testing.T) {
	carts := newMemoryKV()
	projector := &projector{carts: carts}
	storeJSON(t, carts, "user-1", storefront.CartView{
		Cart: &commonv1.CartSnapshot{UserId: "user-1", CartVersion: 2},
	})
	storeJSON(t, carts, "user-1", storefront.CartView{
		Cart: &commonv1.CartSnapshot{UserId: "user-1", CartVersion: 3},
	})

	cart, err := projector.cartView("user-1")
	if err != nil || cart.Cart.GetCartVersion() != 3 {
		t.Fatalf("cart query did not read latest authoritative KV: cart=%#v error=%v", cart, err)
	}
}

func TestCartQueryUsesSynchronizedCatalogCache(t *testing.T) {
	products := newMemoryKV()
	storeJSON(t, products, storefront.CurrencyKey, storefront.CurrencyView{
		BaseCurrencyCode: "USD",
		Rates:            []storefront.Rate{{CurrencyCode: "USD", UnitsPerBase: 1}},
		RateRevision:     1,
	})
	storeJSON(t, products, storefront.ProductKey("sku"), storefront.ProductView{
		Product: &commonv1.ProductSnapshot{
			ProductId: "sku",
			Name:      "cached product",
			PriceUsd:  &commonv1.Money{CurrencyCode: "USD", Units: 10},
		},
		CatalogRevision: 1,
	})
	cache, err := newProjectionReadCache(products)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cache.Close)

	carts, context := newMemoryKV(), newMemoryKV()
	storeJSON(t, carts, "user-1", storefront.CartView{
		Cart: &commonv1.CartSnapshot{
			UserId:      "user-1",
			CartVersion: 1,
			Items: []*commonv1.CartLine{
				{ProductId: "sku", Quantity: 1},
			},
		},
	})
	projector := &projector{
		products: products,
		catalog:  cache,
		carts:    carts,
		context:  context,
	}
	products.resetGetCount()

	response, err := projector.cartQuery(queryRequest{
		UserID: "user-1", CurrencyCode: "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 1 || response.Items[0].Item.Name != "cached product" {
		t.Fatalf("unexpected cart response: %#v", response)
	}
	if gets := products.getCount(); gets != 0 {
		t.Fatalf("cart query performed %d authoritative product KV reads", gets)
	}
}

func TestProductQueryUsesSessionCachesWithoutAuthoritativeReads(t *testing.T) {
	products := newMemoryKV()
	storeJSON(t, products, storefront.CurrencyKey, storefront.CurrencyView{
		BaseCurrencyCode: "USD",
		Rates:            []storefront.Rate{{CurrencyCode: "USD", UnitsPerBase: 1}},
		RateRevision:     1,
	})
	storeJSON(t, products, storefront.ProductKey("sku"), storefront.ProductView{
		Product: &commonv1.ProductSnapshot{
			ProductId: "sku", PriceUsd: &commonv1.Money{CurrencyCode: "USD", Units: 10},
		},
		CatalogRevision: 1,
	})
	storeJSON(t, products, storefront.ProductKey("sku-2"), storefront.ProductView{
		Product: &commonv1.ProductSnapshot{
			ProductId: "sku-2", PriceUsd: &commonv1.Money{CurrencyCode: "USD", Units: 12},
		},
		CatalogRevision: 1,
	})
	catalog, err := newProjectionReadCache(products)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(catalog.Close)

	carts := newMemoryKV()
	storeJSON(t, carts, "user-1", storefront.CartView{
		Cart: &commonv1.CartSnapshot{UserId: "user-1", CartVersion: 3},
	})
	cartCache, err := newBoundedProjectionReadCache(carts, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cartCache.Close)

	context := newMemoryKV()
	storeJSON(t, context, storefront.RecommendationKey("user-1"), storefront.RecommendationView{
		ProductIDs: []string{"sku-2"},
	})
	storeJSON(t, context, storefront.AdKey("user-1"), storefront.AdView{
		Ads: []storefront.Ad{{Text: "cached ad", RedirectURL: "/product/sku"}},
	})
	contextCache, err := newBoundedProjectionReadCache(context, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(contextCache.Close)

	products.resetGetCount()
	carts.resetGetCount()
	context.resetGetCount()
	projector := &projector{
		products: products, catalog: catalog,
		carts: carts, cartCache: cartCache,
		context: context, contextCache: contextCache,
	}
	response, err := projector.productQuery(queryRequest{
		ProductID: "sku", UserID: "user-1", CurrencyCode: "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.CartVersion != 3 || response.Ad == nil || response.Ad.Text != "cached ad" ||
		len(response.Recommendations) != 1 {
		t.Fatalf("unexpected cached product response: %#v", response)
	}
	if products.getCount() != 0 || carts.getCount() != 0 || context.getCount() != 0 {
		t.Fatalf("product query performed authoritative reads: products=%d carts=%d context=%d",
			products.getCount(), carts.getCount(), context.getCount())
	}
}
