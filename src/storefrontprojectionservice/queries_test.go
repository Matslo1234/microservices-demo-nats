// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"testing"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	"github.com/GoogleCloudPlatform/microservices-demo/src/storefrontprojectionservice/internal/storefront"
)

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
	current := storefront.OrderView{OrderID:"order-1", UserID:"user-1", Status:"PROCESSING", Stage:"WAITING_FOR_CAPTURE",
		AggregateVersion:4, Snapshot:&commonv1.SanitizedOrderSnapshot{OrderId:"order-1"}}
	completed := storefront.OrderView{OrderID:"order-1", Status:"COMPLETED", Stage:"COMPLETED", AggregateVersion:4,
		Snapshot:&commonv1.SanitizedOrderSnapshot{OrderId:"order-1", TrackingId:"track-1"}}
	merged := mergeOrder(current, completed)
	if merged.Status != "COMPLETED" || merged.UserID != "user-1" || merged.Snapshot.GetTrackingId() != "track-1" {
		t.Fatalf("same-version terminal facts did not merge: %#v", merged)
	}
	lateStage := mergeOrder(merged, storefront.OrderView{OrderID:"order-1", Status:"PROCESSING", Stage:"WAITING_FOR_CAPTURE", AggregateVersion:4})
	if lateStage.Status != "COMPLETED" || lateStage.Stage != "COMPLETED" { t.Fatalf("late stage downgraded order: %#v", lateStage) }
}
