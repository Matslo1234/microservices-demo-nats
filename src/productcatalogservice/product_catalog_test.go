// Copyright 2023 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"testing"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
)

func TestCatalogIdentityIsDeterministic(t *testing.T) {
	productA := &pb.Product{
		Id: "a", Name: "Alpha", PriceUsd: &pb.Money{CurrencyCode: "USD", Units: 1}, Categories: []string{"one"},
	}
	productB := &pb.Product{
		Id: "b", Name: "Beta", PriceUsd: &pb.Money{CurrencyCode: "USD", Units: 2}, Categories: []string{"two"},
	}
	first, err := identifyCatalog([]*pb.Product{productA, productB})
	if err != nil {
		t.Fatal(err)
	}
	second, err := identifyCatalog([]*pb.Product{productB, productA})
	if err != nil {
		t.Fatal(err)
	}
	if first.revision != second.revision || first.checksum != second.checksum {
		t.Fatalf("catalog identity changed with input order: first=%d/%s second=%d/%s",
			first.revision, first.checksum, second.revision, second.checksum)
	}
	if got := []string{first.products[0].ProductId, first.products[1].ProductId}; got[0] != "a" || got[1] != "b" {
		t.Fatalf("products were not normalized by ID: %v", got)
	}
	if first.products[0].ProductVersion == 0 || first.products[1].ProductVersion == 0 {
		t.Fatal("product versions must be non-zero")
	}
	if deterministicMessageID("catalog", "1") != deterministicMessageID("catalog", "1") {
		t.Fatal("message ID must be deterministic")
	}
}
