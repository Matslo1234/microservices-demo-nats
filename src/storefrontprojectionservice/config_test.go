// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import "testing"

func setValidProjectionEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"STOREFRONT_QUERY_CONCURRENCY",
		"STOREFRONT_CART_CACHE_ENTRIES",
		"STOREFRONT_CONTEXT_CACHE_ENTRIES",
	} {
		t.Setenv(name, "")
	}
	for name, value := range map[string]string{
		"REGION_ID":                     "eu-central-1",
		"REGION_KEY":                    "EU_CENTRAL_1",
		"K8S_CLUSTER_NAME":              "boutique-eu1",
		"NATS_CLUSTER_NAME":             "BOUTIQUE-eu-central-1",
		"STREAM_OWNER_REGION":           "eu-central-1",
		"STOREFRONT_EVENT_STREAM":       "BOUTIQUE_EVENTS",
		"STOREFRONT_PROJECTION_DURABLE": "storefront-projection-eu-central-1-v1",
		"STOREFRONT_PRODUCTS_BUCKET":    "STOREFRONT_PRODUCTS_EU_CENTRAL_1",
		"STOREFRONT_CARTS_BUCKET":       "STOREFRONT_CARTS_EU_CENTRAL_1",
		"STOREFRONT_CONTEXT_BUCKET":     "STOREFRONT_CONTEXT_EU_CENTRAL_1",
		"STOREFRONT_ORDERS_BUCKET":      "STOREFRONT_ORDERS_EU_CENTRAL_1",
		"STOREFRONT_OPERATIONS_BUCKET":  "STOREFRONT_OPERATIONS_EU_CENTRAL_1",
		"LIVE_OPERATION_PREFIX":         "boutique.live.operation.eu-central-1.",
	} {
		t.Setenv(name, value)
	}
}

func TestLoadProjectionConfigAcceptsRegionQualifiedAssets(t *testing.T) {
	setValidProjectionEnvironment(t)
	config, err := loadProjectionConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.regionID != "eu-central-1" || config.eventStream != "BOUTIQUE_EVENTS" ||
		config.productsBucket != "STOREFRONT_PRODUCTS_EU_CENTRAL_1" {
		t.Fatalf("unexpected projection config: %+v", config)
	}
	if config.queryConcurrency != 8 || config.cartCacheEntries != 32768 ||
		config.contextCacheEntries != 65536 {
		t.Fatalf("unexpected performance defaults: %+v", config)
	}
}

func TestLoadProjectionConfigRejectsInvalidPerformanceBounds(t *testing.T) {
	setValidProjectionEnvironment(t)
	t.Setenv("STOREFRONT_QUERY_CONCURRENCY", "0")
	if _, err := loadProjectionConfig(); err == nil {
		t.Fatal("zero query concurrency was accepted")
	}
}

func TestLoadProjectionConfigRejectsUnscopedDurableAndLivePrefix(t *testing.T) {
	setValidProjectionEnvironment(t)
	t.Setenv("STOREFRONT_PROJECTION_DURABLE", "storefront-projection-v1")
	if _, err := loadProjectionConfig(); err == nil {
		t.Fatal("unscoped durable was accepted")
	}
	setValidProjectionEnvironment(t)
	t.Setenv("LIVE_OPERATION_PREFIX", "boutique.live.operation.")
	if _, err := loadProjectionConfig(); err == nil {
		t.Fatal("unscoped live-operation prefix was accepted")
	}
	setValidProjectionEnvironment(t)
	t.Setenv("STOREFRONT_PRODUCTS_BUCKET", "STOREFRONT_PRODUCTS")
	if _, err := loadProjectionConfig(); err == nil {
		t.Fatal("unscoped regional bucket was accepted")
	}
}
