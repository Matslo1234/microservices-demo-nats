// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0

package main

import (
	"fmt"
	"testing"

	"github.com/GoogleCloudPlatform/microservices-demo/src/storefrontprojectionservice/internal/storefront"
)

func TestCachedProjectionKVInvalidatesAfterReplicaConflict(t *testing.T) {
	source := newMemoryKV()
	first := newCachedProjectionKV(source, 8)
	second := newCachedProjectionKV(source, 8)

	revision, err := first.Create("order-1", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	stale, err := second.Get("order-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Update("order-1", []byte("two"), revision); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Update("order-1", []byte("stale"), stale.Revision()); err == nil {
		t.Fatal("stale cached revision unexpectedly updated the KV entry")
	}

	refreshed, err := second.Get("order-1")
	if err != nil {
		t.Fatal(err)
	}
	if string(refreshed.Value()) != "two" {
		t.Fatalf("cache did not refresh after conflict: got %q", refreshed.Value())
	}
}

func TestCachedProjectionKVEnforcesByteLimit(t *testing.T) {
	source := newMemoryKV()
	cache := newCachedProjectionKVWithLimits(source, 10, 8)
	if _, err := cache.Create("a", []byte("1234")); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Create("b", []byte("5678")); err != nil {
		t.Fatal(err)
	}
	if cache.bytes > cache.maxBytes {
		t.Fatalf("cache retained %d bytes, limit %d", cache.bytes, cache.maxBytes)
	}
	if len(cache.entries) != 1 {
		t.Fatalf("cache retained %d entries, want 1 after byte eviction", len(cache.entries))
	}
}

func TestProjectionEventHistoryIsBounded(t *testing.T) {
	var metadata storefront.ProjectionMetadata
	for index := 0; index < projectionEventHistory+10; index++ {
		eventID := fmt.Sprintf("event-%02d", index)
		metadata = mergeProjectionMetadata(metadata, projectionMetadataForTest(eventID))
	}
	if len(metadata.AppliedEventIDs) != projectionEventHistory {
		t.Fatalf("got %d retained event IDs, want %d", len(metadata.AppliedEventIDs), projectionEventHistory)
	}
	if projectionApplied(metadata, "event-00") {
		t.Fatal("oldest event ID was not compacted")
	}
	if !projectionApplied(metadata, fmt.Sprintf("event-%02d", projectionEventHistory+9)) {
		t.Fatal("newest event ID was not retained")
	}
}

func projectionMetadataForTest(eventID string) storefront.ProjectionMetadata {
	return storefront.ProjectionMetadata{
		SourceEventID: eventID, AppliedEventIDs: []string{eventID},
	}
}
