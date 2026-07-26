// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/microservices-demo/src/storefrontprojectionservice/internal/storefront"
	"github.com/nats-io/nats.go"
)

type memoryKVEntry struct {
	key      string
	value    []byte
	revision uint64
}

func (entry memoryKVEntry) Bucket() string             { return "TEST" }
func (entry memoryKVEntry) Key() string                { return entry.key }
func (entry memoryKVEntry) Value() []byte              { return append([]byte(nil), entry.value...) }
func (entry memoryKVEntry) Revision() uint64           { return entry.revision }
func (entry memoryKVEntry) Created() time.Time         { return time.Unix(0, 0).UTC() }
func (entry memoryKVEntry) Delta() uint64              { return 0 }
func (entry memoryKVEntry) Operation() nats.KeyValueOp { return nats.KeyValuePut }

type memoryKV struct {
	mu       sync.Mutex
	entries  map[string]memoryKVEntry
	revision uint64
}

func newMemoryKV() *memoryKV {
	return &memoryKV{entries: make(map[string]memoryKVEntry)}
}

func (bucket *memoryKV) Get(key string) (nats.KeyValueEntry, error) {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	entry, ok := bucket.entries[key]
	if !ok {
		return nil, nats.ErrKeyNotFound
	}
	return entry, nil
}

func (bucket *memoryKV) Create(key string, value []byte) (uint64, error) {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if _, exists := bucket.entries[key]; exists {
		return 0, nats.ErrKeyExists
	}
	return bucket.store(key, value), nil
}

func (bucket *memoryKV) Update(key string, value []byte, last uint64) (uint64, error) {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	current, exists := bucket.entries[key]
	if !exists || current.revision != last {
		return 0, nats.ErrKeyExists
	}
	return bucket.store(key, value), nil
}

func (bucket *memoryKV) Keys(...nats.WatchOpt) ([]string, error) {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if len(bucket.entries) == 0 {
		return nil, nats.ErrNoKeysFound
	}
	keys := make([]string, 0, len(bucket.entries))
	for key := range bucket.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (bucket *memoryKV) store(key string, value []byte) uint64 {
	bucket.revision++
	bucket.entries[key] = memoryKVEntry{
		key: key, value: append([]byte(nil), value...), revision: bucket.revision,
	}
	return bucket.revision
}

func storeJSON(t *testing.T, bucket *memoryKV, key string, value any) uint64 {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	return bucket.store(key, encoded)
}

func TestProjectionSubjectFiltering(t *testing.T) {
	handled := []string{
		"boutique.evt.catalog.product-upserted.v1",
		"boutique.evt.currency.rates-updated.v1",
		"boutique.evt.cart.item-added.v1",
		"boutique.evt.storefront.operation-accepted.v1",
		"boutique.evt.recommendation.generated.v1",
		"boutique.evt.ad.selection-generated.v1",
		"boutique.evt.shipping.cart-quote-updated.v1",
		"boutique.evt.order.completed.v1",
		"boutique.evt.notification.order-confirmation-sent.v1",
	}
	for _, subject := range handled {
		if !projectionHandlesSubject(subject) {
			t.Errorf("projection subject %q is not handled", subject)
		}
	}
	for _, subject := range []string{
		"boutique.evt.payment.authorized.v1",
		"boutique.evt.shipping.shipment-created.v1",
		"boutique.evt.storefront.page-viewed.v1",
	} {
		if projectionHandlesSubject(subject) {
			t.Errorf("irrelevant subject %q is handled", subject)
		}
	}
	if !projectionFiltersMatch("", projectionFilterSubjects) {
		t.Fatal("configured projection filters do not match themselves")
	}
	if projectionFiltersMatch("boutique.evt.>", nil) {
		t.Fatal("legacy catch-all filter unexpectedly matches")
	}
}

func TestProjectionCASConvergesOnNewestAggregateVersion(t *testing.T) {
	bucket := newMemoryKV()
	workers := []*projector{{}, {}, {}}
	var running sync.WaitGroup
	for version := uint64(1); version <= 30; version++ {
		version := version
		running.Add(1)
		go func() {
			defer running.Done()
			worker := workers[int(version)%len(workers)]
			eventID := "event-" + strconv.FormatUint(version, 10)
			err := updateJSON(
				worker,
				bucket,
				storefront.CatalogKey,
				version,
				func(current storefront.CatalogView) uint64 { return current.CatalogRevision },
				func(current storefront.CatalogView) string { return current.SourceEventID },
				eventID,
				time.Unix(int64(version), 0).UTC(),
				storefront.CatalogView{
					ProjectionMetadata: storefront.ProjectionMetadata{
						SourceEventID: eventID,
						SourceVersion: version,
					},
					CatalogRevision: version,
				},
			)
			if err != nil {
				t.Errorf("CAS worker failed: %v", err)
			}
		}()
	}
	running.Wait()

	view, err := getJSON[storefront.CatalogView](bucket, storefront.CatalogKey)
	if err != nil {
		t.Fatal(err)
	}
	if view.CatalogRevision != 30 || view.SourceVersion != 30 {
		t.Fatalf("projection did not converge on version 30: %#v", view)
	}
}

func TestProjectionCASMakesDuplicateAndOlderEventsNoOps(t *testing.T) {
	bucket := newMemoryKV()
	worker := &projector{}
	apply := func(version uint64, eventID string) error {
		return updateJSON(
			worker,
			bucket,
			storefront.CatalogKey,
			version,
			func(current storefront.CatalogView) uint64 { return current.CatalogRevision },
			func(current storefront.CatalogView) string { return current.SourceEventID },
			eventID,
			time.Unix(int64(version), 0).UTC(),
			storefront.CatalogView{
				ProjectionMetadata: storefront.ProjectionMetadata{
					SourceEventID: eventID,
					SourceVersion: version,
				},
				CatalogRevision: version,
			},
		)
	}
	if err := apply(5, "event-5"); err != nil {
		t.Fatal(err)
	}
	entryBefore, err := bucket.Get(storefront.CatalogKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := apply(5, "event-5"); err != nil {
		t.Fatal(err)
	}
	if err := apply(4, "event-4"); err != nil {
		t.Fatal(err)
	}
	entryAfter, err := bucket.Get(storefront.CatalogKey)
	if err != nil {
		t.Fatal(err)
	}
	if entryAfter.Revision() != entryBefore.Revision() {
		t.Fatalf("duplicate or stale event mutated KV: before=%d after=%d",
			entryBefore.Revision(), entryAfter.Revision())
	}
	if worker.staleEventSkips.Load() != 2 {
		t.Fatalf("got %d stale skips, want 2", worker.staleEventSkips.Load())
	}
}

func TestOrderRedeliveryIsNoOpAfterAnotherSameVersionFact(t *testing.T) {
	orders := newMemoryKV()
	worker := &projector{orders: orders}
	first := storefront.OrderView{
		ProjectionMetadata: storefront.ProjectionMetadata{
			SourceEventID:   "order-completed",
			SourceVersion:   7,
			AppliedEventIDs: []string{"order-completed"},
		},
		OrderID: "order-1", UserID: "user-1", Status: "COMPLETED",
		Stage: "COMPLETED", AggregateVersion: 7,
	}
	notification := storefront.OrderView{
		ProjectionMetadata: storefront.ProjectionMetadata{
			SourceEventID:   "notification-sent",
			SourceVersion:   7,
			AppliedEventIDs: []string{"notification-sent"},
		},
		OrderID: "order-1", NotificationStatus: "SENT", AggregateVersion: 7,
	}
	if err := worker.updateOrder(first); err != nil {
		t.Fatal(err)
	}
	if err := worker.updateOrder(notification); err != nil {
		t.Fatal(err)
	}
	before, err := orders.Get(storefront.OrderKey("order-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.updateOrder(first); err != nil {
		t.Fatal(err)
	}
	after, err := orders.Get(storefront.OrderKey("order-1"))
	if err != nil {
		t.Fatal(err)
	}
	if before.Revision() != after.Revision() {
		t.Fatalf("redelivered order event wrote another KV revision: before=%d after=%d",
			before.Revision(), after.Revision())
	}
}
