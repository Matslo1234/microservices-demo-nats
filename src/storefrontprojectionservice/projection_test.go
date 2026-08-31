// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	"github.com/GoogleCloudPlatform/microservices-demo/src/storefrontprojectionservice/internal/storefront"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type memoryKVEntry struct {
	key      string
	value    []byte
	revision uint64
}

func TestProjectionConsumerTerminalErrorsRequireRebind(t *testing.T) {
	for _, err := range []error{nats.ErrBadSubscription, nats.ErrSubscriptionClosed, nats.ErrConsumerDeleted, nats.ErrNoResponders} {
		if !projectionConsumerTerminal(err) {
			t.Fatalf("%v was not classified as terminal", err)
		}
	}
	if projectionConsumerTerminal(nats.ErrTimeout) {
		t.Fatal("fetch timeout was classified as terminal")
	}
}

func TestProjectionRetryDelayUsesBoundedExponentialBackoff(t *testing.T) {
	tests := []struct {
		deliveries uint64
		want       time.Duration
	}{
		{deliveries: 0, want: time.Second},
		{deliveries: 1, want: time.Second},
		{deliveries: 2, want: 2 * time.Second},
		{deliveries: 5, want: 16 * time.Second},
		{deliveries: 6, want: 30 * time.Second},
		{deliveries: 100, want: 30 * time.Second},
	}
	for _, test := range tests {
		if got := projectionRetryDelay(test.deliveries); got != test.want {
			t.Fatalf("delivery %d retry delay = %s, want %s", test.deliveries, got, test.want)
		}
	}
}

func TestQueryRepairBackoffIsBounded(t *testing.T) {
	for failures, want := range map[int]time.Duration{
		-1: time.Second,
		0:  time.Second,
		1:  2 * time.Second,
		4:  16 * time.Second,
		5:  30 * time.Second,
		20: 30 * time.Second,
	} {
		if got := queryRepairBackoff(failures); got != want {
			t.Fatalf("failures %d: delay = %s, want %s", failures, got, want)
		}
	}
}

func TestCartQuoteRefreshReplacesExpiredQuoteAtSameCartVersion(t *testing.T) {
	contextBucket := newMemoryKV()
	worker := &projector{context: contextBucket}
	firstTime := time.Date(2026, time.August, 24, 17, 0, 0, 0, time.UTC)
	first := storefront.CartQuoteView{
		ProjectionMetadata: storefront.ProjectionMetadata{SourceEventID: "quote-1", SourceVersion: 7},
		UserID:             "user-1", CartVersion: 7,
		CostUSD:   &commonv1.Money{CurrencyCode: "USD", Units: 8, Nanos: 990_000_000},
		ExpiresAt: firstTime.Add(15 * time.Minute), UpdatedAt: firstTime,
	}
	if err := worker.updateCartQuote(first); err != nil {
		t.Fatal(err)
	}
	refreshed := first
	refreshed.SourceEventID = "quote-2"
	refreshed.UpdatedAt = firstTime.Add(20 * time.Minute)
	refreshed.ExpiresAt = refreshed.UpdatedAt.Add(15 * time.Minute)
	if err := worker.updateCartQuote(refreshed); err != nil {
		t.Fatal(err)
	}
	got, err := getJSON[storefront.CartQuoteView](contextBucket, storefront.CartQuoteKey(first.UserID))
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceEventID != refreshed.SourceEventID || !got.ExpiresAt.Equal(refreshed.ExpiresAt) {
		t.Fatalf("cart quote was not refreshed: %+v", got)
	}
	entryBefore, err := contextBucket.Get(storefront.CartQuoteKey(first.UserID))
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.updateCartQuote(first); err != nil {
		t.Fatal(err)
	}
	entryAfter, err := contextBucket.Get(storefront.CartQuoteKey(first.UserID))
	if err != nil {
		t.Fatal(err)
	}
	if entryAfter.Revision() != entryBefore.Revision() {
		t.Fatalf("older cart quote rewrote KV: before=%d after=%d", entryBefore.Revision(), entryAfter.Revision())
	}
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
	gets     int
}

func newMemoryKV() *memoryKV {
	return &memoryKV{entries: make(map[string]memoryKVEntry)}
}

func (bucket *memoryKV) Get(key string) (nats.KeyValueEntry, error) {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	bucket.gets++
	entry, ok := bucket.entries[key]
	if !ok {
		return nil, nats.ErrKeyNotFound
	}
	return entry, nil
}

type memoryKeyWatcher struct {
	updates chan nats.KeyValueEntry
	errors  chan error
	once    sync.Once
}

func (watcher *memoryKeyWatcher) Context() context.Context {
	return context.Background()
}

func (watcher *memoryKeyWatcher) Updates() <-chan nats.KeyValueEntry {
	return watcher.updates
}

func (watcher *memoryKeyWatcher) Error() <-chan error {
	return watcher.errors
}

func (watcher *memoryKeyWatcher) Stop() error {
	watcher.once.Do(func() {
		close(watcher.updates)
		close(watcher.errors)
	})
	return nil
}

func (bucket *memoryKV) WatchAll(...nats.WatchOpt) (nats.KeyWatcher, error) {
	bucket.mu.Lock()
	entries := make([]memoryKVEntry, 0, len(bucket.entries))
	for _, entry := range bucket.entries {
		entries = append(entries, entry)
	}
	bucket.mu.Unlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	watcher := &memoryKeyWatcher{
		updates: make(chan nats.KeyValueEntry, len(entries)+1),
		errors:  make(chan error),
	}
	for _, entry := range entries {
		watcher.updates <- entry
	}
	watcher.updates <- nil
	return watcher, nil
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

func (bucket *memoryKV) resetGetCount() {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	bucket.gets = 0
}

func (bucket *memoryKV) getCount() int {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	return bucket.gets
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
	if !projectionFiltersMatch("", criticalProjectionFilterSubjects, criticalProjectionFilterSubjects) {
		t.Fatal("configured projection filters do not match themselves")
	}
	if projectionFiltersMatch("boutique.evt.>", nil, criticalProjectionFilterSubjects) {
		t.Fatal("legacy catch-all filter unexpectedly matches")
	}
	for _, subject := range personalizationProjectionFilterSubjects {
		for _, critical := range criticalProjectionFilterSubjects {
			if subject == critical {
				t.Fatalf("personalization subject %q also blocks the critical projection consumer", subject)
			}
		}
	}
}

func TestProductQueryCacheServesSynchronizedEntriesWithoutRemoteReads(t *testing.T) {
	products := newMemoryKV()
	currencyRevision := storeJSON(t, products, storefront.CurrencyKey, storefront.CurrencyView{
		BaseCurrencyCode: "USD",
		Rates:            []storefront.Rate{{CurrencyCode: "USD", UnitsPerBase: 1}},
		RateRevision:     7,
	})
	productRevision := storeJSON(t, products, storefront.ProductKey("sku"), storefront.ProductView{
		Product: &commonv1.ProductSnapshot{
			ProductId: "sku",
			PriceUsd:  &commonv1.Money{CurrencyCode: "USD", Units: 10},
		},
		CatalogRevision: 9,
	})
	cache, err := newProjectionReadCache(products)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cache.Close)
	products.resetGetCount()

	currency, gotCurrencyRevision, err := getJSONWithRevision[storefront.CurrencyView](
		cache,
		storefront.CurrencyKey,
	)
	if err != nil || currency.RateRevision != 7 || gotCurrencyRevision != currencyRevision {
		t.Fatalf("unexpected cached currency: value=%#v revision=%d error=%v",
			currency, gotCurrencyRevision, err)
	}
	product, gotProductRevision, err := getJSONWithRevision[storefront.ProductView](
		cache,
		storefront.ProductKey("sku"),
	)
	if err != nil || product.Product.GetProductId() != "sku" || gotProductRevision != productRevision {
		t.Fatalf("unexpected cached product: value=%#v revision=%d error=%v",
			product, gotProductRevision, err)
	}
	if gets := products.getCount(); gets != 0 {
		t.Fatalf("cache hits performed %d authoritative KV reads", gets)
	}
	if cache.hits.Load() != 2 || cache.misses.Load() != 0 {
		t.Fatalf("unexpected cache counters: hits=%d misses=%d",
			cache.hits.Load(), cache.misses.Load())
	}
}

func TestProductQueryCacheFallsBackForAJustCreatedKey(t *testing.T) {
	products := newMemoryKV()
	cache, err := newProjectionReadCache(products)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cache.Close)
	products.resetGetCount()
	revision := storeJSON(t, products, storefront.ProductKey("new-sku"), storefront.ProductView{
		Product: &commonv1.ProductSnapshot{
			ProductId: "new-sku",
			PriceUsd:  &commonv1.Money{CurrencyCode: "USD", Units: 5},
		},
	})

	product, gotRevision, err := getJSONWithRevision[storefront.ProductView](
		cache,
		storefront.ProductKey("new-sku"),
	)
	if err != nil || product.Product.GetProductId() != "new-sku" || gotRevision != revision {
		t.Fatalf("unexpected fallback product: value=%#v revision=%d error=%v",
			product, gotRevision, err)
	}
	if gets := products.getCount(); gets != 1 {
		t.Fatalf("cache miss performed %d authoritative KV reads, want 1", gets)
	}
	if _, _, err := getJSONWithRevision[storefront.ProductView](
		cache,
		storefront.ProductKey("new-sku"),
	); err != nil {
		t.Fatal(err)
	}
	if gets := products.getCount(); gets != 1 {
		t.Fatalf("populated cache performed another authoritative read: %d", gets)
	}
	if cache.hits.Load() != 1 || cache.misses.Load() != 1 {
		t.Fatalf("unexpected cache counters: hits=%d misses=%d",
			cache.hits.Load(), cache.misses.Load())
	}
}

func TestCachedProjectionMissDoesNotReadAuthoritativeKV(t *testing.T) {
	entries := newMemoryKV()
	cache, err := newProjectionReadCache(entries)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cache.Close)
	entries.resetGetCount()

	if _, err := cache.Cached("missing"); err != nats.ErrKeyNotFound {
		t.Fatalf("unexpected cached miss error: %v", err)
	}
	if gets := entries.getCount(); gets != 0 {
		t.Fatalf("cached miss performed %d authoritative reads", gets)
	}
}

func TestBoundedProjectionCacheCapsRetainedEntries(t *testing.T) {
	entries := newMemoryKV()
	for index := 0; index < 5; index++ {
		storeJSON(t, entries, fmt.Sprintf("key-%d", index), map[string]int{"value": index})
	}
	cache, err := newBoundedProjectionReadCache(entries, 2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cache.Close)

	cache.mu.RLock()
	retained := len(cache.entries)
	cache.mu.RUnlock()
	if retained != 2 {
		t.Fatalf("bounded cache retained %d entries, want 2", retained)
	}
}

func TestTerminalOrderOutcomeUsesImmutableJetStreamPublishTime(t *testing.T) {
	orders := newMemoryKV()
	worker := &projector{orders: orders}
	occurredAt := time.Unix(1_700_000_000, 0).UTC()
	publishedAt := occurredAt.Add(24 * time.Second)
	payload, err := anypb.New(&eventsv1.OrderCompletedEvent{
		Order: &commonv1.SanitizedOrderSnapshot{
			OrderId: "order-1",
			UserId:  "user-1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.Marshal(&commonv1.MessageEnvelope{
		MessageId:        "completed-1",
		SchemaVersion:    1,
		OccurredAt:       timestamppb.New(occurredAt),
		AggregateType:    "order",
		AggregateId:      "order-1",
		AggregateVersion: 7,
		CorrelationId:    "order-1",
		Data:             payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.apply(
		"boutique.evt.order.completed.v1",
		encoded,
		publishedAt,
	); err != nil {
		t.Fatal(err)
	}
	view, err := getJSON[storefront.OrderView](orders, storefront.OrderKey("order-1"))
	if err != nil {
		t.Fatal(err)
	}
	if view.OutcomeAt == nil || !view.OutcomeAt.Equal(publishedAt) {
		t.Fatalf("outcome time = %v, want publish time %v", view.OutcomeAt, publishedAt)
	}
	if !view.UpdatedAt.Equal(occurredAt) {
		t.Fatalf("event update time = %v, want occurrence time %v", view.UpdatedAt, occurredAt)
	}
}

func TestOrderProjectionPublishesCommittedViewToLiveSubject(t *testing.T) {
	orders := newMemoryKV()
	var subject string
	var update storefront.OrderView
	worker := &projector{
		orders: orders,
		config: projectionConfig{livePrefix: "boutique.live.operation.local."},
		publishLive: func(publishedSubject string, data []byte) error {
			subject = publishedSubject
			return json.Unmarshal(data, &update)
		},
	}
	want := storefront.OrderView{
		OrderID: "order-1", UserID: "user-1", Status: "PROCESSING",
		Stage: "WAITING_FOR_QUOTE", UpdatedAt: time.Now().UTC(),
	}

	if err := worker.updateOrder(want); err != nil {
		t.Fatal(err)
	}
	if subject != "boutique.live.operation.local.order-1" {
		t.Fatalf("live subject = %q, want order operation subject", subject)
	}
	if update.OrderID != want.OrderID || update.UserID != want.UserID ||
		update.Status != want.Status || update.Stage != want.Stage ||
		!update.UpdatedAt.Equal(want.UpdatedAt) {
		t.Fatalf("unexpected live order update: %#v", update)
	}
	committed, err := getJSON[storefront.OrderView](orders, storefront.OrderKey(want.OrderID))
	if err != nil {
		t.Fatal(err)
	}
	if committed.OrderID != update.OrderID || committed.Status != update.Status {
		t.Fatalf("live update %#v does not match committed view %#v", update, committed)
	}
}

func TestProjectionProcessesStreamBeforeFetchBatchCloses(t *testing.T) {
	orders := newMemoryKV()
	worker := &projector{orders: orders}
	payload, err := anypb.New(&eventsv1.OrderCompletedEvent{
		Order: &commonv1.SanitizedOrderSnapshot{
			OrderId: "stream-order",
			UserId:  "stream-user",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.Marshal(&commonv1.MessageEnvelope{
		MessageId:        "stream-completed",
		SchemaVersion:    1,
		OccurredAt:       timestamppb.Now(),
		AggregateType:    "order",
		AggregateId:      "stream-order",
		AggregateVersion: 1,
		CorrelationId:    "stream-order",
		Data:             payload,
	})
	if err != nil {
		t.Fatal(err)
	}

	messages := make(chan *nats.Msg)
	finished := make(chan struct{})
	go func() {
		worker.applyStream(messages)
		close(finished)
	}()
	messages <- &nats.Msg{
		Subject: "boutique.evt.order.completed.v1",
		Data:    encoded,
	}

	deadline := time.Now().Add(time.Second)
	for {
		if _, err := orders.Get(storefront.OrderKey("stream-order")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("projection was not applied until the stream closed")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-finished:
		t.Fatal("stream processor stopped before the batch stream closed")
	default:
	}
	close(messages)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("stream processor did not stop after the batch stream closed")
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
