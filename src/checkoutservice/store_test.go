// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	stateless "github.com/GoogleCloudPlatform/microservices-demo/src/shared/stateless/go"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type countingCheckoutRedisClient struct {
	checkoutRedisClient
	getCalls      atomic.Uint64
	mgetCalls     atomic.Uint64
	pipelineCalls atomic.Uint64
}

func (client *countingCheckoutRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	client.getCalls.Add(1)
	return client.checkoutRedisClient.Get(ctx, key)
}

func (client *countingCheckoutRedisClient) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	client.mgetCalls.Add(1)
	return client.checkoutRedisClient.MGet(ctx, keys...)
}

func (client *countingCheckoutRedisClient) Pipelined(
	ctx context.Context,
	fn func(redis.Pipeliner) error,
) ([]redis.Cmder, error) {
	client.pipelineCalls.Add(1)
	return client.checkoutRedisClient.Pipelined(ctx, fn)
}

func newTestStateStore(t *testing.T) (*stateStore, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	store, err := openStateStoreWithPrefix(server.Addr(), "checkout:test:v2")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, server
}

func openSharedTestStateStore(t *testing.T, server *miniredis.Miniredis) *stateStore {
	t.Helper()
	store, err := openStateStoreWithPrefix(server.Addr(), "checkout:test:v2")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestCheckoutBatchesOrderAndProjectionReads(t *testing.T) {
	store, _ := newTestStateStore(t)
	seedTestCheckoutState(t, store)
	client := &countingCheckoutRedisClient{checkoutRedisClient: store.client}
	store.client = client
	worker := newTestWorker(t, store)

	submitTestOrder(t, worker, "batched-order", testTime)
	if calls := client.getCalls.Load(); calls != 0 {
		t.Fatalf("direct Redis GET calls = %d, want 0", calls)
	}
	if calls := client.mgetCalls.Load(); calls != 1 {
		t.Fatalf("Redis MGET calls = %d, want 1", calls)
	}
	if calls := client.pipelineCalls.Load(); calls != 2 {
		t.Fatalf("Redis pipeline calls = %d, want 2", calls)
	}

	saga, err := store.LoadOrder("batched-order")
	if err != nil || saga == nil {
		t.Fatalf("load batched order: saga=%#v err=%v", saga, err)
	}
	if calls := client.mgetCalls.Load(); calls != 2 {
		t.Fatalf("Redis MGET calls after LoadOrder = %d, want 2", calls)
	}
}

func TestOrderCommitIsPartitionedAndHasNoGlobalRevisionOrOutbox(t *testing.T) {
	store, server := newTestStateStore(t)
	seedTestCheckoutState(t, store)
	worker := newTestWorker(t, store)
	submitTestOrder(t, worker, "partitioned-order", testTime)

	keys := server.Keys()
	hasSaga, hasAccepted, hasJournal, hasDeadline := false, false, false, false
	for _, key := range keys {
		if strings.Contains(key, ":revision") || strings.Contains(key, ":outbox") {
			t.Fatalf("obsolete global key exists: %s", key)
		}
		hasSaga = hasSaga || strings.HasSuffix(key, ":saga")
		hasAccepted = hasAccepted || strings.HasSuffix(key, ":accepted")
		hasJournal = hasJournal || strings.HasSuffix(key, ":results")
		hasDeadline = hasDeadline || strings.HasSuffix(key, ":deadlines")
	}
	if !hasSaga || !hasAccepted || !hasJournal || !hasDeadline {
		t.Fatalf("missing order-local records: saga=%t accepted=%t journal=%t deadline=%t; keys=%v",
			hasSaga, hasAccepted, hasJournal, hasDeadline, keys)
	}
	base := store.orderBase("partitioned-order")
	tagStart, tagEnd := strings.Index(base, "{"), strings.Index(base, "}")
	if tagStart < 0 || tagEnd <= tagStart {
		t.Fatalf("order key has no Redis Cluster hash tag: %s", base)
	}
	tag := base[tagStart : tagEnd+1]
	if !strings.Contains(store.deadlineKey(deadlineShard("partitioned-order")), tag) {
		t.Fatalf("deadline index and order are not co-located: %s / %s", base,
			store.deadlineKey(deadlineShard("partitioned-order")))
	}
}

func TestConcurrentDuplicateReturnsExactStoredJournal(t *testing.T) {
	first, server := newTestStateStore(t)
	second := openSharedTestStateStore(t, server)
	seedTestCheckoutState(t, first)
	workers := []*checkoutWorker{newTestWorker(t, first), newTestWorker(t, second)}
	input := orderSubmitEnvelope(t, "duplicate-order", testTime)

	start := make(chan struct{})
	outcomes := make(chan transitionOutcome, 2)
	failures := make(chan error, 2)
	for _, worker := range workers {
		go func(worker *checkoutWorker) {
			<-start
			outcome, err := worker.processOrderCommand(input)
			outcomes <- outcome
			failures <- err
		}(worker)
	}
	close(start)
	firstOutcome, secondOutcome := <-outcomes, <-outcomes
	for range 2 {
		if err := <-failures; err != nil {
			t.Fatal(err)
		}
	}
	if firstOutcome.Duplicate == secondOutcome.Duplicate {
		t.Fatalf("duplicate flags = %t/%t, want one new and one replay",
			firstOutcome.Duplicate, secondOutcome.Duplicate)
	}
	if len(firstOutcome.Results) != len(secondOutcome.Results) {
		t.Fatalf("journal lengths differ: %d/%d", len(firstOutcome.Results), len(secondOutcome.Results))
	}
	for index := range firstOutcome.Results {
		left, right := firstOutcome.Results[index], secondOutcome.Results[index]
		if left.MessageID != right.MessageID || left.Subject != right.Subject || !bytes.Equal(left.Data, right.Data) {
			t.Fatalf("journal result %d changed across duplicate commit", index)
		}
	}
	saga, err := first.LoadOrder("duplicate-order")
	if err != nil {
		t.Fatal(err)
	}
	if saga == nil || saga.Version != 1 {
		t.Fatalf("duplicate advanced saga: %#v", saga)
	}
}

func TestUnrelatedOrdersDoNotConflictOnOneRevision(t *testing.T) {
	store, server := newTestStateStore(t)
	seedTestCheckoutState(t, store)
	stores := make([]*stateStore, 10)
	workers := make([]*checkoutWorker, 10)
	stores[0], workers[0] = store, newTestWorker(t, store)
	for index := 1; index < len(stores); index++ {
		stores[index] = openSharedTestStateStore(t, server)
		workers[index] = newTestWorker(t, stores[index])
	}

	const orders = 100
	var wait sync.WaitGroup
	errs := make(chan error, orders)
	for index := 0; index < orders; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			orderID := fmt.Sprintf("independent-%03d", index)
			_, err := workers[index%len(workers)].processOrderCommand(orderSubmitEnvelope(t, orderID, testTime))
			errs <- err
		}(index)
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var conflicts uint64
	for _, item := range stores {
		conflicts += item.conflicts.Load()
	}
	if conflicts != 0 {
		t.Fatalf("unrelated order conflicts = %d, want zero", conflicts)
	}
	for index := 0; index < orders; index++ {
		orderID := fmt.Sprintf("independent-%03d", index)
		saga, err := store.LoadOrder(orderID)
		if err != nil || saga == nil {
			t.Fatalf("order %s missing: saga=%#v err=%v", orderID, saga, err)
		}
	}
}

func TestAcceptedProjectionIsImmutable(t *testing.T) {
	store, _ := newTestStateStore(t)
	seedTestCheckoutState(t, store)
	worker := newTestWorker(t, store)
	submitTestOrder(t, worker, "immutable-order", testTime)

	newRates := &eventsv1.CurrencyRatesUpdatedEvent{
		BaseCurrencyCode: "USD", RateRevision: 10,
		Rates: []*eventsv1.CurrencyRate{{CurrencyCode: "USD", UnitsPerBase: 1}},
	}
	if err := store.ApplyProjection("boutique.evt.currency.rates-updated.v1",
		testEnvelope(t, "new-rates", "rates", 10, testTime.Add(time.Minute), newRates)); err != nil {
		t.Fatal(err)
	}
	newCart := &commonv1.CartSnapshot{UserId: "user-1", CartVersion: 4}
	if err := store.ApplyProjection("boutique.evt.cart.cleared.v1",
		testEnvelope(t, "new-cart", "user-1", 4, testTime.Add(time.Minute), &eventsv1.CartClearedEvent{Cart: newCart})); err != nil {
		t.Fatal(err)
	}
	accepted, err := store.LoadAcceptedOrder("immutable-order")
	if err != nil {
		t.Fatal(err)
	}
	if accepted == nil || accepted.Cart.CartVersion != 3 || accepted.RateRevision != 9 ||
		accepted.Order.Items[0].UnitCost.Units != 10 {
		t.Fatalf("accepted order changed with projections: %#v", accepted)
	}
}

func TestDeadlineLeaseCanBeRecoveredByAnotherReplica(t *testing.T) {
	first, server := newTestStateStore(t)
	second := openSharedTestStateStore(t, server)
	seedTestCheckoutState(t, first)
	firstWorker, secondWorker := newTestWorker(t, first), newTestWorker(t, second)
	occurred := time.Now().UTC().Add(-2 * time.Minute)
	submitTestOrder(t, firstWorker, "lease-recovery-order", occurred)
	record, _, err := first.LoadDeadline("lease-recovery-order")
	if err != nil {
		t.Fatal(err)
	}
	leaseStart := time.Now().UTC()
	if _, err := firstWorker.leaseStore.Acquire(t.Context(), record.WorkID, "crashed-replica", leaseStart, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	committed, err := firstWorker.processDeadline(record.OrderID, record.Version, record.Deadline, record.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Duplicate || len(committed.Results) != 2 {
		t.Fatalf("crashed replica did not commit the deadline journal: %#v", committed)
	}
	// Simulate a stop after state commit but before any result publish or lease
	// completion. The next replica advances only its lease clock.
	published := []resultMessage{}
	secondWorker.publishHook = func(result resultMessage) error {
		published = append(published, result)
		return nil
	}
	if err := secondWorker.claimDeadline("lease-recovery-order", leaseStart.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
	if secondWorker.metrics.deadlineLeaseRecoveries.Load() != 1 {
		t.Fatal("expired deadline lease was not recorded as recovered")
	}
	saga, err := second.LoadOrder("lease-recovery-order")
	if err != nil {
		t.Fatal(err)
	}
	if saga.Stage != stageManualReview || len(published) != 2 {
		t.Fatalf("recovered deadline result: stage=%s published=%d", saga.Stage, len(published))
	}
	if _, err := stateless.DeriveResultMessageID(record.WorkID, "order.timeout"); err != nil {
		t.Fatal(err)
	}
}

func TestDeadlineResultsRepublishAfterPublishBeforeLeaseCompletion(t *testing.T) {
	first, server := newTestStateStore(t)
	second := openSharedTestStateStore(t, server)
	seedTestCheckoutState(t, first)
	firstWorker, secondWorker := newTestWorker(t, first), newTestWorker(t, second)
	occurred := time.Now().UTC().Add(-2 * time.Minute)
	submitTestOrder(t, firstWorker, "deadline-publish-crash", occurred)
	record, _, err := first.LoadDeadline("deadline-publish-crash")
	if err != nil {
		t.Fatal(err)
	}
	leaseStart := time.Now().UTC()
	if _, err := firstWorker.leaseStore.Acquire(t.Context(), record.WorkID, firstWorker.workerID,
		leaseStart, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	committed, err := firstWorker.processDeadline(record.OrderID, record.Version, record.Deadline, record.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	firstPublish := []resultMessage{}
	firstWorker.publishHook = func(result resultMessage) error {
		firstPublish = append(firstPublish, result)
		return nil
	}
	if err := firstWorker.finishTransition(committed); err != nil {
		t.Fatal(err)
	}
	// The first worker published every result, then stopped before completing
	// its lease. JetStream may have accepted all of these message IDs.

	replayed := []resultMessage{}
	secondWorker.publishHook = func(result resultMessage) error {
		replayed = append(replayed, result)
		return nil
	}
	if err := secondWorker.claimDeadline(record.OrderID, leaseStart.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != len(firstPublish) {
		t.Fatalf("deadline replay count = %d, want %d", len(replayed), len(firstPublish))
	}
	for index := range firstPublish {
		if replayed[index].MessageID != firstPublish[index].MessageID ||
			!bytes.Equal(replayed[index].Data, firstPublish[index].Data) {
			t.Fatalf("deadline result %d changed after publish/lease crash", index)
		}
	}
	if record, _, err := second.LoadDeadline("deadline-publish-crash"); err != nil || record != nil {
		t.Fatalf("completed deadline remains indexed: record=%#v err=%v", record, err)
	}
}

func TestLiveRedisClusterContinuesAfterReplicaTakeover(t *testing.T) {
	address := strings.TrimSpace(os.Getenv("CHECKOUT_REDIS_TEST_ADDR"))
	if address == "" {
		t.Skip("CHECKOUT_REDIS_TEST_ADDR is not configured")
	}
	prefix := fmt.Sprintf("checkout:phase5-live:%d", time.Now().UnixNano())
	store, err := openStateStore(address, prefix, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedTestCheckoutState(t, store)
	worker := newTestWorker(t, store)
	for index := 0; index < 30; index++ {
		submitTestOrder(t, worker, fmt.Sprintf("before-takeover-%02d", index), testTime)
	}

	cluster, ok := store.client.(*redis.ClusterClient)
	if !ok {
		t.Fatal("live takeover test is not using redis.ClusterClient")
	}
	slots, err := cluster.ClusterSlots(context.Background()).Result()
	if err != nil {
		t.Fatal(err)
	}
	var replica string
	for _, slot := range slots {
		if len(slot.Nodes) > 1 {
			replica = slot.Nodes[1].Addr
			break
		}
	}
	if replica == "" {
		t.Fatal("Redis Cluster has no replica to take over")
	}
	replicaClient := redis.NewClient(&redis.Options{Addr: replica})
	if err := replicaClient.Do(context.Background(), "CLUSTER", "FAILOVER", "TAKEOVER").Err(); err != nil {
		_ = replicaClient.Close()
		t.Fatal(err)
	}
	_ = replicaClient.Close()

	deadline := time.Now().Add(10 * time.Second)
	for {
		cluster.ReloadState(context.Background())
		if store.Ready() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Redis Cluster did not become ready after takeover")
		}
		time.Sleep(50 * time.Millisecond)
	}
	for index := 0; index < 30; index++ {
		submitTestOrder(t, worker, fmt.Sprintf("after-takeover-%02d", index), testTime)
	}
	for _, orderID := range []string{"before-takeover-00", "before-takeover-29", "after-takeover-00", "after-takeover-29"} {
		saga, err := store.LoadOrder(orderID)
		if err != nil || saga == nil || saga.Version != 1 {
			t.Fatalf("%s missing after takeover: saga=%#v err=%v", orderID, saga, err)
		}
	}
}
