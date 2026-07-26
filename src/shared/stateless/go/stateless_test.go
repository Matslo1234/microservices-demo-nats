// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package stateless

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type memoryRevisionBucket struct {
	mu       sync.Mutex
	revision uint64
	values   map[string]RevisionedValue
}

func newMemoryRevisionBucket() *memoryRevisionBucket {
	return &memoryRevisionBucket{values: make(map[string]RevisionedValue)}
}

func (bucket *memoryRevisionBucket) Get(key string) (RevisionedValue, error) {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	value, ok := bucket.values[key]
	if !ok {
		return RevisionedValue{}, ErrRevisionNotFound
	}
	return RevisionedValue{Value: append([]byte(nil), value.Value...), Revision: value.Revision}, nil
}

func (bucket *memoryRevisionBucket) Create(key string, value []byte) (uint64, error) {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if _, ok := bucket.values[key]; ok {
		return 0, ErrRevisionConflict
	}
	bucket.revision++
	bucket.values[key] = RevisionedValue{Value: append([]byte(nil), value...), Revision: bucket.revision}
	return bucket.revision, nil
}

func (bucket *memoryRevisionBucket) Update(key string, value []byte, revision uint64) (uint64, error) {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	current, ok := bucket.values[key]
	if !ok || current.Revision != revision {
		return 0, ErrRevisionConflict
	}
	bucket.revision++
	bucket.values[key] = RevisionedValue{Value: append([]byte(nil), value...), Revision: bucket.revision}
	return bucket.revision, nil
}

func testRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}

func TestResultIDContractVectors(t *testing.T) {
	vectors := map[string]string{
		"01J0INPUT000000000000000001|cart.mutation":                "br1_cfipIWfz73yXKiJIc0nF-uV6vnx5kWbMUG2_o-ukd50",
		"event-order-completed-42|notification.order-confirmation": "br1_BipmFE_ifI2JqRb67NFrgisjZYeejPTlkKhojRP1Mz8",
	}
	for input, want := range vectors {
		parts := stringsCut(input, "|")
		got, err := DeriveResultMessageID(parts[0], parts[1])
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("DeriveResultMessageID(%q, %q) = %q, want %q", parts[0], parts[1], got, want)
		}
	}
}

func stringsCut(value, separator string) [2]string {
	for index := 0; index+len(separator) <= len(value); index++ {
		if value[index:index+len(separator)] == separator {
			return [2]string{value[:index], value[index+len(separator):]}
		}
	}
	return [2]string{value, ""}
}

func TestResultEnvelopeIsStable(t *testing.T) {
	input := &commonv1.MessageEnvelope{
		MessageId:     "command-7",
		CorrelationId: "operation-4",
		Traceparent:   "00-trace-parent",
		Tracestate:    "vendor=value",
	}
	spec := ResultSpec{
		Slot:             "order.rejected",
		MessageType:      "boutique.order.Rejected.v1",
		Producer:         "checkoutservice",
		AggregateType:    "order",
		AggregateID:      "order-9",
		AggregateVersion: 3,
		OccurredAt:       time.Unix(1_700_000_000, 123).UTC(),
		Payload:          &eventsv1.OrderRejectedEvent{OrderId: "order-9", OperationId: "operation-4"},
	}
	first, err := NewResultEnvelope(input, spec)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewResultEnvelope(input, spec)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := MarshalEnvelope(first)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := MarshalEnvelope(second)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstBytes) != string(secondBytes) {
		t.Fatal("same input and result spec produced different stored bytes")
	}
	if first.GetCausationId() != input.GetMessageId() ||
		first.GetCorrelationId() != input.GetCorrelationId() {
		t.Fatal("result did not retain causal identity")
	}
}

func TestAtomicCommitIsAggregateLocalAndDuplicateSafe(t *testing.T) {
	_, client := testRedis(t)
	store, err := NewRedisAggregateStore(client, "cart:v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const workers = 24
	start := make(chan struct{})
	outcomes := make(chan CommitOutcome, workers)
	failures := make(chan error, workers)
	var wait sync.WaitGroup
	for index := 0; index < workers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			outcome, commitErr := store.Commit(ctx, CommitRequest{
				AggregateID:      "user-7",
				InputMessageID:   "command-11",
				ExpectedVersion:  0,
				NextState:        []byte("cart-v1"),
				Journal:          []byte("stable-result"),
				JournalRetention: 7 * 24 * time.Hour,
			})
			if commitErr != nil {
				failures <- commitErr
				return
			}
			outcomes <- outcome
		}()
	}
	close(start)
	wait.Wait()
	close(outcomes)
	close(failures)
	for failure := range failures {
		t.Fatalf("concurrent commit failed: %v", failure)
	}
	newCount := 0
	duplicateCount := 0
	for outcome := range outcomes {
		if outcome.Version != 1 || string(outcome.Journal) != "stable-result" {
			t.Fatalf("unexpected outcome: %#v", outcome)
		}
		if outcome.Duplicate {
			duplicateCount++
		} else {
			newCount++
		}
	}
	if newCount != 1 || duplicateCount != workers-1 {
		t.Fatalf("new=%d duplicate=%d, want 1/%d", newCount, duplicateCount, workers-1)
	}

	keys, err := KeysForAggregate("cart:v1", "user-7", "command-11")
	if err != nil {
		t.Fatal(err)
	}
	if got := client.Get(ctx, keys.Version).Val(); got != "1" {
		t.Fatalf("aggregate version = %q, want 1", got)
	}
}

func TestAtomicCommitConflictDoesNotMutateState(t *testing.T) {
	_, client := testRedis(t)
	store, err := NewRedisAggregateStore(client, "checkout:v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := store.Commit(ctx, CommitRequest{
		AggregateID:      "order-1",
		InputMessageID:   "input-1",
		ExpectedVersion:  0,
		NextState:        []byte("v1"),
		Journal:          []byte("result-1"),
		JournalRetention: time.Hour,
	})
	if err != nil || first.Version != 1 {
		t.Fatalf("first commit = %#v, %v", first, err)
	}
	_, err = store.Commit(ctx, CommitRequest{
		AggregateID:      "order-1",
		InputMessageID:   "input-2",
		ExpectedVersion:  0,
		NextState:        []byte("invalid-v2"),
		Journal:          []byte("result-2"),
		JournalRetention: time.Hour,
	})
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.Actual != 1 {
		t.Fatalf("conflict = %v, want actual version 1", err)
	}
	keys, _ := KeysForAggregate("checkout:v1", "order-1", "input-2")
	if got := client.Get(ctx, keys.State).Val(); got != "v1" {
		t.Fatalf("state changed during conflict: %q", got)
	}
}

func TestLeaseExpiryRecoveryAndFencing(t *testing.T) {
	_, client := testRedis(t)
	store, err := NewRedisLeaseStore(client, "checkout:v1:deadline", 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	start := time.Unix(1_700_000_000, 0).UTC()
	first, err := store.Acquire(ctx, "order-3:payment", "worker-a", start, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Token != 1 || first.Attempts != 1 {
		t.Fatalf("first lease = %#v", first)
	}
	_, err = store.Acquire(ctx, "order-3:payment", "worker-b", start.Add(9*time.Second), 10*time.Second)
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("unexpired acquire = %v, want ErrLeaseHeld", err)
	}
	recovered, err := store.Acquire(ctx, "order-3:payment", "worker-b", start.Add(11*time.Second), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Token != 2 || recovered.Attempts != 2 {
		t.Fatalf("recovered lease = %#v", recovered)
	}
	if err := store.Complete(ctx, first, start.Add(12*time.Second)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale completion = %v, want ErrLeaseLost", err)
	}
	if err := store.Complete(ctx, recovered, start.Add(12*time.Second)); err != nil {
		t.Fatal(err)
	}
	completed, err := store.Acquire(ctx, "order-3:payment", "worker-c", start.Add(30*time.Second), 10*time.Second)
	if !errors.Is(err, ErrLeaseComplete) || !completed.Completed || completed.Token != 2 {
		t.Fatalf("completed acquire = %#v, %v", completed, err)
	}
}

func TestKVBootstrapClaimIsConcurrentAndRecoversAfterExpiry(t *testing.T) {
	store, err := NewKVLeaseStore(newMemoryRevisionBucket(), "bootstrap.catalog")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1_700_000_000, 0).UTC()
	const workers = 20
	begin := make(chan struct{})
	leases := make(chan Lease, workers)
	failures := make(chan error, workers)
	var wait sync.WaitGroup
	for index := 0; index < workers; index++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			<-begin
			lease, acquireErr := store.Acquire(
				"catalog-revision-a",
				fmt.Sprintf("worker-%d", worker),
				start,
				10*time.Second,
			)
			if acquireErr != nil {
				failures <- acquireErr
				return
			}
			leases <- lease
		}(index)
	}
	close(begin)
	wait.Wait()
	close(leases)
	close(failures)
	acquired := 0
	var first Lease
	for lease := range leases {
		acquired++
		first = lease
	}
	held := 0
	for failure := range failures {
		if !errors.Is(failure, ErrLeaseHeld) {
			t.Fatalf("unexpected concurrent claim error: %v", failure)
		}
		held++
	}
	if acquired != 1 || held != workers-1 {
		t.Fatalf("acquired=%d held=%d, want 1/%d", acquired, held, workers-1)
	}
	recovered, err := store.Acquire(
		"catalog-revision-a",
		"replacement",
		start.Add(11*time.Second),
		10*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Token != first.Token+1 || recovered.Attempts != 2 {
		t.Fatalf("recovered claim = %#v", recovered)
	}
	if err := store.Complete(first, start.Add(12*time.Second)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale bootstrap completion = %v, want ErrLeaseLost", err)
	}
	if err := store.Complete(recovered, start.Add(12*time.Second)); err != nil {
		t.Fatal(err)
	}
}
