// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func newTestStateStore(t *testing.T) (*stateStore, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	store, err := openStateStoreWithPrefix(server.Addr(), "checkout:test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store, server
}

func openSharedTestStateStore(t *testing.T, server *miniredis.Miniredis) *stateStore {
	t.Helper()
	store, err := openStateStoreWithPrefix(server.Addr(), "checkout:test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func TestStateStoreRollsBackFailedUpdate(t *testing.T) {
	store, _ := newTestStateStore(t)
	if err := store.Update(func(state *persistedState) error {
		state.CatalogRevision = 7
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	expected := errors.New("injected update failure")
	err := store.Update(func(state *persistedState) error {
		state.CatalogRevision = 99
		return expected
	})
	if !errors.Is(err, expected) {
		t.Fatalf("Update() error = %v, want %v", err, expected)
	}
	state, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if state.CatalogRevision != 7 {
		t.Fatalf("catalog revision = %d, want persisted value 7", state.CatalogRevision)
	}
}

func TestStateStoreSharesCommittedStateAcrossPods(t *testing.T) {
	first, server := newTestStateStore(t)
	second := openSharedTestStateStore(t, server)
	if err := first.Update(func(state *persistedState) error {
		state.setOrder("order-1", &orderSaga{
			OrderID: "order-1",
			Version: 1,
			Stage:   stageWaitingQuote,
		})
		state.setInbox("message-1", time.Unix(100, 0).UTC())
		state.setOutbox(outboxMessage{MessageID: "outbox-1", Subject: "subject", Data: []byte("payload")})
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	state, err := second.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if state.Orders["order-1"] == nil || state.Orders["order-1"].Stage != stageWaitingQuote {
		t.Fatalf("second pod did not observe committed saga: %#v", state.Orders["order-1"])
	}
	if _, ok := state.Inbox["message-1"]; !ok {
		t.Fatal("second pod did not observe committed inbox entry")
	}
	if state.Outbox["outbox-1"].Subject != "subject" {
		t.Fatal("second pod did not observe committed outbox entry")
	}
}

func TestStateStorePersistsNestedOrderMutationWithoutVersionChange(t *testing.T) {
	first, server := newTestStateStore(t)
	second := openSharedTestStateStore(t, server)
	if err := first.Update(func(state *persistedState) error {
		state.setOrder("order-1", &orderSaga{
			OrderID:     "order-1",
			Version:     5,
			Stage:       stageCompensating,
			NeedRelease: true,
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := first.Update(func(state *persistedState) error {
		state.Orders["order-1"].AuthorizationReleased = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	state, err := second.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !state.Orders["order-1"].AuthorizationReleased {
		t.Fatal("nested order mutation without a version change was not persisted")
	}
}

func TestStateStoreSerializesConcurrentPodUpdates(t *testing.T) {
	first, server := newTestStateStore(t)
	second := openSharedTestStateStore(t, server)
	stores := []*stateStore{first, second}

	const updates = 48
	start := make(chan struct{})
	results := make(chan error, updates)
	var wait sync.WaitGroup
	for index := 0; index < updates; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			results <- stores[index%len(stores)].Update(func(state *persistedState) error {
				state.CatalogRevision++
				state.setInbox(fmt.Sprintf("message-%02d", index), time.Unix(int64(index), 0).UTC())
				return nil
			})
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}

	state, err := first.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if state.CatalogRevision != updates {
		t.Fatalf("catalog revision = %d, want %d", state.CatalogRevision, updates)
	}
	if len(state.Inbox) != updates {
		t.Fatalf("inbox entries = %d, want %d", len(state.Inbox), updates)
	}
}

func TestStateStorePreservesIdempotencyAcrossPods(t *testing.T) {
	first, server := newTestStateStore(t)
	second := openSharedTestStateStore(t, server)
	apply := func(store *stateStore) error {
		return store.Update(func(state *persistedState) error {
			if _, processed := state.Inbox["message-1"]; processed {
				return nil
			}
			state.setInbox("message-1", time.Now().UTC())
			state.setOutbox(outboxMessage{MessageID: "result-1", Subject: "result"})
			state.CatalogRevision++
			return nil
		})
	}

	var wait sync.WaitGroup
	results := make(chan error, 2)
	for _, store := range []*stateStore{first, second} {
		wait.Add(1)
		go func(store *stateStore) {
			defer wait.Done()
			results <- apply(store)
		}(store)
	}
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}

	state, err := first.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if state.CatalogRevision != 1 {
		t.Fatalf("duplicate message applied %d times, want once", state.CatalogRevision)
	}
	if len(state.Inbox) != 1 || len(state.Outbox) != 1 {
		t.Fatalf("idempotency transaction was not atomic: inbox=%d outbox=%d", len(state.Inbox), len(state.Outbox))
	}
}

func TestStateStoreRemovesOutboxBatch(t *testing.T) {
	store, server := newTestStateStore(t)
	if err := store.Update(func(state *persistedState) error {
		state.setOutbox(outboxMessage{MessageID: "one"})
		state.setOutbox(outboxMessage{MessageID: "two"})
		state.setOutbox(outboxMessage{MessageID: "three"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveOutboxBatch([]string{"one", "three", "missing"}); err != nil {
		t.Fatal(err)
	}
	other := openSharedTestStateStore(t, server)
	state, err := other.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Outbox) != 1 || state.Outbox["two"].MessageID != "two" {
		t.Fatalf("unexpected persisted outbox: %#v", state.Outbox)
	}
}

func TestStateStoreOutboxReadsOnlyOutboxHash(t *testing.T) {
	store, server := newTestStateStore(t)
	if err := store.Update(func(state *persistedState) error {
		state.setOutbox(outboxMessage{MessageID: "two", Subject: "subject.two"})
		state.setOutbox(outboxMessage{MessageID: "one", Subject: "subject.one"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// An unrelated corrupt entry proves that Outbox does not deserialize a
	// complete state snapshot on every relay poll.
	server.HSet(store.key("orders"), "corrupt-order", "{")

	before := server.CommandCount()
	messages := store.Outbox()
	if commands := server.CommandCount() - before; commands != 1 {
		t.Fatalf("Outbox() issued %d Redis commands, want one outbox-only read", commands)
	}
	if len(messages) != 2 {
		t.Fatalf("Outbox() returned %d messages, want 2", len(messages))
	}
	if messages[0].MessageID != "one" || messages[1].MessageID != "two" {
		t.Fatalf("Outbox() returned messages in unexpected order: %#v", messages)
	}
}

func TestUpdateIfChangedSkipsRevision(t *testing.T) {
	store, server := newTestStateStore(t)
	before, err := server.Get(store.key("revision"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateIfChanged(func(*persistedState) (bool, error) {
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	after, err := server.Get(store.key("revision"))
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("unchanged update advanced revision from %s to %s", before, after)
	}
}
