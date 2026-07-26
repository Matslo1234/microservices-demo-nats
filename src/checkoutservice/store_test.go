// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStateStoreRollsBackFailedUpdate(t *testing.T) {
	store, err := openStateStore(filepath.Join(t.TempDir(), "sagas.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Update(func(state *persistedState) error {
		state.CatalogRevision = 7
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	expected := errors.New("injected update failure")
	err = store.Update(func(state *persistedState) error {
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
	t.Cleanup(func() { _ = store.Close() })
	if state.CatalogRevision != 7 {
		t.Fatalf("catalog revision = %d, want persisted value 7", state.CatalogRevision)
	}
}

func TestStateStoreRemovesOutboxBatch(t *testing.T) {
	store, err := openStateStore(filepath.Join(t.TempDir(), "sagas.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(state *persistedState) error {
		state.Outbox["one"] = outboxMessage{MessageID: "one"}
		state.Outbox["two"] = outboxMessage{MessageID: "two"}
		state.Outbox["three"] = outboxMessage{MessageID: "three"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RemoveOutboxBatch([]string{"one", "three", "missing"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openStateStore(store.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	state, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if len(state.Outbox) != 1 || state.Outbox["two"].MessageID != "two" {
		t.Fatalf("unexpected persisted outbox: %#v", state.Outbox)
	}
}

func TestUpdateIfChangedSkipsPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sagas.json")
	store, err := openStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateIfChanged(func(*persistedState) (bool, error) {
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unchanged update created a state file: %v", err)
	}
}

func TestStateStoreMigratesLegacyJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sagas.json")
	legacy := newPersistedState()
	legacy.CatalogRevision = 17
	legacy.Outbox["message-1"] = outboxMessage{MessageID: "message-1", Subject: "subject", Data: []byte("payload")}
	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := openStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = openStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	state, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if state.CatalogRevision != 17 || state.Outbox["message-1"].Subject != "subject" {
		t.Fatalf("legacy state was not migrated: %#v", state)
	}
}

func TestStateStorePersistsOrderMutationWithoutVersionChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sagas.json")
	store, err := openStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(state *persistedState) error {
		state.Orders["order-1"] = &orderSaga{
			OrderID:     "order-1",
			Version:     5,
			Stage:       stageCompensating,
			NeedRelease: true,
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(state *persistedState) error {
		state.Orders["order-1"].AuthorizationReleased = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = openStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	state, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !state.Orders["order-1"].AuthorizationReleased {
		t.Fatal("order mutation without a version change was not persisted")
	}
}

func TestStateStorePersistsTrackedChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sagas.json")
	store, err := openStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateTracked(func(state *persistedState) error {
		state.setCatalogRevision(23)
		state.setOrder("order-1", &orderSaga{
			OrderID: "order-1",
			Version: 4,
			Stage:   stageCompensating,
		})
		state.setInbox("message-1", time.Unix(100, 0).UTC())
		state.setOutbox(outboxMessage{MessageID: "outbox-1", Subject: "subject", Data: []byte("payload")})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateTracked(func(state *persistedState) error {
		state.Orders["order-1"].AuthorizationReleased = true
		state.markOrder("order-1")
		state.deleteOutbox("outbox-1")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	state, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if state.CatalogRevision != 23 || !state.Orders["order-1"].AuthorizationReleased {
		t.Fatalf("tracked metadata/order changes were not persisted: %#v", state)
	}
	if _, ok := state.Inbox["message-1"]; !ok {
		t.Fatal("tracked inbox change was not persisted")
	}
	if _, ok := state.Outbox["outbox-1"]; ok {
		t.Fatal("tracked outbox deletion was not persisted")
	}
}

func TestStateStoreGroupsConcurrentTrackedUpdates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sagas.json")
	store, err := openStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	store.trackedBatchDelay = 50 * time.Millisecond

	const updates = 24
	start := make(chan struct{})
	results := make(chan error, updates)
	var wait sync.WaitGroup
	for index := 0; index < updates; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			results <- store.UpdateTracked(func(state *persistedState) error {
				key := fmt.Sprintf("message-%02d", index)
				state.setInbox(key, time.Unix(int64(index), 0).UTC())
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

	store.mu.Lock()
	commits := store.trackedBatchCommits
	store.mu.Unlock()
	if commits != 1 {
		t.Fatalf("tracked batch commits = %d, want 1", commits)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	state, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Inbox) != updates {
		t.Fatalf("persisted inbox entries = %d, want %d", len(state.Inbox), updates)
	}
}

func BenchmarkStateStoreUpdateWithHistory(b *testing.B) {
	store, err := openStateStore(filepath.Join(b.TempDir(), "sagas.json"))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Update(func(state *persistedState) error {
		for index := 0; index < 1_000; index++ {
			key := fmt.Sprintf("order-%04d", index)
			state.Orders[key] = &orderSaga{OrderID: key, Version: 1, Stage: stageWaitingQuote}
			state.Inbox["message-"+key] = time.Unix(int64(index), 0).UTC()
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := store.Update(func(state *persistedState) error {
			order := state.Orders["order-0000"]
			order.Version++
			order.Stage = stageWaitingAuthorize
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStateStoreTrackedUpdateWithHistory(b *testing.B) {
	store, err := openStateStore(filepath.Join(b.TempDir(), "sagas.json"))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Update(func(state *persistedState) error {
		for index := 0; index < 1_000; index++ {
			key := fmt.Sprintf("order-%04d", index)
			state.Orders[key] = &orderSaga{OrderID: key, Version: 1, Stage: stageWaitingQuote}
			state.Inbox["message-"+key] = time.Unix(int64(index), 0).UTC()
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := store.UpdateTracked(func(state *persistedState) error {
			order := state.Orders["order-0000"]
			order.Version++
			order.Stage = stageWaitingAuthorize
			state.markOrder(order.OrderID)
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}
