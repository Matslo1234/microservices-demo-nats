// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
)

type persistedState struct {
	Orders          map[string]*orderSaga                `json:"orders"`
	Products        map[string]*commonv1.ProductSnapshot `json:"products"`
	Carts           map[string]*commonv1.CartSnapshot    `json:"carts"`
	Rates           *eventsv1.CurrencyRatesUpdatedEvent  `json:"rates,omitempty"`
	CatalogRevision uint64                               `json:"catalog_revision"`
	Inbox           map[string]time.Time                 `json:"inbox"`
	Outbox          map[string]outboxMessage             `json:"outbox"`
}

type outboxMessage struct {
	MessageID string `json:"message_id"`
	Subject   string `json:"subject"`
	Data      []byte `json:"data"`
}

func newPersistedState() *persistedState {
	return &persistedState{
		Orders: make(map[string]*orderSaga), Products: make(map[string]*commonv1.ProductSnapshot),
		Carts: make(map[string]*commonv1.CartSnapshot), Inbox: make(map[string]time.Time),
		Outbox: make(map[string]outboxMessage),
	}
}

func (state *persistedState) normalize() {
	if state.Orders == nil {
		state.Orders = make(map[string]*orderSaga)
	}
	if state.Products == nil {
		state.Products = make(map[string]*commonv1.ProductSnapshot)
	}
	if state.Carts == nil {
		state.Carts = make(map[string]*commonv1.CartSnapshot)
	}
	if state.Inbox == nil {
		state.Inbox = make(map[string]time.Time)
	}
	if state.Outbox == nil {
		state.Outbox = make(map[string]outboxMessage)
	}
}

type stateStore struct {
	mu    sync.Mutex
	path  string
	state *persistedState
}

func openStateStore(path string) (*stateStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create checkout store directory: %w", err)
	}
	state := newPersistedState()
	encoded, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(encoded, state); err != nil {
			return nil, fmt.Errorf("decode checkout state: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read checkout state: %w", err)
	}
	state.normalize()
	return &stateStore{path: path, state: state}, nil
}

func (store *stateStore) Update(update func(*persistedState) error) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	encoded, err := json.Marshal(store.state)
	if err != nil {
		return err
	}
	copyState := newPersistedState()
	if err := json.Unmarshal(encoded, copyState); err != nil {
		return err
	}
	copyState.normalize()
	if err := update(copyState); err != nil {
		return err
	}
	if err := store.persist(copyState); err != nil {
		return err
	}
	store.state = copyState
	return nil
}

func (store *stateStore) Snapshot() (*persistedState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	encoded, err := json.Marshal(store.state)
	if err != nil {
		return nil, err
	}
	copyState := newPersistedState()
	if err := json.Unmarshal(encoded, copyState); err != nil {
		return nil, err
	}
	copyState.normalize()
	return copyState, nil
}

func (store *stateStore) Outbox() []outboxMessage {
	store.mu.Lock()
	defer store.mu.Unlock()
	messages := make([]outboxMessage, 0, len(store.state.Outbox))
	for _, message := range store.state.Outbox {
		messages = append(messages, message)
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].MessageID < messages[j].MessageID })
	return messages
}

func (store *stateStore) RemoveOutbox(messageID string) error {
	return store.Update(func(state *persistedState) error { delete(state.Outbox, messageID); return nil })
}

func (store *stateStore) persist(state *persistedState) error {
	temporary := store.path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open checkout state temp file: %w", err)
	}
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(state); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, store.path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(store.path))
	if err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}
