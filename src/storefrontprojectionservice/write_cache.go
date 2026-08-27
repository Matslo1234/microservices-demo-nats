// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0

package main

import (
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// cachedProjectionKV keeps the last value and KV revision written or read by
// this replica. CAS remains authoritative: a write made by another replica
// produces a revision conflict, invalidates the local entry, and makes the
// caller's existing retry loop refresh from JetStream.
type cachedProjectionKV struct {
	source     projectionKV
	maxEntries int

	mu      sync.RWMutex
	entries map[string]cachedProjectionEntry
}

func newCachedProjectionKV(source projectionKV, maxEntries int) *cachedProjectionKV {
	return &cachedProjectionKV{
		source: source, maxEntries: maxEntries,
		entries: make(map[string]cachedProjectionEntry),
	}
}

func (cache *cachedProjectionKV) Get(key string) (nats.KeyValueEntry, error) {
	cache.mu.RLock()
	entry, ok := cache.entries[key]
	cache.mu.RUnlock()
	if ok {
		return entry, nil
	}
	entryFromSource, err := cache.source.Get(key)
	if err != nil {
		return nil, err
	}
	cache.store(copyProjectionEntry(entryFromSource))
	return entryFromSource, nil
}

func (cache *cachedProjectionKV) Keys(options ...nats.WatchOpt) ([]string, error) {
	return cache.source.Keys(options...)
}

func (cache *cachedProjectionKV) Create(key string, value []byte) (uint64, error) {
	revision, err := cache.source.Create(key, value)
	if err != nil {
		cache.invalidate(key)
		return 0, err
	}
	cache.store(cachedWriteEntry(key, value, revision))
	return revision, nil
}

func (cache *cachedProjectionKV) Update(key string, value []byte, revision uint64) (uint64, error) {
	nextRevision, err := cache.source.Update(key, value, revision)
	if err != nil {
		cache.invalidate(key)
		return 0, err
	}
	cache.store(cachedWriteEntry(key, value, nextRevision))
	return nextRevision, nil
}

func cachedWriteEntry(key string, value []byte, revision uint64) cachedProjectionEntry {
	return cachedProjectionEntry{
		key: key, value: append([]byte(nil), value...), revision: revision,
		created: time.Now().UTC(), operation: nats.KeyValuePut,
	}
}

func (cache *cachedProjectionKV) store(entry cachedProjectionEntry) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, exists := cache.entries[entry.key]; !exists &&
		cache.maxEntries > 0 && len(cache.entries) >= cache.maxEntries {
		for key := range cache.entries {
			delete(cache.entries, key)
			break
		}
	}
	cache.entries[entry.key] = entry
}

func (cache *cachedProjectionKV) invalidate(key string) {
	cache.mu.Lock()
	delete(cache.entries, key)
	cache.mu.Unlock()
}
