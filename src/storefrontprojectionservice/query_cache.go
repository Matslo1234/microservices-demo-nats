// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

type projectionWatchReader interface {
	projectionReader
	WatchAll(...nats.WatchOpt) (nats.KeyWatcher, error)
}

// projectionReadCache mirrors one KV bucket through its ordered watch stream.
// A miss still consults the authoritative bucket so a just-created key is not
// temporarily reported as absent while its watch update is in flight.
type projectionReadCache struct {
	source  projectionReader
	watcher nats.KeyWatcher

	mu        sync.RWMutex
	entries   map[string]cachedProjectionEntry
	revisions map[string]uint64
	once      sync.Once
	hits      atomic.Uint64
	misses    atomic.Uint64
}

type cachedProjectionEntry struct {
	bucket    string
	key       string
	value     []byte
	revision  uint64
	created   time.Time
	delta     uint64
	operation nats.KeyValueOp
}

func (entry cachedProjectionEntry) Bucket() string             { return entry.bucket }
func (entry cachedProjectionEntry) Key() string                { return entry.key }
func (entry cachedProjectionEntry) Value() []byte              { return entry.value }
func (entry cachedProjectionEntry) Revision() uint64           { return entry.revision }
func (entry cachedProjectionEntry) Created() time.Time         { return entry.created }
func (entry cachedProjectionEntry) Delta() uint64              { return entry.delta }
func (entry cachedProjectionEntry) Operation() nats.KeyValueOp { return entry.operation }

func newProjectionReadCache(source projectionWatchReader) (*projectionReadCache, error) {
	watcher, err := source.WatchAll()
	if err != nil {
		return nil, err
	}
	cache := &projectionReadCache{
		source: source, watcher: watcher,
		entries:   make(map[string]cachedProjectionEntry),
		revisions: make(map[string]uint64),
	}

	updates, watcherErrors := watcher.Updates(), watcher.Error()
	for {
		select {
		case entry, ok := <-updates:
			if !ok {
				_ = watcher.Stop()
				return nil, fmt.Errorf("product query cache watch closed during initialization")
			}
			if entry == nil {
				select {
				case watchErr, ok := <-watcherErrors:
					if ok && watchErr != nil {
						_ = watcher.Stop()
						return nil, watchErr
					}
				default:
				}
				go cache.follow(updates, watcherErrors)
				return cache, nil
			}
			cache.apply(entry)
		case watchErr, ok := <-watcherErrors:
			if !ok {
				watcherErrors = nil
				continue
			}
			if watchErr != nil {
				_ = watcher.Stop()
				return nil, watchErr
			}
		}
	}
}

func (cache *projectionReadCache) follow(
	updates <-chan nats.KeyValueEntry,
	watcherErrors <-chan error,
) {
	for updates != nil || watcherErrors != nil {
		select {
		case entry, ok := <-updates:
			if !ok {
				updates = nil
				continue
			}
			if entry != nil {
				cache.apply(entry)
			}
		case watchErr, ok := <-watcherErrors:
			if !ok {
				watcherErrors = nil
				continue
			}
			if watchErr != nil {
				log.Printf("product query cache watch failed: %v", watchErr)
			}
		}
	}
}

func (cache *projectionReadCache) apply(entry nats.KeyValueEntry) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if revision, exists := cache.revisions[entry.Key()]; exists &&
		revision >= entry.Revision() {
		return
	}
	cache.revisions[entry.Key()] = entry.Revision()
	if entry.Operation() == nats.KeyValueDelete || entry.Operation() == nats.KeyValuePurge {
		delete(cache.entries, entry.Key())
		return
	}
	cache.entries[entry.Key()] = copyProjectionEntry(entry)
}

func copyProjectionEntry(entry nats.KeyValueEntry) cachedProjectionEntry {
	return cachedProjectionEntry{
		bucket: entry.Bucket(), key: entry.Key(),
		value:    append([]byte(nil), entry.Value()...),
		revision: entry.Revision(), created: entry.Created(),
		delta: entry.Delta(), operation: entry.Operation(),
	}
}

func (cache *projectionReadCache) Get(key string) (nats.KeyValueEntry, error) {
	cache.mu.RLock()
	entry, ok := cache.entries[key]
	cache.mu.RUnlock()
	if ok {
		cache.hits.Add(1)
		return entry, nil
	}

	cache.misses.Add(1)
	authoritative, err := cache.source.Get(key)
	if err != nil {
		return nil, err
	}
	cache.apply(authoritative)
	cache.mu.RLock()
	entry, ok = cache.entries[key]
	cache.mu.RUnlock()
	if !ok {
		return nil, nats.ErrKeyNotFound
	}
	return entry, nil
}

func (cache *projectionReadCache) Keys(...nats.WatchOpt) ([]string, error) {
	cache.mu.RLock()
	keys := make([]string, 0, len(cache.entries))
	for key := range cache.entries {
		keys = append(keys, key)
	}
	cache.mu.RUnlock()
	if len(keys) == 0 {
		return nil, nats.ErrNoKeysFound
	}
	sort.Strings(keys)
	return keys, nil
}

func (cache *projectionReadCache) Close() {
	cache.once.Do(func() {
		if err := cache.watcher.Stop(); err != nil &&
			err != nats.ErrBadSubscription &&
			err != nats.ErrConnectionClosed {
			log.Printf("product query cache stop failed: %v", err)
		}
	})
}
