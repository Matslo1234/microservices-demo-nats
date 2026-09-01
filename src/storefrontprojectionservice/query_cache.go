// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

const cartCacheTTL = 10 * time.Minute

type projectionWatchReader interface {
	projectionReader
	WatchAll(...nats.WatchOpt) (nats.KeyWatcher, error)
}

// projectionReadCache mirrors a KV bucket through its ordered watch stream.
// Get falls back to the authoritative bucket on a miss. Cached deliberately
// does not: it is used for latency-tolerant storefront context where an empty
// or briefly stale value is preferable to a remote read on the request path.
type projectionReadCache struct {
	source     projectionReader
	watcher    nats.KeyWatcher
	maxEntries int
	maxBytes   int
	idleTTL    time.Duration
	expiresAt  func([]byte) time.Time

	mu          sync.RWMutex
	entries     map[string]cachedProjectionEntry
	revisions   map[string]uint64
	decoded     map[string]decodedProjectionEntry
	expirations map[string]*cacheExpiration
	generation  atomic.Uint64
	once        sync.Once
	sweepStop   chan struct{}
	sweepDone   chan struct{}
	hits        atomic.Uint64
	misses      atomic.Uint64
	bytes       int
}

type cacheExpiration struct {
	deadline atomic.Int64
}

func projectionExpiresAt(value []byte) time.Time {
	var expiry struct {
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(value, &expiry); err != nil {
		return time.Time{}
	}
	return expiry.ExpiresAt
}

type decodedProjectionEntry struct {
	revision uint64
	value    any
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

type cachedOnlyProjectionReader struct {
	cache *projectionReadCache
}

func (reader cachedOnlyProjectionReader) decodedValue(key string, decode func([]byte) (any, error)) (any, uint64, error) {
	return reader.cache.decodedCachedValue(key, decode)
}

func (reader cachedOnlyProjectionReader) Get(key string) (nats.KeyValueEntry, error) {
	return reader.cache.Cached(key)
}

func (reader cachedOnlyProjectionReader) Keys(options ...nats.WatchOpt) ([]string, error) {
	return reader.cache.Keys(options...)
}

func (entry cachedProjectionEntry) Bucket() string             { return entry.bucket }
func (entry cachedProjectionEntry) Key() string                { return entry.key }
func (entry cachedProjectionEntry) Value() []byte              { return entry.value }
func (entry cachedProjectionEntry) Revision() uint64           { return entry.revision }
func (entry cachedProjectionEntry) Created() time.Time         { return entry.created }
func (entry cachedProjectionEntry) Delta() uint64              { return entry.delta }
func (entry cachedProjectionEntry) Operation() nats.KeyValueOp { return entry.operation }

func newProjectionReadCache(source projectionWatchReader) (*projectionReadCache, error) {
	return newBoundedProjectionReadCache(source, 0)
}

func newBoundedProjectionReadCache(source projectionWatchReader, maxEntries int) (*projectionReadCache, error) {
	return newExpiringProjectionReadCache(source, maxEntries, 0, nil)
}

func newExpiringProjectionReadCache(
	source projectionWatchReader,
	maxEntries int,
	idleTTL time.Duration,
	expiresAt func([]byte) time.Time,
) (*projectionReadCache, error) {
	maxBytes := 0
	if maxEntries > 0 {
		maxBytes = maxEntries * 2048
	}
	return newExpiringProjectionReadCacheWithLimits(source, maxEntries, maxBytes, idleTTL, expiresAt)
}

func newExpiringProjectionReadCacheWithLimits(
	source projectionWatchReader,
	maxEntries, maxBytes int,
	idleTTL time.Duration,
	expiresAt func([]byte) time.Time,
) (*projectionReadCache, error) {
	watcher, err := source.WatchAll()
	if err != nil {
		return nil, err
	}
	cache := &projectionReadCache{
		source: source, watcher: watcher, maxEntries: maxEntries, maxBytes: maxBytes,
		idleTTL: idleTTL, expiresAt: expiresAt,
		entries: make(map[string]cachedProjectionEntry), revisions: make(map[string]uint64),
		decoded: make(map[string]decodedProjectionEntry), expirations: make(map[string]*cacheExpiration),
	}
	if idleTTL > 0 || expiresAt != nil {
		cache.sweepStop = make(chan struct{})
		cache.sweepDone = make(chan struct{})
	}

	updates, watcherErrors := watcher.Updates(), watcher.Error()
	for {
		select {
		case entry, ok := <-updates:
			if !ok {
				_ = watcher.Stop()
				return nil, fmt.Errorf("projection query cache watch closed during initialization")
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
				if cache.sweepStop != nil {
					go cache.sweep()
				}
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

func (cache *projectionReadCache) sweep() {
	defer close(cache.sweepDone)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			cache.sweepExpired(now)
		case <-cache.sweepStop:
			return
		}
	}
}

func (cache *projectionReadCache) sweepExpired(now time.Time) {
	nowUnix := now.UnixNano()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for key, expiration := range cache.expirations {
		deadline := expiration.deadline.Load()
		if deadline == 0 || deadline > nowUnix {
			continue
		}
		cache.bytes -= projectionCacheEntryBytes(cache.entries[key])
		delete(cache.entries, key)
		delete(cache.revisions, key)
		delete(cache.decoded, key)
		delete(cache.expirations, key)
		cache.generation.Add(1)
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
				log.Printf("projection query cache watch failed: %v", watchErr)
			}
		}
	}
}

func (cache *projectionReadCache) apply(entry nats.KeyValueEntry) {
	deadline := time.Time{}
	if cache.idleTTL > 0 {
		deadline = time.Now().Add(cache.idleTTL)
	} else if cache.expiresAt != nil {
		deadline = cache.expiresAt(entry.Value())
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if revision, exists := cache.revisions[entry.Key()]; exists &&
		revision >= entry.Revision() {
		return
	}
	cache.revisions[entry.Key()] = entry.Revision()
	if entry.Operation() == nats.KeyValueDelete || entry.Operation() == nats.KeyValuePurge {
		if previous, exists := cache.entries[entry.Key()]; exists {
			cache.bytes -= projectionCacheEntryBytes(previous)
		}
		delete(cache.entries, entry.Key())
		delete(cache.decoded, entry.Key())
		delete(cache.expirations, entry.Key())
		cache.generation.Add(1)
		return
	}
	if previous, exists := cache.entries[entry.Key()]; exists {
		cache.bytes -= projectionCacheEntryBytes(previous)
		delete(cache.entries, entry.Key())
	}
	entryCopy := copyProjectionEntry(entry)
	entryBytes := projectionCacheEntryBytes(entryCopy)
	for len(cache.entries) > 0 && ((cache.maxEntries > 0 && len(cache.entries) >= cache.maxEntries) ||
		(cache.maxBytes > 0 && cache.bytes+entryBytes > cache.maxBytes)) {
		for key := range cache.entries {
			cache.bytes -= projectionCacheEntryBytes(cache.entries[key])
			delete(cache.entries, key)
			delete(cache.revisions, key)
			delete(cache.decoded, key)
			delete(cache.expirations, key)
			break
		}
	}
	cache.entries[entry.Key()] = entryCopy
	cache.bytes += entryBytes
	delete(cache.decoded, entry.Key())
	if deadline.IsZero() {
		delete(cache.expirations, entry.Key())
	} else {
		expiration := &cacheExpiration{}
		expiration.deadline.Store(deadline.UnixNano())
		cache.expirations[entry.Key()] = expiration
	}
	cache.generation.Add(1)
}

// decodedValue memoizes the immutable decoded representation for the current
// KV revision. Updates invalidate it while holding the same lock, so callers
// cannot pair a decoded value with a different revision.
func (cache *projectionReadCache) decodedValue(key string, decode func([]byte) (any, error)) (any, uint64, error) {
	value, revision, err := cache.decodedCachedValue(key, decode)
	if !errors.Is(err, nats.ErrKeyNotFound) {
		return value, revision, err
	}
	authoritative, err := cache.source.Get(key)
	if err != nil {
		return nil, 0, err
	}
	cache.apply(authoritative)
	value, err = decode(authoritative.Value())
	if err != nil {
		return nil, 0, err
	}
	cache.mu.Lock()
	if current, ok := cache.entries[key]; ok && current.revision == authoritative.Revision() {
		cache.decoded[key] = decodedProjectionEntry{revision: authoritative.Revision(), value: value}
	}
	cache.mu.Unlock()
	return value, authoritative.Revision(), nil
}

func (cache *projectionReadCache) decodedCachedValue(key string, decode func([]byte) (any, error)) (any, uint64, error) {
	cache.mu.RLock()
	entry, ok := cache.entries[key]
	expiration := cache.expirations[key]
	if ok {
		if decoded, found := cache.decoded[key]; found && decoded.revision == entry.revision {
			cache.mu.RUnlock()
			cache.refresh(expiration)
			cache.hits.Add(1)
			return decoded.value, entry.revision, nil
		}
	}
	cache.mu.RUnlock()
	if !ok {
		cache.misses.Add(1)
		return nil, 0, nats.ErrKeyNotFound
	}

	value, err := decode(entry.value)
	if err != nil {
		return nil, 0, err
	}
	cache.mu.Lock()
	current, currentOK := cache.entries[key]
	if currentOK && current.revision == entry.revision {
		cache.decoded[key] = decodedProjectionEntry{revision: entry.revision, value: value}
	}
	cache.mu.Unlock()
	cache.hits.Add(1)
	cache.refresh(expiration)
	return value, entry.revision, nil
}

func (cache *projectionReadCache) refresh(expiration *cacheExpiration) {
	if expiration != nil && cache.idleTTL > 0 {
		expiration.deadline.Store(time.Now().Add(cache.idleTTL).UnixNano())
	}
}

func (cache *projectionReadCache) Generation() uint64 { return cache.generation.Load() }

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
	expiration := cache.expirations[key]
	cache.mu.RUnlock()
	if ok {
		cache.refresh(expiration)
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

func (cache *projectionReadCache) Cached(key string) (nats.KeyValueEntry, error) {
	cache.mu.RLock()
	entry, ok := cache.entries[key]
	expiration := cache.expirations[key]
	cache.mu.RUnlock()
	if !ok {
		cache.misses.Add(1)
		return nil, nats.ErrKeyNotFound
	}
	cache.refresh(expiration)
	cache.hits.Add(1)
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
		if cache.sweepStop != nil {
			close(cache.sweepStop)
			<-cache.sweepDone
		}
		if err := cache.watcher.Stop(); err != nil &&
			err != nats.ErrBadSubscription &&
			err != nats.ErrConnectionClosed {
			log.Printf("projection query cache stop failed: %v", err)
		}
	})
}
