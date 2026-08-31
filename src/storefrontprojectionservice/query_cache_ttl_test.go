// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0

package main

import (
	"testing"
	"time"
)

func TestProjectionReadCacheSweepDoesNotExpireOnAccess(t *testing.T) {
	expiration := &cacheExpiration{}
	expiration.deadline.Store(time.Now().Add(-time.Minute).UnixNano())
	cache := &projectionReadCache{
		entries: map[string]cachedProjectionEntry{
			"recommendation.session": {key: "recommendation.session", value: []byte(`{"expires_at":"2026-01-01T00:00:00Z"}`)},
		},
		revisions:   map[string]uint64{"recommendation.session": 1},
		decoded:     make(map[string]decodedProjectionEntry),
		expirations: map[string]*cacheExpiration{"recommendation.session": expiration},
	}

	if _, err := cache.Cached("recommendation.session"); err != nil {
		t.Fatalf("expired entry should remain readable until a sweep: %v", err)
	}
	cache.sweepExpired(time.Now())
	if _, ok := cache.entries["recommendation.session"]; ok {
		t.Fatal("expired entry remained after sweep")
	}
}

func TestProjectionReadCacheIdleTTLRefreshesOnAccess(t *testing.T) {
	now := time.Now()
	expiration := &cacheExpiration{}
	expiration.deadline.Store(now.Add(-time.Minute).UnixNano())
	cache := &projectionReadCache{
		idleTTL: 10 * time.Minute,
		entries: map[string]cachedProjectionEntry{
			"user": {key: "user", value: []byte(`{}`)},
		},
		revisions:   map[string]uint64{"user": 1},
		decoded:     make(map[string]decodedProjectionEntry),
		expirations: map[string]*cacheExpiration{"user": expiration},
	}

	if _, err := cache.Cached("user"); err != nil {
		t.Fatalf("read cart cache entry: %v", err)
	}
	cache.sweepExpired(now.Add(time.Minute))
	if _, ok := cache.entries["user"]; !ok {
		t.Fatal("recently accessed cart entry was swept")
	}
}

func TestProjectionExpiresAt(t *testing.T) {
	want := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	got := projectionExpiresAt([]byte(`{"expires_at":"2026-08-31T12:00:00Z"}`))
	if !got.Equal(want) {
		t.Fatalf("expiry=%s, want %s", got, want)
	}
	if got := projectionExpiresAt([]byte(`{"other":"value"}`)); !got.IsZero() {
		t.Fatalf("entry without expires_at received expiry %s", got)
	}
}
