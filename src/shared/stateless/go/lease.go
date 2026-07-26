// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package stateless

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrLeaseHeld     = errors.New("work lease is held")
	ErrLeaseLost     = errors.New("work lease ownership was lost")
	ErrLeaseComplete = errors.New("work item is already complete")
)

// Lease is a fencing-token claim. Attempts increase after every successful
// acquisition, including visibility-timeout recovery.
type Lease struct {
	WorkID     string
	WorkerID   string
	Token      uint64
	Attempts   uint64
	LeaseUntil time.Time
	Completed  bool
}

// LeaseHeldError describes the active owner without granting ownership.
type LeaseHeldError struct {
	Owner      string
	LeaseUntil time.Time
}

func (err *LeaseHeldError) Error() string {
	return fmt.Sprintf("%v by %s until %s", ErrLeaseHeld, err.Owner, err.LeaseUntil.UTC().Format(time.RFC3339Nano))
}

func (err *LeaseHeldError) Unwrap() error { return ErrLeaseHeld }

// RedisLeaseStore implements expiring claims with compare-and-set renewal and
// completion. Times are supplied by the caller so tests and synthetic deadline
// inputs remain deterministic.
type RedisLeaseStore struct {
	client    UniversalRedisClient
	prefix    string
	retention time.Duration
}

func NewRedisLeaseStore(
	client UniversalRedisClient,
	prefix string,
	completionRetention time.Duration,
) (*RedisLeaseStore, error) {
	if client == nil {
		return nil, errors.New("Redis client is required")
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), ":")
	if prefix == "" {
		return nil, errors.New("lease key prefix is required")
	}
	if completionRetention <= 0 {
		return nil, errors.New("lease completion retention must be positive")
	}
	return &RedisLeaseStore{
		client:    client,
		prefix:    prefix,
		retention: completionRetention,
	}, nil
}

func (store *RedisLeaseStore) key(workID string) (string, error) {
	if strings.TrimSpace(workID) == "" {
		return "", errors.New("work ID is required")
	}
	return store.prefix + ":{" + digestHex(workID) + "}:lease", nil
}

const acquireLeaseScript = `
local completed = redis.call("HGET", KEYS[1], "completed")
if completed == "1" then
  return {2, redis.call("HGET", KEYS[1], "token") or "0", 0,
          redis.call("HGET", KEYS[1], "attempts") or "0",
          redis.call("HGET", KEYS[1], "owner") or ""}
end
local now = tonumber(ARGV[2])
local lease_until = tonumber(redis.call("HGET", KEYS[1], "lease_until") or "0")
if lease_until > now then
  return {1, redis.call("HGET", KEYS[1], "token") or "0", lease_until,
          redis.call("HGET", KEYS[1], "attempts") or "0",
          redis.call("HGET", KEYS[1], "owner") or ""}
end
local token = redis.call("HINCRBY", KEYS[1], "token", 1)
local attempts = redis.call("HINCRBY", KEYS[1], "attempts", 1)
local next_until = now + tonumber(ARGV[3])
redis.call("HSET", KEYS[1], "owner", ARGV[1], "lease_until", next_until, "completed", "0")
redis.call("PEXPIRE", KEYS[1], ARGV[4])
return {0, token, next_until, attempts, ARGV[1]}
`

// Acquire claims an unowned or expired item. It never steals an unexpired
// lease, even when called again by the same worker.
func (store *RedisLeaseStore) Acquire(
	ctx context.Context,
	workID string,
	workerID string,
	now time.Time,
	duration time.Duration,
) (Lease, error) {
	if strings.TrimSpace(workerID) == "" || now.IsZero() || duration <= 0 {
		return Lease{}, errors.New("worker ID, current time, and positive lease duration are required")
	}
	key, err := store.key(workID)
	if err != nil {
		return Lease{}, err
	}
	ttl := store.retention
	if minimum := duration * 2; ttl < minimum {
		ttl = minimum
	}
	value, err := store.client.Eval(
		ctx,
		acquireLeaseScript,
		[]string{key},
		workerID,
		now.UTC().UnixMilli(),
		duration.Milliseconds(),
		ttl.Milliseconds(),
	).Result()
	if err != nil {
		return Lease{}, err
	}
	lease, status, err := parseLeaseResponse(workID, value)
	if err != nil {
		return Lease{}, err
	}
	switch status {
	case 0:
		return lease, nil
	case 1:
		return Lease{}, &LeaseHeldError{Owner: lease.WorkerID, LeaseUntil: lease.LeaseUntil}
	case 2:
		return lease, ErrLeaseComplete
	default:
		return Lease{}, fmt.Errorf("unknown Redis lease status %d", status)
	}
}

const renewLeaseScript = `
if redis.call("HGET", KEYS[1], "completed") == "1" then return 2 end
if redis.call("HGET", KEYS[1], "owner") ~= ARGV[1] or
   tonumber(redis.call("HGET", KEYS[1], "token") or "0") ~= tonumber(ARGV[2]) or
   tonumber(redis.call("HGET", KEYS[1], "lease_until") or "0") <= tonumber(ARGV[3]) then
  return 1
end
local next_until = tonumber(ARGV[3]) + tonumber(ARGV[4])
redis.call("HSET", KEYS[1], "lease_until", next_until)
redis.call("PEXPIRE", KEYS[1], ARGV[5])
return next_until
`

func (store *RedisLeaseStore) Renew(
	ctx context.Context,
	lease Lease,
	now time.Time,
	duration time.Duration,
) (Lease, error) {
	if lease.Token == 0 || now.IsZero() || duration <= 0 {
		return Lease{}, errors.New("valid lease, current time, and positive duration are required")
	}
	key, err := store.key(lease.WorkID)
	if err != nil {
		return Lease{}, err
	}
	ttl := store.retention
	if minimum := duration * 2; ttl < minimum {
		ttl = minimum
	}
	value, err := store.client.Eval(
		ctx,
		renewLeaseScript,
		[]string{key},
		lease.WorkerID,
		lease.Token,
		now.UTC().UnixMilli(),
		duration.Milliseconds(),
		ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return Lease{}, err
	}
	switch value {
	case 1:
		return Lease{}, ErrLeaseLost
	case 2:
		return Lease{}, ErrLeaseComplete
	default:
		lease.LeaseUntil = time.UnixMilli(value).UTC()
		return lease, nil
	}
}

const completeLeaseScript = `
if redis.call("HGET", KEYS[1], "completed") == "1" then return 2 end
if redis.call("HGET", KEYS[1], "owner") ~= ARGV[1] or
   tonumber(redis.call("HGET", KEYS[1], "token") or "0") ~= tonumber(ARGV[2]) or
   tonumber(redis.call("HGET", KEYS[1], "lease_until") or "0") <= tonumber(ARGV[3]) then
  return 1
end
redis.call("HSET", KEYS[1], "completed", "1", "completed_at", ARGV[3], "lease_until", "0")
redis.call("PEXPIRE", KEYS[1], ARGV[4])
return 0
`

// Complete records terminal completion only for the current unexpired fencing
// token. The marker is retained across redelivery for the configured window.
func (store *RedisLeaseStore) Complete(ctx context.Context, lease Lease, now time.Time) error {
	if lease.Token == 0 || now.IsZero() {
		return errors.New("valid lease and completion time are required")
	}
	key, err := store.key(lease.WorkID)
	if err != nil {
		return err
	}
	status, err := store.client.Eval(
		ctx,
		completeLeaseScript,
		[]string{key},
		lease.WorkerID,
		lease.Token,
		now.UTC().UnixMilli(),
		store.retention.Milliseconds(),
	).Int()
	if err != nil {
		return err
	}
	switch status {
	case 0:
		return nil
	case 1:
		return ErrLeaseLost
	case 2:
		return ErrLeaseComplete
	default:
		return fmt.Errorf("unknown Redis lease completion status %d", status)
	}
}

func parseLeaseResponse(workID string, value any) (Lease, int64, error) {
	values, ok := value.([]any)
	if !ok || len(values) != 5 {
		return Lease{}, 0, fmt.Errorf("unexpected Redis lease response %T", value)
	}
	status, err := redisInteger(values[0])
	if err != nil {
		return Lease{}, 0, err
	}
	token, err := redisInteger(values[1])
	if err != nil {
		return Lease{}, 0, err
	}
	untilMillis, err := redisInteger(values[2])
	if err != nil {
		return Lease{}, 0, err
	}
	attempts, err := redisInteger(values[3])
	if err != nil {
		return Lease{}, 0, err
	}
	owner, err := redisBytes(values[4])
	if err != nil {
		return Lease{}, 0, err
	}
	lease := Lease{
		WorkID:    workID,
		WorkerID:  string(owner),
		Token:     uint64(token),
		Attempts:  uint64(attempts),
		Completed: status == 2,
	}
	if untilMillis > 0 {
		lease.LeaseUntil = time.UnixMilli(untilMillis).UTC()
	}
	return lease, status, nil
}
