// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package stateless

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrConflict means the aggregate version changed before the commit.
	ErrConflict = errors.New("aggregate version conflict")
	// ErrInvalidCommit means the caller supplied an incomplete atomic commit.
	ErrInvalidCommit = errors.New("invalid aggregate commit")
)

// AggregateKeys are intentionally co-located in one Redis Cluster hash slot.
type AggregateKeys struct {
	State   string
	Version string
	Inbox   string
}

// KeysForAggregate derives opaque, injection-safe Redis keys. The same
// aggregate ID always produces the same hash tag in every language.
func KeysForAggregate(prefix, aggregateID, inputMessageID string) (AggregateKeys, error) {
	prefix = strings.Trim(strings.TrimSpace(prefix), ":")
	if prefix == "" {
		return AggregateKeys{}, errors.New("Redis key prefix is required")
	}
	if strings.TrimSpace(aggregateID) == "" {
		return AggregateKeys{}, errors.New("aggregate ID is required")
	}
	if strings.TrimSpace(inputMessageID) == "" {
		return AggregateKeys{}, errors.New("input message ID is required")
	}
	tag := digestHex(aggregateID)
	input := digestHex(inputMessageID)
	base := prefix + ":{" + tag + "}"
	return AggregateKeys{
		State:   base + ":state",
		Version: base + ":version",
		Inbox:   base + ":inbox:" + input,
	}, nil
}

func digestHex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// CommitRequest is the complete deterministic value persisted at the handler
// transaction boundary. Journal must contain all result messages and metadata
// needed to publish them without rerunning business logic.
type CommitRequest struct {
	AggregateID      string
	InputMessageID   string
	ExpectedVersion  uint64
	NextState        []byte
	Journal          []byte
	JournalRetention time.Duration
}

// CommitOutcome distinguishes a newly stored transition from an inbox hit.
type CommitOutcome struct {
	Version   uint64
	Journal   []byte
	Duplicate bool
}

// ConflictError exposes the authoritative version observed by Redis.
type ConflictError struct {
	Expected uint64
	Actual   uint64
}

func (err *ConflictError) Error() string {
	return fmt.Sprintf("%v: expected %d, got %d", ErrConflict, err.Expected, err.Actual)
}

func (err *ConflictError) Unwrap() error { return ErrConflict }

// UniversalRedisClient is satisfied by redis.Client and redis.ClusterClient.
type UniversalRedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd
}

// RedisAggregateStore performs aggregate state, version, inbox, and stored
// result persistence in one aggregate-local Lua operation.
type RedisAggregateStore struct {
	client UniversalRedisClient
	prefix string
}

func NewRedisAggregateStore(client UniversalRedisClient, prefix string) (*RedisAggregateStore, error) {
	if client == nil {
		return nil, errors.New("Redis client is required")
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), ":")
	if prefix == "" {
		return nil, errors.New("Redis key prefix is required")
	}
	return &RedisAggregateStore{client: client, prefix: prefix}, nil
}

const commitScript = `
local existing = redis.call("GET", KEYS[3])
local current = tonumber(redis.call("GET", KEYS[2]) or "0")
if existing then
  return {1, current, existing}
end
local expected = tonumber(ARGV[1])
if current ~= expected then
  return {2, current, ""}
end
local next_version = current + 1
redis.call("SET", KEYS[1], ARGV[2])
redis.call("SET", KEYS[2], tostring(next_version))
redis.call("SET", KEYS[3], ARGV[3], "PX", ARGV[4])
return {0, next_version, ARGV[3]}
`

// Commit stores a transition or loads the previously stored result for a
// duplicate input. A conflict never mutates any key.
func (store *RedisAggregateStore) Commit(ctx context.Context, request CommitRequest) (CommitOutcome, error) {
	if len(request.NextState) == 0 || len(request.Journal) == 0 ||
		request.JournalRetention <= 0 {
		return CommitOutcome{}, ErrInvalidCommit
	}
	keys, err := KeysForAggregate(store.prefix, request.AggregateID, request.InputMessageID)
	if err != nil {
		return CommitOutcome{}, fmt.Errorf("%w: %v", ErrInvalidCommit, err)
	}
	retentionMillis := request.JournalRetention.Milliseconds()
	if retentionMillis <= 0 {
		return CommitOutcome{}, ErrInvalidCommit
	}

	value, err := store.client.Eval(
		ctx,
		commitScript,
		[]string{keys.State, keys.Version, keys.Inbox},
		request.ExpectedVersion,
		request.NextState,
		request.Journal,
		retentionMillis,
	).Result()
	if err != nil {
		return CommitOutcome{}, err
	}
	values, ok := value.([]any)
	if !ok || len(values) != 3 {
		return CommitOutcome{}, fmt.Errorf("unexpected Redis commit response %T", value)
	}
	status, err := redisInteger(values[0])
	if err != nil {
		return CommitOutcome{}, err
	}
	version, err := redisInteger(values[1])
	if err != nil {
		return CommitOutcome{}, err
	}
	journal, err := redisBytes(values[2])
	if err != nil {
		return CommitOutcome{}, err
	}
	switch status {
	case 0:
		return CommitOutcome{Version: uint64(version), Journal: journal}, nil
	case 1:
		return CommitOutcome{Version: uint64(version), Journal: journal, Duplicate: true}, nil
	case 2:
		return CommitOutcome{}, &ConflictError{
			Expected: request.ExpectedVersion,
			Actual:   uint64(version),
		}
	default:
		return CommitOutcome{}, fmt.Errorf("unknown Redis commit status %d", status)
	}
}

// LoadResult returns the exact stored journal bytes for an input identity.
func (store *RedisAggregateStore) LoadResult(
	ctx context.Context,
	aggregateID string,
	inputMessageID string,
) ([]byte, error) {
	keys, err := KeysForAggregate(store.prefix, aggregateID, inputMessageID)
	if err != nil {
		return nil, err
	}
	value, err := store.client.Get(ctx, keys.Inbox).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return value, err
}

func redisInteger(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected Redis integer %T", value)
	}
}

func redisBytes(value any) ([]byte, error) {
	switch typed := value.(type) {
	case string:
		return []byte(typed), nil
	case []byte:
		return typed, nil
	default:
		return nil, fmt.Errorf("unexpected Redis bytes %T", value)
	}
}
