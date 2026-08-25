// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commandsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/commands/v1"
	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	stateless "github.com/GoogleCloudPlatform/microservices-demo/src/shared/stateless/go"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

const (
	redisStateSchemaVersion = 3
	redisTransactionRetries = 16
	defaultRedisStatePrefix = "checkout:v2"
	checkoutDeadlineShards  = 64
	defaultRedisRetention   = 33 * 24 * time.Hour
)

var errStateStoreClosed = errors.New("checkout state store is closed")

type checkoutRedisClient interface {
	stateless.UniversalRedisClient
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error)
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	Ping(ctx context.Context) *redis.StatusCmd
	ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd
	Close() error
}

type stateStore struct {
	client    checkoutRedisClient
	prefix    string
	now       func() time.Time
	retention time.Duration

	closed    atomic.Bool
	conflicts atomic.Uint64
	mu        sync.Mutex
}

type transitionOutcome struct {
	Results   []resultMessage
	Duplicate bool
	Version   uint64
}

type dueDeadline struct {
	OrderID string
	Shard   int
}

type deadlineRecord struct {
	OrderID  string    `json:"order_id"`
	Version  uint64    `json:"version"`
	Deadline time.Time `json:"deadline"`
	WorkID   string    `json:"work_id"`
}

func openStateStoreWithPrefix(address, prefix string) (*stateStore, error) {
	return openStateStore(address, prefix, false)
}

func openStateStore(address, prefix string, clustered bool) (*stateStore, error) {
	return openStateStoreWithRetention(address, prefix, clustered, defaultRedisRetention)
}

func openStateStoreWithRetention(address, prefix string, clustered bool, retention time.Duration) (*stateStore, error) {
	if strings.TrimSpace(address) == "" {
		return nil, errors.New("CHECKOUT_REDIS_ADDR is required")
	}
	if strings.TrimSpace(prefix) == "" {
		prefix = defaultRedisStatePrefix
	}
	if retention <= 0 {
		return nil, errors.New("checkout Redis retention must be positive")
	}
	var client checkoutRedisClient
	if clustered {
		addresses := strings.Split(address, ",")
		for index := range addresses {
			addresses[index] = strings.TrimSpace(addresses[index])
		}
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        addresses,
			MaxRedirects: 16,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		})
	} else {
		options := &redis.Options{Addr: address}
		if strings.Contains(address, "://") {
			parsed, err := redis.ParseURL(address)
			if err != nil {
				return nil, fmt.Errorf("parse checkout Redis URL: %w", err)
			}
			options = parsed
		}
		client = redis.NewClient(options)
	}
	store := &stateStore{
		client: client, prefix: strings.TrimSuffix(prefix, ":"), now: time.Now,
		retention: retention,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect checkout state store: %w", err)
	}
	// A fresh-cluster schema marker catches accidentally reused Phase 1 data
	// without introducing a migration or a global transaction revision.
	schemaKey := store.prefix + ":schema"
	if err := client.SetNX(ctx, schemaKey, redisStateSchemaVersion, 0).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("initialize checkout schema: %w", err)
	}
	version, err := client.Get(ctx, schemaKey).Int()
	if err != nil || version != redisStateSchemaVersion {
		_ = client.Close()
		if err != nil {
			return nil, fmt.Errorf("read checkout schema: %w", err)
		}
		return nil, fmt.Errorf("unsupported checkout schema %d", version)
	}
	return store, nil
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func deadlineShard(orderID string) int {
	sum := sha256.Sum256([]byte(orderID))
	return int(sum[0]) % checkoutDeadlineShards
}

func (store *stateStore) orderBase(orderID string) string {
	shard := deadlineShard(orderID)
	return fmt.Sprintf("%s:{checkout-s%02d}:order:%s", store.prefix, shard, digest(orderID))
}

func (store *stateStore) deadlineKey(shard int) string {
	return fmt.Sprintf("%s:{checkout-s%02d}:deadlines", store.prefix, shard)
}

func (store *stateStore) projectionBase(kind, identity string) string {
	return store.prefix + ":projection:{" + kind + "-" + digest(identity) + "}"
}

func (store *stateStore) projectionMarker(kind string) string {
	return store.prefix + ":projection:{" + kind + "}"
}

func encodeRedisJSON(value any) ([]byte, error) {
	plain, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var encoded bytes.Buffer
	writer, err := gzip.NewWriterLevel(&encoded, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := writer.Write(plain); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return encoded.Bytes(), nil
}

func decodeRedisJSON(encoded []byte, target any) error {
	reader, err := gzip.NewReader(bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	plain, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	return json.Unmarshal(plain, target)
}

const commitOrderScript = `
local existing = redis.call("HGET", KEYS[4], ARGV[1])
local current = tonumber(redis.call("GET", KEYS[2]) or "0")
if existing then
  return {1, current, existing}
end
if current ~= tonumber(ARGV[2]) then
  return {2, current, ""}
end
if ARGV[4] ~= "" then
  redis.call("SET", KEYS[1], ARGV[4])
  redis.call("SET", KEYS[2], ARGV[3])
end
if ARGV[5] ~= "" then
  redis.call("SETNX", KEYS[3], ARGV[5])
end
redis.call("HSET", KEYS[4], ARGV[1], ARGV[6])
redis.call("PEXPIRE", KEYS[4], ARGV[7])
if ARGV[8] == "set" then
  redis.call("ZADD", KEYS[5], ARGV[9], ARGV[10])
  redis.call("SET", KEYS[6], ARGV[11])
elseif ARGV[8] == "remove" then
  redis.call("ZREM", KEYS[5], ARGV[10])
  redis.call("DEL", KEYS[6])
end
return {0, tonumber(ARGV[3]), ARGV[6]}
`

func (store *stateStore) ApplyOrder(
	orderID string,
	input *commonv1.MessageEnvelope,
	base *persistedState,
	update func(*persistedState) error,
) (transitionOutcome, error) {
	if store.closed.Load() {
		return transitionOutcome{}, errStateStoreClosed
	}
	if strings.TrimSpace(orderID) == "" || input == nil || strings.TrimSpace(input.MessageId) == "" {
		return transitionOutcome{}, errors.New("order ID and input message ID are required")
	}
	at := input.GetOccurredAt().AsTime()
	if input.GetOccurredAt() == nil || at.IsZero() {
		return transitionOutcome{}, errors.New("input occurrence time is required")
	}
	deadlineBaseTime := time.Now().UTC()
	if store.now != nil {
		deadlineBaseTime = store.now().UTC()
	}
	baseKey := store.orderBase(orderID)
	keys := []string{
		baseKey + ":saga",
		baseKey + ":version",
		baseKey + ":accepted",
		baseKey + ":results",
		store.deadlineKey(deadlineShard(orderID)),
		baseKey + ":deadline",
	}
	inputKey := digest(input.MessageId)

	for attempt := 0; attempt < redisTransactionRetries; attempt++ {
		state, expected, accepted, err := store.loadOrderWorkspace(orderID, at, base)
		if err != nil {
			return transitionOutcome{}, err
		}
		state.DeadlineStart = deadlineBaseTime
		state.Input = proto.Clone(input).(*commonv1.MessageEnvelope)
		if err := update(state); err != nil {
			return transitionOutcome{}, err
		}
		saga := state.Orders[orderID]
		nextVersion := expected
		var sagaJSON, acceptedJSON []byte
		deadlineMode := "remove"
		var deadlineMillis int64
		var encodedDeadline []byte
		if saga != nil {
			nextVersion = saga.Version
			sagaJSON, err = encodeRedisJSON(saga)
			if err != nil {
				return transitionOutcome{}, fmt.Errorf("encode checkout saga: %w", err)
			}
			if !saga.Deadline.IsZero() {
				deadlineMode = "set"
				deadlineMillis = saga.Deadline.UTC().UnixMilli()
				record := newDeadlineRecord(saga)
				encodedDeadline, err = encodeRedisJSON(record)
				if err != nil {
					return transitionOutcome{}, fmt.Errorf("encode checkout deadline: %w", err)
				}
			}
			if accepted == nil && expected == 0 {
				accepted = acceptedFromState(orderID, state, saga)
				acceptedJSON, err = encodeRedisJSON(accepted)
				if err != nil {
					return transitionOutcome{}, fmt.Errorf("encode accepted order: %w", err)
				}
			}
		}
		journal, err := encodeRedisJSON(state.Results)
		if err != nil {
			return transitionOutcome{}, fmt.Errorf("encode checkout result journal: %w", err)
		}
		if input.MessageType == "boutique.checkout.Deadline.v1" {
			// The due member remains discoverable until its stored results are
			// published and the fencing lease is completed.
			deadlineMode = "keep"
		}
		response, err := store.client.Eval(context.Background(), commitOrderScript, keys,
			inputKey, expected, nextVersion, sagaJSON, acceptedJSON, journal,
			store.retention.Milliseconds(), deadlineMode, deadlineMillis, orderID, encodedDeadline).Result()
		if err != nil {
			if stateless.ClassifyRetry(err) == stateless.RetryDependency && attempt+1 < redisTransactionRetries {
				time.Sleep(stateless.Backoff(attempt, time.Millisecond, 100*time.Millisecond))
				continue
			}
			return transitionOutcome{}, err
		}
		values, ok := response.([]any)
		if !ok || len(values) != 3 {
			return transitionOutcome{}, fmt.Errorf("unexpected checkout commit response %T", response)
		}
		status, err := redisInt64(values[0])
		if err != nil {
			return transitionOutcome{}, err
		}
		version, err := redisInt64(values[1])
		if err != nil {
			return transitionOutcome{}, err
		}
		stored, err := redisBytes(values[2])
		if err != nil {
			return transitionOutcome{}, err
		}
		if status == 2 {
			store.conflicts.Add(1)
			if attempt+1 == redisTransactionRetries {
				return transitionOutcome{}, fmt.Errorf("%w after %d attempts", stateless.ErrConflict, redisTransactionRetries)
			}
			time.Sleep(stateless.Backoff(attempt, time.Millisecond, 50*time.Millisecond))
			continue
		}
		results := []resultMessage{}
		if len(stored) > 0 {
			if err := decodeRedisJSON(stored, &results); err != nil {
				return transitionOutcome{}, fmt.Errorf("decode checkout result journal: %w", err)
			}
		}
		return transitionOutcome{Results: results, Duplicate: status == 1, Version: uint64(version)}, nil
	}
	return transitionOutcome{}, fmt.Errorf("%w: retry limit exceeded", stateless.ErrConflict)
}

func newDeadlineRecord(saga *orderSaga) deadlineRecord {
	deadline := saga.Deadline.UTC()
	return deadlineRecord{
		OrderID: saga.OrderID, Version: saga.Version, Deadline: deadline,
		WorkID: stableID("checkout-deadline", saga.OrderID, strconv.FormatUint(saga.Version, 10),
			strconv.FormatInt(deadline.UnixMilli(), 10)),
	}
}

func (store *stateStore) loadOrderWorkspace(orderID string, at time.Time, base *persistedState) (*persistedState, uint64, *acceptedOrderRecord, error) {
	state := newPersistedState(at)
	if base != nil {
		state.CatalogRevision = base.CatalogRevision
		state.Rates = cloneRates(base.Rates)
		for key, value := range base.Carts {
			state.Carts[key] = cloneCart(value)
		}
		for key, value := range base.Products {
			state.Products[key] = cloneProduct(value)
		}
		for key, value := range base.RemovedProducts {
			state.RemovedProducts[key] = value
		}
	}
	orderBase := store.orderBase(orderID)
	values, err := store.client.MGet(
		context.Background(),
		orderBase+":version",
		orderBase+":saga",
		orderBase+":accepted",
	).Result()
	if err != nil {
		return nil, 0, nil, err
	}
	if len(values) != 3 {
		return nil, 0, nil, fmt.Errorf("unexpected checkout workspace response length %d", len(values))
	}
	var version uint64
	if values[0] != nil {
		parsed, parseErr := redisInt64(values[0])
		if parseErr != nil || parsed < 0 {
			if parseErr != nil {
				return nil, 0, nil, parseErr
			}
			return nil, 0, nil, fmt.Errorf("invalid negative checkout version %d", parsed)
		}
		version = uint64(parsed)
	}
	encoded, err := redisBytes(values[1])
	if err != nil {
		return nil, 0, nil, err
	}
	if len(encoded) > 0 {
		saga := &orderSaga{}
		if err := decodeRedisJSON(encoded, saga); err != nil {
			return nil, 0, nil, fmt.Errorf("decode checkout saga: %w", err)
		}
		state.Orders[orderID] = saga
	}
	var accepted *acceptedOrderRecord
	encoded, err = redisBytes(values[2])
	if err != nil {
		return nil, 0, nil, err
	}
	if len(encoded) > 0 {
		accepted = &acceptedOrderRecord{}
		if err := decodeRedisJSON(encoded, accepted); err != nil {
			return nil, 0, nil, fmt.Errorf("decode accepted order: %w", err)
		}
		state.Rates = cloneRates(accepted.Rates)
		state.CatalogRevision = accepted.CatalogRevision
		if accepted.Cart != nil {
			state.Carts[accepted.Cart.UserId] = cloneCart(accepted.Cart)
		}
		for _, product := range accepted.Products {
			state.Products[product.ProductId] = cloneProduct(product)
		}
	}
	return state, version, accepted, nil
}

func acceptedFromState(orderID string, state *persistedState, saga *orderSaga) *acceptedOrderRecord {
	record := &acceptedOrderRecord{
		OrderID: orderID, Rates: cloneRates(state.Rates), CatalogRevision: saga.CatalogRevision,
		RateRevision: saga.RateRevision,
	}
	if cart := state.Carts[saga.UserID]; cart != nil {
		record.Cart = cloneCart(cart)
		for _, line := range cart.Items {
			if product := state.Products[line.ProductId]; product != nil {
				record.Products = append(record.Products, cloneProduct(product))
			}
		}
	}
	if saga.Snapshot != nil {
		record.Order = proto.Clone(saga.Snapshot).(*commonv1.SanitizedOrderSnapshot)
	}
	return record
}

func cloneCart(value *commonv1.CartSnapshot) *commonv1.CartSnapshot {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*commonv1.CartSnapshot)
}

func cloneProduct(value *commonv1.ProductSnapshot) *commonv1.ProductSnapshot {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*commonv1.ProductSnapshot)
}

func cloneRates(value *eventsv1.CurrencyRatesUpdatedEvent) *eventsv1.CurrencyRatesUpdatedEvent {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*eventsv1.CurrencyRatesUpdatedEvent)
}

const applyProjectionScript = `
local current = tonumber(redis.call("GET", KEYS[2]) or "0")
local incoming = tonumber(ARGV[1])
if incoming <= current then return 0 end
redis.call("SET", KEYS[1], ARGV[2])
redis.call("SET", KEYS[2], incoming)
return 1
`

type productProjection struct {
	Product *commonv1.ProductSnapshot `json:"product,omitempty"`
	Removed bool                      `json:"removed,omitempty"`
}

func (store *stateStore) applyProjectionValue(base string, version uint64, value any) error {
	encoded, err := encodeRedisJSON(value)
	if err != nil {
		return err
	}
	_, err = store.client.Eval(context.Background(), applyProjectionScript,
		[]string{base + ":value", base + ":version"}, version, encoded).Result()
	return err
}

func (store *stateStore) ApplyProjection(subject string, envelope *commonv1.MessageEnvelope) error {
	switch subject {
	case "boutique.evt.catalog.product-upserted.v1":
		payload := &eventsv1.CatalogProductUpsertedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if payload.Product != nil {
			if err := store.applyProjectionValue(store.projectionBase("product", payload.Product.ProductId),
				payload.Product.ProductVersion, productProjection{Product: payload.Product}); err != nil {
				return err
			}
		}
		return store.applyProjectionValue(store.projectionMarker("catalog"), payload.CatalogRevision,
			payload.CatalogRevision)
	case "boutique.evt.catalog.product-removed.v1":
		payload := &eventsv1.CatalogProductRemovedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if err := store.applyProjectionValue(store.projectionBase("product", payload.ProductId),
			payload.ProductVersion, productProjection{Removed: true}); err != nil {
			return err
		}
		return store.applyProjectionValue(store.projectionMarker("catalog"), payload.CatalogRevision,
			payload.CatalogRevision)
	case "boutique.evt.catalog.snapshot-completed.v1":
		payload := &eventsv1.CatalogSnapshotCompletedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return store.applyProjectionValue(store.projectionMarker("catalog"), payload.CatalogRevision,
			payload.CatalogRevision)
	case "boutique.evt.currency.rates-updated.v1":
		payload := &eventsv1.CurrencyRatesUpdatedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		return store.applyProjectionValue(store.projectionMarker("rates"), payload.RateRevision, payload)
	case "boutique.evt.cart.item-added.v1":
		payload := &eventsv1.CartItemAddedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if payload.Cart == nil {
			return nil
		}
		return store.applyProjectionValue(store.projectionBase("cart", payload.Cart.UserId),
			payload.Cart.CartVersion, payload.Cart)
	case "boutique.evt.cart.cleared.v1":
		payload := &eventsv1.CartClearedEvent{}
		if err := envelope.Data.UnmarshalTo(payload); err != nil {
			return err
		}
		if payload.Cart == nil {
			return nil
		}
		return store.applyProjectionValue(store.projectionBase("cart", payload.Cart.UserId),
			payload.Cart.CartVersion, payload.Cart)
	default:
		return nil
	}
}

func (store *stateStore) LoadOrderProjections(command *commandsv1.OrderSubmitCommand, at time.Time) (*persistedState, error) {
	state := newPersistedState(at)
	rates := &eventsv1.CurrencyRatesUpdatedEvent{}
	cart := &commonv1.CartSnapshot{}
	if err := store.loadJSONBatch([]redisJSONLoad{
		{base: store.projectionMarker("catalog"), target: &state.CatalogRevision},
		{base: store.projectionMarker("rates"), target: rates},
		{base: store.projectionBase("cart", command.UserId), target: cart},
	}); err != nil {
		return nil, err
	}
	if rates.RateRevision > 0 {
		state.Rates = rates
	}
	if cart.UserId != "" {
		state.Carts[command.UserId] = cart
		products := make(map[string]*productProjection, len(cart.Items))
		loads := make([]redisJSONLoad, 0, len(cart.Items))
		for _, line := range cart.Items {
			if _, exists := products[line.ProductId]; exists {
				continue
			}
			projection := &productProjection{}
			products[line.ProductId] = projection
			loads = append(loads, redisJSONLoad{
				base: store.projectionBase("product", line.ProductId), target: projection,
			})
		}
		if err := store.loadJSONBatch(loads); err != nil {
			return nil, err
		}
		for productID, projection := range products {
			if projection.Product != nil {
				state.Products[productID] = projection.Product
			}
			if projection.Removed {
				state.RemovedProducts[productID] = true
			}
		}
	}
	return state, nil
}

type redisJSONLoad struct {
	base   string
	target any
}

func (store *stateStore) loadJSONBatch(loads []redisJSONLoad) error {
	if len(loads) == 0 {
		return nil
	}
	commands := make([]*redis.StringCmd, len(loads))
	_, err := store.client.Pipelined(context.Background(), func(pipeline redis.Pipeliner) error {
		for index, load := range loads {
			commands[index] = pipeline.Get(context.Background(), load.base+":value")
		}
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	for index, command := range commands {
		value, commandErr := command.Bytes()
		if errors.Is(commandErr, redis.Nil) {
			continue
		}
		if commandErr != nil {
			return commandErr
		}
		if err := decodeRedisJSON(value, loads[index].target); err != nil {
			return fmt.Errorf("decode projection %s: %w", loads[index].base, err)
		}
	}
	return nil
}

func (store *stateStore) LoadOrder(orderID string) (*orderSaga, error) {
	state, _, _, err := store.loadOrderWorkspace(orderID, time.Unix(1, 0), nil)
	if err != nil {
		return nil, err
	}
	return state.Orders[orderID], nil
}

func (store *stateStore) LoadAcceptedOrder(orderID string) (*acceptedOrderRecord, error) {
	_, _, accepted, err := store.loadOrderWorkspace(orderID, time.Unix(1, 0), nil)
	return accepted, err
}

func (store *stateStore) DueDeadlines(now time.Time, limitPerShard int) ([]dueDeadline, error) {
	if limitPerShard <= 0 {
		limitPerShard = 16
	}
	result := make([]dueDeadline, 0)
	for shard := 0; shard < checkoutDeadlineShards; shard++ {
		values, err := store.client.ZRangeByScore(context.Background(), store.deadlineKey(shard), &redis.ZRangeBy{
			Min: "-inf", Max: strconv.FormatInt(now.UTC().UnixMilli(), 10), Offset: 0, Count: int64(limitPerShard),
		}).Result()
		if err != nil {
			return nil, err
		}
		for _, orderID := range values {
			result = append(result, dueDeadline{OrderID: orderID, Shard: shard})
		}
	}
	return result, nil
}

func (store *stateStore) LoadDeadline(orderID string) (*deadlineRecord, []byte, error) {
	value, err := store.client.Get(context.Background(), store.orderBase(orderID)+":deadline").Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	record := &deadlineRecord{}
	if err := decodeRedisJSON(value, record); err != nil {
		return nil, nil, fmt.Errorf("decode checkout deadline: %w", err)
	}
	return record, value, nil
}

const completeDeadlineScript = `
local current = redis.call("GET", KEYS[2])
if current and current == ARGV[2] then
  redis.call("ZREM", KEYS[1], ARGV[1])
  redis.call("DEL", KEYS[2])
  return 1
end
return 0
`

func (store *stateStore) CompleteDeadline(orderID string, expected []byte) error {
	_, err := store.client.Eval(context.Background(), completeDeadlineScript,
		[]string{store.deadlineKey(deadlineShard(orderID)), store.orderBase(orderID) + ":deadline"},
		orderID, expected).Result()
	return err
}

const removeOrphanDeadlineScript = `
if redis.call("EXISTS", KEYS[2]) == 0 then
  return redis.call("ZREM", KEYS[1], ARGV[1])
end
return 0
`

func (store *stateStore) RemoveOrphanDeadline(orderID string) error {
	_, err := store.client.Eval(context.Background(), removeOrphanDeadlineScript,
		[]string{store.deadlineKey(deadlineShard(orderID)), store.orderBase(orderID) + ":deadline"},
		orderID).Result()
	return err
}

func (store *stateStore) Ready() bool {
	if store == nil || store.closed.Load() {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return store.client.Ping(ctx).Err() == nil
}

func (store *stateStore) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed.Swap(true) {
		return nil
	}
	return store.client.Close()
}

func redisInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	case string:
		return strconv.ParseInt(typed, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected Redis integer %T", value)
	}
}

func redisBytes(value any) ([]byte, error) {
	switch typed := value.(type) {
	case []byte:
		return typed, nil
	case string:
		return []byte(typed), nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected Redis bytes %T", value)
	}
}
