// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	"github.com/redis/go-redis/v9"
)

const (
	redisStateSchemaVersion = 1
	redisTransactionRetries = 128
	defaultRedisStatePrefix = "checkout:v1"
)

var errStateStoreClosed = errors.New("checkout state store is closed")

type orderFingerprint struct {
	operationID           string
	commandID             string
	userID                string
	email                 string
	address               *commonv1.PostalAddress
	currencyCode          string
	paymentToken          string
	cartVersion           uint64
	catalogRevision       uint64
	rateRevision          uint64
	version               uint64
	stage                 string
	deadline              time.Time
	snapshot              *commonv1.SanitizedOrderSnapshot
	authorizationID       string
	shipmentID            string
	trackingID            string
	cancelReason          *commonv1.Failure
	authorizationReleased bool
	shipmentCancelled     bool
	needRelease           bool
	needShipmentCancel    bool
}

type stateFingerprint struct {
	catalogRevision uint64
	rates           *eventsv1.CurrencyRatesUpdatedEvent
	products        map[string]*commonv1.ProductSnapshot
	carts           map[string]*commonv1.CartSnapshot
	orders          map[string]orderFingerprint
	inbox           map[string]time.Time
	outbox          map[string]outboxMessage
}

// stateStore contains only a Redis client and key configuration. All domain
// state is loaded from Redis for each operation, so checkout pods can be
// replaced or share work without relying on pod-local state.
type stateStore struct {
	client *redis.Client
	prefix string

	lifecycleMu sync.RWMutex
	closed      bool
	closeErr    error
}

func openStateStoreWithPrefix(address, prefix string) (*stateStore, error) {
	if strings.TrimSpace(address) == "" {
		return nil, errors.New("CHECKOUT_REDIS_ADDR is required")
	}
	if strings.TrimSpace(prefix) == "" {
		prefix = defaultRedisStatePrefix
	}
	options := &redis.Options{Addr: address}
	if strings.Contains(address, "://") {
		parsed, err := redis.ParseURL(address)
		if err != nil {
			return nil, fmt.Errorf("parse checkout Redis URL: %w", err)
		}
		options = parsed
	}
	client := redis.NewClient(options)
	store := &stateStore{client: client, prefix: strings.TrimSuffix(prefix, ":")}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect checkout state store: %w", err)
	}
	if err := store.initialize(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return store, nil
}

func (store *stateStore) initialize(ctx context.Context) error {
	_, err := store.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSetNX(ctx, store.key("metadata"), "schema_version", redisStateSchemaVersion)
		pipe.SetNX(ctx, store.key("revision"), 0, 0)
		return nil
	})
	if err != nil {
		return fmt.Errorf("initialize checkout state store: %w", err)
	}
	version, err := store.client.HGet(ctx, store.key("metadata"), "schema_version").Int()
	if err != nil {
		return fmt.Errorf("read checkout state schema: %w", err)
	}
	if version != redisStateSchemaVersion {
		return fmt.Errorf("unsupported checkout state schema %d", version)
	}
	return nil
}

func (store *stateStore) key(name string) string {
	return store.prefix + ":" + name
}

func (store *stateStore) load(ctx context.Context, commands redis.Cmdable) (*persistedState, error) {
	var (
		metadataCommand *redis.MapStringStringCmd
		productsCommand *redis.MapStringStringCmd
		cartsCommand    *redis.MapStringStringCmd
		ordersCommand   *redis.MapStringStringCmd
		inboxCommand    *redis.MapStringStringCmd
		outboxCommand   *redis.MapStringStringCmd
	)
	_, err := commands.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		metadataCommand = pipe.HGetAll(ctx, store.key("metadata"))
		productsCommand = pipe.HGetAll(ctx, store.key("products"))
		cartsCommand = pipe.HGetAll(ctx, store.key("carts"))
		ordersCommand = pipe.HGetAll(ctx, store.key("orders"))
		inboxCommand = pipe.HGetAll(ctx, store.key("inbox"))
		outboxCommand = pipe.HGetAll(ctx, store.key("outbox"))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load checkout state: %w", err)
	}

	metadata, err := metadataCommand.Result()
	if err != nil {
		return nil, err
	}
	state := newPersistedState()
	if encodedRevision := metadata["catalog_revision"]; encodedRevision != "" {
		state.CatalogRevision, err = strconv.ParseUint(encodedRevision, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode catalog revision: %w", err)
		}
	}
	if encodedRates := metadata["rates"]; encodedRates != "" {
		state.Rates = &eventsv1.CurrencyRatesUpdatedEvent{}
		if err := json.Unmarshal([]byte(encodedRates), state.Rates); err != nil {
			return nil, fmt.Errorf("decode currency rates: %w", err)
		}
	}
	if err := decodeJSONHash(productsCommand, func(key string, value []byte) error {
		product := &commonv1.ProductSnapshot{}
		if err := json.Unmarshal(value, product); err != nil {
			return err
		}
		state.Products[key] = product
		return nil
	}); err != nil {
		return nil, fmt.Errorf("decode checkout products: %w", err)
	}
	if err := decodeJSONHash(cartsCommand, func(key string, value []byte) error {
		cart := &commonv1.CartSnapshot{}
		if err := json.Unmarshal(value, cart); err != nil {
			return err
		}
		state.Carts[key] = cart
		return nil
	}); err != nil {
		return nil, fmt.Errorf("decode checkout carts: %w", err)
	}
	if err := decodeJSONHash(ordersCommand, func(key string, value []byte) error {
		order := &orderSaga{}
		if err := json.Unmarshal(value, order); err != nil {
			return err
		}
		state.Orders[key] = order
		return nil
	}); err != nil {
		return nil, fmt.Errorf("decode checkout orders: %w", err)
	}
	inbox, err := inboxCommand.Result()
	if err != nil {
		return nil, err
	}
	for messageID, encodedTime := range inbox {
		receivedAt, parseErr := time.Parse(time.RFC3339Nano, encodedTime)
		if parseErr != nil {
			return nil, fmt.Errorf("decode checkout inbox entry %q: %w", messageID, parseErr)
		}
		state.Inbox[messageID] = receivedAt
	}
	if err := decodeJSONHash(outboxCommand, func(key string, value []byte) error {
		message := outboxMessage{}
		if err := json.Unmarshal(value, &message); err != nil {
			return err
		}
		state.Outbox[key] = message
		return nil
	}); err != nil {
		return nil, fmt.Errorf("decode checkout outbox: %w", err)
	}
	return state, nil
}

func decodeJSONHash(command *redis.MapStringStringCmd, decode func(string, []byte) error) error {
	values, err := command.Result()
	if err != nil {
		return err
	}
	for key, value := range values {
		if err := decode(key, []byte(value)); err != nil {
			return err
		}
	}
	return nil
}

func (store *stateStore) Update(update func(*persistedState) error) error {
	return store.update(func(state *persistedState) (bool, error) {
		return true, update(state)
	})
}

func (store *stateStore) UpdateIfChanged(update func(*persistedState) (bool, error)) error {
	return store.update(update)
}

// UpdateTracked retains the saga store API used by message handlers. Redis
// transactions calculate the actual changed keys, so callers cannot
// accidentally lose a mutation by omitting a tracking helper.
func (store *stateStore) UpdateTracked(update func(*persistedState) error) error {
	return store.Update(update)
}

func (store *stateStore) update(update func(*persistedState) (bool, error)) error {
	store.lifecycleMu.RLock()
	defer store.lifecycleMu.RUnlock()
	if store.closed {
		return errStateStoreClosed
	}

	ctx := context.Background()
	for attempt := 0; attempt < redisTransactionRetries; attempt++ {
		err := store.client.Watch(ctx, func(transaction *redis.Tx) error {
			state, err := store.load(ctx, transaction)
			if err != nil {
				return err
			}
			before := fingerprint(state)
			changed, err := update(state)
			if err != nil {
				return err
			}
			if !changed || !stateChanged(before, state) {
				return nil
			}
			_, err = transaction.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				if err := store.writeChanges(ctx, pipe, before, state); err != nil {
					return err
				}
				pipe.Incr(ctx, store.key("revision"))
				return nil
			})
			return err
		}, store.key("revision"))
		if !errors.Is(err, redis.TxFailedErr) {
			return err
		}
		time.Sleep(time.Duration(min(attempt+1, 10)) * time.Millisecond)
	}
	return errors.New("checkout state transaction retry limit exceeded")
}

func (store *stateStore) writeChanges(ctx context.Context, pipe redis.Pipeliner,
	before stateFingerprint, after *persistedState) error {
	metadataKey := store.key("metadata")
	if before.catalogRevision != after.CatalogRevision {
		pipe.HSet(ctx, metadataKey, "catalog_revision", strconv.FormatUint(after.CatalogRevision, 10))
	}
	if before.rates != after.Rates {
		if after.Rates == nil {
			pipe.HDel(ctx, metadataKey, "rates")
		} else if err := setJSONHashValue(ctx, pipe, metadataKey, "rates", after.Rates); err != nil {
			return err
		}
	}
	if err := writePointerMapChanges(ctx, pipe, store.key("products"), before.products, after.Products); err != nil {
		return err
	}
	if err := writePointerMapChanges(ctx, pipe, store.key("carts"), before.carts, after.Carts); err != nil {
		return err
	}
	ordersKey := store.key("orders")
	for key, order := range after.Orders {
		if previous, ok := before.orders[key]; !ok || previous != fingerprintOrder(order) {
			if err := setJSONHashValue(ctx, pipe, ordersKey, key, order); err != nil {
				return err
			}
		}
	}
	deleteMissingHashValues(ctx, pipe, ordersKey, before.orders, after.Orders)

	inboxKey := store.key("inbox")
	for key, receivedAt := range after.Inbox {
		if previous, ok := before.inbox[key]; ok && previous.Equal(receivedAt) {
			continue
		}
		pipe.HSet(ctx, inboxKey, key, receivedAt.UTC().Format(time.RFC3339Nano))
	}
	deleteMissingHashValues(ctx, pipe, inboxKey, before.inbox, after.Inbox)

	outboxKey := store.key("outbox")
	for key, message := range after.Outbox {
		if previous, ok := before.outbox[key]; !ok || !outboxMessagesEqual(previous, message) {
			if err := setJSONHashValue(ctx, pipe, outboxKey, key, message); err != nil {
				return err
			}
		}
	}
	deleteMissingHashValues(ctx, pipe, outboxKey, before.outbox, after.Outbox)
	return nil
}

func setJSONHashValue(ctx context.Context, pipe redis.Pipeliner, hash, key string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	pipe.HSet(ctx, hash, key, encoded)
	return nil
}

func writePointerMapChanges[T any](ctx context.Context, pipe redis.Pipeliner, hash string,
	before, after map[string]*T) error {
	for key, value := range after {
		if previous, ok := before[key]; !ok || previous != value {
			if err := setJSONHashValue(ctx, pipe, hash, key, value); err != nil {
				return err
			}
		}
	}
	deleteMissingHashValues(ctx, pipe, hash, before, after)
	return nil
}

func deleteMissingHashValues[Before any, After any](ctx context.Context, pipe redis.Pipeliner,
	hash string, before map[string]Before, after map[string]After) {
	for key := range before {
		if _, ok := after[key]; !ok {
			pipe.HDel(ctx, hash, key)
		}
	}
}

func fingerprint(state *persistedState) stateFingerprint {
	value := stateFingerprint{
		catalogRevision: state.CatalogRevision,
		rates:           state.Rates,
		products:        make(map[string]*commonv1.ProductSnapshot, len(state.Products)),
		carts:           make(map[string]*commonv1.CartSnapshot, len(state.Carts)),
		orders:          make(map[string]orderFingerprint, len(state.Orders)),
		inbox:           make(map[string]time.Time, len(state.Inbox)),
		outbox:          make(map[string]outboxMessage, len(state.Outbox)),
	}
	for key, product := range state.Products {
		value.products[key] = product
	}
	for key, cart := range state.Carts {
		value.carts[key] = cart
	}
	for key, order := range state.Orders {
		value.orders[key] = fingerprintOrder(order)
	}
	for key, receivedAt := range state.Inbox {
		value.inbox[key] = receivedAt
	}
	for key, message := range state.Outbox {
		value.outbox[key] = message
	}
	return value
}

func fingerprintOrder(order *orderSaga) orderFingerprint {
	if order == nil {
		return orderFingerprint{}
	}
	return orderFingerprint{
		operationID: order.OperationID, commandID: order.CommandID, userID: order.UserID,
		email: order.Email, address: order.Address, currencyCode: order.CurrencyCode,
		paymentToken: order.PaymentToken, cartVersion: order.CartVersion,
		catalogRevision: order.CatalogRevision, rateRevision: order.RateRevision,
		version: order.Version, stage: order.Stage, deadline: order.Deadline,
		snapshot: order.Snapshot, authorizationID: order.AuthorizationID,
		shipmentID: order.ShipmentID, trackingID: order.TrackingID,
		cancelReason: order.CancelReason, authorizationReleased: order.AuthorizationReleased,
		shipmentCancelled: order.ShipmentCancelled, needRelease: order.NeedRelease,
		needShipmentCancel: order.NeedShipmentCancel,
	}
}

func stateChanged(before stateFingerprint, after *persistedState) bool {
	if before.catalogRevision != after.CatalogRevision || before.rates != after.Rates ||
		len(before.products) != len(after.Products) || len(before.carts) != len(after.Carts) ||
		len(before.orders) != len(after.Orders) || len(before.inbox) != len(after.Inbox) ||
		len(before.outbox) != len(after.Outbox) {
		return true
	}
	for key, value := range after.Products {
		if before.products[key] != value {
			return true
		}
	}
	for key, value := range after.Carts {
		if before.carts[key] != value {
			return true
		}
	}
	for key, value := range after.Orders {
		if before.orders[key] != fingerprintOrder(value) {
			return true
		}
	}
	for key, value := range after.Inbox {
		if previous, ok := before.inbox[key]; !ok || !previous.Equal(value) {
			return true
		}
	}
	for key, value := range after.Outbox {
		if previous, ok := before.outbox[key]; !ok || !outboxMessagesEqual(previous, value) {
			return true
		}
	}
	return false
}

func outboxMessagesEqual(left, right outboxMessage) bool {
	return left.MessageID == right.MessageID &&
		left.Subject == right.Subject &&
		bytes.Equal(left.Data, right.Data)
}

func (store *stateStore) View(view func(*persistedState) error) error {
	store.lifecycleMu.RLock()
	defer store.lifecycleMu.RUnlock()
	if store.closed {
		return errStateStoreClosed
	}
	state, err := store.load(context.Background(), store.client)
	if err != nil {
		return err
	}
	return view(state)
}

func (store *stateStore) Snapshot() (*persistedState, error) {
	store.lifecycleMu.RLock()
	defer store.lifecycleMu.RUnlock()
	if store.closed {
		return nil, errStateStoreClosed
	}
	return store.load(context.Background(), store.client)
}

func (store *stateStore) Outbox() []outboxMessage {
	store.lifecycleMu.RLock()
	defer store.lifecycleMu.RUnlock()
	if store.closed {
		log.WithError(errStateStoreClosed).Error("load checkout outbox failed")
		return nil
	}
	values, err := store.client.HGetAll(context.Background(), store.key("outbox")).Result()
	if err != nil {
		log.WithError(err).Error("load checkout outbox failed")
		return nil
	}
	messages := make([]outboxMessage, 0, len(values))
	for messageID, encoded := range values {
		message := outboxMessage{}
		if err := json.Unmarshal([]byte(encoded), &message); err != nil {
			log.WithError(err).WithField("message_id", messageID).Error("decode checkout outbox entry failed")
			return nil
		}
		messages = append(messages, message)
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].MessageID < messages[j].MessageID })
	return messages
}

func (store *stateStore) RemoveOutboxBatch(messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	return store.Update(func(state *persistedState) error {
		for _, messageID := range messageIDs {
			state.deleteOutbox(messageID)
		}
		return nil
	})
}

func (store *stateStore) Ready() bool {
	store.lifecycleMu.RLock()
	defer store.lifecycleMu.RUnlock()
	if store.closed {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if store.client.Ping(ctx).Err() != nil {
		return false
	}
	return true
}

func (store *stateStore) Close() error {
	store.lifecycleMu.Lock()
	defer store.lifecycleMu.Unlock()
	if store.closed {
		return store.closeErr
	}
	store.closed = true
	store.closeErr = store.client.Close()
	return store.closeErr
}
