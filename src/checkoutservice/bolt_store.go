// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"bytes"
	"encoding/binary"
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
	bolt "go.etcd.io/bbolt"
)

const stateSchemaVersion = 1

const (
	trackedUpdateBatchDelay = 2 * time.Millisecond
	trackedUpdateBatchSize  = 64
)

var (
	metadataBucket = []byte("metadata")
	productsBucket = []byte("products")
	cartsBucket    = []byte("carts")
	ordersBucket   = []byte("orders")
	inboxBucket    = []byte("inbox")
	outboxBucket   = []byte("outbox")

	schemaVersionKey   = []byte("schema_version")
	catalogRevisionKey = []byte("catalog_revision")
	ratesKey           = []byte("rates")
)

var errStateStoreClosed = errors.New("checkout state store is closed")

type trackedUpdateRequest struct {
	update func(*persistedState) error
	result chan error
}

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

type stateStore struct {
	mu                  sync.Mutex
	lifecycleMu         sync.RWMutex
	path                string
	dbPath              string
	db                  *bolt.DB
	state               *persistedState
	trackedUpdates      chan trackedUpdateRequest
	stopTrackedUpdates  chan struct{}
	trackedUpdatesDone  chan struct{}
	closed              bool
	closeErr            error
	trackedBatchDelay   time.Duration
	trackedBatchSize    int
	trackedBatchCommits uint64
}

func openStateStore(path string) (*stateStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create checkout store directory: %w", err)
	}
	dbPath := path + ".bolt"
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open checkout state database: %w", err)
	}
	store := &stateStore{
		path:               path,
		dbPath:             dbPath,
		db:                 db,
		trackedUpdates:     make(chan trackedUpdateRequest, trackedUpdateBatchSize),
		stopTrackedUpdates: make(chan struct{}),
		trackedUpdatesDone: make(chan struct{}),
		trackedBatchDelay:  trackedUpdateBatchDelay,
		trackedBatchSize:   trackedUpdateBatchSize,
	}
	if err := store.initializeOrLoad(); err != nil {
		_ = db.Close()
		return nil, err
	}
	go store.runTrackedUpdateBatcher()
	return store, nil
}

func loadLegacyState(path string) (*persistedState, bool, error) {
	encoded, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return newPersistedState(), false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read legacy checkout state: %w", err)
	}
	state := newPersistedState()
	if err := json.Unmarshal(encoded, state); err != nil {
		return nil, false, fmt.Errorf("decode legacy checkout state: %w", err)
	}
	state.normalize()
	return state, true, nil
}

func (store *stateStore) initializeOrLoad() error {
	initialized := false
	if err := store.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(metadataBucket)
		if bucket == nil {
			return nil
		}
		version := bucket.Get(schemaVersionKey)
		if version == nil {
			return nil
		}
		if len(version) != 8 || binary.BigEndian.Uint64(version) != stateSchemaVersion {
			return fmt.Errorf("unsupported checkout state schema")
		}
		initialized = true
		return nil
	}); err != nil {
		return err
	}
	if initialized {
		state, err := store.load()
		if err != nil {
			return err
		}
		store.state = state
		return nil
	}

	legacy, legacyExists, err := loadLegacyState(store.path)
	if err != nil {
		return err
	}
	initial := newPersistedState()
	if legacyExists {
		initial = legacy
	}
	if err := store.db.Update(func(tx *bolt.Tx) error {
		if err := createStateBuckets(tx); err != nil {
			return err
		}
		if err := writeCompleteState(tx, initial); err != nil {
			return err
		}
		var encodedVersion [8]byte
		binary.BigEndian.PutUint64(encodedVersion[:], stateSchemaVersion)
		return tx.Bucket(metadataBucket).Put(schemaVersionKey, encodedVersion[:])
	}); err != nil {
		return fmt.Errorf("initialize checkout state database: %w", err)
	}
	store.state = initial
	return nil
}

func createStateBuckets(tx *bolt.Tx) error {
	for _, name := range [][]byte{
		metadataBucket,
		productsBucket,
		cartsBucket,
		ordersBucket,
		inboxBucket,
		outboxBucket,
	} {
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	return nil
}

func writeCompleteState(tx *bolt.Tx, state *persistedState) error {
	if err := putUint64(tx.Bucket(metadataBucket), catalogRevisionKey, state.CatalogRevision); err != nil {
		return err
	}
	if state.Rates != nil {
		if err := putJSON(tx.Bucket(metadataBucket), ratesKey, state.Rates); err != nil {
			return err
		}
	}
	for key, value := range state.Products {
		if err := putJSON(tx.Bucket(productsBucket), []byte(key), value); err != nil {
			return err
		}
	}
	for key, value := range state.Carts {
		if err := putJSON(tx.Bucket(cartsBucket), []byte(key), value); err != nil {
			return err
		}
	}
	for key, value := range state.Orders {
		if err := putJSON(tx.Bucket(ordersBucket), []byte(key), value); err != nil {
			return err
		}
	}
	for key, value := range state.Inbox {
		encoded, err := value.MarshalBinary()
		if err != nil {
			return err
		}
		if err := tx.Bucket(inboxBucket).Put([]byte(key), encoded); err != nil {
			return err
		}
	}
	for key, value := range state.Outbox {
		if err := putJSON(tx.Bucket(outboxBucket), []byte(key), value); err != nil {
			return err
		}
	}
	return nil
}

func putUint64(bucket *bolt.Bucket, key []byte, value uint64) error {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return bucket.Put(key, encoded[:])
}

func putJSON(bucket *bolt.Bucket, key []byte, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put(key, encoded)
}

func (store *stateStore) load() (*persistedState, error) {
	state := newPersistedState()
	err := store.db.View(func(tx *bolt.Tx) error {
		metadata := tx.Bucket(metadataBucket)
		if metadata == nil {
			return errors.New("checkout metadata bucket is missing")
		}
		if value := metadata.Get(catalogRevisionKey); len(value) == 8 {
			state.CatalogRevision = binary.BigEndian.Uint64(value)
		}
		if value := metadata.Get(ratesKey); value != nil {
			rates := &eventsv1.CurrencyRatesUpdatedEvent{}
			if err := json.Unmarshal(value, rates); err != nil {
				return err
			}
			state.Rates = rates
		}
		if err := loadJSONBucket(tx.Bucket(productsBucket), func(key string, value []byte) error {
			product := &commonv1.ProductSnapshot{}
			if err := json.Unmarshal(value, product); err != nil {
				return err
			}
			state.Products[key] = product
			return nil
		}); err != nil {
			return err
		}
		if err := loadJSONBucket(tx.Bucket(cartsBucket), func(key string, value []byte) error {
			cart := &commonv1.CartSnapshot{}
			if err := json.Unmarshal(value, cart); err != nil {
				return err
			}
			state.Carts[key] = cart
			return nil
		}); err != nil {
			return err
		}
		if err := loadJSONBucket(tx.Bucket(ordersBucket), func(key string, value []byte) error {
			order := &orderSaga{}
			if err := json.Unmarshal(value, order); err != nil {
				return err
			}
			state.Orders[key] = order
			return nil
		}); err != nil {
			return err
		}
		inbox := tx.Bucket(inboxBucket)
		if inbox == nil {
			return errors.New("checkout inbox bucket is missing")
		}
		if err := inbox.ForEach(func(key, value []byte) error {
			var receivedAt time.Time
			if err := receivedAt.UnmarshalBinary(value); err != nil {
				return err
			}
			state.Inbox[string(key)] = receivedAt
			return nil
		}); err != nil {
			return err
		}
		return loadJSONBucket(tx.Bucket(outboxBucket), func(key string, value []byte) error {
			message := outboxMessage{}
			if err := json.Unmarshal(value, &message); err != nil {
				return err
			}
			state.Outbox[key] = message
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("load checkout state database: %w", err)
	}
	state.normalize()
	return state, nil
}

func loadJSONBucket(bucket *bolt.Bucket, load func(string, []byte) error) error {
	if bucket == nil {
		return errors.New("checkout state bucket is missing")
	}
	return bucket.ForEach(func(key, value []byte) error {
		return load(string(key), value)
	})
}

func (store *stateStore) Update(update func(*persistedState) error) error {
	store.lifecycleMu.RLock()
	defer store.lifecycleMu.RUnlock()
	if store.closed {
		return errStateStoreClosed
	}
	return store.update(func(state *persistedState) (bool, error) {
		return true, update(state)
	})
}

func (store *stateStore) UpdateIfChanged(update func(*persistedState) (bool, error)) error {
	store.lifecycleMu.RLock()
	defer store.lifecycleMu.RUnlock()
	if store.closed {
		return errStateStoreClosed
	}
	return store.update(update)
}

// UpdateTracked persists only keys explicitly changed through persistedState's
// mutation helpers. Production message handlers use this path so update cost
// does not grow with retained order and inbox history. Update remains available
// for migrations and tests that directly mutate state.
func (store *stateStore) UpdateTracked(update func(*persistedState) error) error {
	store.lifecycleMu.RLock()
	defer store.lifecycleMu.RUnlock()
	if store.closed {
		return errStateStoreClosed
	}

	request := trackedUpdateRequest{
		update: update,
		result: make(chan error, 1),
	}
	store.trackedUpdates <- request
	return <-request.result
}

func (store *stateStore) runTrackedUpdateBatcher() {
	defer close(store.trackedUpdatesDone)
	for {
		request, ok := store.nextTrackedUpdate()
		if !ok {
			return
		}
		batch := []trackedUpdateRequest{request}
		timer := time.NewTimer(store.trackedBatchDelay)
	collect:
		for len(batch) < store.trackedBatchSize {
			select {
			case request := <-store.trackedUpdates:
				batch = append(batch, request)
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		store.commitTrackedUpdateBatch(batch)
	}
}

func (store *stateStore) nextTrackedUpdate() (trackedUpdateRequest, bool) {
	select {
	case request := <-store.trackedUpdates:
		return request, true
	case <-store.stopTrackedUpdates:
		select {
		case request := <-store.trackedUpdates:
			return request, true
		default:
			return trackedUpdateRequest{}, false
		}
	}
}

func (store *stateStore) commitTrackedUpdateBatch(batch []trackedUpdateRequest) {
	store.mu.Lock()
	changes := newStateChanges()
	store.state.changes = changes
	var batchErr error
	for _, request := range batch {
		if err := request.update(store.state); err != nil {
			batchErr = err
			break
		}
	}
	store.state.changes = nil
	if batchErr != nil {
		batchErr = store.restoreAfter(batchErr)
	} else if !changes.empty() {
		if err := store.db.Update(func(tx *bolt.Tx) error {
			return writeTrackedStateChanges(tx, changes, store.state)
		}); err != nil {
			batchErr = store.restoreAfter(fmt.Errorf("persist tracked checkout state changes: %w", err))
		} else {
			store.trackedBatchCommits++
		}
	}
	store.mu.Unlock()

	for _, request := range batch {
		request.result <- batchErr
	}
}

func (store *stateStore) update(update func(*persistedState) (bool, error)) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	before := fingerprint(store.state)
	changed, err := update(store.state)
	if err != nil {
		return store.restoreAfter(err)
	}
	if !changed || !stateChanged(before, store.state) {
		return nil
	}
	if err := store.db.Update(func(tx *bolt.Tx) error {
		return writeStateChanges(tx, before, store.state)
	}); err != nil {
		return store.restoreAfter(fmt.Errorf("persist checkout state changes: %w", err))
	}
	return nil
}

func writeTrackedStateChanges(tx *bolt.Tx, changes *stateChanges, state *persistedState) error {
	if changes.metadata {
		metadata := tx.Bucket(metadataBucket)
		if err := putUint64(metadata, catalogRevisionKey, state.CatalogRevision); err != nil {
			return err
		}
		if state.Rates == nil {
			if err := metadata.Delete(ratesKey); err != nil {
				return err
			}
		} else if err := putJSON(metadata, ratesKey, state.Rates); err != nil {
			return err
		}
	}
	if err := writeTrackedJSONChanges(tx.Bucket(productsBucket), changes.products, state.Products); err != nil {
		return err
	}
	if err := writeTrackedJSONChanges(tx.Bucket(cartsBucket), changes.carts, state.Carts); err != nil {
		return err
	}
	if err := writeTrackedJSONChanges(tx.Bucket(ordersBucket), changes.orders, state.Orders); err != nil {
		return err
	}
	inbox := tx.Bucket(inboxBucket)
	for key := range changes.inbox {
		receivedAt, ok := state.Inbox[key]
		if !ok {
			if err := inbox.Delete([]byte(key)); err != nil {
				return err
			}
			continue
		}
		encoded, err := receivedAt.MarshalBinary()
		if err != nil {
			return err
		}
		if err := inbox.Put([]byte(key), encoded); err != nil {
			return err
		}
	}
	return writeTrackedJSONChanges(tx.Bucket(outboxBucket), changes.outbox, state.Outbox)
}

func writeTrackedJSONChanges[T any](bucket *bolt.Bucket, keys map[string]struct{}, values map[string]T) error {
	for key := range keys {
		value, ok := values[key]
		if !ok {
			if err := bucket.Delete([]byte(key)); err != nil {
				return err
			}
			continue
		}
		if err := putJSON(bucket, []byte(key), value); err != nil {
			return err
		}
	}
	return nil
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
		operationID:           order.OperationID,
		commandID:             order.CommandID,
		userID:                order.UserID,
		email:                 order.Email,
		address:               order.Address,
		currencyCode:          order.CurrencyCode,
		paymentToken:          order.PaymentToken,
		cartVersion:           order.CartVersion,
		catalogRevision:       order.CatalogRevision,
		rateRevision:          order.RateRevision,
		version:               order.Version,
		stage:                 order.Stage,
		deadline:              order.Deadline,
		snapshot:              order.Snapshot,
		authorizationID:       order.AuthorizationID,
		shipmentID:            order.ShipmentID,
		trackingID:            order.TrackingID,
		cancelReason:          order.CancelReason,
		authorizationReleased: order.AuthorizationReleased,
		shipmentCancelled:     order.ShipmentCancelled,
		needRelease:           order.NeedRelease,
		needShipmentCancel:    order.NeedShipmentCancel,
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

func writeStateChanges(tx *bolt.Tx, before stateFingerprint, after *persistedState) error {
	if before.catalogRevision != after.CatalogRevision {
		if err := putUint64(tx.Bucket(metadataBucket), catalogRevisionKey, after.CatalogRevision); err != nil {
			return err
		}
	}
	if before.rates != after.Rates {
		if after.Rates == nil {
			if err := tx.Bucket(metadataBucket).Delete(ratesKey); err != nil {
				return err
			}
		} else if err := putJSON(tx.Bucket(metadataBucket), ratesKey, after.Rates); err != nil {
			return err
		}
	}
	if err := writePointerMapChanges(tx.Bucket(productsBucket), before.products, after.Products); err != nil {
		return err
	}
	if err := writePointerMapChanges(tx.Bucket(cartsBucket), before.carts, after.Carts); err != nil {
		return err
	}
	orders := tx.Bucket(ordersBucket)
	for key, order := range after.Orders {
		if previous, ok := before.orders[key]; !ok || previous != fingerprintOrder(order) {
			if err := putJSON(orders, []byte(key), order); err != nil {
				return err
			}
		}
	}
	for key := range before.orders {
		if _, ok := after.Orders[key]; !ok {
			if err := orders.Delete([]byte(key)); err != nil {
				return err
			}
		}
	}
	inbox := tx.Bucket(inboxBucket)
	for key, receivedAt := range after.Inbox {
		if previous, ok := before.inbox[key]; ok && previous.Equal(receivedAt) {
			continue
		}
		encoded, err := receivedAt.MarshalBinary()
		if err != nil {
			return err
		}
		if err := inbox.Put([]byte(key), encoded); err != nil {
			return err
		}
	}
	for key := range before.inbox {
		if _, ok := after.Inbox[key]; !ok {
			if err := inbox.Delete([]byte(key)); err != nil {
				return err
			}
		}
	}
	outbox := tx.Bucket(outboxBucket)
	for key, message := range after.Outbox {
		if previous, ok := before.outbox[key]; !ok || !outboxMessagesEqual(previous, message) {
			if err := putJSON(outbox, []byte(key), message); err != nil {
				return err
			}
		}
	}
	for key := range before.outbox {
		if _, ok := after.Outbox[key]; !ok {
			if err := outbox.Delete([]byte(key)); err != nil {
				return err
			}
		}
	}
	return nil
}

func writePointerMapChanges[T any](bucket *bolt.Bucket, before, after map[string]*T) error {
	for key, value := range after {
		if previous, ok := before[key]; !ok || previous != value {
			if err := putJSON(bucket, []byte(key), value); err != nil {
				return err
			}
		}
	}
	for key := range before {
		if _, ok := after[key]; !ok {
			if err := bucket.Delete([]byte(key)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (store *stateStore) restoreAfter(cause error) error {
	state, err := store.load()
	if err != nil {
		return fmt.Errorf("%w (also failed to restore checkout state: %v)", cause, err)
	}
	store.state = state
	return cause
}

func (store *stateStore) View(view func(*persistedState) error) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	return view(store.state)
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

func (store *stateStore) RemoveOutboxBatch(messageIDs []string) error {
	if len(messageIDs) == 0 {
		return nil
	}
	return store.UpdateTracked(func(state *persistedState) error {
		for _, messageID := range messageIDs {
			if _, ok := state.Outbox[messageID]; ok {
				state.deleteOutbox(messageID)
			}
		}
		return nil
	})
}

func (store *stateStore) Close() error {
	store.lifecycleMu.Lock()
	if store.closed {
		err := store.closeErr
		store.lifecycleMu.Unlock()
		return err
	}
	store.closed = true
	close(store.stopTrackedUpdates)
	store.lifecycleMu.Unlock()

	<-store.trackedUpdatesDone
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return nil
	}
	store.closeErr = store.db.Close()
	store.db = nil
	return store.closeErr
}
