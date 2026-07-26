// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package stateless

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	ErrRevisionNotFound = errors.New("revisioned value not found")
	ErrRevisionConflict = errors.New("revisioned value conflict")
)

var kvLeasePrefixPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$`)

// RevisionedValue and RevisionBucket make the lease state machine reusable and
// independently testable. NATSRevisionBucket supplies the JetStream KV adapter.
type RevisionedValue struct {
	Value    []byte
	Revision uint64
}

type RevisionBucket interface {
	Get(key string) (RevisionedValue, error)
	Create(key string, value []byte) (uint64, error)
	Update(key string, value []byte, revision uint64) (uint64, error)
}

type NATSRevisionBucket struct {
	bucket nats.KeyValue
}

func NewNATSRevisionBucket(bucket nats.KeyValue) (*NATSRevisionBucket, error) {
	if bucket == nil {
		return nil, errors.New("NATS KV bucket is required")
	}
	return &NATSRevisionBucket{bucket: bucket}, nil
}

func (bucket *NATSRevisionBucket) Get(key string) (RevisionedValue, error) {
	entry, err := bucket.bucket.Get(key)
	if errors.Is(err, nats.ErrKeyNotFound) || errors.Is(err, nats.ErrKeyDeleted) {
		return RevisionedValue{}, ErrRevisionNotFound
	}
	if err != nil {
		return RevisionedValue{}, err
	}
	return RevisionedValue{Value: entry.Value(), Revision: entry.Revision()}, nil
}

func (bucket *NATSRevisionBucket) Create(key string, value []byte) (uint64, error) {
	revision, err := bucket.bucket.Create(key, value)
	if errors.Is(err, nats.ErrKeyExists) {
		return 0, ErrRevisionConflict
	}
	return revision, err
}

func (bucket *NATSRevisionBucket) Update(key string, value []byte, revision uint64) (uint64, error) {
	next, err := bucket.bucket.Update(key, value, revision)
	if errors.Is(err, nats.ErrKeyExists) ||
		(err != nil && strings.Contains(strings.ToLower(err.Error()), "wrong last sequence")) {
		return 0, ErrRevisionConflict
	}
	return next, err
}

type leaseRecord struct {
	Owner       string `json:"owner"`
	Token       uint64 `json:"token"`
	Attempts    uint64 `json:"attempts"`
	LeaseUntil  int64  `json:"lease_until_unix_ms"`
	Completed   bool   `json:"completed"`
	CompletedAt int64  `json:"completed_at_unix_ms,omitempty"`
}

// KVLeaseStore uses JetStream KV CAS for catalog/currency bootstrap claims.
// The JSON record is intentionally language-neutral for the Node implementation.
type KVLeaseStore struct {
	bucket RevisionBucket
	prefix string
}

func NewKVLeaseStore(bucket RevisionBucket, prefix string) (*KVLeaseStore, error) {
	if bucket == nil {
		return nil, errors.New("revision bucket is required")
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), ".")
	if !kvLeasePrefixPattern.MatchString(prefix) {
		return nil, fmt.Errorf("invalid KV lease key prefix %q", prefix)
	}
	return &KVLeaseStore{bucket: bucket, prefix: prefix}, nil
}

func (store *KVLeaseStore) key(workID string) (string, error) {
	if strings.TrimSpace(workID) == "" {
		return "", errors.New("work ID is required")
	}
	return store.prefix + "." + digestHex(workID), nil
}

func (store *KVLeaseStore) Acquire(
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
	nowMillis := now.UTC().UnixMilli()
	for attempt := 0; attempt < 32; attempt++ {
		current, getErr := store.bucket.Get(key)
		if errors.Is(getErr, ErrRevisionNotFound) {
			record := leaseRecord{
				Owner:      workerID,
				Token:      1,
				Attempts:   1,
				LeaseUntil: nowMillis + duration.Milliseconds(),
			}
			payload, marshalErr := json.Marshal(record)
			if marshalErr != nil {
				return Lease{}, marshalErr
			}
			if _, createErr := store.bucket.Create(key, payload); errors.Is(createErr, ErrRevisionConflict) {
				continue
			} else if createErr != nil {
				return Lease{}, createErr
			}
			return record.lease(workID), nil
		}
		if getErr != nil {
			return Lease{}, getErr
		}
		record, decodeErr := decodeLeaseRecord(current.Value)
		if decodeErr != nil {
			return Lease{}, decodeErr
		}
		if record.Completed {
			return record.lease(workID), ErrLeaseComplete
		}
		if record.LeaseUntil > nowMillis {
			lease := record.lease(workID)
			return Lease{}, &LeaseHeldError{Owner: lease.WorkerID, LeaseUntil: lease.LeaseUntil}
		}
		record.Owner = workerID
		record.Token++
		record.Attempts++
		record.LeaseUntil = nowMillis + duration.Milliseconds()
		payload, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			return Lease{}, marshalErr
		}
		if _, updateErr := store.bucket.Update(key, payload, current.Revision); errors.Is(updateErr, ErrRevisionConflict) {
			continue
		} else if updateErr != nil {
			return Lease{}, updateErr
		}
		return record.lease(workID), nil
	}
	return Lease{}, fmt.Errorf("%w: KV lease acquisition exceeded retry limit", ErrConflict)
}

func (store *KVLeaseStore) Renew(
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
	nowMillis := now.UTC().UnixMilli()
	for attempt := 0; attempt < 32; attempt++ {
		current, getErr := store.bucket.Get(key)
		if errors.Is(getErr, ErrRevisionNotFound) {
			return Lease{}, ErrLeaseLost
		}
		if getErr != nil {
			return Lease{}, getErr
		}
		record, decodeErr := decodeLeaseRecord(current.Value)
		if decodeErr != nil {
			return Lease{}, decodeErr
		}
		if record.Completed {
			return Lease{}, ErrLeaseComplete
		}
		if record.Owner != lease.WorkerID || record.Token != lease.Token ||
			record.LeaseUntil <= nowMillis {
			return Lease{}, ErrLeaseLost
		}
		record.LeaseUntil = nowMillis + duration.Milliseconds()
		payload, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			return Lease{}, marshalErr
		}
		if _, updateErr := store.bucket.Update(key, payload, current.Revision); errors.Is(updateErr, ErrRevisionConflict) {
			continue
		} else if updateErr != nil {
			return Lease{}, updateErr
		}
		return record.lease(lease.WorkID), nil
	}
	return Lease{}, fmt.Errorf("%w: KV lease renewal exceeded retry limit", ErrConflict)
}

func (store *KVLeaseStore) Complete(lease Lease, now time.Time) error {
	if lease.Token == 0 || now.IsZero() {
		return errors.New("valid lease and completion time are required")
	}
	key, err := store.key(lease.WorkID)
	if err != nil {
		return err
	}
	nowMillis := now.UTC().UnixMilli()
	for attempt := 0; attempt < 32; attempt++ {
		current, getErr := store.bucket.Get(key)
		if errors.Is(getErr, ErrRevisionNotFound) {
			return ErrLeaseLost
		}
		if getErr != nil {
			return getErr
		}
		record, decodeErr := decodeLeaseRecord(current.Value)
		if decodeErr != nil {
			return decodeErr
		}
		if record.Completed {
			return ErrLeaseComplete
		}
		if record.Owner != lease.WorkerID || record.Token != lease.Token ||
			record.LeaseUntil <= nowMillis {
			return ErrLeaseLost
		}
		record.Completed = true
		record.CompletedAt = nowMillis
		record.LeaseUntil = 0
		payload, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			return marshalErr
		}
		if _, updateErr := store.bucket.Update(key, payload, current.Revision); errors.Is(updateErr, ErrRevisionConflict) {
			continue
		} else if updateErr != nil {
			return updateErr
		}
		return nil
	}
	return fmt.Errorf("%w: KV lease completion exceeded retry limit", ErrConflict)
}

func decodeLeaseRecord(value []byte) (leaseRecord, error) {
	var record leaseRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return leaseRecord{}, fmt.Errorf("decode KV lease: %w", err)
	}
	if record.Token == 0 || record.Attempts == 0 {
		return leaseRecord{}, errors.New("KV lease record is incomplete")
	}
	return record, nil
}

func (record leaseRecord) lease(workID string) Lease {
	lease := Lease{
		WorkID:    workID,
		WorkerID:  record.Owner,
		Token:     record.Token,
		Attempts:  record.Attempts,
		Completed: record.Completed,
	}
	if record.LeaseUntil > 0 {
		lease.LeaseUntil = time.UnixMilli(record.LeaseUntil).UTC()
	}
	return lease
}
