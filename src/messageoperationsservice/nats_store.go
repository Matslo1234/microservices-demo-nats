// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/nats-io/nats.go"
)

var errCaseConflict = errors.New("dead-letter case changed concurrently")

type rawMessage struct {
	Subject  string
	Sequence uint64
	Header   nats.Header
	Data     []byte
}

type publishResult struct {
	Stream    string
	Sequence  uint64
	Duplicate bool
}

type messageBroker interface {
	GetMessage(context.Context, string, uint64) (rawMessage, error)
	GetLastMessage(context.Context, string, string) (rawMessage, error)
	Publish(context.Context, *nats.Msg) (publishResult, error)
	DeleteMessage(context.Context, string, uint64) error
}

type caseRepository interface {
	Create(context.Context, deadLetterCase) (storedCase, error)
	Get(context.Context, string) (storedCase, error)
	Update(context.Context, deadLetterCase, uint64) (storedCase, error)
	List(context.Context) ([]deadLetterCase, error)
}

type natsMessageBroker struct {
	js nats.JetStreamContext
}

func (broker *natsMessageBroker) GetMessage(ctx context.Context, stream string, sequence uint64) (rawMessage, error) {
	message, err := broker.js.GetMsg(stream, sequence, nats.Context(ctx))
	if err != nil {
		return rawMessage{}, err
	}
	return rawMessage{Subject: message.Subject, Sequence: message.Sequence, Header: message.Header, Data: message.Data}, nil
}

func (broker *natsMessageBroker) GetLastMessage(ctx context.Context, stream, subject string) (rawMessage, error) {
	message, err := broker.js.GetLastMsg(stream, subject, nats.Context(ctx))
	if err != nil {
		return rawMessage{}, err
	}
	return rawMessage{Subject: message.Subject, Sequence: message.Sequence, Header: message.Header, Data: message.Data}, nil
}

func (broker *natsMessageBroker) Publish(ctx context.Context, message *nats.Msg) (publishResult, error) {
	ack, err := broker.js.PublishMsg(message, nats.Context(ctx))
	if err != nil {
		return publishResult{}, err
	}
	return publishResult{Stream: ack.Stream, Sequence: ack.Sequence, Duplicate: ack.Duplicate}, nil
}

func (broker *natsMessageBroker) DeleteMessage(ctx context.Context, stream string, sequence uint64) error {
	return broker.js.DeleteMsg(stream, sequence, nats.Context(ctx))
}

type kvCaseRepository struct {
	kv nats.KeyValue
}

func encodeCase(value deadLetterCase) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode dead-letter case: %w", err)
	}
	return encoded, nil
}

func decodeCase(data []byte) (deadLetterCase, error) {
	var value deadLetterCase
	if err := json.Unmarshal(data, &value); err != nil {
		return deadLetterCase{}, fmt.Errorf("decode dead-letter case: %w", err)
	}
	return value, nil
}

func (repository *kvCaseRepository) Create(_ context.Context, value deadLetterCase) (storedCase, error) {
	encoded, err := encodeCase(value)
	if err != nil {
		return storedCase{}, err
	}
	revision, err := repository.kv.Create(value.ID, encoded)
	if errors.Is(err, nats.ErrKeyExists) {
		return repository.Get(context.Background(), value.ID)
	}
	if err != nil {
		return storedCase{}, fmt.Errorf("create dead-letter case: %w", err)
	}
	return storedCase{Case: value, Revision: revision}, nil
}

func (repository *kvCaseRepository) Get(_ context.Context, id string) (storedCase, error) {
	entry, err := repository.kv.Get(id)
	if err != nil {
		return storedCase{}, err
	}
	value, err := decodeCase(entry.Value())
	if err != nil {
		return storedCase{}, err
	}
	return storedCase{Case: value, Revision: entry.Revision()}, nil
}

func (repository *kvCaseRepository) Update(_ context.Context, value deadLetterCase, revision uint64) (storedCase, error) {
	encoded, err := encodeCase(value)
	if err != nil {
		return storedCase{}, err
	}
	nextRevision, err := repository.kv.Update(value.ID, encoded, revision)
	if err != nil {
		if errors.Is(err, nats.ErrKeyExists) {
			return storedCase{}, errCaseConflict
		}
		return storedCase{}, fmt.Errorf("update dead-letter case: %w", err)
	}
	return storedCase{Case: value, Revision: nextRevision}, nil
}

func (repository *kvCaseRepository) List(ctx context.Context) ([]deadLetterCase, error) {
	keys, err := repository.kv.Keys(nats.Context(ctx))
	if errors.Is(err, nats.ErrNoKeysFound) {
		return []deadLetterCase{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list dead-letter cases: %w", err)
	}
	values := make([]deadLetterCase, 0, len(keys))
	for _, key := range keys {
		entry, getErr := repository.Get(ctx, key)
		if errors.Is(getErr, nats.ErrKeyNotFound) {
			continue
		}
		if getErr != nil {
			return nil, getErr
		}
		values = append(values, entry.Case)
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].DeadLetteredAt.After(values[j].DeadLetteredAt)
	})
	return values, nil
}
