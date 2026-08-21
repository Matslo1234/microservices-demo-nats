// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

type fakeBroker struct {
	mu            sync.Mutex
	messages      map[string]map[uint64]rawMessage
	last          map[string]rawMessage
	next          map[string]uint64
	dedupe        map[string]publishResult
	publishErrors map[string]error
	published     []*nats.Msg
	deleted       []string
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{
		messages: map[string]map[uint64]rawMessage{}, last: map[string]rawMessage{},
		next:   map[string]uint64{dlqStream: 0, "BOUTIQUE_EVENTS": 100, "BOUTIQUE_COMMANDS": 100},
		dedupe: map[string]publishResult{}, publishErrors: map[string]error{},
	}
}

func (broker *fakeBroker) add(stream string, sequence uint64, subject string, header nats.Header, data []byte) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.messages[stream] == nil {
		broker.messages[stream] = map[uint64]rawMessage{}
	}
	broker.messages[stream][sequence] = rawMessage{Subject: subject, Sequence: sequence, Header: header, Data: append([]byte(nil), data...)}
}

func (broker *fakeBroker) GetMessage(_ context.Context, stream string, sequence uint64) (rawMessage, error) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	message, ok := broker.messages[stream][sequence]
	if !ok {
		return rawMessage{}, nats.ErrMsgNotFound
	}
	return message, nil
}

func (broker *fakeBroker) GetLastMessage(_ context.Context, stream, subject string) (rawMessage, error) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	message, ok := broker.last[stream+"\x00"+subject]
	if !ok {
		return rawMessage{}, nats.ErrMsgNotFound
	}
	return message, nil
}

func cloneNATSMessage(message *nats.Msg) *nats.Msg {
	return &nats.Msg{Subject: message.Subject, Header: nats.Header(cloneHeaders(message.Header)), Data: append([]byte(nil), message.Data...)}
}

func (broker *fakeBroker) Publish(_ context.Context, message *nats.Msg) (publishResult, error) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if err := broker.publishErrors[message.Subject]; err != nil {
		return publishResult{}, err
	}
	stream := message.Header.Get(nats.ExpectedStreamHdr)
	dedupeKey := stream + "\x00" + message.Header.Get(nats.MsgIdHdr)
	if existing, ok := broker.dedupe[dedupeKey]; ok && message.Header.Get(nats.MsgIdHdr) != "" {
		existing.Duplicate = true
		return existing, nil
	}
	broker.next[stream]++
	sequence := broker.next[stream]
	stored := rawMessage{Subject: message.Subject, Sequence: sequence, Header: nats.Header(cloneHeaders(message.Header)), Data: append([]byte(nil), message.Data...)}
	if broker.messages[stream] == nil {
		broker.messages[stream] = map[uint64]rawMessage{}
	}
	broker.messages[stream][sequence] = stored
	broker.last[stream+"\x00"+message.Subject] = stored
	broker.published = append(broker.published, cloneNATSMessage(message))
	result := publishResult{Stream: stream, Sequence: sequence}
	if message.Header.Get(nats.MsgIdHdr) != "" {
		broker.dedupe[dedupeKey] = result
	}
	return result, nil
}

func (broker *fakeBroker) DeleteMessage(_ context.Context, stream string, sequence uint64) error {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if _, ok := broker.messages[stream][sequence]; !ok {
		return nats.ErrMsgNotFound
	}
	delete(broker.messages[stream], sequence)
	broker.deleted = append(broker.deleted, stream)
	return nil
}

type fakeCases struct {
	mu       sync.Mutex
	values   map[string]storedCase
	revision uint64
}

func newFakeCases() *fakeCases { return &fakeCases{values: map[string]storedCase{}} }

func (repository *fakeCases) Create(_ context.Context, value deadLetterCase) (storedCase, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if existing, ok := repository.values[value.ID]; ok {
		return existing, nil
	}
	repository.revision++
	stored := storedCase{Case: value, Revision: repository.revision}
	repository.values[value.ID] = stored
	return stored, nil
}

func (repository *fakeCases) Get(_ context.Context, id string) (storedCase, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	value, ok := repository.values[id]
	if !ok {
		return storedCase{}, nats.ErrKeyNotFound
	}
	return value, nil
}

func (repository *fakeCases) Update(_ context.Context, value deadLetterCase, revision uint64) (storedCase, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	existing, ok := repository.values[value.ID]
	if !ok {
		return storedCase{}, nats.ErrKeyNotFound
	}
	if existing.Revision != revision {
		return storedCase{}, errCaseConflict
	}
	repository.revision++
	stored := storedCase{Case: value, Revision: repository.revision}
	repository.values[value.ID] = stored
	return stored, nil
}

func (repository *fakeCases) List(_ context.Context) ([]deadLetterCase, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	values := make([]deadLetterCase, 0, len(repository.values))
	for _, value := range repository.values {
		values = append(values, value.Case)
	}
	return values, nil
}

func testOperations(broker *fakeBroker, cases *fakeCases) *operationsService {
	service := newOperationsService(broker, cases, slog.New(slog.NewTextHandler(io.Discard, nil)))
	service.now = func() time.Time { return time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC) }
	return service
}

func advisoryJSON(t *testing.T, stream, consumer string, sequence uint64) []byte {
	t.Helper()
	data, err := json.Marshal(maxDeliverAdvisory{
		Type: "io.nats.jetstream.advisory.v1.max_deliver", ID: "advisory-1",
		Timestamp: time.Now(), Stream: stream, Consumer: consumer,
		StreamSeq: sequence, Deliveries: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func envelopeBytes(t *testing.T) []byte {
	t.Helper()
	data, err := proto.Marshal(&commonv1.MessageEnvelope{
		MessageId: "message-1", MessageType: "boutique.order.Completed.v1",
		CorrelationId: "correlation-1", SchemaVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestEventTransferPersistsExactPayloadWithoutDeletingSource(t *testing.T) {
	broker, cases := newFakeBroker(), newFakeCases()
	originalHeader := nats.Header{"Content-Type": []string{"application/protobuf"}, nats.MsgIdHdr: []string{"message-1"}}
	originalData := envelopeBytes(t)
	broker.add("BOUTIQUE_EVENTS", 42, "boutique.evt.order.completed.v1", originalHeader, originalData)
	service := testOperations(broker, cases)

	value, err := service.HandleAdvisory(context.Background(), advisoryJSON(t, "BOUTIQUE_EVENTS", "email-order-completed-v1", 42))
	if err != nil {
		t.Fatal(err)
	}
	if value.Status != statusOpen || !value.ReplayAvailable || value.MessageID != "message-1" || value.CorrelationID != "correlation-1" {
		t.Fatalf("unexpected case: %+v", value)
	}
	if len(broker.deleted) != 0 {
		t.Fatal("limits-retention event was deleted during dead-letter transfer")
	}
	if len(broker.published) != 2 || broker.published[0].Header.Get(nats.ExpectedStreamHdr) != dlqStream ||
		broker.published[1].Subject != "boutique.evt.ops.message-dead-lettered.v1" {
		t.Fatalf("unexpected DLQ publications: %+v", broker.published)
	}
	var record deadLetterRecord
	if err := json.Unmarshal(broker.published[0].Data, &record); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&commonv1.MessageEnvelope{MessageId: "message-1", MessageType: "boutique.order.Completed.v1", CorrelationId: "correlation-1", SchemaVersion: 1}, mustEnvelope(t, record.OriginalData)) {
		t.Fatal("DLQ record did not preserve exact source payload")
	}
	if record.OriginalHeaders["Content-Type"][0] != "application/protobuf" {
		t.Fatal("DLQ record did not preserve source headers")
	}
}

func mustEnvelope(t *testing.T, data []byte) *commonv1.MessageEnvelope {
	t.Helper()
	var envelope commonv1.MessageEnvelope
	if err := proto.Unmarshal(data, &envelope); err != nil {
		t.Fatal(err)
	}
	return &envelope
}

func TestCommandTransferCopiesBeforeDeletingAndIsIdempotent(t *testing.T) {
	broker, cases := newFakeBroker(), newFakeCases()
	broker.add("BOUTIQUE_COMMANDS", 7, "boutique.cmd.payment.capture.v1", nats.Header{}, envelopeBytes(t))
	service := testOperations(broker, cases)
	advisory := advisoryJSON(t, "BOUTIQUE_COMMANDS", "payment-commands-v1", 7)

	first, err := service.HandleAdvisory(context.Background(), advisory)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.HandleAdvisory(context.Background(), advisory)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || len(broker.published) != 2 || len(broker.deleted) != 1 {
		t.Fatalf("transfer was not idempotent: first=%+v second=%+v published=%d deleted=%d", first, second, len(broker.published), len(broker.deleted))
	}
}

func TestTransferIsNotReplayableUntilOperationalEventAndCleanupComplete(t *testing.T) {
	broker, cases := newFakeBroker(), newFakeCases()
	broker.add("BOUTIQUE_COMMANDS", 7, "boutique.cmd.payment.capture.v1", nats.Header{}, envelopeBytes(t))
	broker.publishErrors["boutique.evt.ops.message-dead-lettered.v1"] = errors.New("events unavailable")
	service := testOperations(broker, cases)
	advisory := advisoryJSON(t, "BOUTIQUE_COMMANDS", "payment-commands-v1", 7)

	if _, err := service.HandleAdvisory(context.Background(), advisory); err == nil {
		t.Fatal("expected operational event failure")
	}
	caseID := deadLetterCaseID("BOUTIQUE_COMMANDS", "payment-commands-v1", 7)
	stored, err := cases.Get(context.Background(), caseID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Case.Status != statusTransferInProgress || stored.Case.ReplayAvailable || len(broker.deleted) != 0 {
		t.Fatalf("partially transferred case became actionable: %+v deleted=%v", stored.Case, broker.deleted)
	}
	if _, err := service.Replay(context.Background(), caseID, "admin", "premature"); !errors.Is(err, errCaseNotOpen) {
		t.Fatalf("got %v, want case-state conflict", err)
	}

	delete(broker.publishErrors, "boutique.evt.ops.message-dead-lettered.v1")
	completed, err := service.HandleAdvisory(context.Background(), advisory)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != statusOpen || !completed.ReplayAvailable || len(broker.deleted) != 1 {
		t.Fatalf("transfer did not complete on retry: %+v deleted=%v", completed, broker.deleted)
	}
}

func TestMissingSourceCreatesVisibleNonReplayableCase(t *testing.T) {
	broker, cases := newFakeBroker(), newFakeCases()
	service := testOperations(broker, cases)
	value, err := service.HandleAdvisory(context.Background(), advisoryJSON(t, "BOUTIQUE_EVENTS", "projection-v1", 99))
	if err != nil {
		t.Fatal(err)
	}
	if value.ReplayAvailable || value.Status != statusOpen {
		t.Fatalf("unexpected source-missing case: %+v", value)
	}
	if _, err := service.Replay(context.Background(), value.ID, "admin", "fixed"); !errors.Is(err, errReplayMissing) {
		t.Fatalf("got %v, want unavailable-payload error", err)
	}
}

func TestReplayPreservesBusinessIdentityAndUsesNewTransportIdentity(t *testing.T) {
	broker, cases := newFakeBroker(), newFakeCases()
	original := envelopeBytes(t)
	broker.add("BOUTIQUE_EVENTS", 42, "boutique.evt.order.completed.v1", nats.Header{
		nats.MsgIdHdr: []string{"message-1"}, nats.ExpectedStreamHdr: []string{"wrong"}, "Traceparent": []string{"trace"},
	}, original)
	service := testOperations(broker, cases)
	value, err := service.HandleAdvisory(context.Background(), advisoryJSON(t, "BOUTIQUE_EVENTS", "email-order-completed-v1", 42))
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := service.Replay(context.Background(), value.ID, "admin", "email provider recovered")
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Status != statusReplayPublished || len(replayed.Replays) != 1 || replayed.Replays[0].SourceSequence == 0 {
		t.Fatalf("unexpected replay state: %+v", replayed)
	}
	message := broker.published[len(broker.published)-1]
	if !proto.Equal(mustEnvelope(t, original), mustEnvelope(t, message.Data)) {
		t.Fatal("replay changed the business envelope")
	}
	if message.Header.Get(nats.MsgIdHdr) == "message-1" || message.Header.Get(headerReplayCaseID) != value.ID ||
		message.Header.Get(headerReplayOriginalConsumer) != value.Consumer || message.Header.Get(nats.ExpectedStreamHdr) != value.SourceStream ||
		message.Header.Get("Traceparent") != "trace" {
		t.Fatalf("unexpected replay headers: %+v", message.Header)
	}
	if _, err := service.Replay(context.Background(), value.ID, "admin", "duplicate click"); !errors.Is(err, errCaseNotOpen) {
		t.Fatalf("got %v, want case-state conflict", err)
	}
}

func TestStaleReplayLockResumesTheSameAttempt(t *testing.T) {
	broker, cases := newFakeBroker(), newFakeCases()
	broker.add("BOUTIQUE_EVENTS", 42, "boutique.evt.order.completed.v1", nats.Header{}, envelopeBytes(t))
	service := testOperations(broker, cases)
	value, err := service.HandleAdvisory(context.Background(), advisoryJSON(t, "BOUTIQUE_EVENTS", "email-order-completed-v1", 42))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := cases.Get(context.Background(), value.ID)
	if err != nil {
		t.Fatal(err)
	}
	requestedAt := service.now().Add(-2 * replayLockTimeout)
	attempt := replayAttempt{ID: replayID(value.ID, 1), Number: 1, Actor: "first-admin", Reason: "first request", RequestedAt: requestedAt, Status: "PUBLISHING"}
	stored.Case.Status = statusReplayInProgress
	stored.Case.UpdatedAt = requestedAt
	stored.Case.Replays = []replayAttempt{attempt}
	if _, err := cases.Update(context.Background(), stored.Case, stored.Revision); err != nil {
		t.Fatal(err)
	}

	replayed, err := service.Replay(context.Background(), value.ID, "second-admin", "recover stale replay")
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Status != statusReplayPublished || len(replayed.Replays) != 1 || replayed.Replays[0].ID != attempt.ID || replayed.Replays[0].Actor != "first-admin" {
		t.Fatalf("stale replay was not resumed safely: %+v", replayed)
	}
}

func TestInvalidAdvisoryIsRejected(t *testing.T) {
	service := testOperations(newFakeBroker(), newFakeCases())
	if _, err := service.HandleAdvisory(context.Background(), []byte(`{"type":"unexpected"}`)); !errors.Is(err, errInvalidAdvisory) {
		t.Fatalf("got %v, want invalid advisory", err)
	}
}
