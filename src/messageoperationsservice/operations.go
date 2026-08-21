// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	errInvalidAdvisory = errors.New("invalid max-delivery advisory")
	errCaseNotOpen     = errors.New("dead-letter case is not open for replay")
	errReplayMissing   = errors.New("dead-letter payload is unavailable")
)

const replayLockTimeout = time.Minute

type operationMetrics struct {
	transfers      atomic.Uint64
	transferErrors atomic.Uint64
	replays        atomic.Uint64
	replayErrors   atomic.Uint64
	resolved       atomic.Uint64
}

type operationsService struct {
	broker  messageBroker
	cases   caseRepository
	logger  *slog.Logger
	now     func() time.Time
	metrics operationMetrics
}

func newOperationsService(broker messageBroker, cases caseRepository, logger *slog.Logger) *operationsService {
	return &operationsService{broker: broker, cases: cases, logger: logger, now: func() time.Time { return time.Now().UTC() }}
}

func deadLetterCaseID(stream, consumer string, sequence uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("boutique.dlq.v1\x00%s\x00%s\x00%d", stream, consumer, sequence)))
	return base64.RawURLEncoding.EncodeToString(digest[:18])
}

func replayID(caseID string, number int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("boutique.replay.v1\x00%s\x00%d", caseID, number)))
	return base64.RawURLEncoding.EncodeToString(digest[:18])
}

func supportedSourceStream(stream string) bool {
	return stream == "BOUTIQUE_COMMANDS" || stream == "BOUTIQUE_EVENTS"
}

func decodeAdvisory(data []byte) (maxDeliverAdvisory, error) {
	var advisory maxDeliverAdvisory
	if err := json.Unmarshal(data, &advisory); err != nil {
		return advisory, fmt.Errorf("%w: %v", errInvalidAdvisory, err)
	}
	if advisory.Type != "io.nats.jetstream.advisory.v1.max_deliver" ||
		!supportedSourceStream(advisory.Stream) || advisory.Consumer == "" ||
		advisory.StreamSeq == 0 || advisory.Deliveries == 0 {
		return advisory, errInvalidAdvisory
	}
	return advisory, nil
}

func messageContext(data []byte) (messageID, messageType, correlationID string) {
	var envelope commonv1.MessageEnvelope
	if err := proto.Unmarshal(data, &envelope); err != nil {
		return "", "", ""
	}
	return envelope.MessageId, envelope.MessageType, envelope.CorrelationId
}

func cloneHeaders(header nats.Header) map[string][]string {
	if len(header) == 0 {
		return nil
	}
	cloned := make(map[string][]string, len(header))
	for key, values := range header {
		cloned[key] = append([]string(nil), values...)
	}
	return cloned
}

func (service *operationsService) HandleAdvisory(ctx context.Context, data []byte) (deadLetterCase, error) {
	advisory, err := decodeAdvisory(data)
	if err != nil {
		service.metrics.transferErrors.Add(1)
		return deadLetterCase{}, err
	}
	caseID := deadLetterCaseID(advisory.Stream, advisory.Consumer, advisory.StreamSeq)
	if existing, getErr := service.cases.Get(ctx, caseID); getErr == nil {
		if existing.Case.Status == statusTransferInProgress {
			completed, transitioned, completeErr := service.completeTransfer(ctx, existing)
			if completeErr != nil {
				service.metrics.transferErrors.Add(1)
				return deadLetterCase{}, completeErr
			}
			if transitioned {
				service.recordCompletedTransfer(completed.Case)
			}
			return completed.Case, nil
		}
		// This cleanup also makes upgrades safe for a case written by an older
		// controller that exposed OPEN before deleting its work-queue source.
		if advisory.Stream == "BOUTIQUE_COMMANDS" && existing.Case.ReplayAvailable {
			deleteErr := service.broker.DeleteMessage(ctx, advisory.Stream, advisory.StreamSeq)
			if deleteErr != nil && !errors.Is(deleteErr, nats.ErrMsgNotFound) {
				service.metrics.transferErrors.Add(1)
				return deadLetterCase{}, fmt.Errorf("delete transferred work-queue message: %w", deleteErr)
			}
		}
		if eventErr := service.publishDeadLetterEvent(ctx, existing.Case); eventErr != nil {
			service.metrics.transferErrors.Add(1)
			return deadLetterCase{}, eventErr
		}
		return existing.Case, nil
	} else if !errors.Is(getErr, nats.ErrKeyNotFound) {
		service.metrics.transferErrors.Add(1)
		return deadLetterCase{}, fmt.Errorf("look up dead-letter case: %w", getErr)
	}

	record := deadLetterRecord{
		Version: 1, CaseID: caseID, AdvisoryID: advisory.ID,
		SourceStream: advisory.Stream, SourceSequence: advisory.StreamSeq,
		Consumer: advisory.Consumer, Deliveries: advisory.Deliveries,
		SafeErrorCode: "MAX_DELIVERIES_EXCEEDED", DeadLetteredAt: service.now(),
	}
	original, getErr := service.broker.GetMessage(ctx, advisory.Stream, advisory.StreamSeq)
	if getErr != nil {
		if !errors.Is(getErr, nats.ErrMsgNotFound) {
			service.metrics.transferErrors.Add(1)
			return deadLetterCase{}, fmt.Errorf("retrieve source message: %w", getErr)
		}
		record.SourceFetchError = "SOURCE_MESSAGE_UNAVAILABLE"
	} else {
		record.SourceSubject = original.Subject
		record.OriginalData = append([]byte(nil), original.Data...)
		record.OriginalHeaders = cloneHeaders(original.Header)
		record.MessageID, record.MessageType, record.CorrelationID = messageContext(original.Data)
		record.ParentCaseID = original.Header.Get(headerReplayCaseID)
		record.ParentReplayID = original.Header.Get(headerReplayID)
	}

	dlqSubject := "boutique.dlq.case." + caseID
	dlqSequence, err := service.persistDeadLetterRecord(ctx, dlqSubject, record)
	if err != nil {
		service.metrics.transferErrors.Add(1)
		return deadLetterCase{}, err
	}
	value := deadLetterCase{
		Version: 1, ID: caseID, Status: statusTransferInProgress, AdvisoryID: advisory.ID,
		SourceStream: advisory.Stream, SourceSubject: record.SourceSubject,
		SourceSequence: advisory.StreamSeq, Consumer: advisory.Consumer,
		Deliveries: advisory.Deliveries, MessageID: record.MessageID,
		MessageType: record.MessageType, CorrelationID: record.CorrelationID,
		SafeErrorCode: record.SafeErrorCode, ReplayAvailable: false,
		DLQSubject: dlqSubject, DLQSequence: dlqSequence,
		DeadLetteredAt: record.DeadLetteredAt, UpdatedAt: record.DeadLetteredAt,
		ParentCaseID: record.ParentCaseID, ParentReplayID: record.ParentReplayID,
	}
	created, err := service.cases.Create(ctx, value)
	if err != nil {
		service.metrics.transferErrors.Add(1)
		return deadLetterCase{}, err
	}
	completed, transitioned, err := service.completeTransfer(ctx, created)
	if err != nil {
		service.metrics.transferErrors.Add(1)
		return deadLetterCase{}, err
	}
	if transitioned {
		service.recordCompletedTransfer(completed.Case)
	}
	return completed.Case, nil
}

func (service *operationsService) completeTransfer(ctx context.Context, stored storedCase) (storedCase, bool, error) {
	if stored.Case.Status != statusTransferInProgress {
		return stored, false, nil
	}
	if err := service.publishDeadLetterEvent(ctx, stored.Case); err != nil {
		return storedCase{}, false, err
	}
	if stored.Case.SourceStream == "BOUTIQUE_COMMANDS" && stored.Case.SourceSubject != "" {
		if err := service.broker.DeleteMessage(ctx, stored.Case.SourceStream, stored.Case.SourceSequence); err != nil && !errors.Is(err, nats.ErrMsgNotFound) {
			return storedCase{}, false, fmt.Errorf("delete transferred work-queue message: %w", err)
		}
	}
	if stored.Case.ParentCaseID != "" {
		if err := service.markParentReplayFailed(ctx, stored.Case.ParentCaseID, stored.Case.ParentReplayID, stored.Case.ID); err != nil {
			return storedCase{}, false, fmt.Errorf("record failed parent replay: %w", err)
		}
	}
	value := stored.Case
	value.Status = statusOpen
	value.ReplayAvailable = value.SourceSubject != ""
	value.UpdatedAt = service.now()
	updated, err := service.cases.Update(ctx, value, stored.Revision)
	if errors.Is(err, errCaseConflict) {
		latest, getErr := service.cases.Get(ctx, value.ID)
		if getErr == nil && latest.Case.Status != statusTransferInProgress {
			return latest, false, nil
		}
	}
	if err != nil {
		return storedCase{}, false, fmt.Errorf("complete dead-letter case: %w", err)
	}
	return updated, true, nil
}

func (service *operationsService) recordCompletedTransfer(value deadLetterCase) {
	service.metrics.transfers.Add(1)
	service.logger.Warn("message transferred to dead-letter stream",
		"case_id", value.ID, "source_stream", value.SourceStream,
		"source_sequence", value.SourceSequence, "consumer", value.Consumer,
		"deliveries", value.Deliveries, "correlation_id", value.CorrelationID)
}

func (service *operationsService) publishDeadLetterEvent(ctx context.Context, value deadLetterCase) error {
	payload := &eventsv1.OpsMessageDeadLetteredEvent{
		OriginalStream: value.SourceStream, OriginalSubject: value.SourceSubject,
		OriginalStreamSequence: value.SourceSequence, Consumer: value.Consumer,
		Attempts: uint32(value.Deliveries), ErrorCategory: value.SafeErrorCode,
		CorrelationId: value.CorrelationID,
	}
	wrapped, err := anypb.New(payload)
	if err != nil {
		return fmt.Errorf("wrap dead-letter event: %w", err)
	}
	messageID := "dlqevt_" + value.ID
	correlationID := value.CorrelationID
	if correlationID == "" {
		correlationID = value.ID
	}
	envelope := &commonv1.MessageEnvelope{
		MessageId: messageID, MessageType: "boutique.ops.MessageDeadLettered.v1",
		SchemaVersion: 1, OccurredAt: timestamppb.New(value.DeadLetteredAt),
		Producer: "messageoperationsservice/v1", AggregateType: "dead_letter",
		AggregateId: value.ID, AggregateVersion: 1, CorrelationId: correlationID,
		CausationId: value.MessageID, Data: wrapped,
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode dead-letter event: %w", err)
	}
	message := &nats.Msg{
		Subject: "boutique.evt.ops.message-dead-lettered.v1",
		Header:  nats.Header{}, Data: data,
	}
	message.Header.Set(nats.MsgIdHdr, messageID)
	message.Header.Set(nats.ExpectedStreamHdr, "BOUTIQUE_EVENTS")
	ack, err := service.broker.Publish(ctx, message)
	if err != nil {
		return fmt.Errorf("publish dead-letter operational event: %w", err)
	}
	if ack.Stream != "BOUTIQUE_EVENTS" {
		return fmt.Errorf("dead-letter event reached unexpected stream %q", ack.Stream)
	}
	return nil
}

func (service *operationsService) persistDeadLetterRecord(ctx context.Context, subject string, record deadLetterRecord) (uint64, error) {
	if existing, err := service.broker.GetLastMessage(ctx, dlqStream, subject); err == nil {
		return existing.Sequence, nil
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return 0, fmt.Errorf("encode dead-letter record: %w", err)
	}
	message := &nats.Msg{Subject: subject, Header: nats.Header{}, Data: encoded}
	message.Header.Set(nats.MsgIdHdr, "dlq/"+record.CaseID)
	message.Header.Set(nats.ExpectedStreamHdr, dlqStream)
	message.Header.Set(nats.ExpectedLastSubjSeqHdr, "0")
	ack, err := service.broker.Publish(ctx, message)
	if err != nil {
		// A concurrent controller may have won the expected-sequence race.
		if existing, getErr := service.broker.GetLastMessage(ctx, dlqStream, subject); getErr == nil {
			return existing.Sequence, nil
		}
		return 0, fmt.Errorf("publish dead-letter record: %w", err)
	}
	if ack.Stream != dlqStream {
		return 0, fmt.Errorf("dead-letter publish reached unexpected stream %q", ack.Stream)
	}
	return ack.Sequence, nil
}

func replayHeaders(original map[string][]string, value deadLetterCase, attempt replayAttempt) nats.Header {
	header := nats.Header{}
	for key, values := range original {
		lower := strings.ToLower(key)
		if lower == strings.ToLower(nats.MsgIdHdr) || strings.HasPrefix(lower, "nats-expected-") ||
			lower == "nats-stream" || lower == "nats-sequence" || lower == "nats-time-stamp" ||
			lower == "nats-last-sequence" || lower == "nats-num-pending" {
			continue
		}
		header[key] = append([]string(nil), values...)
	}
	header.Set(nats.MsgIdHdr, fmt.Sprintf("dlq-replay/%s/%d", value.ID, attempt.Number))
	header.Set(nats.ExpectedStreamHdr, value.SourceStream)
	header.Set(headerReplayID, attempt.ID)
	header.Set(headerReplayCaseID, value.ID)
	header.Set(headerReplayOriginalConsumer, value.Consumer)
	header.Set(headerOriginalStreamSeq, strconv.FormatUint(value.SourceSequence, 10))
	return header
}

func (service *operationsService) Replay(ctx context.Context, id, actor, reason string) (deadLetterCase, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > 500 {
		return deadLetterCase{}, errors.New("a replay reason between 1 and 500 characters is required")
	}
	stored, err := service.cases.Get(ctx, id)
	if err != nil {
		return deadLetterCase{}, err
	}
	if stored.Case.Status != statusOpen && stored.Case.Status != statusReplayFailed && stored.Case.Status != statusReplayInProgress {
		return deadLetterCase{}, errCaseNotOpen
	}
	if !stored.Case.ReplayAvailable {
		return deadLetterCase{}, errReplayMissing
	}
	now := service.now()
	var attempt replayAttempt
	switch stored.Case.Status {
	case statusOpen, statusReplayFailed:
		attempt = replayAttempt{
			Number: len(stored.Case.Replays) + 1, Actor: actor, Reason: reason,
			RequestedAt: now, Status: "PUBLISHING",
		}
		attempt.ID = replayID(id, attempt.Number)
		stored.Case.Replays = append(stored.Case.Replays, attempt)
	case statusReplayInProgress:
		if len(stored.Case.Replays) == 0 || now.Sub(stored.Case.UpdatedAt) < replayLockTimeout {
			return deadLetterCase{}, errCaseNotOpen
		}
		attempt = stored.Case.Replays[len(stored.Case.Replays)-1]
		if attempt.Status != "PUBLISHING" {
			return deadLetterCase{}, errCaseNotOpen
		}
	default:
		return deadLetterCase{}, errCaseNotOpen
	}
	stored.Case.Status = statusReplayInProgress
	stored.Case.UpdatedAt = now
	locked, err := service.cases.Update(ctx, stored.Case, stored.Revision)
	if err != nil {
		return deadLetterCase{}, err
	}

	fail := func(cause error) (deadLetterCase, error) {
		service.metrics.replayErrors.Add(1)
		value := locked.Case
		value.Status = statusReplayFailed
		value.UpdatedAt = service.now()
		last := len(value.Replays) - 1
		value.Replays[last].Status = "FAILED"
		value.Replays[last].Error = safeOperationError(cause)
		updated, updateErr := service.cases.Update(ctx, value, locked.Revision)
		if updateErr != nil {
			return value, fmt.Errorf("%v; also failed to record replay failure: %w", cause, updateErr)
		}
		return updated.Case, cause
	}

	raw, err := service.broker.GetMessage(ctx, dlqStream, locked.Case.DLQSequence)
	if err != nil {
		return fail(fmt.Errorf("load dead-letter payload: %w", err))
	}
	var record deadLetterRecord
	if err := json.Unmarshal(raw.Data, &record); err != nil {
		return fail(fmt.Errorf("decode dead-letter payload: %w", err))
	}
	if record.CaseID != id || record.SourceStream != locked.Case.SourceStream ||
		record.SourceSubject != locked.Case.SourceSubject || len(record.OriginalData) == 0 {
		return fail(errReplayMissing)
	}
	message := &nats.Msg{
		Subject: record.SourceSubject,
		Header:  replayHeaders(record.OriginalHeaders, locked.Case, attempt),
		Data:    append([]byte(nil), record.OriginalData...),
	}
	ack, err := service.broker.Publish(ctx, message)
	if err != nil {
		return fail(fmt.Errorf("publish replay: %w", err))
	}
	if ack.Stream != locked.Case.SourceStream {
		return fail(fmt.Errorf("replay reached unexpected stream %q", ack.Stream))
	}
	value := locked.Case
	value.Status = statusReplayPublished
	value.UpdatedAt = service.now()
	last := len(value.Replays) - 1
	value.Replays[last].Status = "PUBLISHED"
	value.Replays[last].PublishedAt = value.UpdatedAt
	value.Replays[last].SourceSequence = ack.Sequence
	updated, err := service.cases.Update(ctx, value, locked.Revision)
	if err != nil {
		service.metrics.replayErrors.Add(1)
		return value, fmt.Errorf("record published replay: %w", err)
	}
	service.metrics.replays.Add(1)
	service.logger.Info("dead-letter message replayed",
		"case_id", id, "replay_id", attempt.ID, "actor", actor,
		"stream", ack.Stream, "stream_sequence", ack.Sequence,
		"original_consumer", value.Consumer)
	return updated.Case, nil
}

func safeOperationError(err error) string {
	switch {
	case errors.Is(err, errReplayMissing):
		return "DLQ_PAYLOAD_UNAVAILABLE"
	default:
		return "REPLAY_PUBLISH_FAILED"
	}
}

func (service *operationsService) Resolve(ctx context.Context, id, actor, reason string) (deadLetterCase, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > 500 {
		return deadLetterCase{}, errors.New("a resolution reason between 1 and 500 characters is required")
	}
	stored, err := service.cases.Get(ctx, id)
	if err != nil {
		return deadLetterCase{}, err
	}
	if stored.Case.Status == statusTransferInProgress || stored.Case.Status == statusResolved || stored.Case.Status == statusReplayInProgress {
		return deadLetterCase{}, errors.New("dead-letter case cannot be resolved from its current state")
	}
	stored.Case.Status = statusResolved
	stored.Case.ResolvedAt = service.now()
	stored.Case.UpdatedAt = stored.Case.ResolvedAt
	stored.Case.ResolvedBy = actor
	stored.Case.ResolutionReason = reason
	updated, err := service.cases.Update(ctx, stored.Case, stored.Revision)
	if err != nil {
		return deadLetterCase{}, err
	}
	service.metrics.resolved.Add(1)
	service.logger.Info("dead-letter case resolved", "case_id", id, "actor", actor)
	return updated.Case, nil
}

func (service *operationsService) markParentReplayFailed(ctx context.Context, parentID, replayID, childID string) error {
	stored, err := service.cases.Get(ctx, parentID)
	if errors.Is(err, nats.ErrKeyNotFound) || (err == nil && stored.Case.Status != statusReplayPublished) {
		return nil
	}
	if err != nil {
		return err
	}
	for index := range stored.Case.Replays {
		if stored.Case.Replays[index].ID == replayID {
			stored.Case.Replays[index].Status = "DEAD_LETTERED_AGAIN"
			stored.Case.Replays[index].Error = "REPLAY_MAX_DELIVERIES_EXCEEDED:" + childID
			stored.Case.Status = statusReplayFailed
			stored.Case.UpdatedAt = service.now()
			_, err = service.cases.Update(ctx, stored.Case, stored.Revision)
			return err
		}
	}
	return nil
}
