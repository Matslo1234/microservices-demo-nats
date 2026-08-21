// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import "time"

const (
	advisoryStream  = "BOUTIQUE_ADVISORIES"
	advisorySubject = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"
	advisoryDurable = "messageoperations-dlq-controller-v1"
	dlqStream       = "BOUTIQUE_DLQ"
	dlqCaseBucket   = "DLQ_CASES"

	headerReplayID               = "Boutique-Replay-Id"
	headerReplayCaseID           = "Boutique-DLQ-Case-Id"
	headerReplayOriginalConsumer = "Boutique-Replay-Original-Consumer"
	headerOriginalStreamSeq      = "Boutique-Original-Stream-Sequence"
)

type caseStatus string

const (
	statusTransferInProgress caseStatus = "TRANSFER_IN_PROGRESS"
	statusOpen               caseStatus = "OPEN"
	statusReplayInProgress   caseStatus = "REPLAY_IN_PROGRESS"
	statusReplayPublished    caseStatus = "REPLAY_PUBLISHED"
	statusReplayFailed       caseStatus = "REPLAY_FAILED"
	statusResolved           caseStatus = "RESOLVED"
)

type maxDeliverAdvisory struct {
	Type       string    `json:"type"`
	ID         string    `json:"id"`
	Timestamp  time.Time `json:"timestamp"`
	Stream     string    `json:"stream"`
	Consumer   string    `json:"consumer"`
	StreamSeq  uint64    `json:"stream_seq"`
	Deliveries uint64    `json:"deliveries"`
}

type deadLetterRecord struct {
	Version          int                 `json:"version"`
	CaseID           string              `json:"case_id"`
	AdvisoryID       string              `json:"advisory_id"`
	SourceStream     string              `json:"source_stream"`
	SourceSubject    string              `json:"source_subject,omitempty"`
	SourceSequence   uint64              `json:"source_sequence"`
	Consumer         string              `json:"consumer"`
	Deliveries       uint64              `json:"deliveries"`
	MessageID        string              `json:"message_id,omitempty"`
	MessageType      string              `json:"message_type,omitempty"`
	CorrelationID    string              `json:"correlation_id,omitempty"`
	SafeErrorCode    string              `json:"safe_error_code"`
	SourceFetchError string              `json:"source_fetch_error,omitempty"`
	OriginalHeaders  map[string][]string `json:"original_headers,omitempty"`
	OriginalData     []byte              `json:"original_data,omitempty"`
	DeadLetteredAt   time.Time           `json:"dead_lettered_at"`
	ParentCaseID     string              `json:"parent_case_id,omitempty"`
	ParentReplayID   string              `json:"parent_replay_id,omitempty"`
}

type replayAttempt struct {
	ID             string    `json:"id"`
	Number         int       `json:"number"`
	Actor          string    `json:"actor"`
	Reason         string    `json:"reason"`
	RequestedAt    time.Time `json:"requested_at"`
	PublishedAt    time.Time `json:"published_at,omitempty"`
	SourceSequence uint64    `json:"source_sequence,omitempty"`
	Status         string    `json:"status"`
	Error          string    `json:"error,omitempty"`
}

type deadLetterCase struct {
	Version          int             `json:"version"`
	ID               string          `json:"id"`
	Status           caseStatus      `json:"status"`
	AdvisoryID       string          `json:"advisory_id"`
	SourceStream     string          `json:"source_stream"`
	SourceSubject    string          `json:"source_subject,omitempty"`
	SourceSequence   uint64          `json:"source_sequence"`
	Consumer         string          `json:"consumer"`
	Deliveries       uint64          `json:"deliveries"`
	MessageID        string          `json:"message_id,omitempty"`
	MessageType      string          `json:"message_type,omitempty"`
	CorrelationID    string          `json:"correlation_id,omitempty"`
	SafeErrorCode    string          `json:"safe_error_code"`
	ReplayAvailable  bool            `json:"replay_available"`
	DLQSubject       string          `json:"dlq_subject"`
	DLQSequence      uint64          `json:"dlq_sequence"`
	DeadLetteredAt   time.Time       `json:"dead_lettered_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	ResolvedAt       time.Time       `json:"resolved_at,omitempty"`
	ResolvedBy       string          `json:"resolved_by,omitempty"`
	ResolutionReason string          `json:"resolution_reason,omitempty"`
	ParentCaseID     string          `json:"parent_case_id,omitempty"`
	ParentReplayID   string          `json:"parent_replay_id,omitempty"`
	Replays          []replayAttempt `json:"replays,omitempty"`
}

type storedCase struct {
	Case     deadLetterCase
	Revision uint64
}

type replayRequest struct {
	Reason string `json:"reason"`
}

type resolveRequest struct {
	Reason string `json:"reason"`
}
