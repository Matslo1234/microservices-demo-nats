// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

// Package stateless contains the storage and message primitives shared by
// stateless Online Boutique handlers.
package stateless

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const resultIDDomain = "boutique.result.v1"

var resultSlotPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$`)

// ResultSpec supplies the fields which differ between an input envelope and a
// deterministic result envelope. OccurredAt is explicit: callers must persist
// the input-derived event time instead of reading the wall clock on each retry.
type ResultSpec struct {
	Slot             string
	MessageType      string
	Producer         string
	AggregateType    string
	AggregateID      string
	AggregateVersion uint64
	OccurredAt       time.Time
	Payload          proto.Message
}

// DeriveResultMessageID implements the cross-language Phase 0
// sha256-length-prefixed-v1 contract.
func DeriveResultMessageID(inputMessageID, resultSlot string) (string, error) {
	if strings.TrimSpace(inputMessageID) == "" {
		return "", errors.New("input message ID is required")
	}
	if !resultSlotPattern.MatchString(resultSlot) {
		return "", fmt.Errorf("invalid result slot %q", resultSlot)
	}

	input := []byte(inputMessageID)
	slot := []byte(resultSlot)
	hash := sha256.New()
	_, _ = hash.Write([]byte(resultIDDomain))
	_, _ = hash.Write([]byte{0})
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(input)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write(input)
	binary.BigEndian.PutUint32(length[:], uint32(len(slot)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write(slot)
	return "br1_" + base64.RawURLEncoding.EncodeToString(hash.Sum(nil)), nil
}

// NewResultEnvelope copies causal and trace identity from input and derives a
// stable message ID from the input identity and the result slot.
func NewResultEnvelope(input *commonv1.MessageEnvelope, spec ResultSpec) (*commonv1.MessageEnvelope, error) {
	if input == nil {
		return nil, errors.New("input envelope is required")
	}
	if strings.TrimSpace(input.GetMessageId()) == "" {
		return nil, errors.New("input envelope message ID is required")
	}
	if strings.TrimSpace(input.GetCorrelationId()) == "" {
		return nil, errors.New("input envelope correlation ID is required")
	}
	if strings.TrimSpace(spec.MessageType) == "" || strings.TrimSpace(spec.Producer) == "" {
		return nil, errors.New("result message type and producer are required")
	}
	if strings.TrimSpace(spec.AggregateID) == "" || strings.TrimSpace(spec.AggregateType) == "" {
		return nil, errors.New("result aggregate type and ID are required")
	}
	if spec.AggregateVersion == 0 {
		return nil, errors.New("result aggregate version must be positive")
	}
	if spec.OccurredAt.IsZero() {
		return nil, errors.New("result occurrence time is required")
	}
	if spec.Payload == nil {
		return nil, errors.New("result payload is required")
	}

	messageID, err := DeriveResultMessageID(input.GetMessageId(), spec.Slot)
	if err != nil {
		return nil, err
	}
	payload, err := anypb.New(spec.Payload)
	if err != nil {
		return nil, fmt.Errorf("wrap result payload: %w", err)
	}
	occurredAt := timestamppb.New(spec.OccurredAt.UTC())
	if err := occurredAt.CheckValid(); err != nil {
		return nil, fmt.Errorf("invalid result occurrence time: %w", err)
	}

	return &commonv1.MessageEnvelope{
		MessageId:        messageID,
		MessageType:      spec.MessageType,
		SchemaVersion:    1,
		OccurredAt:       occurredAt,
		Producer:         spec.Producer,
		AggregateType:    spec.AggregateType,
		AggregateId:      spec.AggregateID,
		AggregateVersion: spec.AggregateVersion,
		CorrelationId:    input.GetCorrelationId(),
		CausationId:      input.GetMessageId(),
		Traceparent:      input.GetTraceparent(),
		Tracestate:       input.GetTracestate(),
		Data:             payload,
	}, nil
}

// MarshalEnvelope produces stable bytes suitable for a stored result journal.
func MarshalEnvelope(envelope *commonv1.MessageEnvelope) ([]byte, error) {
	if envelope == nil {
		return nil, errors.New("result envelope is required")
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(envelope)
}
