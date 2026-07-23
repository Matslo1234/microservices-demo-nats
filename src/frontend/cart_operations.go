// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	commandsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/commands/v1"
	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	cartAddSubject            = "boutique.cmd.cart.add-item.v1"
	cartClearSubject          = "boutique.cmd.cart.clear.v1"
	operationAcceptedSubject  = "boutique.evt.storefront.operation-accepted.v1"
	operationPollInterval     = 25 * time.Millisecond
	maxHTTPIdempotencyKeySize = 256
)

type cartOperation struct {
	OperationID string    `json:"operation_id"`
	CommandID   string    `json:"command_id"`
	Kind        string    `json:"kind"`
	Status      string    `json:"status"`
	UserID      string    `json:"user_id,omitempty"`
	FailureCode string    `json:"failure_code,omitempty"`
	Retryable   bool      `json:"retryable,omitempty"`
	SafeMessage string    `json:"safe_message,omitempty"`
	CartVersion uint64    `json:"cart_version,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func cartOperationID(r *http.Request, userID, kind string) (string, error) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(key) > maxHTTPIdempotencyKeySize {
		return "", fmt.Errorf("Idempotency-Key exceeds %d bytes", maxHTTPIdempotencyKeySize)
	}
	if key == "" {
		return requestID(r.Context()), nil
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("boutique/cart/"+kind+"/"+userID+"/"+key)).String(), nil
}

func (fe *frontendServer) publishCartAdd(ctx context.Context, operationID, userID, productID string,
	quantity int32, expectedVersion uint64) error {
	payload := &commandsv1.CartAddItemCommand{
		CommandId: operationID, UserId: userID, ProductId: productID,
		Quantity: quantity, ExpectedCartVersion: expectedVersion,
	}
	return fe.publishCartCommand(ctx, cartAddSubject, "boutique.cart.AddItem.v1", "cart.add-item",
		operationID, userID, expectedVersion, payload)
}

func (fe *frontendServer) publishCartClear(ctx context.Context, operationID, userID string,
	expectedVersion uint64) error {
	payload := &commandsv1.CartClearCommand{
		CommandId: operationID, UserId: userID, ExpectedCartVersion: expectedVersion,
		Reason: "user-request",
	}
	return fe.publishCartCommand(ctx, cartClearSubject, "boutique.cart.Clear.v1", "cart.clear",
		operationID, userID, expectedVersion, payload)
}

func (fe *frontendServer) publishCartCommand(ctx context.Context, subject, messageType, kind,
	operationID, userID string, expectedVersion uint64, payload proto.Message) error {
	now := time.Now().UTC()
	wrapped, err := anypb.New(payload)
	if err != nil {
		return err
	}
	envelope := &commonv1.MessageEnvelope{
		MessageId: operationID, MessageType: messageType, SchemaVersion: 1,
		OccurredAt: timestamppb.New(now), Producer: "frontend/phase4",
		AggregateType: "cart", AggregateId: userID, AggregateVersion: expectedVersion,
		CorrelationId: operationID, Data: wrapped,
	}
	setEnvelopeTrace(ctx, envelope)
	if err := fe.publishEnvelope(ctx, subject, operationID, envelope); err != nil {
		return fmt.Errorf("publish cart command: %w", err)
	}

	return fe.publishOperationAccepted(context.Background(), ctx, operationID, operationID, kind, userID, now)
}

func (fe *frontendServer) publishOperationAccepted(publishContext, traceContext context.Context, operationID, commandID, kind, userID string, now time.Time) error {
	acceptedID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("boutique/operation-accepted/"+operationID)).String()
	acceptedPayload := &eventsv1.StorefrontOperationAcceptedEvent{
		OperationId: operationID, CommandId: commandID, OperationKind: kind,
		Status: "QUEUED", UserOrSessionId: userID, AcceptedAt: timestamppb.New(now),
	}
	acceptedData, err := anypb.New(acceptedPayload)
	if err != nil {
		return err
	}
	accepted := &commonv1.MessageEnvelope{
		MessageId: acceptedID, MessageType: "boutique.storefront.OperationAccepted.v1", SchemaVersion: 1,
		OccurredAt: timestamppb.New(now), Producer: "frontend/phase4",
		AggregateType: "operation", AggregateId: operationID, AggregateVersion: uint64(now.UnixNano()),
		CorrelationId: operationID, CausationId: operationID, Data: acceptedData,
	}
	setEnvelopeTrace(traceContext, accepted)
	if err := fe.publishEnvelope(publishContext, operationAcceptedSubject, acceptedID, accepted); err != nil {
		return fmt.Errorf("publish accepted operation: %w", err)
	}
	return nil
}

func (fe *frontendServer) publishEnvelope(ctx context.Context, subject, messageID string,
	envelope *commonv1.MessageEnvelope) error {
	encoded, err := proto.Marshal(envelope)
	if err != nil {
		return err
	}
	publishContext, cancel := context.WithTimeout(ctx, fe.natsPublishTimeout)
	defer cancel()
	message := &nats.Msg{Subject: subject, Data: encoded, Header: nats.Header{}}
	message.Header.Set("Nats-Msg-Id", messageID)
	message.Header.Set("Content-Type", "application/protobuf")
	ack, err := fe.natsJS.PublishMsg(message, nats.Context(publishContext), nats.MsgId(messageID))
	if err != nil {
		return err
	}
	if ack == nil {
		return fmt.Errorf("JetStream did not acknowledge %s", subject)
	}
	kind := messageKind(subject)
	fe.log.WithFields(logrus.Fields{
		"topic":          subject,
		"message_kind":   kind,
		"correlation_id": correlationID(envelope.CorrelationId),
	}).Debug("NATS " + kind + " sent")
	return nil
}

func messageKind(topic string) string {
	switch {
	case strings.HasPrefix(topic, "boutique.cmd."):
		return "command"
	case strings.HasPrefix(topic, "boutique.qry."):
		return "query"
	default:
		return "event"
	}
}

func correlationID(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}

func setEnvelopeTrace(ctx context.Context, envelope *commonv1.MessageEnvelope) {
	headers := nats.Header{}
	otel.GetTextMapPropagator().Inject(ctx, natsHeaderCarrier(headers))
	envelope.Traceparent = headers.Get("traceparent")
	envelope.Tracestate = headers.Get("tracestate")
}

func (fe *frontendServer) waitForCartOperation(ctx context.Context, operationID, userID string) (*cartOperation, error) {
	waitContext, cancel := context.WithTimeout(ctx, fe.cartOperationWaitTimeout)
	defer cancel()
	ticker := time.NewTicker(operationPollInterval)
	defer ticker.Stop()
	for {
		response, err := fe.storefrontQuery(waitContext, "operation", storefrontQueryRequest{
			OperationID: operationID, UserID: userID, CorrelationID: operationID,
		})
		if err == nil && response.Operation != nil {
			switch response.Operation.Status {
			case "SUCCEEDED", "REJECTED":
				return response.Operation, nil
			}
		}
		select {
		case <-waitContext.Done():
			return nil, waitContext.Err()
		case <-ticker.C:
		}
	}
}

func (fe *frontendServer) operationHandler(w http.ResponseWriter, r *http.Request) {
	operationID := mux.Vars(r)["id"]
	response, err := fe.storefrontQuery(r.Context(), "operation", storefrontQueryRequest{
		OperationID: operationID, UserID: sessionID(r), CorrelationID: operationID,
	})
	if err != nil {
		switch {
		case errors.Is(err, errProjectionNotFound):
			http.Error(w, "operation not found", http.StatusNotFound)
		case errors.Is(err, errProjectionUnavailable):
			w.Header().Set("Retry-After", "1")
			http.Error(w, "operation status unavailable", http.StatusServiceUnavailable)
		default:
			http.Error(w, "operation status unavailable", http.StatusInternalServerError)
		}
		return
	}
	writeOperationJSON(w, http.StatusOK, response.Operation)
}

func writeOperationJSON(w http.ResponseWriter, status int, operation *cartOperation) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(operation)
}

func setOperationHeaders(w http.ResponseWriter, operationID string) {
	w.Header().Set("X-Operation-ID", operationID)
	w.Header().Set("Content-Location", baseUrl+"/operations/"+operationID)
}

func acceptsHTML(r *http.Request) bool {
	for _, value := range strings.Split(r.Header.Get("Accept"), ",") {
		mediaType := strings.ToLower(strings.TrimSpace(strings.SplitN(value, ";", 2)[0]))
		if mediaType == "text/html" || mediaType == "application/xhtml+xml" {
			return true
		}
	}
	return false
}

func writeCartOperationResponse(w http.ResponseWriter, r *http.Request, status int,
	operation *cartOperation, successLocation string) {
	setOperationHeaders(w, operation.OperationID)
	w.Header().Set("Location", baseUrl+"/operations/"+operation.OperationID)
	if status == http.StatusAccepted {
		w.Header().Set("Retry-After", "1")
	}
	if !acceptsHTML(r) {
		writeOperationJSON(w, status, operation)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = templates.ExecuteTemplate(w, "cart-operation", injectCommonTemplateData(r, map[string]interface{}{
		"show_currency":    false,
		"operation":        operation,
		"operation_url":    baseUrl + "/operations/" + operation.OperationID,
		"success_location": successLocation,
		"failure_location": baseUrl + "/cart",
	}))
}

func writeAcceptedOperation(w http.ResponseWriter, r *http.Request, operationID, kind, successLocation string) {
	operation := &cartOperation{
		OperationID: operationID, CommandID: operationID, Kind: kind, Status: "QUEUED",
		UpdatedAt: time.Now().UTC(),
	}
	writeCartOperationResponse(w, r, http.StatusAccepted, operation, successLocation)
}

func writeRejectedOperation(w http.ResponseWriter, r *http.Request, operation *cartOperation,
	successLocation string) {
	writeCartOperationResponse(w, r, http.StatusConflict, operation, successLocation)
}
