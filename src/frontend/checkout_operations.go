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
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const orderSubmitSubject = "boutique.cmd.order.submit.v1"

type paymentCard struct {
	Number                               string
	ExpirationMonth, ExpirationYear, CVV int32
}
type paymentTokenResponse struct {
	PaymentToken string    `json:"payment_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	Error        string    `json:"error"`
	SafeMessage  string    `json:"safe_message"`
}

type orderStatus struct {
	OrderID            string                           `json:"order_id"`
	UserID             string                           `json:"user_id,omitempty"`
	Status             string                           `json:"status"`
	Stage              string                           `json:"stage,omitempty"`
	Snapshot           *commonv1.SanitizedOrderSnapshot `json:"snapshot,omitempty"`
	FailureCode        string                           `json:"failure_code,omitempty"`
	Retryable          bool                             `json:"retryable,omitempty"`
	SafeMessage        string                           `json:"safe_message,omitempty"`
	NotificationStatus string                           `json:"notification_status,omitempty"`
	UpdatedAt          time.Time                        `json:"updated_at"`
}

func checkoutOrderID(request *http.Request, userID string) (string, error) {
	key := strings.TrimSpace(request.Header.Get("Idempotency-Key"))
	if len(key) > maxHTTPIdempotencyKeySize {
		return "", fmt.Errorf("Idempotency-Key exceeds %d bytes", maxHTTPIdempotencyKeySize)
	}
	if key == "" {
		return requestID(request.Context()), nil
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("boutique/order/"+userID+"/"+key)).String(), nil
}

func (fe *frontendServer) tokenizePayment(ctx context.Context, orderID string, card paymentCard) (string, error) {
	request := map[string]interface{}{"order_id": orderID, "idempotency_key": orderID,
		"correlation_id":     orderID,
		"credit_card_number": card.Number, "credit_card_expiration_month": card.ExpirationMonth,
		"credit_card_expiration_year": card.ExpirationYear, "credit_card_cvv": card.CVV}
	encoded, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	requestContext, cancel := context.WithTimeout(ctx, fe.natsRequestTimeout)
	defer cancel()
	topic := "boutique.qry.payment.tokenize.v1"
	fe.log.WithFields(logrus.Fields{
		"topic":          topic,
		"message_kind":   "query",
		"correlation_id": orderID,
	}).Debug("NATS query sent")
	message, err := fe.natsConn.RequestWithContext(requestContext, topic, encoded)
	if err != nil {
		return "", fmt.Errorf("payment tokenization unavailable: %w", err)
	}
	var response paymentTokenResponse
	if err := json.Unmarshal(message.Data, &response); err != nil {
		return "", err
	}
	if response.Error != "" || response.PaymentToken == "" {
		return "", errors.New(response.SafeMessage)
	}
	return response.PaymentToken, nil
}

func (fe *frontendServer) publishOrder(ctx context.Context, orderID, userID, email, currency string,
	address *commonv1.PostalAddress, paymentToken string, cartVersion, catalogRevision, rateRevision uint64) error {
	now := time.Now().UTC()
	command := &commandsv1.OrderSubmitCommand{CommandId: orderID, OperationId: orderID, OrderId: orderID, UserId: userID,
		ExpectedCartVersion: cartVersion, ExpectedCatalogRevision: catalogRevision, ExpectedRateRevision: rateRevision,
		CurrencyCode: currency, ShippingAddress: address, Email: email, PaymentToken: paymentToken}
	wrapper, err := anypb.New(command)
	if err != nil {
		return err
	}
	envelope := &commonv1.MessageEnvelope{MessageId: orderID, MessageType: "boutique.order.Submit.v1", SchemaVersion: 1,
		OccurredAt: timestamppb.New(now), Producer: "frontend/phase5", AggregateType: "order", AggregateId: orderID,
		CorrelationId: orderID, Data: wrapper}
	setEnvelopeTrace(ctx, envelope)
	if err := fe.publishEnvelope(ctx, orderSubmitSubject, orderID, envelope); err != nil {
		return err
	}
	return fe.publishOperationAccepted(context.Background(), ctx, orderID, orderID, "order.submit", userID, now)
}

func (fe *frontendServer) orderHandler(response http.ResponseWriter, request *http.Request) {
	orderID := mux.Vars(request)["id"]
	view, err := fe.storefrontQuery(request.Context(), "order", storefrontQueryRequest{
		OrderID: orderID, UserID: sessionID(request), CorrelationID: orderID,
	})
	if err != nil {
		if errors.Is(err, errProjectionNotFound) {
			http.Error(response, "order not found", http.StatusNotFound)
			return
		}
		response.Header().Set("Retry-After", "1")
		http.Error(response, "order status unavailable", http.StatusServiceUnavailable)
		return
	}
	if !acceptsHTML(request) {
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(view.Order)
		return
	}
	response.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = templates.ExecuteTemplate(response, "order", injectCommonTemplateData(request, map[string]interface{}{
		"show_currency": false, "order": view.Order, "order_url": baseUrl + "/orders/" + orderID,
	}))
}

func writeAcceptedOrder(response http.ResponseWriter, request *http.Request, orderID string) {
	status := &orderStatus{OrderID: orderID, UserID: sessionID(request), Status: "QUEUED", Stage: "QUEUED", UpdatedAt: time.Now().UTC()}
	writeOrderResponse(response, request, http.StatusAccepted, status)
}

func writeOrderResponse(response http.ResponseWriter, request *http.Request, code int, status *orderStatus) {
	orderID := status.OrderID
	location := baseUrl + "/orders/" + orderID
	response.Header().Set("Location", location)
	response.Header().Set("Content-Location", location)
	response.Header().Set("X-Order-ID", orderID)
	if acceptsHTML(request) {
		// A browser must land on the GET order resource before the progress page
		// starts reloading. Rendering that page as the POST response would make
		// every refresh resubmit checkout and potentially create another order.
		response.WriteHeader(http.StatusSeeOther)
		return
	}
	response.Header().Set("Retry-After", "1")
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(code)
	_ = json.NewEncoder(response).Encode(status)
}
