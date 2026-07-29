// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	"github.com/sirupsen/logrus"
)

func TestFilterCurrenciesPreservesProjectionOrder(t *testing.T) {
	actual := filterCurrencies([]string{"AUD", "CAD", "EUR", "JPY", "USD"})
	expected := []string{"CAD", "EUR", "JPY", "USD"}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("unexpected currencies: got %v want %v", actual, expected)
	}
}

func TestJSONLoggerDefaultsToInfoAndAllowsDebugOverride(t *testing.T) {
	t.Setenv("LOG_LEVEL", "")
	if level := newJSONLogger().GetLevel(); level != logrus.InfoLevel {
		t.Fatalf("default log level is %s, want info", level)
	}
	t.Setenv("LOG_LEVEL", "debug")
	if level := newJSONLogger().GetLevel(); level != logrus.DebugLevel {
		t.Fatalf("configured log level is %s, want debug", level)
	}
}

func TestRenderCurrencyLogo(t *testing.T) {
	for currency, expected := range map[string]string{
		"USD": "$",
		"CAD": "$",
		"JPY": "¥",
		"EUR": "€",
		"TRY": "₺",
		"GBP": "£",
		"XXX": "$",
	} {
		if actual := renderCurrencyLogo(currency); actual != expected {
			t.Fatalf("renderCurrencyLogo(%q) = %q, want %q", currency, actual, expected)
		}
	}
}

func TestLogHandlerOnlyWrapsResponsesForDebugAccounting(t *testing.T) {
	for _, test := range []struct {
		name        string
		level       logrus.Level
		wantWrapped bool
	}{
		{name: "info", level: logrus.InfoLevel, wantWrapped: false},
		{name: "debug", level: logrus.DebugLevel, wantWrapped: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			logger := newJSONLogger()
			logger.SetLevel(test.level)
			logger.SetOutput(io.Discard)
			wrapped := false
			handler := &logHandler{
				log: logger,
				next: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					_, wrapped = w.(*responseRecorder)
					w.WriteHeader(http.StatusNoContent)
				}),
			}

			handler.ServeHTTP(
				httptest.NewRecorder(),
				httptest.NewRequest(http.MethodGet, "/", nil),
			)

			if wrapped != test.wantWrapped {
				t.Fatalf("response wrapped = %t, want %t", wrapped, test.wantWrapped)
			}
		})
	}
}

func TestCartOperationIDIsStableForIdempotencyKey(t *testing.T) {
	first := httptest.NewRequest("POST", "/cart", nil)
	first.Header.Set("Idempotency-Key", "retry-1")
	second := httptest.NewRequest("POST", "/cart", nil)
	second.Header.Set("Idempotency-Key", "retry-1")
	firstID, err := cartOperationID(first, "user-1", "add-item")
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := cartOperationID(second, "user-1", "add-item")
	if err != nil {
		t.Fatal(err)
	}
	if firstID != secondID {
		t.Fatalf("idempotency key produced different operation IDs: %s != %s", firstID, secondID)
	}
	clearID, err := cartOperationID(second, "user-1", "clear")
	if err != nil {
		t.Fatal(err)
	}
	if clearID == firstID {
		t.Fatal("different cart operation kinds must not share an operation ID")
	}
}

func TestCartOperationIDUsesRequestIDWithoutKey(t *testing.T) {
	request := httptest.NewRequest("POST", "/cart", nil)
	request = request.WithContext(context.WithValue(request.Context(), ctxKeyRequestID{}, "request-1"))
	operationID, err := cartOperationID(request, "user-1", "add-item")
	if err != nil {
		t.Fatal(err)
	}
	if operationID != "request-1" {
		t.Fatalf("got %q, want request ID", operationID)
	}
}

func TestSessionCookieIsPortableAcrossReplicasAndRejectsTampering(t *testing.T) {
	t.Setenv("NATS_PASSWORD", "replica-shared-secret")
	sessionID := "12345678-1234-1234-1234-123456789123"
	signedByReplicaA := signSessionCookie(sessionID)

	verifiedByReplicaB, err := verifySessionCookie(signedByReplicaA)
	if err != nil || verifiedByReplicaB != sessionID {
		t.Fatalf("replica B could not verify replica A cookie: session=%q error=%v",
			verifiedByReplicaB, err)
	}
	tampered := strings.Replace(signedByReplicaA, "12345678", "87654321", 1)
	if _, err := verifySessionCookie(tampered); err == nil {
		t.Fatal("tampered session identity was accepted")
	}
}

func TestWriteAcceptedOperationRendersHTMLForBrowser(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/cart", nil)
	request.Header.Set("Accept", "text/html,application/xhtml+xml")
	recorder := httptest.NewRecorder()

	writeAcceptedOperation(recorder, request, "operation-1", "cart.add-item", "/cart")

	response := recorder.Result()
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("got status %d, want %d", response.StatusCode, http.StatusAccepted)
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "text/html; charset=utf-8" {
		t.Fatalf("got content type %q, want browser progress page", contentType)
	}
	body := recorder.Body.String()
	for _, expected := range []string{"Updating your cart", "data-operation-url=\"/operations/operation-1\"", "window.location.replace"} {
		if !strings.Contains(body, expected) {
			t.Fatalf("progress page is missing %q", expected)
		}
	}
	if strings.HasPrefix(strings.TrimSpace(body), "{") {
		t.Fatal("browser response exposed the JSON operation representation")
	}
}

func TestWriteAcceptedOperationKeepsJSONForAPIClient(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/cart", nil)
	request.Header.Set("Accept", "application/json")
	recorder := httptest.NewRecorder()

	writeAcceptedOperation(recorder, request, "operation-2", "cart.add-item", "/cart")

	response := recorder.Result()
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("got status %d, want %d", response.StatusCode, http.StatusAccepted)
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("got content type %q, want JSON", contentType)
	}
	var operation cartOperation
	if err := json.NewDecoder(response.Body).Decode(&operation); err != nil {
		t.Fatal(err)
	}
	if operation.Status != "QUEUED" || operation.UpdatedAt.IsZero() {
		t.Fatalf("unexpected queued operation: %+v", operation)
	}
}

func TestWriteAcceptedOrderRedirectsBrowserToGetResource(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/cart/checkout", nil)
	request.Header.Set("Accept", "text/html,application/xhtml+xml")
	request = request.WithContext(context.WithValue(request.Context(), ctxKeySessionID{}, "user-1"))
	recorder := httptest.NewRecorder()

	writeAcceptedOrder(recorder, request, "order-1")

	response := recorder.Result()
	defer response.Body.Close()
	if response.StatusCode != http.StatusSeeOther {
		t.Fatalf("got status %d, want %d", response.StatusCode, http.StatusSeeOther)
	}
	for header, expected := range map[string]string{
		"Location":         "/orders/order-1",
		"Content-Location": "/orders/order-1",
		"X-Order-ID":       "order-1",
	} {
		if actual := response.Header.Get(header); actual != expected {
			t.Fatalf("got %s %q, want %q", header, actual, expected)
		}
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("redirect unexpectedly rendered the POST response body: %q", recorder.Body.String())
	}
}

func TestWriteAcceptedOrderKeepsJSONForAPIClient(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/cart/checkout", nil)
	request.Header.Set("Accept", "application/json")
	request = request.WithContext(context.WithValue(request.Context(), ctxKeySessionID{}, "user-1"))
	recorder := httptest.NewRecorder()

	writeAcceptedOrder(recorder, request, "order-2")

	response := recorder.Result()
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("got status %d, want %d", response.StatusCode, http.StatusAccepted)
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("got content type %q, want JSON", contentType)
	}
	if retryAfter := response.Header.Get("Retry-After"); retryAfter != orderAPIPollRetryAfter {
		t.Fatalf("got Retry-After %q, want %q", retryAfter, orderAPIPollRetryAfter)
	}
	if orderAPIPollRetryAfter != "1" {
		t.Fatalf("API clients must poll orders after one second, got %q", orderAPIPollRetryAfter)
	}
	var order orderStatus
	if err := json.NewDecoder(response.Body).Decode(&order); err != nil {
		t.Fatal(err)
	}
	if order.OrderID != "order-2" || order.Status != "QUEUED" || order.UpdatedAt.IsZero() {
		t.Fatalf("unexpected queued order: %+v", order)
	}
}

func TestQueryOrderRetriesNotFoundThreeTimes(t *testing.T) {
	attempts := 0
	expected := &storefrontQueryResponse{
		Order: &orderStatus{OrderID: "order-3", Status: "PROCESSING"},
	}

	actual, err := queryOrderWithRetry(context.Background(), 0, true, func(context.Context) (*storefrontQueryResponse, error) {
		attempts++
		if attempts < orderQueryAttempts {
			return nil, errProjectionNotFound
		}
		return expected, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != orderQueryAttempts {
		t.Fatalf("got %d query attempts, want %d", attempts, orderQueryAttempts)
	}
	if actual != expected {
		t.Fatalf("got response %p, want %p", actual, expected)
	}
}

func TestQueryOrderReturnsNotFoundAfterThreeAttempts(t *testing.T) {
	attempts := 0

	_, err := queryOrderWithRetry(context.Background(), 0, true, func(context.Context) (*storefrontQueryResponse, error) {
		attempts++
		return nil, errProjectionNotFound
	})
	if !errors.Is(err, errProjectionNotFound) {
		t.Fatalf("got error %v, want projection not found", err)
	}
	if attempts != orderQueryAttempts {
		t.Fatalf("got %d query attempts, want %d", attempts, orderQueryAttempts)
	}
}

func TestQueryOrderDoesNotRetryNotFoundForAPIClient(t *testing.T) {
	attempts := 0

	_, err := queryOrderWithRetry(context.Background(), 0, false, func(context.Context) (*storefrontQueryResponse, error) {
		attempts++
		return nil, errProjectionNotFound
	})
	if !errors.Is(err, errProjectionNotFound) {
		t.Fatalf("got error %v, want projection not found", err)
	}
	if attempts != 1 {
		t.Fatalf("got %d query attempts, want 1", attempts)
	}
}

func TestQueryOrderDoesNotRetryOtherErrors(t *testing.T) {
	attempts := 0

	_, err := queryOrderWithRetry(context.Background(), 0, true, func(context.Context) (*storefrontQueryResponse, error) {
		attempts++
		return nil, errProjectionUnavailable
	})
	if !errors.Is(err, errProjectionUnavailable) {
		t.Fatalf("got error %v, want projection unavailable", err)
	}
	if attempts != 1 {
		t.Fatalf("got %d query attempts, want 1", attempts)
	}
}

func TestOrderNeedsPollingUntilOutcomeAndSettlementAreTerminal(t *testing.T) {
	tests := []struct {
		name  string
		order *orderStatus
		want  bool
	}{
		{name: "missing", want: true},
		{name: "queued", order: &orderStatus{Status: "QUEUED"}, want: true},
		{name: "processing", order: &orderStatus{Status: "PROCESSING"}, want: true},
		{name: "cancelled", order: &orderStatus{Status: "CANCELLED"}, want: false},
		{name: "completed but unsettled", order: &orderStatus{Status: "COMPLETED"}, want: true},
		{name: "completed and settled", order: &orderStatus{
			Status: "COMPLETED", NotificationStatus: "SENT", CartClearStatus: "SUCCEEDED",
		}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := orderNeedsPolling(test.order); got != test.want {
				t.Fatalf("orderNeedsPolling() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestOrderProgressPageNavigatesToGetResource(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/orders/order-3", nil)
	request = request.WithContext(context.WithValue(request.Context(), ctxKeySessionID{}, "user-1"))
	recorder := httptest.NewRecorder()
	order := &orderStatus{OrderID: "order-3", Status: "PROCESSING", Stage: "WAITING_FOR_QUOTE"}

	if err := templates.ExecuteTemplate(recorder, "order", injectCommonTemplateData(request, map[string]interface{}{
		"show_currency": false,
		"order":         order,
		"order_url":     "/orders/order-3",
	})); err != nil {
		t.Fatal(err)
	}

	body := recorder.Body.String()
	for _, expected := range []string{"Current stage: WAITING_FOR_QUOTE", `window.location.replace("/orders/order-3");`} {
		if !strings.Contains(body, expected) {
			t.Fatalf("order progress page is missing %q", expected)
		}
	}
	if strings.Contains(body, `\"/orders/order-3\"`) {
		t.Fatal("order progress page double-encoded its redirect URL")
	}
	if strings.Contains(body, "location.reload") {
		t.Fatal("order progress page can still reload the checkout POST response")
	}
}

func TestOrderCompletePageShowsTotalPaidAfterTracking(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/orders/order-4", nil)
	request = request.WithContext(context.WithValue(request.Context(), ctxKeySessionID{}, "user-1"))
	recorder := httptest.NewRecorder()
	order := &orderStatus{
		OrderID: "order-4",
		Status:  "COMPLETED",
		Snapshot: &commonv1.SanitizedOrderSnapshot{
			TrackingId: "tracking-4",
			Total:      &commonv1.Money{CurrencyCode: "EUR", Units: 42, Nanos: 990000000},
		},
	}

	if err := templates.ExecuteTemplate(recorder, "order", injectCommonTemplateData(request, map[string]interface{}{
		"show_currency": false,
		"order":         order,
		"order_url":     "/orders/order-4",
	})); err != nil {
		t.Fatal(err)
	}

	body := recorder.Body.String()
	trackingPosition := strings.Index(body, "Tracking #")
	totalPosition := strings.Index(body, "Total Paid")
	if trackingPosition == -1 || totalPosition == -1 {
		t.Fatalf("order complete page is missing tracking or total paid row: %q", body)
	}
	if totalPosition < trackingPosition {
		t.Fatal("total paid row appears before tracking row")
	}
	if !strings.Contains(body, "€42.99") {
		t.Fatalf("order complete page has incorrectly formatted total: %q", body)
	}
}

func TestWriteRejectedOperationRendersSafeBrowserPage(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/cart", nil)
	request.Header.Set("Accept", "text/html")
	recorder := httptest.NewRecorder()
	operation := &cartOperation{
		OperationID: "operation-3",
		CommandID:   "operation-3",
		Kind:        "cart.add-item",
		Status:      "REJECTED",
		SafeMessage: "cart changed <script>alert(1)</script>",
	}

	writeRejectedOperation(recorder, request, operation, "/cart")

	if recorder.Code != http.StatusConflict {
		t.Fatalf("got status %d, want %d", recorder.Code, http.StatusConflict)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "We couldn't update your cart") {
		t.Fatal("rejection page is missing its browser-safe heading")
	}
	if strings.Contains(body, "<script>alert(1)</script>") {
		t.Fatal("rejection page rendered the safe message as executable HTML")
	}
}
