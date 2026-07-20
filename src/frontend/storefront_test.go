// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestFilterCurrenciesPreservesProjectionOrder(t *testing.T) {
	actual := filterCurrencies([]string{"AUD", "CAD", "EUR", "JPY", "USD"})
	expected := []string{"CAD", "EUR", "JPY", "USD"}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("unexpected currencies: got %v want %v", actual, expected)
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
	var order orderStatus
	if err := json.NewDecoder(response.Body).Decode(&order); err != nil {
		t.Fatal(err)
	}
	if order.OrderID != "order-2" || order.Status != "QUEUED" || order.UpdatedAt.IsZero() {
		t.Fatalf("unexpected queued order: %+v", order)
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
