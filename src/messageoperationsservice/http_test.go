// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testHTTPHandler(t *testing.T) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	operations := testOperations(newFakeBroker(), newFakeCases())
	server, err := newAdminHTTPServer(operations, logger, "administrator", strings.Repeat("t", 32), func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler()
}

func TestAdminEndpointsRequireAuthentication(t *testing.T) {
	handler := testHTTPHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/api/admin/dead-letters", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || response.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("got status %d and headers %+v", response.Code, response.Header())
	}
}

func TestAuthenticatedListDoesNotExposePayloadFields(t *testing.T) {
	handler := testHTTPHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/api/admin/dead-letters", nil)
	request.SetBasicAuth("administrator", strings.Repeat("t", 32))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("got status %d: %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "original_data") || strings.Contains(response.Body.String(), "original_headers") {
		t.Fatal("admin case response exposed restricted replay payload")
	}
}

func TestAdminPageUsesSecurityHeaders(t *testing.T) {
	handler := testHTTPHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/admin/dead-letters", nil)
	request.SetBasicAuth("administrator", strings.Repeat("t", 32))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Security-Policy") == "" || !strings.Contains(response.Body.String(), "Dead-letter administration") {
		t.Fatalf("unexpected admin page response: status=%d headers=%+v", response.Code, response.Header())
	}
}
