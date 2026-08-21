// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/nats-io/nats.go"
)

type actorContextKey struct{}

type adminHTTPServer struct {
	operations *operationsService
	logger     *slog.Logger
	adminUser  string
	adminToken string
	ready      func() bool
	template   *template.Template
}

func newAdminHTTPServer(operations *operationsService, logger *slog.Logger, adminUser, adminToken string, ready func() bool) (*adminHTTPServer, error) {
	page, err := template.New("admin").Parse(adminPage)
	if err != nil {
		return nil, err
	}
	return &adminHTTPServer{
		operations: operations, logger: logger, adminUser: adminUser,
		adminToken: adminToken, ready: ready, template: page,
	}, nil
}

func (server *adminHTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(writer, "ok\n")
	})
	mux.HandleFunc("GET /readyz", func(writer http.ResponseWriter, _ *http.Request) {
		if !server.ready() {
			http.Error(writer, "NATS or DLQ storage unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(writer, "ok\n")
	})
	mux.HandleFunc("GET /metrics", server.metrics)
	mux.Handle("GET /admin/dead-letters", server.auth(http.HandlerFunc(server.adminPage)))
	mux.Handle("GET /api/admin/dead-letters", server.auth(http.HandlerFunc(server.listCases)))
	mux.Handle("GET /api/admin/dead-letters/{id}", server.auth(http.HandlerFunc(server.getCase)))
	mux.Handle("POST /api/admin/dead-letters/{id}/replays", server.auth(http.HandlerFunc(server.replay)))
	mux.Handle("POST /api/admin/dead-letters/{id}/resolve", server.auth(http.HandlerFunc(server.resolve)))
	return securityHeaders(mux)
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("X-Frame-Options", "DENY")
		writer.Header().Set("Referrer-Policy", "no-referrer")
		writer.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'")
		next.ServeHTTP(writer, request)
	})
}

func constantTimeEqual(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func (server *adminHTTPServer) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		actor := server.adminUser
		authorized := false
		if user, password, ok := request.BasicAuth(); ok {
			authorized = constantTimeEqual(user, server.adminUser) && constantTimeEqual(password, server.adminToken)
			actor = user
		} else if token := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "); token != "" {
			authorized = constantTimeEqual(token, server.adminToken)
		}
		if !authorized {
			writer.Header().Set("WWW-Authenticate", `Basic realm="Online Boutique DLQ administration", charset="UTF-8"`)
			http.Error(writer, "administrator authentication required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(writer, request.WithContext(context.WithValue(request.Context(), actorContextKey{}, actor)))
	})
}

func requestActor(request *http.Request) string {
	actor, _ := request.Context().Value(actorContextKey{}).(string)
	if actor == "" {
		return "admin"
	}
	return actor
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func decodeSmallJSON(request *http.Request, target any) error {
	if !strings.HasPrefix(request.Header.Get("Content-Type"), "application/json") {
		return errors.New("Content-Type must be application/json")
	}
	decoder := json.NewDecoder(io.LimitReader(request.Body, 4097))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("request must contain one JSON object")
	}
	return nil
}

func (server *adminHTTPServer) adminPage(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := server.template.Execute(writer, nil); err != nil {
		server.logger.Error("render admin page", "error", err)
	}
}

func (server *adminHTTPServer) listCases(writer http.ResponseWriter, request *http.Request) {
	values, err := server.operations.cases.List(request.Context())
	if err != nil {
		http.Error(writer, "dead-letter cases unavailable", http.StatusServiceUnavailable)
		return
	}
	status := caseStatus(strings.ToUpper(request.URL.Query().Get("status")))
	if status != "" {
		filtered := make([]deadLetterCase, 0, len(values))
		for _, value := range values {
			if value.Status == status {
				filtered = append(filtered, value)
			}
		}
		values = filtered
	}
	writeJSON(writer, http.StatusOK, map[string]any{"cases": values, "count": len(values)})
}

func (server *adminHTTPServer) getCase(writer http.ResponseWriter, request *http.Request) {
	value, err := server.operations.cases.Get(request.Context(), request.PathValue("id"))
	if errors.Is(err, nats.ErrKeyNotFound) {
		http.Error(writer, "dead-letter case not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(writer, "dead-letter case unavailable", http.StatusServiceUnavailable)
		return
	}
	writeJSON(writer, http.StatusOK, value.Case)
}

func (server *adminHTTPServer) replay(writer http.ResponseWriter, request *http.Request) {
	var input replayRequest
	if err := decodeSmallJSON(request, &input); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	value, err := server.operations.Replay(request.Context(), request.PathValue("id"), requestActor(request), input.Reason)
	if errors.Is(err, nats.ErrKeyNotFound) {
		http.Error(writer, "dead-letter case not found", http.StatusNotFound)
		return
	}
	if errors.Is(err, errCaseNotOpen) || errors.Is(err, errCaseConflict) {
		http.Error(writer, err.Error(), http.StatusConflict)
		return
	}
	if errors.Is(err, errReplayMissing) {
		http.Error(writer, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	if err != nil {
		http.Error(writer, "replay failed", http.StatusServiceUnavailable)
		return
	}
	writeJSON(writer, http.StatusAccepted, value)
}

func (server *adminHTTPServer) resolve(writer http.ResponseWriter, request *http.Request) {
	var input resolveRequest
	if err := decodeSmallJSON(request, &input); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	value, err := server.operations.Resolve(request.Context(), request.PathValue("id"), requestActor(request), input.Reason)
	if errors.Is(err, nats.ErrKeyNotFound) {
		http.Error(writer, "dead-letter case not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(writer, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (server *adminHTTPServer) metrics(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
	connected := 0
	if server.ready() {
		connected = 1
	}
	values, err := server.operations.cases.List(request.Context())
	counts := map[caseStatus]int{}
	if err == nil {
		for _, value := range values {
			counts[value.Status]++
		}
	}
	_, _ = fmt.Fprintf(writer, "boutique_dependency_ready{service=\"messageoperationsservice\",dependency=\"nats\"} %d\n", connected)
	for _, status := range []caseStatus{statusTransferInProgress, statusOpen, statusReplayInProgress, statusReplayPublished, statusReplayFailed, statusResolved} {
		_, _ = fmt.Fprintf(writer, "boutique_dlq_cases{status=\"%s\"} %d\n", status, counts[status])
	}
	_, _ = fmt.Fprintf(writer, "boutique_dlq_transfers_total %d\n", server.operations.metrics.transfers.Load())
	_, _ = fmt.Fprintf(writer, "boutique_dlq_transfer_errors_total %d\n", server.operations.metrics.transferErrors.Load())
	_, _ = fmt.Fprintf(writer, "boutique_dlq_replays_total %d\n", server.operations.metrics.replays.Load())
	_, _ = fmt.Fprintf(writer, "boutique_dlq_replay_errors_total %d\n", server.operations.metrics.replayErrors.Load())
	_, _ = fmt.Fprintf(writer, "boutique_dlq_resolved_total %d\n", server.operations.metrics.resolved.Load())
}

const adminPage = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Online Boutique dead letters</title>
  <style>
    :root { color-scheme: light dark; font-family: system-ui, sans-serif; }
    body { margin: 0; background: #f5f7fb; color: #182230; }
    header { background: #1d3557; color: white; padding: 1rem 2rem; display: flex; align-items: center; gap: 1rem; }
    header h1 { font-size: 1.3rem; margin: 0; }
    #badge { background: #d92d20; border-radius: 999px; padding: .2rem .65rem; font-weight: 700; }
    main { max-width: 1200px; margin: 2rem auto; padding: 0 1rem; }
    .notice { background: #fff4e5; border-left: 4px solid #f79009; padding: .8rem 1rem; margin-bottom: 1rem; }
    table { border-collapse: collapse; width: 100%; background: white; box-shadow: 0 2px 8px #0001; }
    th, td { padding: .75rem; text-align: left; border-bottom: 1px solid #e5e7eb; vertical-align: top; }
    th { background: #eef2f6; }
    code { font-size: .8rem; }
    button { border: 0; border-radius: .3rem; padding: .5rem .75rem; margin: .15rem; cursor: pointer; background: #175cd3; color: white; }
    button.resolve { background: #475467; }
    .status { font-weight: 700; white-space: nowrap; }
    .error { color: #b42318; }
    @media (prefers-color-scheme: dark) { body { background:#101828; color:#f2f4f7 } table { background:#1d2939 } th { background:#344054 } th,td { border-color:#475467 } .notice { background:#3b2a13 } }
  </style>
</head>
<body>
<header><h1>Dead-letter administration</h1><span id="badge">0 active</span></header>
<main>
  <div class="notice">Replay only after fixing the underlying cause. A replay is published to the original subject with the original business message ID and a new transport replay ID. Event replays may be seen again by every matching durable consumer.</div>
  <p id="message" role="status"></p>
  <table><thead><tr><th>Status</th><th>Message</th><th>Source</th><th>Consumer</th><th>Attempts</th><th>Dead-lettered</th><th>Actions</th></tr></thead><tbody id="cases"></tbody></table>
</main>
<script>
const escapeHTML = value => String(value ?? '').replace(/[&<>'"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));
const canReplay = item => item.replay_available && (item.status === 'OPEN' || item.status === 'REPLAY_FAILED' || (item.status === 'REPLAY_IN_PROGRESS' && Date.now() - Date.parse(item.updated_at) >= 60000));
async function load() {
  const response = await fetch('/api/admin/dead-letters', {headers:{Accept:'application/json'}});
  if (!response.ok) throw new Error('Could not load dead-letter cases');
  const body = await response.json();
  const active = body.cases.filter(item => item.status === 'TRANSFER_IN_PROGRESS' || item.status === 'OPEN' || item.status === 'REPLAY_IN_PROGRESS' || item.status === 'REPLAY_FAILED').length;
  document.getElementById('badge').textContent = active + ' active';
  document.getElementById('cases').innerHTML = body.cases.map(item =>
    '<tr><td class="status">' + escapeHTML(item.status) + '</td>' +
    '<td><code>' + escapeHTML(item.message_type || item.message_id || item.id) + '</code><br>' + escapeHTML(item.safe_error_code) + '</td>' +
    '<td><code>' + escapeHTML(item.source_stream) + ':' + escapeHTML(item.source_sequence) + '</code><br>' + escapeHTML(item.source_subject) + '</td>' +
    '<td><code>' + escapeHTML(item.consumer) + '</code></td><td>' + escapeHTML(item.deliveries) + '</td>' +
    '<td>' + escapeHTML(new Date(item.dead_lettered_at).toLocaleString()) + '</td>' +
    '<td><button onclick="replay(\'' + item.id + '\')" ' + (canReplay(item) ? '' : 'disabled') + '>Replay</button>' +
    '<button class="resolve" onclick="resolveCase(\'' + item.id + '\')" ' + (item.status === 'TRANSFER_IN_PROGRESS' || item.status === 'RESOLVED' || item.status === 'REPLAY_IN_PROGRESS' ? 'disabled' : '') + '>Resolve</button></td></tr>'
  ).join('');
}
async function action(id, suffix, promptText) {
  const reason = window.prompt(promptText);
  if (!reason) return;
  const response = await fetch('/api/admin/dead-letters/' + encodeURIComponent(id) + suffix, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({reason})});
  const message = document.getElementById('message');
  if (!response.ok) { message.className='error'; message.textContent=await response.text(); return; }
  message.className=''; message.textContent='Operation accepted.'; await load();
}
const replay = id => action(id, '/replays', 'Why is this message safe to replay now?');
const resolveCase = id => action(id, '/resolve', 'Why is this case being resolved without another replay?');
load().catch(error => { document.getElementById('message').className='error'; document.getElementById('message').textContent=error.message; });
setInterval(() => load().catch(() => {}), 10000);
</script>
</body></html>`
