// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))
	nc, js, err := connectNATS()
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()
	projector, err := newProjector(js)
	if err != nil {
		log.Fatal(err)
	}
	subscription, rebuilding, err := projector.subscribe()
	if err != nil {
		log.Fatal(err)
	}
	queryService, queryEndpointCount, err := projector.registerQueries(nc)
	if err != nil {
		log.Fatal(err)
	}
	defer queryService.Stop()
	log.Printf("storefront projection consumer established (rebuilding=%t, query_subscriptions=%d)", rebuilding, queryEndpointCount)

	var ready atomic.Bool
	ready.Store(true)
	stop := make(chan struct{})
	go projector.run(subscription, stop)
	nc.SetDisconnectErrHandler(func(_ *nats.Conn, disconnectErr error) {
		log.Printf("NATS disconnected: %v", disconnectErr)
		ready.Store(false)
	})
	nc.SetReconnectHandler(func(_ *nats.Conn) { ready.Store(true) })

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) { _, _ = response.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", func(response http.ResponseWriter, _ *http.Request) {
		if !ready.Load() || !nc.IsConnected() {
			http.Error(response, "not ready", http.StatusServiceUnavailable)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]bool{"ready": true})
	})
	mux.HandleFunc("/metrics", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/plain; version=0.0.4")
		connected := 0
		if ready.Load() && nc.IsConnected() {
			connected = 1
		}
		_, _ = fmt.Fprintf(response, "boutique_dependency_ready{service=\"storefrontprojectionservice\",dependency=\"nats\"} %d\n", connected)
		_, _ = fmt.Fprintln(response, "boutique_dependency_ready{service=\"storefrontprojectionservice\",dependency=\"kv\"} 1")
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server failed: %v", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	ready.Store(false)
	close(stop)
	_ = server.Close()
	if err := queryService.Stop(); err != nil {
		log.Printf("NATS query service drain failed: %v", err)
	}
	if err := nc.Drain(); err != nil {
		log.Printf("NATS drain failed: %v", err)
	}
}
