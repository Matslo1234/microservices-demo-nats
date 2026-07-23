// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

var log = newLogger()

func newLogger() *logrus.Logger {
	logger := logrus.New()
	logger.Level = logrus.DebugLevel
	logger.Formatter = &logrus.JSONFormatter{TimestampFormat: time.RFC3339Nano}
	logger.Out = os.Stdout
	return logger
}

func main() {
	storePath := os.Getenv("CHECKOUT_STORE_PATH")
	if storePath == "" {
		storePath = "/tmp/checkout/sagas.json"
	}
	store, err := openStateStore(storePath)
	if err != nil {
		log.Fatal(err)
	}
	worker, err := startCheckoutWorker(store)
	if err != nil {
		log.Fatal(err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(response http.ResponseWriter, _ *http.Request) {
		if !worker.Ready() {
			http.Error(response, "checkout messaging is not ready", http.StatusServiceUnavailable)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]bool{"ready": true})
	})
	mux.HandleFunc("/metrics", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/plain; version=0.0.4")
		ready := 0
		if worker.Ready() {
			ready = 1
		}
		_, _ = fmt.Fprintf(response, "boutique_dependency_ready{service=\"checkoutservice\",dependency=\"nats\"} %d\n", ready)
		_, _ = fmt.Fprintln(response, "boutique_dependency_ready{service=\"checkoutservice\",dependency=\"saga_store\"} 1")
	})
	server := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.ListenAndServe() }()
	log.WithFields(logrus.Fields{"port": port, "store": storePath}).Info("checkout saga service started")

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case signal := <-signals:
		log.WithField("signal", signal.String()).Info("shutting down")
	case serveErr := <-serveErrors:
		if serveErr != nil && serveErr != http.ErrServerClosed {
			log.WithError(serveErr).Error("checkout health server stopped")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	if err := worker.Close(); err != nil {
		log.WithError(err).Warn("checkout NATS drain failed")
	}
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return parsed, nil
}
