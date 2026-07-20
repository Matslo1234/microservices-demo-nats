// Copyright 2018 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/profiler"
	"github.com/sirupsen/logrus"
)

var log *logrus.Logger

func init() {
	log = logrus.New()
	log.Level = logrus.DebugLevel
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.Out = os.Stdout
}

func main() {
	if os.Getenv("DISABLE_PROFILER") == "" {
		log.Info("Profiling enabled.")
		go initProfiling("shippingservice", "1.0.0")
	} else {
		log.Info("Profiling disabled.")
	}

	eventWorker, err := startShippingEvents()
	if err != nil {
		log.Fatalf("failed to start shipping NATS consumers: %v", err)
	}
	defer eventWorker.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(response http.ResponseWriter, _ *http.Request) {
		if !eventWorker.Ready() {
			http.Error(response, "shipping NATS consumers are not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = response.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/metrics", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/plain; version=0.0.4")
		ready := 0
		if eventWorker.Ready() {
			ready = 1
		}
		_, _ = fmt.Fprintf(response, "boutique_dependency_ready{service=\"shippingservice\",dependency=\"nats\"} %d\n", ready)
		providerReady := 0
		if eventWorker == nil || eventWorker.provider != nil {
			providerReady = 1
		}
		_, _ = fmt.Fprintf(response, "boutique_dependency_ready{service=\"shippingservice\",dependency=\"provider_store\"} %d\n", providerReady)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	server := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	serveErrors := make(chan error, 1)
	go func() {
		log.Infof("shipping health server listening on :%s", port)
		serveErrors <- server.ListenAndServe()
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case received := <-signals:
		log.WithField("signal", received.String()).Info("shutting down")
	case serveErr := <-serveErrors:
		if serveErr != nil && serveErr != http.ErrServerClosed {
			log.WithError(serveErr).Error("shipping health server stopped")
		}
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownContext)
}

func initProfiling(service, version string) {
	for attempt := 1; attempt <= 3; attempt++ {
		if err := profiler.Start(profiler.Config{Service: service, ServiceVersion: version}); err != nil {
			log.Warnf("failed to start profiler: %+v", err)
		} else {
			log.Info("started Stackdriver profiler")
			return
		}
		time.Sleep(time.Second * 10 * time.Duration(attempt))
	}
	log.Warn("could not initialize Stackdriver profiler after retrying, giving up")
}
