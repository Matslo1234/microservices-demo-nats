// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	stateless "github.com/GoogleCloudPlatform/microservices-demo/src/shared/stateless/go"
	telemetry "github.com/GoogleCloudPlatform/microservices-demo/src/shared/telemetry/go"
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

// checkoutRuntime starts the health endpoint before dependencies and retries
// initial Redis/NATS failures. Once connected, both clients handle redirects
// and reconnects without making the pod restart.
type checkoutRuntime struct {
	mu      sync.RWMutex
	worker  *checkoutWorker
	stop    chan struct{}
	done    chan struct{}
	closing sync.Once
}

func newCheckoutRuntime() *checkoutRuntime {
	return &checkoutRuntime{stop: make(chan struct{}), done: make(chan struct{})}
}

func (runtime *checkoutRuntime) run(address, prefix string, clustered bool, retention time.Duration) {
	defer close(runtime.done)
	for attempt := 0; ; attempt++ {
		select {
		case <-runtime.stop:
			return
		default:
		}
		store, err := openStateStoreWithRetention(address, prefix, clustered, retention)
		if err == nil {
			var worker *checkoutWorker
			worker, err = startCheckoutWorker(store)
			if err == nil {
				runtime.mu.Lock()
				runtime.worker = worker
				runtime.mu.Unlock()
				log.WithFields(logrus.Fields{
					"store": "redis-cluster", "store_prefix": prefix, "worker_id": worker.workerID,
				}).Info("checkout dependencies are ready")
				select {
				case <-runtime.stop:
					return
				case workerErr := <-worker.failed:
					err = workerErr
					log.WithError(workerErr).Error("checkout consumer lifecycle failed; rebuilding dependencies")
					runtime.mu.Lock()
					if runtime.worker == worker {
						runtime.worker = nil
					}
					runtime.mu.Unlock()
					_ = worker.Close()
				}
			}
			if worker == nil {
				_ = store.Close()
			}
		}
		delay := stateless.Backoff(attempt, 250*time.Millisecond, 10*time.Second)
		log.WithError(err).WithField("retry_in", delay.String()).Warn("checkout dependency initialization failed")
		timer := time.NewTimer(delay)
		select {
		case <-runtime.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (runtime *checkoutRuntime) current() *checkoutWorker {
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	return runtime.worker
}

func (runtime *checkoutRuntime) Ready() bool {
	worker := runtime.current()
	return worker != nil && worker.Ready()
}

func (runtime *checkoutRuntime) Metrics() string {
	worker := runtime.current()
	if worker == nil {
		return "boutique_checkout_transitions_total 0\n" +
			"boutique_checkout_result_republishes_total 0\n" +
			"boutique_checkout_transition_conflicts_total 0\n" +
			"boutique_checkout_deadline_lease_recoveries_total 0\n"
	}
	return worker.Metrics()
}

func (runtime *checkoutRuntime) Close() error {
	var result error
	runtime.closing.Do(func() {
		close(runtime.stop)
		<-runtime.done
		if worker := runtime.current(); worker != nil {
			result = worker.Close()
		}
	})
	return result
}

func main() {
	shutdownTracing, tracingErr := telemetry.Init(context.Background(), "checkoutservice")
	if tracingErr != nil {
		log.WithError(tracingErr).Warn("tracing initialization failed")
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdownTracing(ctx); err != nil {
				log.WithError(err).Warn("tracing shutdown failed")
			}
		}()
	}
	redisAddress := os.Getenv("CHECKOUT_REDIS_ADDR")
	redisPrefix := os.Getenv("CHECKOUT_REDIS_PREFIX")
	if redisPrefix == "" {
		redisPrefix = defaultRedisStatePrefix
	}
	clustered := !strings.EqualFold(os.Getenv("CHECKOUT_REDIS_MODE"), "standalone")
	redisRetention, err := durationEnv("CHECKOUT_REDIS_RETENTION", defaultRedisRetention)
	if err != nil || redisRetention <= 0 {
		if err == nil {
			err = errors.New("retention must be positive")
		}
		log.WithError(err).Fatal("invalid CHECKOUT_REDIS_RETENTION")
	}
	runtime := newCheckoutRuntime()
	go runtime.run(redisAddress, redisPrefix, clustered, redisRetention)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(response http.ResponseWriter, _ *http.Request) {
		if !runtime.Ready() {
			http.Error(response, "checkout dependencies are not ready", http.StatusServiceUnavailable)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]bool{"ready": true})
	})
	mux.HandleFunc("/metrics", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/plain; version=0.0.4")
		ready := 0
		if runtime.Ready() {
			ready = 1
		}
		_, _ = fmt.Fprintf(response,
			"boutique_dependency_ready{service=\"checkoutservice\",dependency=\"nats\"} %d\n"+
				"boutique_dependency_ready{service=\"checkoutservice\",dependency=\"saga_store\"} %d\n%s",
			ready, ready, runtime.Metrics())
	})
	server := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.ListenAndServe() }()
	log.WithFields(logrus.Fields{
		"port": port, "store_prefix": redisPrefix, "redis_cluster": clustered,
		"redis_retention": redisRetention.String(),
	}).Info("checkout saga service started")

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
	if err := runtime.Close(); err != nil {
		log.WithError(err).Warn("checkout shutdown failed")
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
