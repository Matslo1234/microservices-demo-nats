// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	telemetry "github.com/GoogleCloudPlatform/microservices-demo/src/shared/telemetry/go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

func main() {
	logLevel := slog.LevelInfo
	if strings.EqualFold(os.Getenv("LOG_LEVEL"), "debug") {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))
	shutdownTracing, tracingErr := telemetry.Init(context.Background(), "storefrontprojectionservice")
	if tracingErr != nil {
		slog.Warn("tracing initialization failed", "error", tracingErr)
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdownTracing(ctx); err != nil {
				slog.Warn("tracing shutdown failed", "error", err)
			}
		}()
	}

	var ready atomic.Bool
	var activeProjector atomic.Pointer[projector]

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) { _, _ = response.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", func(response http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(response, "not ready", http.StatusServiceUnavailable)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]bool{"ready": true})
	})
	mux.HandleFunc("/metrics", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/plain; version=0.0.4")
		connected := 0
		if ready.Load() {
			connected = 1
		}
		_, _ = fmt.Fprintf(response, "boutique_dependency_ready{service=\"storefrontprojectionservice\",dependency=\"nats\"} %d\n", connected)
		kvReady := 0
		if current := activeProjector.Load(); current != nil {
			kvReady = 1
			labels := fmt.Sprintf(
				"region=\"%s\",k8s_cluster=\"%s\",nats_cluster=\"%s\",stream_owner_region=\"%s\",stream=\"%s\",consumer=\"%s\"",
				current.config.regionID, current.config.k8sClusterName, current.config.natsClusterName, current.config.streamOwnerRegion,
				current.config.eventStream, current.config.durable,
			)
			_, _ = fmt.Fprintf(response, "boutique_projection_kv_conflict_retries_total %d\n", current.kvConflictRetries.Load())
			_, _ = fmt.Fprintf(response, "boutique_projection_stale_events_total %d\n", current.staleEventSkips.Load())
			_, _ = fmt.Fprintf(response, "boutique_storefront_query_revision %d\n", current.queryRevision.Load())
			_, _ = fmt.Fprintf(response, "boutique_projection_age_seconds{%s} %.6f\n", labels, current.projectionAgeSeconds(time.Now()))
			_, _ = fmt.Fprintf(response, "boutique_projection_consumer_pending{%s} %d\n", labels, current.consumerPending.Load())
			_, _ = fmt.Fprintf(response, "boutique_projection_consumer_ack_pending{%s} %d\n", labels, current.consumerAckPending.Load())
			_, _ = fmt.Fprintf(response, "boutique_projection_last_event_unixtime{%s} %.9f\n", labels, float64(current.lastProjectedUnix.Load())/1e9)
			_, _ = fmt.Fprintf(response,
				"boutique_projection_identity_info{region=\"%s\",consumer=\"%s\",products_bucket=\"%s\",carts_bucket=\"%s\",context_bucket=\"%s\",orders_bucket=\"%s\",operations_bucket=\"%s\"} 1\n",
				current.config.regionID, current.config.durable, current.config.productsBucket,
				current.config.cartsBucket, current.config.contextBucket, current.config.ordersBucket,
				current.config.operationsBucket,
			)
			if current.catalog != nil {
				_, _ = fmt.Fprintf(response, "boutique_storefront_catalog_cache_hits_total %d\n", current.catalog.hits.Load())
				_, _ = fmt.Fprintf(response, "boutique_storefront_catalog_cache_misses_total %d\n", current.catalog.misses.Load())
			}
		}
		_, _ = fmt.Fprintf(response, "boutique_dependency_ready{service=\"storefrontprojectionservice\",dependency=\"kv\"} %d\n", kvReady)
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	serveErrors := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serveErrors <- err
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	var runtime *projectionRuntime
	for runtime == nil {
		select {
		case received := <-signals:
			log.Printf("received %s while dependencies were unavailable", received)
			_ = server.Close()
			return
		case serveErr := <-serveErrors:
			log.Printf("HTTP server failed: %v", serveErr)
			return
		default:
		}
		var err error
		runtime, err = initializeProjectionRuntime(&ready)
		if err != nil {
			log.Printf("storefront projection dependencies are unavailable; retrying: %v", err)
			time.Sleep(time.Second)
		}
	}
	activeProjector.Store(runtime.projector)
	ready.Store(true)
	log.Printf(
		"storefront projection consumer established (region=%s k8s_cluster=%s nats_cluster=%s stream=%s durable=%s rebuilding=%t query_subscriptions=%d)",
		runtime.projector.config.regionID,
		runtime.projector.config.k8sClusterName,
		runtime.projector.config.natsClusterName,
		runtime.projector.config.eventStream,
		runtime.projector.config.durable,
		runtime.rebuilding,
		runtime.queryEndpointCount,
	)

	select {
	case <-signals:
	case serveErr := <-serveErrors:
		log.Printf("HTTP server failed: %v", serveErr)
	}
	ready.Store(false)
	activeProjector.Store(nil)
	close(runtime.stop)
	_ = server.Close()
	if err := runtime.queryService.Stop(); err != nil {
		log.Printf("NATS query service drain failed: %v", err)
	}
	runtime.projector.close()
	if err := runtime.nc.Drain(); err != nil {
		log.Printf("NATS drain failed: %v", err)
	}
}

type projectionRuntime struct {
	nc                 *nats.Conn
	projector          *projector
	subscription       *nats.Subscription
	queryService       micro.Service
	queryEndpointCount int
	rebuilding         bool
	stop               chan struct{}
}

func initializeProjectionRuntime(ready *atomic.Bool) (*projectionRuntime, error) {
	config, err := loadProjectionConfig()
	if err != nil {
		return nil, err
	}
	nc, js, err := connectNATS(config)
	if err != nil {
		return nil, err
	}
	projector, err := newProjector(js, config)
	if err != nil {
		nc.Close()
		return nil, err
	}
	projector.publishLive = nc.Publish
	subscription, rebuilding, err := projector.subscribe()
	if err != nil {
		projector.close()
		nc.Close()
		return nil, err
	}
	queryService, queryEndpointCount, err := projector.registerQueries(nc)
	if err != nil {
		_ = subscription.Unsubscribe()
		projector.close()
		nc.Close()
		return nil, err
	}
	nc.SetDisconnectErrHandler(func(_ *nats.Conn, disconnectErr error) {
		log.Printf("NATS disconnected: %v", disconnectErr)
		ready.Store(false)
	})
	nc.SetReconnectHandler(func(_ *nats.Conn) {
		ready.Store(false)
		go func() {
			if err := projector.waitForInitialReplay(config.catchupTimeout); err != nil {
				log.Printf("NATS reconnected but projection catch-up is incomplete: %v", err)
				return
			}
			ready.Store(true)
		}()
	})
	runtime := &projectionRuntime{
		nc: nc, projector: projector, subscription: subscription,
		queryService: queryService, queryEndpointCount: queryEndpointCount,
		rebuilding: rebuilding, stop: make(chan struct{}),
	}
	go runtime.projector.run(runtime.subscription, runtime.stop)
	if err := projector.waitForInitialReplay(config.catchupTimeout); err != nil {
		close(runtime.stop)
		_ = runtime.queryService.Stop()
		_ = runtime.subscription.Unsubscribe()
		projector.close()
		nc.Close()
		return nil, err
	}
	return runtime, nil
}
