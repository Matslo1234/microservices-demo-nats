// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
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

	var activeProjector atomic.Pointer[projector]
	var activeRuntime atomic.Pointer[projectionRuntime]

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) {
		if current := activeRuntime.Load(); current != nil && !current.healthy() {
			http.Error(response, "runtime cannot recover", http.StatusServiceUnavailable)
			return
		}
		_, _ = response.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(response http.ResponseWriter, _ *http.Request) {
		current := activeRuntime.Load()
		if current == nil || !current.ready() {
			http.Error(response, "not ready", http.StatusServiceUnavailable)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]bool{"ready": true})
	})
	mux.HandleFunc("/metrics", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/plain; version=0.0.4")
		connected := 0
		if current := activeRuntime.Load(); current != nil && current.ready() {
			connected = 1
		}
		_, _ = fmt.Fprintf(response, "boutique_dependency_ready{service=\"storefrontprojectionservice\",dependency=\"nats\"} %d\n", connected)
		kvReady := 0
		if current := activeProjector.Load(); current != nil {
			kvReady = 1
			runtime := activeRuntime.Load()
			projectionEnabled := runtime != nil && runtime.projectionEnabled
			labels := fmt.Sprintf(
				"region=\"%s\",k8s_cluster=\"%s\",nats_cluster=\"%s\",stream_owner_region=\"%s\",stream=\"%s\",consumer=\"%s\"",
				current.config.regionID, current.config.k8sClusterName, current.config.natsClusterName, current.config.streamOwnerRegion,
				current.config.eventStream, current.config.durable,
			)
			if projectionEnabled {
				_, _ = fmt.Fprintf(response, "boutique_projection_kv_conflict_retries_total %d\n", current.kvConflictRetries.Load())
				_, _ = fmt.Fprintf(response, "boutique_projection_stale_events_total %d\n", current.staleEventSkips.Load())
			}
			_, _ = fmt.Fprintf(response, "boutique_storefront_query_revision %d\n", current.queryRevision.Load())
			if projectionEnabled {
				_, _ = fmt.Fprintf(response, "boutique_projection_age_seconds{%s} %.6f\n", labels, current.projectionAgeSeconds(time.Now()))
				_, _ = fmt.Fprintf(response, "boutique_projection_consumer_pending{%s} %d\n", labels, current.consumerPending.Load())
				_, _ = fmt.Fprintf(response, "boutique_projection_consumer_ack_pending{%s} %d\n", labels, current.consumerAckPending.Load())
				_, _ = fmt.Fprintf(response, "boutique_projection_last_event_unixtime{%s} %.9f\n", labels, float64(current.lastProjectedUnix.Load())/1e9)
			}
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
		runtime, err = initializeProjectionRuntime()
		if err != nil {
			log.Printf("storefront projection dependencies are unavailable; retrying: %v", err)
			time.Sleep(time.Second)
		}
	}
	activeProjector.Store(runtime.projector)
	activeRuntime.Store(runtime)
	log.Printf(
		"storefront runtime established (region=%s k8s_cluster=%s nats_cluster=%s stream=%s durable=%s projection=%t queries=%t rebuilding=%t query_subscriptions=%d)",
		runtime.projector.config.regionID,
		runtime.projector.config.k8sClusterName,
		runtime.projector.config.natsClusterName,
		runtime.projector.config.eventStream,
		runtime.projector.config.durable,
		runtime.projectionEnabled,
		runtime.queryEnabled,
		runtime.rebuilding,
		runtime.queryEndpointCount(),
	)

	repairFailures := make(map[string]int)
	repairedAt := make(map[string]time.Time)
	running := true
	for running {
		select {
		case <-signals:
			running = false
		case serveErr := <-serveErrors:
			log.Printf("HTTP server failed: %v", serveErr)
			running = false
		case role := <-runtime.queryStopped:
			// A slow-consumer error stops the complete micro.Service. Keep the
			// projector alive and back off repairs so overload cannot become a
			// tight stop/re-register loop.
			if last := repairedAt[role]; !last.IsZero() && time.Since(last) >= 30*time.Second {
				repairFailures[role] = 0
			}
			for running {
				delay := queryRepairBackoff(repairFailures[role])
				jitter := time.Duration(rand.Int64N(int64(delay/4) + 1))
				timer := time.NewTimer(delay + jitter)
				select {
				case <-signals:
					timer.Stop()
					running = false
					continue
				case serveErr := <-serveErrors:
					timer.Stop()
					log.Printf("HTTP server failed: %v", serveErr)
					running = false
					continue
				case <-timer.C:
				}
				if err := runtime.repairQueryService(role); err != nil {
					repairFailures[role]++
					log.Printf("NATS query service recovery failed role=%q error=%v; backing off", role, err)
					continue
				}
				repairFailures[role]++
				repairedAt[role] = time.Now()
				break
			}
		}
	}
	activeRuntime.Store(nil)
	activeProjector.Store(nil)
	_ = server.Close()
	runtime.close()
}

type queryServiceRuntime struct {
	role          string
	nc            *nats.Conn
	service       micro.Service
	endpointCount int
}

type projectionRuntime struct {
	nc                *nats.Conn
	projector         *projector
	projectionEnabled bool
	queryEnabled      bool
	subscription      *nats.Subscription
	subscriptionMu    sync.RWMutex
	projectionReady   atomic.Bool
	consumerHealthy   atomic.Bool
	queryMu           sync.RWMutex
	queryServices     map[string]*queryServiceRuntime
	queryStopped      chan string
	rebuilding        bool
	stop              chan struct{}
	closeOnce         sync.Once
}

func initializeProjectionRuntime() (*projectionRuntime, error) {
	runtimeRole := strings.ToLower(strings.TrimSpace(os.Getenv("STOREFRONT_RUNTIME_ROLE")))
	if runtimeRole == "" {
		runtimeRole = "combined"
	}
	if runtimeRole != "combined" && runtimeRole != "projection" && runtimeRole != "query" {
		return nil, fmt.Errorf("STOREFRONT_RUNTIME_ROLE must be combined, projection, or query")
	}
	projectionEnabled := runtimeRole != "query"
	queryEnabled := runtimeRole != "projection"
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
	var subscription *nats.Subscription
	rebuilding := false
	if projectionEnabled {
		subscription, rebuilding, err = projector.subscribe()
		if err != nil {
			projector.close()
			nc.Close()
			return nil, err
		}
	}
	runtime := &projectionRuntime{
		nc: nc, projector: projector, subscription: subscription,
		projectionEnabled: projectionEnabled, queryEnabled: queryEnabled,
		queryServices: make(map[string]*queryServiceRuntime),
		queryStopped:  make(chan string, 8), rebuilding: rebuilding,
		stop: make(chan struct{}),
	}
	nc.SetDisconnectErrHandler(func(_ *nats.Conn, disconnectErr error) {
		log.Printf("NATS disconnected: %v", disconnectErr)
		runtime.projectionReady.Store(false)
	})
	nc.SetReconnectHandler(func(_ *nats.Conn) {
		runtime.projectionReady.Store(false)
		if !runtime.projectionEnabled {
			runtime.projectionReady.Store(true)
			return
		}
		go func() {
			if err := projector.waitForInitialReplay(config.catchupTimeout); err != nil {
				log.Printf("NATS reconnected but projection catch-up is incomplete: %v", err)
				return
			}
			runtime.projectionReady.Store(true)
		}()
	})
	if queryEnabled {
		for _, role := range []string{browseQueryRole, trackingQueryRole} {
			queryConnection, err := connectNATSConnection(config, "queries-"+role)
			if err != nil {
				runtime.close()
				return nil, err
			}
			configureQueryConnection(queryConnection, role)
			service, endpointCount, err := projector.registerQueries(queryConnection, role, runtime.queryStopped)
			if err != nil {
				queryConnection.Close()
				runtime.close()
				return nil, err
			}
			runtime.queryServices[role] = &queryServiceRuntime{
				role: role, nc: queryConnection, service: service, endpointCount: endpointCount,
			}
		}
	}
	runtime.consumerHealthy.Store(true)
	if projectionEnabled {
		go runtime.superviseProjectionConsumer(subscription)
		if err := projector.waitForInitialReplay(config.catchupTimeout); err != nil {
			runtime.close()
			return nil, err
		}
	}
	runtime.projectionReady.Store(true)
	return runtime, nil
}

func configureQueryConnection(nc *nats.Conn, role string) {
	nc.SetDisconnectErrHandler(func(_ *nats.Conn, err error) {
		log.Printf("NATS query connection disconnected role=%q error=%v", role, err)
	})
	nc.SetReconnectHandler(func(_ *nats.Conn) {
		log.Printf("NATS query connection reconnected role=%q", role)
	})
}

func (runtime *projectionRuntime) ready() bool {
	if !runtime.projectionReady.Load() || !runtime.consumerHealthy.Load() ||
		runtime.nc == nil || !runtime.nc.IsConnected() {
		return false
	}
	runtime.queryMu.RLock()
	defer runtime.queryMu.RUnlock()
	wantedQueries := 0
	if runtime.queryEnabled {
		wantedQueries = len(queryNamesByRole)
	}
	if len(runtime.queryServices) != wantedQueries {
		return false
	}
	for _, queryRuntime := range runtime.queryServices {
		if queryRuntime.nc == nil || !queryRuntime.nc.IsConnected() ||
			queryRuntime.service == nil || queryRuntime.service.Stopped() {
			return false
		}
	}
	return true
}

func (runtime *projectionRuntime) healthy() bool {
	// Query services and the projection consumer supervise and repair their own
	// transient failures. Readiness exposes those failures; liveness only asks
	// whether the process still owns a runtime capable of recovery.
	return runtime.nc != nil && !runtime.nc.IsClosed()
}

func queryRepairBackoff(failures int) time.Duration {
	if failures < 0 {
		failures = 0
	}
	delay := time.Second
	for i := 0; i < failures && delay < 30*time.Second; i++ {
		delay *= 2
	}
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func (runtime *projectionRuntime) setSubscription(subscription *nats.Subscription) {
	runtime.subscriptionMu.Lock()
	runtime.subscription = subscription
	runtime.subscriptionMu.Unlock()
}

func (runtime *projectionRuntime) currentSubscription() *nats.Subscription {
	runtime.subscriptionMu.RLock()
	defer runtime.subscriptionMu.RUnlock()
	return runtime.subscription
}

func (runtime *projectionRuntime) superviseProjectionConsumer(initial *nats.Subscription) {
	subscription := initial
	needsCatchup := false
	for {
		runtime.setSubscription(subscription)
		if !needsCatchup {
			runtime.consumerHealthy.Store(true)
		}
		runDone := make(chan error, 1)
		go func(current *nats.Subscription) {
			runDone <- runtime.projector.run(current, runtime.stop)
		}(subscription)

		var runErr error
		if needsCatchup {
			catchupDone := make(chan error, 1)
			go func() {
				catchupDone <- runtime.projector.waitForInitialReplay(runtime.projector.config.catchupTimeout)
			}()
			select {
			case <-runtime.stop:
				return
			case runErr = <-runDone:
			case catchupErr := <-catchupDone:
				if catchupErr != nil {
					_ = subscription.Unsubscribe()
					runErr = <-runDone
					log.Printf("projection consumer recovery did not catch up: %v", catchupErr)
				} else {
					runtime.consumerHealthy.Store(true)
					runtime.projectionReady.Store(runtime.nc.IsConnected())
					log.Printf("projection consumer recovered durable=%q", runtime.projector.config.durable)
					select {
					case <-runtime.stop:
						return
					case runErr = <-runDone:
					}
				}
			}
		} else {
			select {
			case <-runtime.stop:
				return
			case runErr = <-runDone:
			}
		}

		runtime.consumerHealthy.Store(false)
		runtime.projectionReady.Store(false)
		select {
		case <-runtime.stop:
			return
		default:
		}
		log.Printf("projection consumer interrupted durable=%q error=%v; rebinding", runtime.projector.config.durable, runErr)
		for {
			timer := time.NewTimer(time.Second)
			select {
			case <-runtime.stop:
				timer.Stop()
				return
			case <-timer.C:
			}
			next, _, err := runtime.projector.subscribe()
			if err != nil {
				log.Printf("projection consumer rebind failed durable=%q error=%v", runtime.projector.config.durable, err)
				continue
			}
			subscription = next
			needsCatchup = true
			break
		}
	}
}

func (runtime *projectionRuntime) queryEndpointCount() int {
	runtime.queryMu.RLock()
	defer runtime.queryMu.RUnlock()
	total := 0
	for _, queryRuntime := range runtime.queryServices {
		total += queryRuntime.endpointCount
	}
	return total
}

func (runtime *projectionRuntime) repairQueryService(role string) error {
	runtime.queryMu.RLock()
	current := runtime.queryServices[role]
	runtime.queryMu.RUnlock()
	if current == nil {
		return fmt.Errorf("query service role %q is not registered", role)
	}
	if !current.service.Stopped() {
		return nil
	}

	queryConnection := current.nc
	createdConnection := false
	if queryConnection.IsClosed() {
		var err error
		queryConnection, err = connectNATSConnection(runtime.projector.config, "queries-"+role)
		if err != nil {
			return err
		}
		createdConnection = true
		configureQueryConnection(queryConnection, role)
	}
	service, endpointCount, err := runtime.projector.registerQueries(queryConnection, role, runtime.queryStopped)
	if err != nil {
		if createdConnection {
			queryConnection.Close()
		}
		return err
	}

	runtime.queryMu.Lock()
	runtime.queryServices[role] = &queryServiceRuntime{
		role: role, nc: queryConnection, service: service, endpointCount: endpointCount,
	}
	runtime.queryMu.Unlock()
	if createdConnection {
		current.nc.Close()
	}
	log.Printf("NATS query service recovered role=%q query_subscriptions=%d", role, endpointCount)
	return nil
}

func (runtime *projectionRuntime) close() {
	runtime.closeOnce.Do(func() {
		runtime.projectionReady.Store(false)
		close(runtime.stop)
		if subscription := runtime.currentSubscription(); subscription != nil {
			_ = subscription.Unsubscribe()
		}
		runtime.queryMu.RLock()
		queryRuntimes := make([]*queryServiceRuntime, 0, len(runtime.queryServices))
		for _, queryRuntime := range runtime.queryServices {
			queryRuntimes = append(queryRuntimes, queryRuntime)
		}
		runtime.queryMu.RUnlock()
		for _, queryRuntime := range queryRuntimes {
			if queryRuntime.service != nil && !queryRuntime.service.Stopped() {
				if err := queryRuntime.service.Stop(); err != nil {
					log.Printf("NATS query service drain failed role=%q error=%v", queryRuntime.role, err)
				}
			}
			if queryRuntime.nc != nil && !queryRuntime.nc.IsClosed() {
				if err := queryRuntime.nc.Drain(); err != nil {
					log.Printf("NATS query connection drain failed role=%q error=%v", queryRuntime.role, err)
				}
			}
		}
		if runtime.projector != nil {
			runtime.projector.close()
		}
		if runtime.nc != nil && !runtime.nc.IsClosed() {
			if err := runtime.nc.Drain(); err != nil {
				log.Printf("NATS drain failed: %v", err)
			}
		}
	})
}
