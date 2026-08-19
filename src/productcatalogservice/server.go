// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/profiler"
	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	telemetry "github.com/GoogleCloudPlatform/microservices-demo/src/shared/telemetry/go"
	"github.com/sirupsen/logrus"
)

var (
	log          *logrus.Logger
	catalogMutex = &sync.Mutex{}
)

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
	shutdownTracing, tracingErr := telemetry.Init(context.Background(), "productcatalogservice")
	if tracingErr != nil {
		log.Warnf("failed to start tracer: %+v", tracingErr)
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdownTracing(ctx); err != nil {
				log.Warnf("failed to shut down tracer: %+v", err)
			}
		}()
	}
	if os.Getenv("ENABLE_TRACING") == "1" {
		log.Info("Tracing enabled.")
	} else {
		log.Info("Tracing disabled.")
	}

	if os.Getenv("DISABLE_PROFILER") == "" {
		log.Info("Profiling enabled.")
		go initProfiling("productcatalogservice", "1.0.0")
	} else {
		log.Info("Profiling disabled.")
	}

	catalog := &pb.ListProductsResponse{}
	if err := loadCatalog(catalog); err != nil {
		log.Fatalf("could not parse product catalog: %v", err)
	}

	var catalogNATS atomic.Pointer[catalogEventPublisher]
	ready := func() bool {
		publisher := catalogNATS.Load()
		return len(catalog.Products) > 0 &&
			(!natsIsRequired() || (publisher != nil && publisher.nc.IsConnected()))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(response http.ResponseWriter, _ *http.Request) {
		if !ready() {
			http.Error(response, "catalog publisher is not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = response.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/metrics", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/plain; version=0.0.4")
		natsReady := 0
		publisher := catalogNATS.Load()
		if !natsIsRequired() || (publisher != nil && publisher.nc.IsConnected()) {
			natsReady = 1
		}
		_, _ = fmt.Fprintln(response, "boutique_dependency_ready{service=\"productcatalogservice\",dependency=\"catalog\"} 1")
		_, _ = fmt.Fprintf(response, "boutique_dependency_ready{service=\"productcatalogservice\",dependency=\"nats\"} %d\n", natsReady)
		_, _ = fmt.Fprintf(response, "boutique_catalog_products %d\n", len(catalog.Products))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	server := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	serveErrors := make(chan error, 1)
	go func() {
		log.Infof("product catalog health server listening on :%s", port)
		serveErrors <- server.ListenAndServe()
	}()
	bootstrapStop := make(chan struct{})
	if natsIsRequired() {
		go initializeCatalogPublisher(catalog.Products, &catalogNATS, bootstrapStop)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	select {
	case received := <-signals:
		log.WithField("signal", received.String()).Info("shutting down")
	case serveErr := <-serveErrors:
		if serveErr != nil && serveErr != http.ErrServerClosed {
			log.WithError(serveErr).Error("product catalog health server stopped")
		}
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	close(bootstrapStop)
	_ = server.Shutdown(shutdownContext)
	if publisher := catalogNATS.Swap(nil); publisher != nil {
		publisher.drain()
	}
}

func initializeCatalogPublisher(
	products []*pb.Product,
	target *atomic.Pointer[catalogEventPublisher],
	stop <-chan struct{},
) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		publisher, err := connectCatalogPublisher()
		if err == nil {
			err = publisher.publishBootstrap(products)
		}
		if err == nil {
			target.Store(publisher)
			return
		}
		if publisher != nil {
			publisher.drain()
		}
		log.WithField("correlation_id", "unknown").
			WithError(err).
			Warn("catalog NATS bootstrap is unavailable; retrying")
		timer := time.NewTimer(time.Second)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
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
