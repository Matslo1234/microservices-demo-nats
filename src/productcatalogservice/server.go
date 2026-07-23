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
	"syscall"
	"time"

	"cloud.google.com/go/profiler"
	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var (
	log          *logrus.Logger
	catalogNATS  *catalogEventPublisher
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
	if os.Getenv("ENABLE_TRACING") == "1" {
		if err := initTracing(); err != nil {
			log.Warnf("failed to start tracer: %+v", err)
		}
	} else {
		log.Info("Tracing disabled.")
	}

	if os.Getenv("DISABLE_PROFILER") == "" {
		log.Info("Profiling enabled.")
		go initProfiling("productcatalogservice", "1.0.0")
	} else {
		log.Info("Profiling disabled.")
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))
	catalog := &pb.ListProductsResponse{}
	if err := loadCatalog(catalog); err != nil {
		log.Fatalf("could not parse product catalog: %v", err)
	}
	if natsIsRequired() {
		var err error
		catalogNATS, err = connectCatalogPublisher()
		if err != nil {
			log.Fatalf("could not initialize required NATS publisher: %v", err)
		}
		if err := catalogNATS.publishBootstrap(catalog.Products); err != nil {
			log.WithField("correlation_id", "unknown").Fatalf("could not publish catalog bootstrap events: %v", err)
		}
	}

	ready := func() bool {
		return len(catalog.Products) > 0 &&
			(!natsIsRequired() || (catalogNATS != nil && catalogNATS.nc.IsConnected()))
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
		if !natsIsRequired() || (catalogNATS != nil && catalogNATS.nc.IsConnected()) {
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
	_ = server.Shutdown(shutdownContext)
	if catalogNATS != nil {
		catalogNATS.drain()
	}
}

func initTracing() error {
	collectorAddr := os.Getenv("COLLECTOR_SERVICE_ADDR")
	if collectorAddr == "" {
		return fmt.Errorf("COLLECTOR_SERVICE_ADDR is required when tracing is enabled")
	}
	ctx := context.Background()
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(collectorAddr), otlptracegrpc.WithInsecure())
	if err != nil {
		return err
	}
	otel.SetTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample())))
	return nil
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
