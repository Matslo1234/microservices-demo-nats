// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

func environmentDuration(name string, fallback time.Duration) (time.Duration, error) {
	if value := os.Getenv(name); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return 0, fmt.Errorf("invalid %s: %w", name, err)
		}
		return parsed, nil
	}
	return fallback, nil
}

func connectNATS() (*nats.Conn, nats.JetStreamContext, error) {
	url, user, password, caFile := os.Getenv("NATS_URL"), os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD"), os.Getenv("NATS_CA_FILE")
	if url == "" || user == "" || password == "" || caFile == "" {
		return nil, nil, errors.New("NATS_URL, NATS_USER, NATS_PASSWORD, and NATS_CA_FILE are required")
	}
	connectTimeout, err := environmentDuration("NATS_CONNECT_TIMEOUT", 2*time.Second)
	if err != nil {
		return nil, nil, err
	}
	reconnectWait, err := environmentDuration("NATS_RECONNECT_WAIT", 2*time.Second)
	if err != nil {
		return nil, nil, err
	}
	maxReconnects := -1
	if value := os.Getenv("NATS_MAX_RECONNECTS"); value != "" {
		maxReconnects, err = strconv.Atoi(value)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid NATS_MAX_RECONNECTS: %w", err)
		}
	}
	nc, err := nats.Connect(url,
		nats.Name("messageoperationsservice/v1"), nats.UserInfo(user, password),
		nats.RootCAs(caFile), nats.Timeout(connectTimeout), nats.ReconnectWait(reconnectWait),
		nats.MaxReconnects(maxReconnects), nats.PingInterval(20*time.Second), nats.MaxPingsOutstanding(2),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to NATS: %w", err)
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create JetStream context: %w", err)
	}
	return nc, js, nil
}

func startAdvisoryConsumer(ctx context.Context, js nats.JetStreamContext, operations *operationsService, logger *slog.Logger) (*nats.Subscription, error) {
	subscription, err := js.PullSubscribe(advisorySubject, advisoryDurable,
		nats.BindStream(advisoryStream), nats.ManualAck(), nats.AckExplicit(),
		nats.DeliverAll(), nats.AckWait(30*time.Second), nats.MaxAckPending(256),
	)
	if err != nil {
		return nil, fmt.Errorf("create advisory consumer: %w", err)
	}
	go func() {
		for ctx.Err() == nil {
			messages, fetchErr := subscription.Fetch(16, nats.MaxWait(time.Second))
			if errors.Is(fetchErr, nats.ErrTimeout) {
				continue
			}
			if fetchErr != nil {
				if ctx.Err() == nil {
					logger.Error("fetch max-delivery advisories", "error", fetchErr)
					time.Sleep(time.Second)
				}
				continue
			}
			for _, message := range messages {
				_, handleErr := operations.HandleAdvisory(ctx, message.Data)
				if errors.Is(handleErr, errInvalidAdvisory) {
					logger.Error("discarding invalid max-delivery advisory", "error", handleErr)
					_ = message.Term()
					continue
				}
				if handleErr != nil {
					logger.Error("dead-letter transfer failed; requesting advisory redelivery", "error", handleErr)
					_ = message.NakWithDelay(time.Second)
					continue
				}
				if ackErr := message.AckSync(); ackErr != nil {
					logger.Error("acknowledge handled max-delivery advisory", "error", ackErr)
				}
			}
		}
	}()
	return subscription, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	adminToken := os.Getenv("ADMIN_API_TOKEN")
	if len(adminToken) < 32 {
		logger.Error("ADMIN_API_TOKEN must contain at least 32 characters")
		os.Exit(1)
	}
	adminUser := os.Getenv("ADMIN_USER")
	if adminUser == "" {
		adminUser = "admin"
	}
	nc, js, err := connectNATS()
	if err != nil {
		logger.Error("NATS startup failed", "error", err)
		os.Exit(1)
	}
	defer nc.Close()
	kv, err := js.KeyValue(dlqCaseBucket)
	if err != nil {
		logger.Error("open DLQ case bucket", "error", err)
		os.Exit(1)
	}
	operations := newOperationsService(&natsMessageBroker{js: js}, &kvCaseRepository{kv: kv}, logger)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	subscription, err := startAdvisoryConsumer(ctx, js, operations, logger)
	if err != nil {
		logger.Error("advisory consumer startup failed", "error", err)
		os.Exit(1)
	}
	serverHandler, err := newAdminHTTPServer(operations, logger, adminUser, adminToken, func() bool {
		return nc.IsConnected() && subscription.IsValid()
	})
	if err != nil {
		logger.Error("admin server startup failed", "error", err)
		os.Exit(1)
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	httpServer := &http.Server{
		Addr: ":" + port, Handler: serverHandler.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 16 << 10,
	}
	serveErrors := make(chan error, 1)
	go func() {
		logger.Info("message operations service ready", "port", port)
		serveErrors <- httpServer.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		logger.Info("shutting down message operations service")
	case serveErr := <-serveErrors:
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Error("admin HTTP server stopped", "error", serveErr)
		}
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownContext)
	_ = subscription.Drain()
	_ = nc.Drain()
}
