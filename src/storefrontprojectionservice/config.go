// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
)

func envDuration(name string, fallback time.Duration) (time.Duration, error) {
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

func connectNATS() (*nats.Conn, nats.JetStreamContext, error) {
	url, user, password, caFile := os.Getenv("NATS_URL"), os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD"), os.Getenv("NATS_CA_FILE")
	if url == "" || user == "" || password == "" || caFile == "" {
		return nil, nil, fmt.Errorf("NATS_URL, NATS_USER, NATS_PASSWORD, and NATS_CA_FILE are required")
	}
	connectTimeout, err := envDuration("NATS_CONNECT_TIMEOUT", 2*time.Second)
	if err != nil {
		return nil, nil, err
	}
	reconnectWait, err := envDuration("NATS_RECONNECT_WAIT", 2*time.Second)
	if err != nil {
		return nil, nil, err
	}
	pingInterval, err := envDuration("NATS_PING_INTERVAL", 20*time.Second)
	if err != nil {
		return nil, nil, err
	}
	maxReconnects, maxPings := -1, 2
	if value := os.Getenv("NATS_MAX_RECONNECTS"); value != "" {
		maxReconnects, err = strconv.Atoi(value)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid NATS_MAX_RECONNECTS: %w", err)
		}
	}
	if value := os.Getenv("NATS_MAX_PINGS_OUT"); value != "" {
		maxPings, err = strconv.Atoi(value)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid NATS_MAX_PINGS_OUT: %w", err)
		}
	}
	nc, err := nats.Connect(url,
		nats.Name("storefrontprojectionservice/phase3"),
		nats.UserInfo(user, password),
		nats.RootCAs(caFile),
		nats.Timeout(connectTimeout),
		nats.ReconnectWait(reconnectWait),
		nats.MaxReconnects(maxReconnects),
		nats.PingInterval(pingInterval),
		nats.MaxPingsOutstanding(maxPings),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to NATS: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create JetStream context: %w", err)
	}
	return nc, js, nil
}
