// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	regionIDPattern    = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
	assetNamePattern   = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)
	durableNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

type projectionConfig struct {
	regionID            string
	regionKey           string
	k8sClusterName      string
	natsClusterName     string
	streamOwnerRegion   string
	eventStream         string
	durable             string
	productsBucket      string
	cartsBucket         string
	contextBucket       string
	ordersBucket        string
	operationsBucket    string
	livePrefix          string
	catchupTimeout      time.Duration
	queryConcurrency    int
	cartCacheEntries    int
	contextCacheEntries int
}

func requiredEnvironment(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func loadProjectionConfig() (projectionConfig, error) {
	values := map[string]string{}
	for _, name := range []string{
		"REGION_ID", "REGION_KEY", "K8S_CLUSTER_NAME", "NATS_CLUSTER_NAME", "STOREFRONT_EVENT_STREAM",
		"STOREFRONT_PROJECTION_DURABLE", "STOREFRONT_PRODUCTS_BUCKET",
		"STOREFRONT_CARTS_BUCKET", "STOREFRONT_CONTEXT_BUCKET",
		"STOREFRONT_ORDERS_BUCKET", "STOREFRONT_OPERATIONS_BUCKET",
		"LIVE_OPERATION_PREFIX",
	} {
		value, err := requiredEnvironment(name)
		if err != nil {
			return projectionConfig{}, err
		}
		values[name] = value
	}
	if !regionIDPattern.MatchString(values["REGION_ID"]) {
		return projectionConfig{}, fmt.Errorf("REGION_ID must be a stable lower-case DNS label")
	}
	wantedRegionKey := strings.ToUpper(strings.ReplaceAll(values["REGION_ID"], "-", "_"))
	if values["REGION_KEY"] != wantedRegionKey {
		return projectionConfig{}, fmt.Errorf("REGION_KEY must equal %q", wantedRegionKey)
	}
	if !regionIDPattern.MatchString(values["K8S_CLUSTER_NAME"]) {
		return projectionConfig{}, fmt.Errorf("K8S_CLUSTER_NAME must be a lower-case DNS label")
	}
	if !durableNamePattern.MatchString(values["NATS_CLUSTER_NAME"]) {
		return projectionConfig{}, fmt.Errorf("NATS_CLUSTER_NAME must be a safe cluster name")
	}
	for _, name := range []string{
		"STOREFRONT_EVENT_STREAM", "STOREFRONT_PRODUCTS_BUCKET",
		"STOREFRONT_CARTS_BUCKET", "STOREFRONT_CONTEXT_BUCKET",
		"STOREFRONT_ORDERS_BUCKET", "STOREFRONT_OPERATIONS_BUCKET",
	} {
		if !assetNamePattern.MatchString(values[name]) {
			return projectionConfig{}, fmt.Errorf("%s is not a safe NATS asset name", name)
		}
		if name != "STOREFRONT_EVENT_STREAM" && !strings.HasSuffix(values[name], "_"+wantedRegionKey) {
			return projectionConfig{}, fmt.Errorf("%s must end in _%s", name, wantedRegionKey)
		}
	}
	if !durableNamePattern.MatchString(values["STOREFRONT_PROJECTION_DURABLE"]) ||
		!strings.Contains(values["STOREFRONT_PROJECTION_DURABLE"], values["REGION_ID"]) {
		return projectionConfig{}, fmt.Errorf("STOREFRONT_PROJECTION_DURABLE must be safe and contain REGION_ID")
	}
	wantedPrefix := "boutique.live.operation." + values["REGION_ID"] + "."
	if values["LIVE_OPERATION_PREFIX"] != wantedPrefix {
		return projectionConfig{}, fmt.Errorf("LIVE_OPERATION_PREFIX must equal %q", wantedPrefix)
	}
	catchupTimeout, err := envDuration("STOREFRONT_INITIAL_REPLAY_TIMEOUT", 10*time.Minute)
	if err != nil {
		return projectionConfig{}, err
	}
	queryConcurrency, err := boundedEnvInt("STOREFRONT_QUERY_CONCURRENCY", 8, 1, 64)
	if err != nil {
		return projectionConfig{}, err
	}
	cartCacheEntries, err := boundedEnvInt("STOREFRONT_CART_CACHE_ENTRIES", 32768, 1, 262144)
	if err != nil {
		return projectionConfig{}, err
	}
	contextCacheEntries, err := boundedEnvInt("STOREFRONT_CONTEXT_CACHE_ENTRIES", 65536, 1, 524288)
	if err != nil {
		return projectionConfig{}, err
	}
	ownerRegion := strings.TrimSpace(os.Getenv("STREAM_OWNER_REGION"))
	if ownerRegion == "" {
		ownerRegion = values["REGION_ID"]
	}
	if !regionIDPattern.MatchString(ownerRegion) {
		return projectionConfig{}, fmt.Errorf("STREAM_OWNER_REGION must be a stable lower-case DNS label")
	}
	return projectionConfig{
		regionID:            values["REGION_ID"],
		regionKey:           values["REGION_KEY"],
		k8sClusterName:      values["K8S_CLUSTER_NAME"],
		natsClusterName:     values["NATS_CLUSTER_NAME"],
		streamOwnerRegion:   ownerRegion,
		eventStream:         values["STOREFRONT_EVENT_STREAM"],
		durable:             values["STOREFRONT_PROJECTION_DURABLE"],
		productsBucket:      values["STOREFRONT_PRODUCTS_BUCKET"],
		cartsBucket:         values["STOREFRONT_CARTS_BUCKET"],
		contextBucket:       values["STOREFRONT_CONTEXT_BUCKET"],
		ordersBucket:        values["STOREFRONT_ORDERS_BUCKET"],
		operationsBucket:    values["STOREFRONT_OPERATIONS_BUCKET"],
		livePrefix:          values["LIVE_OPERATION_PREFIX"],
		catchupTimeout:      catchupTimeout,
		queryConcurrency:    queryConcurrency,
		cartCacheEntries:    cartCacheEntries,
		contextCacheEntries: contextCacheEntries,
	}, nil
}

func boundedEnvInt(name string, fallback, minimum, maximum int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	if parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d", name, minimum, maximum)
	}
	return parsed, nil
}

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

func connectNATS(config projectionConfig) (*nats.Conn, nats.JetStreamContext, error) {
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
	connectionName := fmt.Sprintf("storefrontprojectionservice/phase3/%s/%s", config.regionID, config.k8sClusterName)
	nc, err := nats.Connect(url,
		nats.Name(connectionName),
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
