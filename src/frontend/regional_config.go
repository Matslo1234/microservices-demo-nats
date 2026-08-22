// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"
)

var frontendRegionPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

type frontendRegionalConfig struct {
	regionID        string
	k8sClusterName  string
	natsClusterName string
	livePrefix      string
	cookieKey       string
}

func loadFrontendRegionalConfig() (frontendRegionalConfig, error) {
	config := frontendRegionalConfig{
		regionID:        strings.TrimSpace(os.Getenv("REGION_ID")),
		k8sClusterName:  strings.TrimSpace(os.Getenv("K8S_CLUSTER_NAME")),
		natsClusterName: strings.TrimSpace(os.Getenv("NATS_CLUSTER_NAME")),
		livePrefix:      strings.TrimSpace(os.Getenv("LIVE_OPERATION_PREFIX")),
		cookieKey:       os.Getenv("FRONTEND_COOKIE_KEY"),
	}
	if !frontendRegionPattern.MatchString(config.regionID) {
		return frontendRegionalConfig{}, fmt.Errorf("REGION_ID must be a stable lower-case DNS label")
	}
	if !frontendRegionPattern.MatchString(config.k8sClusterName) {
		return frontendRegionalConfig{}, fmt.Errorf("K8S_CLUSTER_NAME must be a lower-case DNS label")
	}
	if config.natsClusterName == "" || strings.ContainsAny(config.natsClusterName, " \t\r\n") {
		return frontendRegionalConfig{}, fmt.Errorf("NATS_CLUSTER_NAME must be a non-empty token")
	}
	wantedPrefix := "boutique.live.operation." + config.regionID + "."
	if config.livePrefix != wantedPrefix {
		return frontendRegionalConfig{}, fmt.Errorf("LIVE_OPERATION_PREFIX must equal %q", wantedPrefix)
	}
	if len(config.cookieKey) < 32 {
		return frontendRegionalConfig{}, fmt.Errorf("FRONTEND_COOKIE_KEY must contain at least 32 bytes")
	}
	return config, nil
}

func secretFingerprint(secret string) string {
	digest := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(digest[:8])
}
