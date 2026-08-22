// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import "testing"

func TestLoadFrontendRegionalConfigRequiresSharedCookieKey(t *testing.T) {
	t.Setenv("REGION_ID", "eu-central-1")
	t.Setenv("K8S_CLUSTER_NAME", "boutique-eu1")
	t.Setenv("NATS_CLUSTER_NAME", "BOUTIQUE-eu-central-1")
	t.Setenv("LIVE_OPERATION_PREFIX", "boutique.live.operation.eu-central-1.")
	t.Setenv("FRONTEND_COOKIE_KEY", "")
	if _, err := loadFrontendRegionalConfig(); err == nil {
		t.Fatal("missing shared cookie key was accepted")
	}
	t.Setenv("FRONTEND_COOKIE_KEY", "a-shared-cookie-key-with-at-least-32-bytes")
	config, err := loadFrontendRegionalConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.livePrefix != "boutique.live.operation.eu-central-1." {
		t.Fatalf("unexpected live prefix %q", config.livePrefix)
	}
}
