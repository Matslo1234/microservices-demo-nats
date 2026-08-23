// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type advisoryJetStreamStub struct {
	nats.JetStreamContext
	addedStream string
	addedConfig *nats.ConsumerConfig
}

func (stub *advisoryJetStreamStub) ConsumerInfo(string, string, ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	return nil, nats.ErrConsumerNotFound
}

func (stub *advisoryJetStreamStub) AddConsumer(stream string, config *nats.ConsumerConfig, _ ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	copy := *config
	stub.addedStream = stream
	stub.addedConfig = &copy
	return &nats.ConsumerInfo{Config: copy}, nil
}

func TestAdvisoryConsumerIsCreatedBeforeBinding(t *testing.T) {
	stub := &advisoryJetStreamStub{}
	if err := ensureAdvisoryConsumer(stub); err != nil {
		t.Fatal(err)
	}
	config := stub.addedConfig
	if stub.addedStream != advisoryStream || config == nil {
		t.Fatalf("advisory consumer was not added to %s", advisoryStream)
	}
	if config.Durable != advisoryDurable ||
		config.FilterSubject != advisorySubject ||
		config.DeliverPolicy != nats.DeliverAllPolicy ||
		config.AckPolicy != nats.AckExplicitPolicy ||
		config.AckWait != 30*time.Second ||
		config.MaxDeliver != -1 ||
		config.MaxAckPending != 256 {
		t.Fatalf("unexpected advisory consumer config: %+v", *config)
	}
}
