// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"testing"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	"google.golang.org/protobuf/proto"
)

func TestCheckoutWorkflowConsumerFilters(t *testing.T) {
	if !checkoutConsumerFiltersMatch("", checkoutShippingSagaSubjects, checkoutShippingSagaSubjects) {
		t.Fatal("shipping saga filters do not match themselves")
	}
	if checkoutConsumerFiltersMatch("boutique.evt.shipping.>", nil, checkoutShippingSagaSubjects) {
		t.Fatal("legacy broad shipping filter unexpectedly matches optimized filters")
	}
	if !checkoutConsumerFiltersMatch(
		checkoutPaymentSagaSubjects[0],
		nil,
		[]string{checkoutPaymentSagaSubjects[0]},
	) {
		t.Fatal("single-subject consumer filter did not match")
	}
	for _, subject := range append(
		append([]string(nil), checkoutShippingSagaSubjects...),
		checkoutPaymentSagaSubjects...,
	) {
		if !isCheckoutSagaEvent(subject) {
			t.Errorf("configured workflow subject %q is not handled", subject)
		}
	}
	if isCheckoutSagaEvent("boutique.evt.shipping.cart-quote-updated.v1") {
		t.Fatal("cart quote event unexpectedly enters the order saga")
	}
}

func TestCheckoutMessageGroupUsesAggregateIdentity(t *testing.T) {
	encoded, err := proto.Marshal(&commonv1.MessageEnvelope{
		MessageId:     "message-1",
		CorrelationId: "correlation-1",
		AggregateType: "order",
		AggregateId:   "order-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := checkoutMessageGroup(encoded), "order\x00order-1"; got != want {
		t.Fatalf("message group = %q, want %q", got, want)
	}
}
