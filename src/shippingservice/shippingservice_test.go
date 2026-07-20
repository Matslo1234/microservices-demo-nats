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
	"path/filepath"
	"regexp"
	"testing"

	commandsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/commands/v1"
	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestShippingCommandOutcomesAreStableAndPersistent(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "provider.json")
	store, err := openShippingProviderStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	command := &commandsv1.ShippingCreateShipmentCommand{CommandId: "ship-command", OrderId: "order-1", IdempotencyKey: "order-1/shipment"}
	wrapped, err := anypb.New(command)
	if err != nil {
		t.Fatal(err)
	}
	envelope := &commonv1.MessageEnvelope{MessageId: "ship-command", AggregateId: "order-1", AggregateVersion: 3, CorrelationId: "order-1", Data: wrapped}
	first, err := buildShippingOutcome("boutique.cmd.shipping.create-shipment.v1", envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.record(envelope.MessageId, first); err != nil {
		t.Fatal(err)
	}
	reopened, err := openShippingProviderStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	second, ok := reopened.outcome(envelope.MessageId)
	if !ok || first.MessageID != second.MessageID || string(first.Data) != string(second.Data) {
		t.Fatal("stored provider outcome changed after restart")
	}
}

func TestShippingFailureInjectionCoversEveryProviderStep(t *testing.T) {
	tests := []struct {
		mode, subject, expected string
		command                 proto.Message
	}{
		{"quote", "boutique.cmd.shipping.calculate-order-quote.v1", "boutique.evt.shipping.order-quote-failed.v1", &commandsv1.ShippingCalculateOrderQuoteCommand{OrderId: "order-q", Cart: &commonv1.CartSnapshot{}}},
		{"shipment", "boutique.cmd.shipping.create-shipment.v1", "boutique.evt.shipping.shipment-creation-failed.v1", &commandsv1.ShippingCreateShipmentCommand{OrderId: "order-s"}},
		{"cancel", "boutique.cmd.shipping.cancel-shipment.v1", "boutique.evt.shipping.shipment-cancellation-failed.v1", &commandsv1.ShippingCancelShipmentCommand{OrderId: "order-c", ShipmentId: "shipment-c"}},
	}
	for _, test := range tests {
		t.Run(test.mode, func(t *testing.T) {
			t.Setenv("SHIPPING_FAILURE_MODE", test.mode)
			wrapped, err := anypb.New(test.command)
			if err != nil {
				t.Fatal(err)
			}
			envelope := &commonv1.MessageEnvelope{MessageId: "command-" + test.mode, AggregateId: "order-" + test.mode, Data: wrapped}
			outcome, err := buildShippingOutcome(test.subject, envelope)
			if err != nil {
				t.Fatal(err)
			}
			if outcome.Subject != test.expected {
				t.Fatalf("subject = %s, want %s", outcome.Subject, test.expected)
			}
		})
	}
}

// TestTrackingIdFormat verifies the tracking ID matches the expected pattern.
func TestTrackingIdFormat(t *testing.T) {
	pattern := regexp.MustCompile(`^[A-Z]{2}-\d+-\d+$`)

	for i := 0; i < 20; i++ {
		id := CreateTrackingId("test-salt-value")
		if !pattern.MatchString(id) {
			t.Errorf("CreateTrackingId: '%s' does not match expected pattern '[A-Z]{2}-\\d+-\\d+'", id)
		}
	}
}

// TestTrackingIdUniqueness checks that generated IDs are not all identical.
func TestTrackingIdUniqueness(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 50; i++ {
		id := CreateTrackingId("same-salt")
		seen[id] = true
	}
	if len(seen) < 2 {
		t.Errorf("CreateTrackingId: expected unique IDs but got %d distinct values out of 50", len(seen))
	}
}

// TestCreateQuoteFromFloat verifies quote creation from float values.
func TestCreateQuoteFromFloat(t *testing.T) {
	tests := []struct {
		name    string
		value   float64
		dollars uint32
		cents   uint32
	}{
		{"zero", 0.0, 0, 0},
		{"whole dollars", 10.0, 10, 0},
		{"with cents", 8.99, 8, 99},
		{"small value", 0.50, 0, 50},
		{"large value", 100.01, 100, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := CreateQuoteFromFloat(tc.value)
			if q.Dollars != tc.dollars || q.Cents != tc.cents {
				t.Errorf("CreateQuoteFromFloat(%v) = $%d.%d, want $%d.%d",
					tc.value, q.Dollars, q.Cents, tc.dollars, tc.cents)
			}
		})
	}
}

// TestCreateQuoteFromCount verifies count-based quote generation.
func TestCreateQuoteFromCount(t *testing.T) {
	zeroQuote := CreateQuoteFromCount(0)
	if zeroQuote.Dollars != 0 || zeroQuote.Cents != 0 {
		t.Errorf("CreateQuoteFromCount(0) = %s, want $0.0", zeroQuote)
	}

	nonZeroQuote := CreateQuoteFromCount(5)
	if nonZeroQuote.Dollars == 0 && nonZeroQuote.Cents == 0 {
		t.Error("CreateQuoteFromCount(5) returned zero, expected a non-zero quote")
	}
}

func TestShippingMessageIDIsStablePerCause(t *testing.T) {
	first := shippingMessageID(shippingQuoteSubject, "cause-1")
	if first != shippingMessageID(shippingQuoteSubject, "cause-1") {
		t.Fatal("message ID changed for the same causal event")
	}
	if first == shippingMessageID(shippingQuoteSubject, "cause-2") {
		t.Fatal("different causal events received the same message ID")
	}
}

// TestGetRandomLetterCode verifies the output is a valid uppercase letter.
func TestGetRandomLetterCode(t *testing.T) {
	for i := 0; i < 100; i++ {
		code := getRandomLetterCode()
		if code < 65 || code > 90 {
			t.Errorf("getRandomLetterCode: got %d (%c), expected range 65-90 (A-Z)", code, code)
		}
	}
}

// TestGetRandomNumber verifies the output has the correct number of digits.
func TestGetRandomNumber(t *testing.T) {
	for _, digits := range []int{1, 3, 5, 7, 10} {
		result := getRandomNumber(digits)
		if len(result) != digits {
			t.Errorf("getRandomNumber(%d) = '%s' (len %d), expected length %d",
				digits, result, len(result), digits)
		}
	}
}

// TestQuoteString verifies the string representation of a Quote.
func TestQuoteString(t *testing.T) {
	q := Quote{Dollars: 8, Cents: 99}
	expected := "$8.99"
	if q.String() != expected {
		t.Errorf("Quote.String() = '%s', want '%s'", q.String(), expected)
	}
}
