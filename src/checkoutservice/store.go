// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
)

// persistedState is a bounded, order-local transition workspace. It is never
// serialized as one Redis object: the store loads one order and only the
// projections required by that order.
type persistedState struct {
	Orders          map[string]*orderSaga
	Products        map[string]*commonv1.ProductSnapshot
	RemovedProducts map[string]bool
	Carts           map[string]*commonv1.CartSnapshot
	Rates           *eventsv1.CurrencyRatesUpdatedEvent
	CatalogRevision uint64
	Inbox           map[string]time.Time
	Results         []resultMessage
	TransitionTime  time.Time
	DeadlineStart   time.Time
	Input           *commonv1.MessageEnvelope
}

// resultMessage is the exact handler-owned journal entry which is loaded and
// republished on redelivery. Data contains deterministic protobuf bytes.
type resultMessage struct {
	Slot      string `json:"slot"`
	MessageID string `json:"message_id"`
	Subject   string `json:"subject"`
	Data      []byte `json:"data"`
}

type acceptedOrderRecord struct {
	OrderID         string                              `json:"order_id"`
	Cart            *commonv1.CartSnapshot              `json:"cart"`
	Products        []*commonv1.ProductSnapshot         `json:"products"`
	Rates           *eventsv1.CurrencyRatesUpdatedEvent `json:"rates"`
	CatalogRevision uint64                              `json:"catalog_revision"`
	RateRevision    uint64                              `json:"rate_revision"`
	Order           *commonv1.SanitizedOrderSnapshot    `json:"order"`
}

func newPersistedState(at time.Time) *persistedState {
	return &persistedState{
		Orders:          make(map[string]*orderSaga),
		Products:        make(map[string]*commonv1.ProductSnapshot),
		RemovedProducts: make(map[string]bool),
		Carts:           make(map[string]*commonv1.CartSnapshot),
		Inbox:           make(map[string]time.Time),
		Results:         make([]resultMessage, 0, 4),
		TransitionTime:  at.UTC(),
		DeadlineStart:   at.UTC(),
	}
}

func (state *persistedState) deadlineAfter(timeout time.Duration) time.Time {
	base := state.DeadlineStart
	if base.IsZero() {
		base = state.TransitionTime
	}
	return base.Add(timeout)
}

func (state *persistedState) setCatalogRevision(revision uint64) {
	state.CatalogRevision = revision
}

func (state *persistedState) setRates(rates *eventsv1.CurrencyRatesUpdatedEvent) {
	state.Rates = rates
}

func (state *persistedState) setProduct(productID string, product *commonv1.ProductSnapshot) {
	state.Products[productID] = product
}

func (state *persistedState) deleteProduct(productID string) {
	delete(state.Products, productID)
}

func (state *persistedState) setCart(userID string, cart *commonv1.CartSnapshot) {
	state.Carts[userID] = cart
}

func (state *persistedState) setOrder(orderID string, order *orderSaga) {
	state.Orders[orderID] = order
}

func (state *persistedState) markOrder(string) {}

func (state *persistedState) setInbox(messageID string, receivedAt time.Time) {
	state.Inbox[messageID] = receivedAt
}

func (state *persistedState) addResult(message resultMessage) {
	state.Results = append(state.Results, message)
}
