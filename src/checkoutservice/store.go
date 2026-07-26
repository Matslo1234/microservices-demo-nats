// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
)

type persistedState struct {
	Orders          map[string]*orderSaga                `json:"orders"`
	Products        map[string]*commonv1.ProductSnapshot `json:"products"`
	Carts           map[string]*commonv1.CartSnapshot    `json:"carts"`
	Rates           *eventsv1.CurrencyRatesUpdatedEvent  `json:"rates,omitempty"`
	CatalogRevision uint64                               `json:"catalog_revision"`
	Inbox           map[string]time.Time                 `json:"inbox"`
	Outbox          map[string]outboxMessage             `json:"outbox"`
	changes         *stateChanges
}

type outboxMessage struct {
	MessageID string `json:"message_id"`
	Subject   string `json:"subject"`
	Data      []byte `json:"data"`
}

type stateChanges struct {
	metadata bool
	products map[string]struct{}
	carts    map[string]struct{}
	orders   map[string]struct{}
	inbox    map[string]struct{}
	outbox   map[string]struct{}
}

func newStateChanges() *stateChanges {
	return &stateChanges{
		products: make(map[string]struct{}),
		carts:    make(map[string]struct{}),
		orders:   make(map[string]struct{}),
		inbox:    make(map[string]struct{}),
		outbox:   make(map[string]struct{}),
	}
}

func (changes *stateChanges) empty() bool {
	return !changes.metadata && len(changes.products) == 0 && len(changes.carts) == 0 &&
		len(changes.orders) == 0 && len(changes.inbox) == 0 && len(changes.outbox) == 0
}

func newPersistedState() *persistedState {
	return &persistedState{
		Orders: make(map[string]*orderSaga), Products: make(map[string]*commonv1.ProductSnapshot),
		Carts: make(map[string]*commonv1.CartSnapshot), Inbox: make(map[string]time.Time),
		Outbox: make(map[string]outboxMessage),
	}
}

func (state *persistedState) normalize() {
	if state.Orders == nil {
		state.Orders = make(map[string]*orderSaga)
	}
	if state.Products == nil {
		state.Products = make(map[string]*commonv1.ProductSnapshot)
	}
	if state.Carts == nil {
		state.Carts = make(map[string]*commonv1.CartSnapshot)
	}
	if state.Inbox == nil {
		state.Inbox = make(map[string]time.Time)
	}
	if state.Outbox == nil {
		state.Outbox = make(map[string]outboxMessage)
	}
}

func (state *persistedState) setCatalogRevision(revision uint64) {
	state.CatalogRevision = revision
	if state.changes != nil {
		state.changes.metadata = true
	}
}

func (state *persistedState) setRates(rates *eventsv1.CurrencyRatesUpdatedEvent) {
	state.Rates = rates
	if state.changes != nil {
		state.changes.metadata = true
	}
}

func (state *persistedState) setProduct(productID string, product *commonv1.ProductSnapshot) {
	state.Products[productID] = product
	if state.changes != nil {
		state.changes.products[productID] = struct{}{}
	}
}

func (state *persistedState) deleteProduct(productID string) {
	delete(state.Products, productID)
	if state.changes != nil {
		state.changes.products[productID] = struct{}{}
	}
}

func (state *persistedState) setCart(userID string, cart *commonv1.CartSnapshot) {
	state.Carts[userID] = cart
	if state.changes != nil {
		state.changes.carts[userID] = struct{}{}
	}
}

func (state *persistedState) setOrder(orderID string, order *orderSaga) {
	state.Orders[orderID] = order
	state.markOrder(orderID)
}

func (state *persistedState) markOrder(orderID string) {
	if state.changes != nil {
		state.changes.orders[orderID] = struct{}{}
	}
}

func (state *persistedState) setInbox(messageID string, receivedAt time.Time) {
	state.Inbox[messageID] = receivedAt
	if state.changes != nil {
		state.changes.inbox[messageID] = struct{}{}
	}
}

func (state *persistedState) setOutbox(message outboxMessage) {
	state.Outbox[message.MessageID] = message
	if state.changes != nil {
		state.changes.outbox[message.MessageID] = struct{}{}
	}
}

func (state *persistedState) deleteOutbox(messageID string) {
	delete(state.Outbox, messageID)
	if state.changes != nil {
		state.changes.outbox[messageID] = struct{}{}
	}
}
