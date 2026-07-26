// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import "testing"

func TestProjectionSubjectFiltering(t *testing.T) {
	handled := []string{
		"boutique.evt.catalog.product-upserted.v1",
		"boutique.evt.currency.rates-updated.v1",
		"boutique.evt.cart.item-added.v1",
		"boutique.evt.storefront.operation-accepted.v1",
		"boutique.evt.recommendation.generated.v1",
		"boutique.evt.ad.selection-generated.v1",
		"boutique.evt.shipping.cart-quote-updated.v1",
		"boutique.evt.order.completed.v1",
		"boutique.evt.notification.order-confirmation-sent.v1",
	}
	for _, subject := range handled {
		if !projectionHandlesSubject(subject) {
			t.Errorf("projection subject %q is not handled", subject)
		}
	}
	for _, subject := range []string{
		"boutique.evt.payment.authorized.v1",
		"boutique.evt.shipping.shipment-created.v1",
		"boutique.evt.storefront.page-viewed.v1",
	} {
		if projectionHandlesSubject(subject) {
			t.Errorf("irrelevant subject %q is handled", subject)
		}
	}
	if !projectionFiltersMatch("", projectionFilterSubjects) {
		t.Fatal("configured projection filters do not match themselves")
	}
	if projectionFiltersMatch("boutique.evt.>", nil) {
		t.Fatal("legacy catch-all filter unexpectedly matches")
	}
}
