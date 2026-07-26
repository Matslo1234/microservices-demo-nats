// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package stateless

// Shared metric names keep dashboards stable while individual handlers migrate.
const (
	MetricHandlerOutcomes      = "boutique_handler_outcomes_total"
	MetricStateConflicts       = "boutique_state_conflicts_total"
	MetricHandlerRedeliveries  = "boutique_handler_redeliveries_total"
	MetricClaimOutcomes        = "boutique_claim_outcomes_total"
	MetricClaimLeaseAgeSeconds = "boutique_claim_lease_age_seconds"
	MetricResultRepublishes    = "boutique_result_republishes_total"
)
