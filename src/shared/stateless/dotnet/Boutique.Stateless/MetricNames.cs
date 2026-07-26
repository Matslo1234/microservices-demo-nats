// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

namespace Boutique.Stateless;

public static class MetricNames
{
    public const string HandlerOutcomes = "boutique_handler_outcomes_total";
    public const string StateConflicts = "boutique_state_conflicts_total";
    public const string HandlerRedeliveries = "boutique_handler_redeliveries_total";
    public const string ClaimOutcomes = "boutique_claim_outcomes_total";
    public const string ClaimLeaseAgeSeconds = "boutique_claim_lease_age_seconds";
    public const string ResultRepublishes = "boutique_result_republishes_total";
}
