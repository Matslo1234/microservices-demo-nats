# storefrontprojectionservice

This service builds region-local storefront read models from
critical domain events from `BOUTIQUE_EVENTS` and serves bounded Core NATS
request/reply queries. Recommendation and ad results are consumed separately
from the short-lived, single-replica `BOUTIQUE_PERSONALIZATION` stream.
JetStream
KV remains the authoritative projection store; process-local caches are
disposable accelerators rebuilt from KV watches or reads.

The critical and personalization projections use distinct NATS connections,
durables, and worker pools. Readiness and initial replay wait only for the
critical projection.
Personalization uses eight lanes with at most 256 unacknowledged messages, so
its backlog cannot occupy the critical durable or its 128 projection lanes.

## Query isolation and admission

Browse and tracking endpoints use separate NATS service roles. Checkout also
has a dedicated browse-role subject:

- `boutique.qry.storefront.cart.v1` serves cart pages.
- `boutique.qry.storefront.checkout.v1` serves order preparation.
- `boutique.qry.storefront.operation.v1` and `.order.v1` serve status tracking.

Every subject has its own admission counter. The default maximum is 48 active
handlers per subject per pod. When a subject reaches its limit, the service
immediately returns `{"error":"OVERLOADED","retry_after_seconds":1}`. NATS
endpoint pending-message and pending-byte limits provide an additional bounded
transport queue.

Relevant settings are:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `STOREFRONT_QUERY_CONCURRENCY` | `8` | NATS endpoint slots created for each query subject |
| `STOREFRONT_QUERY_MAX_IN_FLIGHT` | `48` | Active handler limit for each subject |
| `STOREFRONT_QUERY_PENDING_MESSAGES` | `512` | Pending NATS messages allowed per endpoint |
| `STOREFRONT_QUERY_PENDING_BYTES` | `2097152` | Pending NATS bytes allowed per endpoint |
| `STOREFRONT_CART_CACHE_ENTRIES` | `65536` | Maximum watch-fed cart entries retained per pod |

Increasing pending limits does not increase processing capacity and can turn
overload into tail latency. Tune admission only after measuring handler and KV
latency.

## Version-aware checkout reads

The checkout request may contain `min_cart_version`. The responder first reads
the cart from the watch-fed cache. It uses that value only if its cart version
is at least the requested minimum. A cache miss or older cached version falls
back to the authoritative cart KV bucket. Product and currency data used to
compose the response remain versioned projection inputs.

This preserves the checkout precondition while removing the per-order cart KV
read in the normal case. `checkoutservice` remains the final authority: it
compares the submitted expected versions with its own event-derived state.

## Projection write cache

Projection writers keep a bounded cache of values and KV revisions previously
read or written by that replica. An update attempts the authoritative KV CAS
with the cached revision. If another replica has advanced the key, the CAS
conflict invalidates the cache; the normal retry loop rereads JetStream and
reapplies duplicate/version checks before trying again.

The cache therefore removes an authoritative read from uncontended updates but
does not replace JetStream KV, relax ordering, or permit last-write-wins state.
