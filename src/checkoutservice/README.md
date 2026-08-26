# checkoutservice

`checkoutservice` is a stateless NATS saga worker. Any replica can process the
next event for an order because each saga, immutable accepted-order snapshot,
applied-input result journal, and deadline record is stored in the
checkout-owned Redis Cluster.

## Workflow consumers

Checkout isolates workflow stages that have different load behavior:

| Durable consumer | Subjects | Handler |
| --- | --- | --- |
| `checkout-saga-shipping-quote-v1` | `boutique.evt.shipping.order-quote-calculated.v1`, `boutique.evt.shipping.order-quote-failed.v1` | Shipping quote response handler |
| `checkout-saga-shipping-v1` | Shipment creation/cancellation success and failure subjects | Shipment response handler |
| `checkout-saga-payment-authorization-v1` | Authorization success and decline subjects | Payment authorization response handler |
| `checkout-saga-payment-v1` | Capture and authorization-release success and failure subjects | Late-stage payment response handler |
| `checkout-order-commands-v1` | `boutique.cmd.order.submit.v1` | Order admission handler |

Every durable owns a separate pull loop and 32-lane processing pool. Messages
for one aggregate remain ordered within that consumer, while unrelated orders
can use different lanes. Separating quote and shipment responses prevents the
continuous quote rate from occupying the same handler capacity as shipment
responses. Separating authorization from capture responses likewise reserves
an independent processing pool for capture outcomes, which emit
`boutique.evt.order.completed.v1`.

Workflow response consumers start before the order-command consumer. During an
upgrade from a combined consumer, each new early-stage durable is created first
at its legacy durable's acknowledgement floor; only then is the legacy durable
narrowed to late-stage outcomes. For shipping, the new quote durable migrates
from `checkout-saga-shipping-v1`; for payment, the new authorization durable
migrates from `checkout-saga-payment-v1`, whose cursor remains attached to
capture and release outcomes. This prevents a filter gap and avoids replaying
all historical early-stage events. Any overlap can deliver a duplicate, which
the Redis input journal handles safely. On a fresh or recovery deployment where
the legacy consumer is absent, the new durable uses `DeliverAll` so retained
responses are not skipped.

Readiness must include both members of each shipping and payment split.
Autoscaling aggregates their pending counts, but operators should inspect each
consumer independently when diagnosing overload: quote and authorization
backlogs indicate early-stage pressure, whereas shipment and capture/release
backlogs can directly suppress completed-order throughput.

## Redis storage format

Redis schema version 3 changes every checkout JSON payload—sagas, immutable
accepted-order snapshots, applied-input result journals, deadline records, and
shared projections—from plain JSON to gzip-compressed JSON. The schema was
bumped from version 2 because compressed bytes cannot be decoded by the v2
reader and the v3 reader intentionally has no uncompressed fallback. Avoiding a
dual-format hot path keeps every read consistent and ensures new records always
receive the memory reduction.

Before deploying a v3 checkoutservice, remove the complete existing
`checkout:v2:*` keyspace, including `checkout:v2:schema`. The service will then
create a fresh schema marker with value `3`. If the old schema marker remains,
startup fails with an unsupported-schema error instead of interpreting v2 data
as v3. The Redis key prefix remains independently configurable and therefore
still defaults to `checkout:v2`; the schema marker, not the prefix name,
identifies the encoded record format.

One order transition touches keys in a single Redis Cluster hash slot. The Lua
commit compares only that order's version, stores the exact deterministic
result envelopes for the input, and updates one of 64 deadline indexes.
Unrelated orders do not share a transaction revision.

The handling replica publishes the stored results with stable `Nats-Msg-Id`
values and acknowledges the input only after JetStream acknowledges every
publish. A duplicate delivery reloads and republishes the same bytes.

All replicas scan bounded deadline shards every five seconds. This avoids
continuously polling all 64 Redis indexes while keeping timeout dispatch within
one scan interval under normal operation. Expiring fencing leases and stable
synthetic input IDs let another replica recover a deadline after a stop at
either the state-commit or result-publication boundary. Projection consumers
process independent aggregates concurrently while preserving the stream order
within each aggregate; an envelope is decoded once before it is assigned to
that aggregate's lane.

Each saga stage gets its full timeout from when checkout begins processing the
transition that enters that stage. Result and stage-event occurrence times
still come from the input envelope so duplicate delivery republishes the same
deterministic bytes even when processing was delayed in a queue.

Required runtime configuration:

- `CHECKOUT_REDIS_ADDR`: one or more comma-separated Redis Cluster seed
  addresses;
- `CHECKOUT_REDIS_MODE`: `cluster` in deployed environments, or `standalone`
  for local tests; and
- `NATS_URL`, `NATS_USER`, `NATS_PASSWORD`, and `NATS_CA_FILE`.

`CHECKOUT_REDIS_PREFIX` defaults to `checkout:v2`.
`CHECKOUT_REDIS_RETENTION` controls applied-input result-journal and completed
deadline-lease retention; it defaults to `792h` (33 days).
