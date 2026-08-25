# checkoutservice

`checkoutservice` is a stateless NATS saga worker. Any replica can process the
next event for an order because each saga, immutable accepted-order snapshot,
applied-input result journal, and deadline record is stored in the
checkout-owned Redis Cluster.

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

All replicas scan bounded deadline shards. Expiring fencing leases and stable
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
