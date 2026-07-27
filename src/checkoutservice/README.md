# checkoutservice

`checkoutservice` is a stateless NATS saga worker. Any replica can process the
next event for an order because each saga, immutable accepted-order snapshot,
applied-input result journal, and deadline record is stored in the
checkout-owned Redis Cluster.

One order transition touches keys in a single Redis Cluster hash slot. The Lua
commit compares only that order's version, stores the exact deterministic
result envelopes for the input, and updates one of 64 deadline indexes.
Unrelated orders do not share a transaction revision.

The handling replica publishes the stored results with stable `Nats-Msg-Id`
values and acknowledges the input only after JetStream acknowledges every
publish. A duplicate delivery reloads and republishes the same bytes.

All replicas scan bounded deadline shards. Expiring fencing leases and stable
synthetic input IDs let another replica recover a deadline after a stop at
either the state-commit or result-publication boundary.

Required runtime configuration:

- `CHECKOUT_REDIS_ADDR`: one or more comma-separated Redis Cluster seed
  addresses;
- `CHECKOUT_REDIS_MODE`: `cluster` in deployed environments, or `standalone`
  for local tests; and
- `NATS_URL`, `NATS_USER`, `NATS_PASSWORD`, and `NATS_CA_FILE`.

`CHECKOUT_REDIS_PREFIX` defaults to `checkout:v2`.
