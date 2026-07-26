# checkoutservice

`checkoutservice` is a stateless NATS saga worker. Any replica can process the
next event for an order because orders, projections, inbox entries, deadlines,
and outbox entries are stored in a shared checkout-owned Redis service.

Each handler watches a shared state revision, reloads the latest committed
state, and atomically writes its inbox, state, outbox, and revision changes.
Concurrent transactions retry. Duplicate deliveries observe the shared inbox,
and outgoing messages retain stable `Nats-Msg-Id` values.

The bundled Redis owner uses AOF with `appendfsync always`, keeping the
JetStream acknowledgement boundary crash-durable.

Required runtime configuration:

- `CHECKOUT_REDIS_ADDR`: Redis address or URL;
- `NATS_URL`, `NATS_USER`, `NATS_PASSWORD`, and `NATS_CA_FILE`.

`CHECKOUT_REDIS_PREFIX` defaults to `checkout:v1`.
