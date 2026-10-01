# Shipping Service

The Shipping service provides price quote, tracking IDs, and the impression of order fulfillment & shipping processes.

## NATS cart quote processing

The `shipping-cart-quotes-v1` durable consumes complete cart snapshots and
publishes one deterministic, versioned quote result for every accepted cart
event. A fetched batch remains 32 messages and is dispatched across 32 worker
lanes keyed by cart user ID. Work for one lane is sequential; unrelated users
publish concurrently. An input is acknowledged only after JetStream confirms
its result publish, retaining the existing retry and deduplication behavior.

Ordering across replicas is intentionally resolved by the cart version in the
storefront projection rather than by delivery time.

## NATS shipping command processing

Shipping commands use three subject-specific durable consumers with persistent
bounded worker pools. The pull loops continue
fetching while earlier commands are running, so a delayed shipment does not
create a barrier at the end of each fetched batch. Shipment creation has a
dedicated pool; immediate quote and cancellation commands have reserved workers
and cannot be starved by delayed work. Commands are not assigned to hashed
correlation-ID lanes: the checkout saga and deterministic result IDs provide the
required state-transition and redelivery safety across replicas.

Shipment creation retains the configured simulated provider delay (200 ms in
the benchmark). Quote and cancellation commands do not incur that delay. Every
input is still acknowledged only after JetStream acknowledges its deterministic
result publication.

`SHIPPING_REJECTION_PERCENTAGE` controls deterministic shipment-creation
rejection injection. It accepts a number from `0` through `100` and defaults to
`0` when unset. The decision is derived from the provider secret and business
idempotency key, so retries and different replicas produce the same outcome.
Percentage-based rejections publish `shipment-creation-failed` with failure code
`CARRIER_REJECTED`; quote and cancellation requests are unaffected.

## Local

Run the following command to restore dependencies to `vendor/` directory:

    dep ensure --vendor-only

## Build

From `src/shippingservice`, run:

```
docker build ./
```

## Test

```
go test .
```
