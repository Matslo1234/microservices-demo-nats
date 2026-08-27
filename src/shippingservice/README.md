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
