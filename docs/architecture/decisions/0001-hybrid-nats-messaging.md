# ADR 0001: Hybrid NATS messaging architecture

- Status: Accepted
- Date: 2026-07-18
- Decision owners: Online Boutique service owners
- Related plan: [`../../nats-event-driven-upgrade-plan.md`](../../nats-event-driven-upgrade-plan.md)

## Context

The current storefront composes pages and checkout through synchronous gRPC
calls. This creates request fan-out, couples service availability to the HTTP
request, and provides no durable workflow state or application-level
idempotency. Some reads still need an immediate answer, while commands and
multi-service workflows need durable delivery and replayable facts.

## Decision

Use a hybrid NATS design:

- Durable state-changing intent uses JetStream commands. Each command has one
  logical owner and is acknowledged only after the owner's state change, inbox
  record, and outbox result are durable.
- Accepted state changes publish versioned JetStream domain events. Domain-owner
  stores remain authoritative; the event stream is the integration log.
- Immediate, side-effect-free storefront reads use Core NATS request/reply
  against `storefrontprojectionservice`, a read model built from events. Queries
  are bounded, ephemeral, and never written to a stream.
- Browser writes become asynchronous HTTP operations. The frontend returns an
  operation or order resource after JetStream confirms persistence; a bounded
  compatibility wait may preserve existing redirects during migration.
- Payment card number and CVV never enter JetStream, KV, logs, traces, or a DLQ.
  The frontend exchanges card fields for a short-lived, order-bound opaque token
  through TLS-protected Core NATS request/reply (or directly with a provider).
  Only the opaque token may appear in the order command, and no payment token
  may appear in payment result events.

## Ownership boundaries

Each domain service owns its state and publishes facts about that state:

| Owner | Authoritative state / responsibility |
| --- | --- |
| Product catalog | Products and catalog revisions |
| Currency | Rate-set revisions |
| Cart | Cart contents and monotonically increasing cart versions |
| Checkout | Orders and durable saga state |
| Shipping | Quotes, shipments, and carrier idempotency |
| Payment | Token vault, authorizations, captures, and provider idempotency |
| Email | Delivery attempts and notification outcome |
| Recommendation / ad / assistant | Generated result and failure outcome |
| Storefront projection | Disposable, rebuildable query views; never domain truth |
| Message operations | Max-delivery handling, restricted DLQ, and alerts |

A service may keep a local projection of another owner's facts, but may not
mutate that owner's state or treat its projection as authoritative. Owners must
be able to publish versioned snapshots so consumers can recover after event
retention expires.

## Delivery and consistency semantics

- Delivery is at least once. A duplicate is normal, not exceptional.
- Publishers use a stable `message_id` as `Nats-Msg-Id` and wait for a publish
  acknowledgement. Consumers use durable pull consumers and explicit acks.
- Every handler stores an inbox record keyed by `message_id`. Domain state and
  outbox records are committed atomically; external effects use business-level
  idempotency keys because transport deduplication cannot make them atomic.
- Per-aggregate ordering is enforced with `aggregate_version` and optimistic
  concurrency. Projections ignore duplicate or older aggregate versions.
- Consumers ack only after durable processing. Retry/backoff and max-delivery
  settings are handler-specific. Terminal poison messages follow the observed
  copy-to-DLQ, publish-ack, remove-original, alert procedure.
- Eventual consistency is visible to clients through operation status,
  projection revision, and `updated_at`. Query timeouts and no-responders map to
  bounded unavailable responses rather than hidden synchronous fallbacks.

## Contract rules

Contracts live under `protos/common/v1`, `protos/commands/v1`, and
`protos/events/v1`; the subject-to-payload registry is
[`../../../protos/contracts-v1.json`](../../../protos/contracts-v1.json).
Additive protobuf changes retain the `.v1` subject. Breaking changes require a
new protobuf package/subject version and a coexistence period. The complete
metadata and error conventions are in
[`../../development/nats-message-conventions.md`](../../development/nats-message-conventions.md).

## Consequences

The design removes synchronous write workflows and read fan-out, supports
replay, and makes failures observable and retryable. It also introduces
eventual consistency, projection lag, durable workflow storage, inbox/outbox
maintenance, schema governance, and operational responsibility for NATS.

## Rejected alternatives

- JetStream for every query: durable query round-trips add latency and state
  without useful replay semantics.
- Core NATS for state-changing commands: an unavailable subscriber would lose
  the request.
- Keeping synchronous checkout orchestration: it preserves the charge/ship
  failure window and cannot safely resume after a process failure.
- Publishing raw card data for checkout: stream retention, replay, observability,
  and DLQ paths make this an unacceptable data boundary.
