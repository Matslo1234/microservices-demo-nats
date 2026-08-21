# Message operations service

`messageoperationsservice` turns JetStream maximum-delivery advisories into a
recoverable dead-letter workflow. It durably consumes advisories, copies the
exhausted message and its headers to the restricted `BOUTIQUE_DLQ` stream,
records administrator-facing lifecycle metadata in the `DLQ_CASES` KV bucket,
and emits `boutique.evt.ops.message-dead-lettered.v1`.

For `BOUTIQUE_COMMANDS`, the source message is deleted only after the DLQ record
and case metadata are durable. A `BOUTIQUE_EVENTS` source is not deleted because
that stream is a replayable fact log. Transfer and operational-event publishes
use stable message IDs, while case updates use KV compare-and-set revisions so
duplicate advisories and concurrent administrators are safe.

## Administration

The ClusterIP Service exposes an authenticated page at
`/admin/dead-letters` and an equivalent JSON API. Health, readiness, and
Prometheus metrics are the only unauthenticated routes. The admin API returns
message identity, stream, consumer, status, and replay history; it never
returns the stored source payload or headers.

Access the page without exposing it outside the cluster:

```sh
kubectl -n default port-forward service/messageoperationsservice 18080:80
kubectl -n default get secret messageoperations-admin-api \
  -o go-template='user: {{index .data "ADMIN_USER" | base64decode}}{{"\n"}}password: {{index .data "ADMIN_API_TOKEN" | base64decode}}{{"\n"}}'
```

Open <http://127.0.0.1:18080/admin/dead-letters> and use those credentials.
Cases remain `TRANSFER_IN_PROGRESS` and non-actionable until the operational
event and any command cleanup are durable. Replay requires a reason and is
allowed from `OPEN` or `REPLAY_FAILED`. Resolve also requires a reason and
records the administrator identity.

A replay publishes the exact source bytes to the original subject. It retains
the business envelope and message ID, replaces transport deduplication headers
with a replay-attempt ID, and records the original case, consumer, and stream
sequence in headers. A command goes back to its work-queue owner. Because an
event subject can match several independent durable consumers, an event replay
can be delivered to all of them; their inbox/idempotency handling is the safety
boundary. `REPLAY_PUBLISHED` confirms JetStream accepted the new message, not
that every handler completed it. If it exhausts delivery again, a child DLQ
case is created and the parent becomes `REPLAY_FAILED`. A replay lock left by a
controller crash can be resumed after one minute with the same replay identity.

The runtime interaction document also describes the
[`dead-letter and replay flow`](../../docs/current-service-interactions.md#dead-letter-and-replay-flow).

## Local verification

```sh
GOPROXY=off go test ./...
```
