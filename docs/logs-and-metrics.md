# Logging, metrics, and tracing

The repository implements observability in two layers:

1. Every deployed domain service writes container logs, exposes a small
   Prometheus text endpoint on port `8080`, and exports OpenTelemetry traces.
2. An optional, standalone stack in
   [`kubernetes-manifests/observability`](../kubernetes-manifests/observability)
   collects those signals with Grafana Alloy, Tempo, Loki, Prometheus, and
   Grafana.

The observability stack is not part of the main application kustomization or
release manifest. It is installed separately:

```sh
kubectl apply -k kubernetes-manifests/observability
```

## Logging

### Application output

Services log to their container output streams using their language's normal
logging library. Structured loggers target stdout; the storefront projection's
remaining standard-library logger writes to stderr. Most output is
newline-delimited JSON, but there is no shared cross-language logging package
or completely uniform schema.

| Runtime / services | Implementation |
| --- | --- |
| Go: frontend, product catalog, shipping | Logrus at debug level, with JSON fields renamed to `timestamp`, `severity`, and `message`. |
| Go: checkout | Logrus JSON at debug level, using Logrus's default field names. |
| Go: storefront projection | `slog` JSON at debug level for structured event/query records. Some operational and error paths still use the standard `log` package, so this service also emits plain-text lines. |
| Go: message operations | `slog` JSON at INFO level with case, source stream/sequence, consumer, replay, actor, and safe error metadata; restricted payloads are never logged. |
| Node.js: currency, payment | Pino JSON at debug level with `message` and `severity` fields. |
| Python: email, recommendation | `python-json-logger` writes JSON to stdout at INFO level by default and adds `timestamp`, uppercase `severity`, logger `name`, and `message`. Set `LOG_LEVEL=DEBUG` to include per-event debug records. |
| C#: cart | A custom `SeverityJsonConsoleFormatter` emits `timestamp`, normalized `severity`, logger `name`, `message`, structured state, event metadata, and exceptions. The default level is Information, with debug enabled for the NATS relay. |
| Java: ad | Log4j2 writes compact JSON to stdout. Its layout includes trace/span fields understood by Google Cloud Logging plus `correlation_id`; the root level is TRACE. |

The event-driven paths commonly add `topic`, `message_kind`, `message_id`, and
`correlation_id` fields. Frontend request logging also records a generated
request/correlation ID, HTTP method and path, response status and size, and
elapsed milliseconds. These fields make it possible to follow an operation
across NATS messages even though the logging implementations are
language-specific.

Representative implementations are:

- [`src/frontend/main.go`](../src/frontend/main.go) and
  [`src/frontend/middleware.go`](../src/frontend/middleware.go)
- [`src/cartservice/src/logging/SeverityJsonConsoleFormatter.cs`](../src/cartservice/src/logging/SeverityJsonConsoleFormatter.cs)
- [`src/paymentservice/logger.js`](../src/paymentservice/logger.js)
- [`src/emailservice/logger.py`](../src/emailservice/logger.py)
- [`src/adservice/src/main/resources/log4j2.xml`](../src/adservice/src/main/resources/log4j2.xml)
- [`src/storefrontprojectionservice/main.go`](../src/storefrontprojectionservice/main.go)

The NATS deployment also runs an advisory watcher which prints max-delivery
advisories for low-level diagnostics. The durable handling path is
`messageoperationsservice`, whose case logs omit source payloads. Alloy collects
both like logs from any other pod.

### Collection and storage

[`alloy.yaml`](../kubernetes-manifests/observability/alloy.yaml) deploys Grafana
Alloy as a DaemonSet. Each Alloy instance discovers only pods scheduled on its
own node and reads their container logs through the Kubernetes API. It does not
mount node log directories and application pods do not receive logging
sidecars.

Discovery attaches the following Loki stream labels:

- `namespace`, `pod`, `container`, and `node`
- `app`, from either the `app` or `app.kubernetes.io/name` pod label
- `job`, derived from namespace and container metadata
- `container_runtime`
- the static `cluster="online-boutique"` label

Alloy forwards the original log line without a JSON or logfmt parsing stage.
Consequently, application JSON fields such as `correlation_id` remain in the
line body rather than becoming indexed Loki labels. Queries can search those
values, but cannot select them as labels unless Alloy is extended with parsing
and label-extraction stages.

[`loki.yaml`](../kubernetes-manifests/observability/loki.yaml) runs one
filesystem-backed Loki replica in monolithic mode. Chunks, indexes, caches, and
compactor data use a 10 GiB `ReadWriteOnce` persistent volume. Retention is
seven days (`168h`), and samples older than seven days are rejected.

### Log presentation

Grafana is provisioned with Loki as a data source and a read-only **Online
Boutique Observability** dashboard. Its log panels provide:

- namespace, application, excluded-application, pod, and free-text filters;
- log volume per namespace;
- a raw container-log view; and
- a five-minute error count based on a case-insensitive text search for
  `error`, `exception`, `fatal`, or `panic`.

The error count is a text heuristic, not a parsed severity query. For example,
an error word in a non-error message can match, while an error line without one
of the searched words can be missed.

## Tracing

Application deployments enable OTLP/gRPC tracing and send spans to the Alloy
Service in the `observability` namespace. Alloy batches and forwards traces to
a single filesystem-backed Tempo instance. Grafana provisions Tempo as a data
source, including TraceQL search and trace-to-log navigation. The default
dashboard includes an **OpenTelemetry traces** table for the selected time
range. Its **Trace service** and **Correlation ID** text boxes accept regular
expressions and filter `resource.service.name` and `span.correlation.id`,
respectively. The table's **View waterfall** link loads the selected trace into
an embedded span timeline; the **Selected trace ID** text box also accepts a
trace ID directly. The trace ID's built-in link opens the complete trace for
span inspection and related-log navigation in Explore.

The frontend creates the initial HTTP server span. NATS publishers inject W3C
`traceparent` and `tracestate` values into message envelopes, and consumers
extract those values before starting `producer` and `consumer` spans. This
keeps the trace causal chain intact across durable delivery, retries, and
language boundaries.

Every operation span also has `correlation.id`, using the same value written as
`correlation_id` in logs. Correlation IDs remain stable request or business
identifiers; they are not converted into OpenTelemetry trace IDs. This lets an
operator search logs by `correlation_id` and traces with a TraceQL selector
such as:

```traceql
{ span.correlation.id = "<correlation-id>" }
```

Trace export is deliberately non-blocking. If the optional observability stack
is absent, application work continues while SDK exporters retry or drop spans
according to their bounded queues.

## Metrics

### Application metrics

Every deployed domain workload has `prometheus.io/scrape: "true"`,
`prometheus.io/port: "8080"`, and `prometheus.io/path: /metrics` pod
annotations. Each service implements `/metrics` directly on the same `:8080`
HTTP listener used by `/healthz` and `/readyz` (and, for the frontend, business
HTTP traffic).

The endpoints hand-render Prometheus text format; they do not use a Prometheus
client library. Their main metric is:

```text
boutique_dependency_ready{service="<service>",dependency="<dependency>"} 0|1
```

The value is `1` when the dependency is considered ready and `0` otherwise.
The emitted dependencies are:

| Service | Dependencies |
| --- | --- |
| `adservice` | `nats` |
| `cartservice` | `cart_store`, `nats` |
| `checkoutservice` | `nats`, `saga_store` |
| `currencyservice` | `nats` |
| `emailservice` | `nats`, `provider_store` |
| `frontend` | `nats` |
| `paymentservice` | `nats`, `token_verifier` |
| `productcatalogservice` | `catalog`, `nats` |
| `recommendationservice` | `nats` |
| `shippingservice` | `nats`, `provider_store` |
| `storefrontprojectionservice` | `nats`, `kv` |
| `messageoperationsservice` | `nats` |

`productcatalogservice` additionally exposes `boutique_catalog_products`, the
number of loaded products.

`messageoperationsservice` also exposes `boutique_dlq_cases{status=...}` and
transfer, replay, replay-error, and resolution counters. The observability
rules fire `BoutiqueMessageDeadLettered` for cases requiring action and
`BoutiqueDLQTransferFailing` when an advisory could not be durably transferred.

These endpoints describe dependency readiness and catalog size only. They do
not currently expose application request counts, latency histograms, error
counters, process/runtime collectors, or per-message processing metrics. Some
dependency series are constant `1` placeholders for an in-process or
successfully initialized store, while NATS series generally track live
connection or worker readiness.

### NATS metrics

Each NATS pod has a
[`prometheus-nats-exporter`](../kubernetes-manifests/nats/base/statefulset.yaml)
sidecar. The exporter listens on `7777`, reads the local NATS monitoring
endpoint on `8222`, and enables `varz`, `routez`, `subz`, and complete
JetStream (`jsz=all`) metrics. A headless `nats-metrics` Service exposes one
metrics endpoint per NATS pod.

This supplies the detailed broker and JetStream series used for server
availability, connection counts, message rates, storage, quorum, consumer
pending messages, acknowledgement backlog, and redeliveries. Application
services do not reproduce those broker metrics.

The benchmark metrics gateway also samples `/raftz?acc=BOUTIQUE` directly on
port `8222`. This path is independent of the exporter's comparatively expensive
full JetStream scrape, so saturation artifacts retain per-group WAL sequence
and size, commit/apply lag, internal queue depth, term, and leader observations
when the exporter is delayed. Stream position/size, meta snapshot state,
traffic, API errors, and metrics-source freshness are summarized per rung.

Checkout shipping- and payment-stage lag must be inspected by durable consumer,
because early- and late-stage responses intentionally have independent handler
capacity:

```promql
max by (consumer_name) (
  jetstream_consumer_num_pending{
    account="BOUTIQUE",
    consumer_name=~"checkout-saga-(shipping(-quote)?|payment(-authorization)?)-v1",
    is_consumer_leader="true"
  }
)
```

`checkout-saga-shipping-quote-v1` reports quote-response pressure;
`checkout-saga-shipping-v1` reports shipment creation/cancellation pressure.
`checkout-saga-payment-authorization-v1` reports authorization-response
pressure; `checkout-saga-payment-v1` reports capture and release pressure.
Growth in either late-stage durable can directly delay completed-order events.
The benchmark HPA recording rule sums all four queues for checkout scaling,
while keeping the `consumer_name` series available for stage-level diagnosis.

Recommendation pending counts have different semantics. A non-zero
`recommendation-page-views-v1` value can represent approximately the configured
freshness window while new page views arrive; stale and same-batch-superseded
views are acknowledged without result publication. The value may therefore
plateau above zero under steady load without indicating checkout pressure.
Recommendation fetches are pipelined with a bounded two-batch buffer, and
catalog reads use a short-lived immutable cache, so `num_ack_pending` can cover
both actively executing handlers and prefetched work. Cache refresh is bounded
by `RECOMMENDATION_CATALOG_CACHE_REFRESH` (default `1s`).

`recommendation-cart-v1` also coalesces full cart snapshots within each fetched
batch, so pending cart triggers count inputs rather than required recommendation
outputs. Diagnose order throughput with the checkout consumers above and the
independent completed-order observer instead of the aggregate application
consumer-pending sum.

`ad-page-views-v1` likewise acknowledges page views older than
`AD_PAGE_VIEW_MAX_AGE` (default `5s`) and same-batch-superseded views without
publishing obsolete ad selections. Under sustained load its pending count can
temporarily represent the freshness window rather than required outputs. A
queue that drains quickly after arrival stops, without redeliveries, indicates
capacity pressure rather than processing failures.

`shipping-cart-quotes-v1` preserves every cart-version output but processes an
existing fetch through per-user worker lanes. A sustained pending count there
represents quote input arriving faster than the keyed parallel
publisher can confirm results; it is not caused by intentional coalescing.

### Prometheus discovery, storage, and rules

[`prometheus.yaml`](../kubernetes-manifests/observability/prometheus.yaml) runs
one Prometheus replica with a 15-second scrape and evaluation interval:

- The `kubernetes-pods` job discovers annotated pods, drops completed pods,
  honors the annotated path, scheme, and port, and adds namespace, pod, node,
  container, and service labels.
- NATS pods are excluded from that job to avoid duplicate targets.
- The `nats` job discovers the `nats-metrics` Service endpoints and scrapes
  each exporter separately.

Prometheus keeps at most seven days or 8 GB of time-series data on a 10 GiB
`ReadWriteOnce` persistent volume.

The Prometheus instance evaluates four bundled alert rules:

- `BoutiqueMetricsTargetDown`
- `BoutiqueDependencyUnavailable`
- `NatsMetricsUnavailable`
- `NatsSlowConsumers`

No Alertmanager or other notification receiver is configured, so firing alerts
are visible in Prometheus but are not delivered externally.

The NATS base manifests also define a broader
[`nats-prometheus-rules`](../kubernetes-manifests/nats/base/monitoring.yaml)
ConfigMap for storage, quorum, lag, acknowledgement backlog, and redelivery
conditions. That ConfigMap is not mounted by the standalone Prometheus
StatefulSet. Likewise, the NATS-specific Grafana dashboard ConfigMap is not
loaded by the standalone Grafana deployment. An operator must explicitly wire
those two NATS ConfigMaps into a different Prometheus/Grafana installation if
they are to take effect.

### Metric presentation

Grafana is provisioned with Prometheus as its default data source. The bundled
dashboard refreshes every 15 seconds and shows:

- application scrape-target health;
- average application dependency readiness;
- the number of NATS exporters that are up;
- readiness by service and dependency; and
- aggregate NATS inbound and outbound message rates.

Prometheus and Loki can also be queried directly through Grafana Explore.

## Deployment and security characteristics

All observability resources run in the `observability` namespace. Read-only
cluster roles allow Prometheus to discover pods, Services, endpoints, and
EndpointSlices, and allow Alloy to discover workloads and read `pods/log`.

Default-deny network policies are supplemented with narrowly scoped paths for
in-namespace communication, DNS, Kubernetes API access, application scrapes on
`8080`, and NATS exporter scrapes on `7777`. The NATS namespace independently
permits port `7777` ingress from namespaces carrying the observability
enablement label.

Grafana is a cluster-internal `ClusterIP` Service. It enables anonymous Viewer
access and disables the login form, so the intended access path is an
authenticated `kubectl port-forward`:

```sh
kubectl -n observability port-forward service/grafana 3000:3000
```

The stack is sized for development and modest demo traffic, not high
availability:

- Prometheus, Loki, and Tempo are single replicas with node-bound `ReadWriteOnce`
  storage.
- Loki and Tempo use the local filesystem rather than object storage.
- Grafana state and Alloy working data use `emptyDir`; durable configuration
  comes from provisioned ConfigMaps.
- A default dynamic `StorageClass` is required for the Prometheus, Loki, and Tempo
  claims.

The bundled trace path is intended for development and modest demo traffic;
production deployments should use durable object storage, authentication,
TLS, and a scaled collector/backend topology.
