# Benchmark implementation progress

This document tracks implementation against
[`benchmark-plan.md`](benchmark-plan.md). The benchmark service has been
implemented and validated without deploying it or starting a benchmark.

## Completed

### Benchmark controller and UI

- Added [`src/benchmarkservice`](../../src/benchmarkservice) as a separate
  Python/Locust project. The existing `loadgenerator` is unchanged.
- Added a small standard-library HTTP controller and frontend. A pod starts
  idle; a benchmark starts only after an explicit request from the UI/API.
- The controller validates all run settings, allows only one active run,
  starts Locust in a separate process, supports an explicit stop request, and
  shuts the child down gracefully when the controller terminates.
- The UI selects closed- or open-loop workload parameters and displays saved
  run state and summaries. It exports summary JSON, raw business CSV, or a ZIP
  containing all artifacts.
- `APPLICATION_TYPE` is required and accepts exactly `GRPC` or `NATS`.
  `FRONTEND_ADDR` selects the target without changing the image.

### Comparable workloads

- Implemented the original common closed-loop task mix: home, currency,
  product, add-to-cart, view-cart, and checkout with 1–10 second think time.
- Removed `productMeta` from the parity workload.
- The benchmark does not issue deliberate idempotent duplicates. Each NATS
  mutation still carries one unique idempotency key.
- The GRPC adapter treats the successful synchronous checkout response as the
  logical outcome.
- The NATS adapter separately records checkout acceptance, follows the
  `Location` resource, honors `Retry-After` (defaulting to one second), and
  stops only at a terminal order state or a configured deadline.
- Individual HTTP requests remain in Locust diagnostics. The additional
  comparable measurement is `BUSINESS/checkout_to_outcome`, calculated from
  the checkout request start and the terminal order projection's immutable
  `outcome_at`. Status polls detect completion but neither determine this
  latency nor count as completed business operations.
- `BUSINESS/checkout_to_settled` starts at checkout submission and finishes
  when notification and the checkout-correlated cart-clear operation have
  both terminated. Settlement tracking continues independently after the
  closed-loop user has received the primary order outcome.

### Open-loop and drain behavior

- Implemented a fixed-rate scheduler based on absolute target times. It
  creates checkout transaction greenlets independently of the greenlets
  tracking earlier completions, avoiding closed-loop throttling under load.
- Every open-loop transaction uses an isolated storefront session.
- Warm-up, steady submission, and drain are explicit phases. Warm-up data is
  retained raw but excluded from the comparison summary.
- Submission stops before drain. Orders whose normal outcome timeout extends
  beyond the global drain deadline are classified as `INCOMPLETE`; ordinary
  per-order deadline expiry is classified as `TIMEOUT`.
- Accepted non-terminal order counts are written as a time series.

### Results and metrics

- Raw synthetic business samples are retained as JSON Lines and CSV.
- Summaries include p50/p95/p99/max successful outcome and settlement latency,
  completed-order goodput, acceptance/completion rates, and counts and rates
  for completed, rejected, cancelled, manual-review, timeout, and incomplete
  outcomes.
- Notification and cart-clear terminal outcomes are reported independently.
- A low-frequency embedded collector reads kubelet summary data through the
  Kubernetes API and calculates steady-state CPU-seconds, memory byte-seconds,
  memory, and receive/transmit bytes for the application footprint. The
  benchmark pod itself is excluded.
- NATS runs scrape the three existing exporter sidecars for JetStream consumer
  pending and acknowledgement-pending messages, redeliveries, and storage.
  Replicated consumer series are de-duplicated by stream/consumer.
- Open-loop summaries make a sustainability assessment. They do not declare a
  configuration sustainable when transactions remain incomplete or
  outstanding, or when NATS consumer pending messages increase. Missing NATS
  backlog metrics make the result indeterminate rather than sustainable.
- CPU, memory, and network totals are also normalized per completed order.

### NATS settlement visibility

- Extended the projected order JSON with `cart_clear_status` and
  `cart_clear_failure_code`.
- Successful checkout-triggered clear events correlate through their
  `order_id`; rejected clears correlate through the event envelope's order
  correlation ID. User-requested cart clears cannot create order settlement
  records.
- Projection merging preserves notification and cart-clear facts across
  independent event arrival order.

### Packaging and cluster integration

- Added a multi-stage, non-root
  [`Dockerfile`](../../src/benchmarkservice/Dockerfile) with Locust 2.43.1.
- Added `benchmarkservice` to the image build/push script.
- Added the NATS-mode Deployment, ClusterIP Service, ServiceAccount, and
  read-only node-summary RBAC to
  [`kubernetes-manifests/benchmarkservice.yaml`](../../kubernetes-manifests/benchmarkservice.yaml).
- Added the equivalent GRPC-mode resources in
  [`release/benchmarkservice-grpc.yaml`](../../release/benchmarkservice-grpc.yaml)
  for use alongside `release/benchmark-original-app.yaml`.
- Added narrowly scoped frontend, Kubernetes API, and NATS exporter network
  paths. No Prometheus/Grafana annotations or dependencies were added to the
  benchmark service.
- Results use pod-local `emptyDir` storage to avoid adding a persistent storage
  workload to the experiment. Results must be exported before replacing the
  pod.

## Validation completed

No validation step submitted application traffic.

- 8 benchmarkservice unit tests pass.
- Python byte-compilation passes.
- Locust 2.43.1 loads the locustfile and lists both `ClosedLoopUser` and
  `OpenLoopDriver`.
- The frontend and storefront projection Go test suites pass.
- The controller passed a no-load local smoke test for health, configuration,
  and an empty run list; no run was created.
- The `benchmarkservice:local` container image builds successfully.
- The current Kubernetes kustomization renders successfully, and the
  standalone GRPC manifest passes client-only create validation.
- Phase 0, 3, 4, and 5 static contract checks pass. The Phase 6 checker accepts
  the benchmarkservice exception from ordinary Prometheus scraping, then
  reaches an unrelated pre-existing failure because
  `docs/nats-event-driven-upgrade-plan.md` lacks its expected
  `Phase 6 implementation note` marker.
- `git diff --check` passes.

## Remaining

The following work is intentionally deferred:

- Publish the benchmarkservice image and replace the manifest's placeholder
  image with the same immutable digest for both application variants.
- Regenerate release bundles if benchmarkservice is to be embedded in the
  generated NATS release manifest; the source NATS manifest and standalone
  GRPC manifest are present now.
- Deploy against the original and NATS clusters. Live-validate frontend route
  compatibility, ServiceAccount access to kubelet summaries, NATS exporter
  metric labels, network policy behavior, artifact export, and graceful stop.
- Run a functional smoke transaction only after deployment. No checkout or
  other benchmark request has been issued during implementation.
- Calibrate generator CPU/memory, open-loop concurrency, warm-up, steady,
  drain, and timeout values so the generator is not the bottleneck.
- Establish low-load, saturation-knee, and overload levels; randomize GRPC/NATS
  order; and execute the planned 10–30 repetitions per configuration.
- Keep nodes, placement, limits, replica counts, product/currency data, and
  autoscaling settings identical during those runs. Do not install the
  observability stack in benchmark clusters.
- Export and analyze raw results, verify backlog stability, and report
  confidence/variation across repetitions.
- Implement the separate reliability workload for deliberate idempotent
  retries. It is intentionally absent from parity runs.
- Add an energy collector only if energy-per-order is required; it remains an
  optional metric in the plan.
