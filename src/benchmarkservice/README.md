# Benchmark service

The benchmark supports two execution modes. `standalone.py` runs Locust on a
workstation, VM, or a Job in a separate cluster and writes artifacts locally.
This is the preferred mode because load generation does not consume resources
in the cluster being measured. `benchmarkservice` remains available as a
horizontally scalable API that creates disposable Jobs in the cluster where
that API is deployed.

Every run requires two explicit URLs:

- `target_url` (or standalone `--url`) is the externally reachable frontend;
- `metrics_url` is the tested cluster's `/snapshot` endpoint.

The target URL is part of the run configuration; there is no implicit
`frontend` Service default. This prevents a remotely deployed runner from
silently benchmarking its own cluster.

API replicas retain no run, lease, process, log, or artifact state in their
pods:

- run definitions, status, heartbeats, summaries, and the expiring
  one-active-run lease are stored in the replicated `BENCHMARK_RUNS`
  JetStream KV bucket;
- raw JSONL/CSV/log files and complete archives are stored in the replicated
  `BENCHMARK_ARTIFACTS` JetStream Object Store bucket; and
- every accepted run creates one disposable Kubernetes Job. Larger runs use
  Indexed Job completions so each Locust worker has an isolated pod; Locust is
  never a child of an API process.

Any API replica can list, inspect, download, stop, or reconcile any run. Run
submission uses KV compare-and-set on the expiring lease, so concurrent API
requests create one workload Job. A runner renews that lease while active and
publishes terminal state and artifacts before exiting. If a Job fails before
finalization, another API replica observes its Kubernetes status and records
the failure.

## Workloads

- `closed` uses the original common task mix and waits for each logical
  checkout outcome.
- `open` schedules checkout transactions at an absolute arrival rate.
  Submission scheduling is independent of completion tracking, so slow
  outcomes do not reduce the requested arrival rate.
- `saturation` runs an open-loop load ladder. After the optional warm-up at
  10 orders/s, it measures 10 orders/s for 30 seconds by default and adds 10
  orders/s each rung. The rung duration is configurable with
  `saturation_step_seconds` (or `--saturation-step-seconds` in standalone
  mode). Saturation runs default to a 600-second steady interval, which reaches
  200 orders/s with the default ladder. It records the first rung where observed goodput no longer
  increases or, for NATS, total application-consumer pending grows by at least
  10 messages/s. These observations do not end the load schedule: it runs for
  the complete steady interval and holds at `saturation_max_rate` if it reaches
  that cap before the interval ends.
- `fault_tolerance` uses the same absolute-rate scheduler as `open`, while the
  dedicated standalone controller scales `paymentservice` and then
  `shippingservice` to zero for configured intervals. It is intentionally not
  available through the benchmarkservice API because the target cluster can be
  remote and the API must not receive permission to mutate it.

For NATS runs, accepted checkouts are followed through the order SSE endpoint.
`checkout_to_outcome` ends when the terminal order event is received by the
benchmark worker, and the same stream continues through notification and cart
clear settlement. The business artifacts include UTC/epoch receipt times and
SSE event IDs for the outcome and settlement events. Failed order events also
retain `failure_code`, `safe_message`, `failure_message`, and
`failure_received_at`; notification and cart-clear failures have their own
receipt-time fields.

Open-loop, saturation, and fault-tolerance runs use one worker for every 100
requested orders/s. Closed-loop runs use one worker for every 1,000 users, and
divide the requested user spawn rate evenly across those workers. Worker start
times and saturation rung decisions are synchronized. Worker zero samples
shared application resources once, then merges every worker's business and
outstanding-order records into the run-level report. Individual Locust CSVs and
logs remain in the complete artifact archive for diagnostics.

Warm-up samples remain in raw artifacts but are excluded from summaries. After
the steady interval, submission stops for the configured drain period.

## NATS prerequisite

Every checked-in benchmark bundle requires a compatible NATS configuration to
be installed and ready before the bundle is applied. The benchmark YAML files
do not create or configure NATS. This applies to the original-GRPC comparison
bundle too, because the benchmark controller and runner use NATS JetStream for
shared run state and artifacts.

For a local standalone cluster, install the repository's standard NATS
configuration with a default dynamic `StorageClass` available, then wait for
its client resources and benchmark stores:

```sh
kubectl apply -k kubernetes-manifests/nats/fresh-cluster
kubectl -n nats rollout status deployment/nats-setup --timeout=5m
kubectl -n nats rollout status statefulset/nats --timeout=10m
kubectl -n nats wait --for=condition=complete job/nats-global-bootstrap --timeout=10m
kubectl -n nats wait --for=condition=complete job/nats-regional-bootstrap --timeout=10m
```

You can then apply one benchmark bundle, for example:

```sh
kubectl apply -f benchmark/benchmark-nats.yaml
```

An existing regional or otherwise customized NATS installation is also valid
when it provides the `nats-client-config` and `nats-ca` ConfigMaps,
`nats-credentials-*`, `global-application-secrets`, and
`messageoperations-admin-api` Secrets, plus the configured benchmark KV/Object
Store buckets expected by the application namespace.

## Running outside the target cluster

Every checked-in benchmark bundle deploys `benchmarkmetrics`, a small metrics
gateway, and exposes it through the `benchmarkmetrics-external` LoadBalancer
Service.
It reads kubelet summaries and local NATS exporter endpoints in the tested
cluster. Locust only performs HTTP requests to the supplied frontend and
metrics URLs.

After applying a benchmark bundle, obtain both external addresses:

```sh
kubectl get service frontend-external benchmarkmetrics-external
```

Run from a checkout with the Python requirements installed:

```sh
python src/benchmarkservice/standalone.py \
  --application-type NATS \
  --url http://FRONTEND_EXTERNAL_IP \
  --metrics-url http://BENCHMARKMETRICS_EXTERNAL_IP/snapshot \
  --workload closed \
  --users 100 \
  --spawn-rate 10 \
  --output ./benchmark-results
```

The same image can run as a Job in a separate cluster. A Kubernetes `command`
of `python standalone.py` replaces the image's API entrypoint; pass the same
arguments shown above. The standalone runner starts multiple synchronized
Locust processes automatically above 100 open-loop, saturation, or
fault-tolerance orders/s, or 1,000 closed-loop users. For example, a saturation
run can be started with:

```sh
python src/benchmarkservice/standalone.py \
  --application-type NATS \
  --url http://FRONTEND_EXTERNAL_IP \
  --metrics-url http://BENCHMARKMETRICS_EXTERNAL_IP/snapshot \
  --workload saturation \
  --warmup-seconds 0 \
  --duration-seconds 600 \
  --saturation-max-rate 1000 \
  --output ./benchmark-results
```

When the original GRPC application is deployed separately instead of through
`benchmark/benchmark-original-app.yaml`, deploy its metrics gateway with:

```sh
kubectl apply -k benchmark/manifests/target-metrics-grpc
```

For clusters without a LoadBalancer implementation, port-forwarding is enough
for a runner on the operator's host:

```sh
kubectl port-forward service/benchmarkmetrics-external 18080:80
```

Then use `--metrics-url http://127.0.0.1:18080/snapshot`.

## Fault-tolerance benchmark

Run fault injection from a host that has `kubectl` access to the target
cluster. The current kube context is used unless `--context` is supplied. The
script validates both Deployments and records their replica counts before it
starts load generation, then restores those counts after each fault and again
on errors or interrupts.

```sh
python src/benchmarkservice/fault_tolerance.py \
  --application-type NATS \
  --url http://FRONTEND_EXTERNAL_IP \
  --metrics-url http://BENCHMARKMETRICS_EXTERNAL_IP/snapshot \
  --namespace default \
  --arrival-rate 20 \
  --startup-time 30 \
  --disable-paymentservice-time 30 \
  --between-disable-time 30 \
  --disable-shippingservice-time 30 \
  --recovery-time 60 \
  --output ./benchmark-results
```

The account used by `kubectl` needs `get` access to Deployments and `update`
access to their `scale` subresource in the application namespace. The script
checks scale access itself; you can also verify it before starting:

```sh
kubectl auth can-i get deployments --namespace default
kubectl auth can-i update deployment/paymentservice \
  --subresource=scale --namespace default
```

Requests continue at the configured open-loop arrival rate through the
baseline, both outages, and both recovery windows. Metrics are sampled every
second. `summary.json` contains fault totals, the actual scale timestamps, a
per-second series, and convergence results; `fault-tolerance.csv` contains the
same time series in tabular form. Failed requests are recorded outcomes other
than `COMPLETED`. Lost requests are scheduled arrivals for which no logical
outcome record was written before the run ended.

Convergence starts when the scale-up command is issued, so it includes pod
startup. It is the first window of three seconds by default whose successfully
processed request rate is at least 90% of the pre-fault baseline. NATS runs
also require application-consumer pending events to return to the pre-fault
level. These thresholds can be changed with
`--convergence-stable-seconds`, `--convergence-success-fraction`, and
`--convergence-pending-tolerance`. `--recovery-time` must leave enough traffic
after the shipping fault to observe convergence.

The gateway can require a bearer token. Create a Secret named
`benchmark-metrics-auth` with the key `BENCHMARK_METRICS_TOKEN` in the target
cluster, restart `benchmarkmetrics`, and set the same environment variable on
the standalone runner or in the cluster hosting benchmark Jobs. The generated
bundles leave metrics authentication optional; restrict the LoadBalancer's
source ranges or enable the token before using it on an untrusted network.

## API configuration

The Deployment supplies `APPLICATION_TYPE`, the standard `NATS_*` connection
settings, and NATS credentials. `target_url` and `metrics_url` are required in
each `POST /api/runs` request and in the web form. Optional bucket overrides are
`BENCHMARK_RUN_BUCKET` and `BENCHMARK_ARTIFACT_BUCKET`.

The API discovers its own immutable container image through the Kubernetes API
and uses that exact image for Jobs. This keeps Job and controller code at the
same release without a mutable image environment variable.

The prerequisite NATS bootstrap creates the replicated benchmark stores used
by both NATS application runs and original-GRPC comparison runs. No benchmark
bundle contains its own broker, client configuration, credentials, or store
bootstrap resources.

Run-level settings include workload, warm-up/steady/drain durations, user,
arrival or saturation maximum rate, outcome/settlement timeouts, random seed,
and collector interval.
The web UI can also submit additional runs with the same settings, waiting for
each run to finish and for a configurable delay before submitting the next.
Each re-run has its own run ID and artifacts. Re-run scheduling is owned by the
browser tab, which must remain open until the sequence finishes.
The **Download All** action places every available artifact at the root of one
ZIP archive, prefixing each filename with its run ID to prevent collisions.

## Artifacts and resource collection

Each API Job uses bounded `emptyDir` volumes only as disposable staging space
and uploads its artifacts before exiting. Standalone runs write the equivalent
artifacts to their run directory. Depending on the workload, these include:

- `business.jsonl` and `business.csv`;
- `outstanding.jsonl` and `resources.jsonl`;
- `saturation.jsonl` for saturation rung observations and stop decisions;
- `fault-plan.json`, `faults.jsonl`, and `fault-tolerance.csv` for
  fault-tolerance runs;
- `summary.json`, `runner.log`, `config.json`, and `status.json`;
- diagnostic `locust_*.csv` files; and
- one ZIP archive containing the complete run.

Downloaded summary files can be analyzed without additional Python packages:

```sh
python src/benchmarkservice/parse_results.py ./benchmark-results
```

For every saturation summary, the script writes SVGs of goodput and P95 outcome
latency against the requested rate beside the input file. Rungs without a P95
latency sample are omitted from the latency graph. NATS saturation summaries
also get an SVG of maximum consumer-pending events. Fault-tolerance summaries
get an SVG of successfully processed requests per second; NATS fault-tolerance
summaries also get an SVG of queued events per second. Closed-run summaries
include the configured closed-loop user count and are grouped by that count
into `closed_results.tex` in the supplied results folder; the table reports
the average attempted orders per run, aggregate success rate, and the median
and population standard deviation of run-level P95 outcome latency.

The remote collector fetches kubelet summary and cAdvisor statistics, NATS
exporter metrics, and NATS micro endpoint statistics from the tested cluster's
metrics gateway. It does not use the Kubernetes API or NATS endpoints of the
cluster hosting the worker.
API pod restarts do not affect active Jobs, run visibility, or completed
artifacts.
