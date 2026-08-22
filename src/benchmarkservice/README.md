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
  10 orders/s, it measures 10 orders/s for 10 seconds and adds 10 orders/s
  every 10 seconds. It stops when observed goodput no longer increases. A NATS
  run also stops when total application-consumer pending grows by at least 10
  messages/s during a rung. `saturation_max_rate` and the steady duration are
  safety bounds rather than fixed target rates.

For NATS runs, accepted checkouts are followed through the order SSE endpoint.
`checkout_to_outcome` ends when the terminal order event is received by the
benchmark worker, and the same stream continues through notification and cart
clear settlement. The business artifacts include UTC/epoch receipt times and
SSE event IDs for the outcome and settlement events. Failed order events also
retain `failure_code`, `safe_message`, `failure_message`, and
`failure_received_at`; notification and cart-clear failures have their own
receipt-time fields.

Open-loop and saturation runs use one worker for every 100 requested orders/s.
Closed-loop runs use one worker for every 1,000 users, and divide the requested
user spawn rate evenly across those workers. Worker start times and saturation
rung decisions are synchronized. Worker zero samples shared application
resources once, then merges every worker's business and outstanding-order
records into the run-level report. Individual Locust CSVs and logs remain in
the complete artifact archive for diagnostics.

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
kubectl apply -f benchmark/benchmark-nats-multiple-replicas.yaml
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
Locust processes automatically above 100 open-loop/saturation orders/s or
1,000 closed-loop users. For example, a saturation run can be started with:

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

Each Job uses bounded `emptyDir` volumes only as disposable staging space.
Before it exits it uploads:

- `business.jsonl` and `business.csv`;
- `outstanding.jsonl` and `resources.jsonl`;
- `saturation.jsonl` for saturation rung observations and stop decisions;
- `summary.json`, `runner.log`, `config.json`, and `status.json`;
- diagnostic `locust_*.csv` files; and
- one ZIP archive containing the complete run.

The remote collector fetches kubelet summary statistics and NATS exporter
metrics from the tested cluster's metrics gateway. It does not use the
Kubernetes API or NATS exporter endpoints of the cluster hosting the worker.
API pod restarts do not affect active Jobs, run visibility, or completed
artifacts.
