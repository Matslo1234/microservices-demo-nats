# Benchmark service

`benchmarkservice` is a horizontally scalable API for manually triggered
Locust comparisons. API replicas retain no run, lease, process, log, or
artifact state in their pods:

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

For NATS runs, accepted checkouts are followed through the order SSE endpoint.
`checkout_to_outcome` ends when the terminal order event is received by the
benchmark worker, and the same stream continues through notification and cart
clear settlement. The business artifacts include UTC/epoch receipt times and
SSE event IDs for the outcome and settlement events. Failed order events also
retain `failure_code`, `safe_message`, `failure_message`, and
`failure_received_at`; notification and cart-clear failures have their own
receipt-time fields.

Open-loop runs use one worker for every 100 requested orders/s. Closed-loop
runs use one worker for every 1,000 users, and divide the requested user spawn
rate evenly across those workers. Worker start times are synchronized. Worker
zero samples shared application resources once, then merges every worker's
business and outstanding-order records into the run-level report. Individual
Locust CSVs and logs remain in the complete artifact archive for diagnostics.

Warm-up samples remain in raw artifacts but are excluded from summaries. After
the steady interval, submission stops for the configured drain period.

## Configuration

The Deployment supplies `APPLICATION_TYPE`, `FRONTEND_ADDR`, the standard
`NATS_*` connection settings, and NATS credentials. Optional bucket overrides
are `BENCHMARK_RUN_BUCKET` and `BENCHMARK_ARTIFACT_BUCKET`.

The API discovers its own immutable container image through the Kubernetes API
and uses that exact image for Jobs. This keeps Job and controller code at the
same release without a mutable image environment variable.

For NATS application runs, the standard fresh-cluster bootstrap creates the
replicated stores. For original-GRPC comparison runs,
`benchmark/benchmark-original-app.yaml` includes an isolated JetStream
control/artifact store that is excluded from measured application resources.

Run-level settings include workload, warm-up/steady/drain durations, user or
arrival rate, outcome/settlement timeouts, random seed, and collector interval.
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
- `summary.json`, `runner.log`, `config.json`, and `status.json`;
- diagnostic `locust_*.csv` files; and
- one ZIP archive containing the complete run.

The embedded collector samples kubelet summary statistics and NATS exporter
metrics. API pod restarts do not affect active Jobs, run visibility, or
completed artifacts.
