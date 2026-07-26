# Benchmark service

`benchmarkservice` is the manually triggered Locust runner used to compare the
original synchronous Online Boutique application (`APPLICATION_TYPE=GRPC`)
with this repository's asynchronous application (`APPLICATION_TYPE=NATS`).
It does not start a load test when the process or pod starts.

The controller listens on port `8080`. Its small web interface starts one run
at a time, reports run state, and exports the summary, raw business samples,
and complete run artifacts. Locust executes in a child process so the
controller is effectively idle during a run.

## Workloads

- `closed` uses the original common task mix: home, currency, product,
  add-to-cart, cart, and checkout. A user waits for a logical checkout outcome
  before continuing.
- `open` schedules checkout transactions at an absolute arrival rate.
  Submission scheduling is separate from the greenlets that track completion,
  so slower completion does not reduce the requested arrival rate.

The GRPC adapter records a successful synchronous checkout response as the
business outcome. The NATS adapter records `202` acceptance separately, honors
`Retry-After` while polling, and records one `BUSINESS/checkout_to_outcome`
sample when the order becomes terminal. The NATS sample is the difference
between the checkout request start and the order projection's immutable
`outcome_at`; the polling interval therefore does not inflate the reported
latency. It also records
`BUSINESS/checkout_to_settled` when notification and the correlated cart-clear
operation have both terminated.

Warm-up samples remain in the raw artifacts but are excluded from the summary.
After the steady submission interval the service stops creating work and
allows the configured drain period. Accepted orders that cannot use their full
outcome timeout before the drain deadline are classified as `INCOMPLETE`.

## Configuration

Pod-level environment:

| Variable | Required | Meaning |
| --- | --- | --- |
| `APPLICATION_TYPE` | yes | Exactly `GRPC` or `NATS`. |
| `FRONTEND_ADDR` | yes | Frontend host/port or HTTP(S) URL. |
| `RESULTS_DIR` | no | Artifact directory; defaults to `/var/lib/benchmarkservice`. |
| `PORT` | no | Controller port; defaults to `8080`. |
| `NATS_METRICS_URLS` | no | Comma-separated exporter URLs for NATS runs. |

Run-level settings are validated by the controller and saved in each run's
`config.json`. They include workload, warm-up/steady/drain durations, closed
user and spawn counts, open arrival rate, outcome/settlement timeouts, random
seed, and collector interval.

The embedded collector samples kubelet summary statistics through the
Kubernetes API. It derives CPU-seconds, memory byte-seconds, and network bytes
for application pods while excluding the benchmark pod. NATS runs also scrape
the three existing NATS exporter sidecars for consumer pending/ack-pending
messages, redeliveries, and JetStream storage. No observability stack is
required.

## Artifacts

Each run directory contains:

- `business.jsonl` and `business.csv`: raw synthetic business samples;
- `outstanding.jsonl`: accepted non-terminal order count changes;
- `resources.jsonl`: low-frequency Kubernetes and NATS samples;
- `summary.json`: steady-state latency, rates, outcomes, goodput, and resource
  totals;
- `locust_*.csv`: diagnostic per-HTTP-request Locust statistics;
- `runner.log`, `config.json`, and `status.json`.

Aggregate HTTP request throughput is diagnostic only. Comparisons should use
the `BUSINESS/checkout_to_outcome` samples and completed-order goodput.

The supplied Kubernetes manifests mount `RESULTS_DIR` from `emptyDir` so the
experiment does not add persistent storage activity. Export needed artifacts
before replacing the benchmark pod.
