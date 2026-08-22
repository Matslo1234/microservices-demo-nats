# External NATS supercluster benchmark

This benchmark measures event travel across both directions of the two-cluster
supercluster from an administrator workstation. It uses one Python process and
one monotonic clock, so the latency calculation does not depend on synchronized
regional clocks.

For each direction it records:

- Core NATS delivery latency from the local publish call to the callback on a
  client connected to the other cluster;
- JetStream delivery latency and publish-ack latency for temporary R1 and R3
  file-backed streams placed in the receiving cluster, including the R3-minus-R1
  median difference that isolates replication-factor impact;
- the stream leader and two followers as seen through both clusters, including
  replica current/offline/lag state and post-publish convergence wait; and
- disconnect detection, reconnect time, Core NATS loss, durable JetStream
  queued-message age, backlog drain time, and duplicate delivery while the
  receiving client connection is unavailable.

Every measured event is retained in the result CSV. The report gives the
median and population standard deviation, plus minimum, p95, maximum, and
missing count.

## Safety and scope

The runner does not expose a public NATS client port, deploy a pod, edit NATS
configuration, restart a broker, or interrupt a gateway. It opens two local
`kubectl port-forward` processes to the private `nats` Services. The failure
phase terminates only the benchmark's receiving port-forward, publishes while
that external client is disconnected, and restores the same tunnel on the
configured wall-clock deadline so the client can reconnect and resubscribe.
The Core and JetStream outage batches run concurrently, and JetStream publish
requests are pipelined so source-side publish-ack RTT does not extend the drop.

The run creates uniquely named R1 control and R3 streams with one-hour message
retention in each destination cluster, plus a durable push consumer on R3. It
deletes both temporary streams after that direction completes. Use
`--keep-streams` only when the temporary assets must remain for diagnosis.

The existing `metrics` sidecar on every NATS pod already runs
`prometheus-nats-exporter` with `gatewayz` and full `jsz` collection. No extra
metrics deployment or image is needed, and this benchmark intentionally adds
no NATS configuration manifest. Raw `/gatewayz` snapshots from each cluster
are saved before and after the measurements.

## Requirements

- Python 3.11 or newer; no third-party Python modules are required.
- `kubectl` contexts and credentials for both inventory entries.
- permission to port-forward `service/nats`, read `configmap/nats-ca` and
  `secret/nats-admin-credentials`, and exec into `nats-0` for the optional
  gateway snapshots.
- NATS account permission to create/delete streams and consumers and to
  publish/subscribe. The generated regional admin credential supplies this.

Refresh the AWS login and verify both contexts before a run:

```sh
aws sts get-caller-identity
kubectl --context arn:aws:eks:us-east-1:301085418229:cluster/eks-cluster get nodes
kubectl --context nats-eu-central-1 get nodes
```

The runner reads both contexts and NATS cluster names from
`kubernetes-manifests/regions/aws-supercluster-inventory.yaml` by default.
Secret values stay in process memory and are never written to the result
artifacts or command output.

## Run

From the repository root:

```sh
python3 benchmark/supercluster/supercluster_benchmark.py --events 100
```

Useful options include:

```sh
python3 benchmark/supercluster/supercluster_benchmark.py \
  --events 250 \
  --interval 0.02 \
  --drop-seconds 10 \
  --delivery-timeout 60 \
  --output /tmp/nats-supercluster-results
```

`--directions first-to-second` or `--directions second-to-first` restricts a
diagnostic run to the inventory order. The default is `both`. Run
`python3 benchmark/supercluster/supercluster_benchmark.py --help` for the full
interface. The paced outage publish span, `(events - 1) * interval`, must be
shorter than `--drop-seconds`; invalid combinations fail before connecting.
For example, 1,000 events at a 10 ms interval require a drop longer than 9.99
seconds.

Unless `--output` is set, results are written under
`benchmark/supercluster/results/<run-id>/`:

- `events.csv`: one row per event, including send/delivery timestamps,
  delivery latency, JetStream ack latency and sequence, missing state, and
  duplicate count;
- `summary.json`: machine-readable medians, population standard deviations,
  connection behavior, and replica health;
- `report.md`: the human-readable comparison tables; and
- `gateway-snapshots.json`: raw broker gateway state before and after the run.

The process exits nonzero if a baseline or recovery event is missing, a
JetStream event published during the dropped connection is not recovered, or
either cluster reports an unhealthy R3 stream view. It also fails a direction
if scheduling pressure causes any outage event to be sent after tunnel restart
begins. Core events sent after the disconnect are expected to be lost because
Core NATS does not persist for an absent subscriber; that count is reported
rather than treated as a failure.

## Reading the timings

Core delivery latency is the externally observed path: workstation to the
source cluster through one Kubernetes API tunnel, across the NATS gateway, and
back to the workstation through the destination tunnel. The per-cluster NATS
PING RTT samples in `summary.json` provide tunnel context, but should not be
naively subtracted from event latency.

JetStream publish-ack latency is the time until the stream acknowledges the
publish. Because both streams are placed in the receiving cluster, each
request and event must cross the gateway. R1 supplies the persistence control;
R3 must additionally reach the destination quorum. The report gives both
absolute distributions and the R3-minus-R1 median difference. R3 JetStream
delivery latency measures the same event arriving at the durable consumer.

The connection-drop report deliberately calls this value **queued age**, not
normal delivery latency: it includes the intentional outage from each event's
publish until durable delivery. Availability timing reports the scheduled drop,
actual tunnel-restart start, and client reconnect separately. Backlog recovery
reports time to the first recovered event after tunnel restart and the time
from first to last recovered event. This keeps regional publish-ack RTT,
wall-clock outage duration, and durable-consumer drain behavior from being
conflated.
