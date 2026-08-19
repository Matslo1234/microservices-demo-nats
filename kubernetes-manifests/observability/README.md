# Online Boutique observability

This directory is a standalone Kubernetes observability stack. It is not
referenced by the application kustomization and does not add sidecars to
application pods. The application manifests configure each domain service to
export OTLP traces to Alloy; exporters retry harmlessly while this optional
stack is absent.

The stack contains:

- Prometheus, which discovers existing `prometheus.io/*` pod annotations and
  stores seven days of metrics.
- Grafana Alloy, which runs on each node, streams container logs through the
  Kubernetes API, and receives OTLP/gRPC traces on port `4317`.
- Loki in monolithic mode, which stores seven days of logs.
- Tempo in monolithic mode, which stores seven days of traces.
- Grafana, which provisions Prometheus, Loki, and Tempo data sources plus an
  Online Boutique metrics, logs, and OpenTelemetry traces dashboard.

Prometheus, Loki, and Tempo each request a 10 GiB `ReadWriteOnce` persistent
volume.
The default configuration is intended for development and modest demo traffic.
Loki and Tempo use single filesystem-backed replicas; use object storage and
appropriately scaled deployments for production telemetry volumes.

## Deploy

The application and NATS can be deployed before or after this stack. A default
dynamic `StorageClass` is required.

```sh
kubectl apply -k kubernetes-manifests/observability

kubectl -n observability rollout status statefulset/prometheus --timeout=5m
kubectl -n observability rollout status statefulset/loki --timeout=5m
kubectl -n observability rollout status statefulset/tempo --timeout=5m
kubectl -n observability rollout status daemonset/alloy --timeout=5m
kubectl -n observability rollout status deployment/grafana --timeout=5m
```

The observability resources live only in the `observability` namespace. The two
cluster roles are read-only and are limited to Kubernetes discovery and pod log
streaming.

## Open the frontend

Grafana is exposed as a cluster-internal Service and uses anonymous Viewer
access, so no password is needed through an authenticated `kubectl`
port-forward:

```sh
kubectl -n observability port-forward service/grafana 3000:3000
```

Open <http://localhost:3000>. The **Online Boutique Observability** dashboard is
the home page. It combines service and NATS metrics, namespace/pod-filtered
logs, and recent OpenTelemetry traces. Use the **Trace service** and
**Correlation ID** dashboard fields to filter traces; trace IDs open the full
span timeline and related logs. Choose **View waterfall** from a trace-table row
to load that trace into the dashboard's waterfall timeline, or paste its ID into
the **Selected trace ID** field. Grafana Explore remains available for ad hoc
PromQL, LogQL, and TraceQL queries.

To deliberately expose Grafana through a cloud load balancer:

```sh
kubectl -n observability patch service grafana \
  --type merge \
  --patch '{"spec":{"type":"LoadBalancer"}}'
```

Do not expose the anonymous frontend to an untrusted network. Configure Grafana
authentication or place it behind an authenticated ingress before enabling
external access.

## Verify data ingestion

Check Prometheus discovery, a Loki label, and Tempo readiness after the pods
are ready:

```sh
kubectl -n observability port-forward service/prometheus 9090:9090
```

In another terminal:

```sh
curl --fail 'http://localhost:9090/api/v1/query?query=up'

kubectl -n observability port-forward service/loki 3100:3100
curl --fail 'http://localhost:3100/loki/api/v1/labels'

kubectl -n observability port-forward service/tempo 3200:3200
curl --fail 'http://localhost:3200/ready'
```

Configuration updates to Prometheus, Loki, Tempo, or Alloy require a rollout
restart of the corresponding workload. Grafana polls its provisioned dashboard
files every 30 seconds. Grafana loads data sources only during startup, so the
Kustomization generates their ConfigMap with a content hash; reapplying it
changes the Deployment volume reference and automatically rolls Grafana when
the data-source configuration changes.
