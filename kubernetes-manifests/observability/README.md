# Online Boutique observability

This directory is a standalone Kubernetes observability stack. It is not
referenced by the application kustomization, does not add sidecars to application
pods, and does not require application source changes.

The stack contains:

- Prometheus, which discovers existing `prometheus.io/*` pod annotations and
  stores seven days of metrics.
- Grafana Alloy, which runs on each node and streams container logs through the
  Kubernetes API.
- Loki in monolithic mode, which stores seven days of logs.
- Grafana, which provisions Prometheus and Loki data sources plus an Online
  Boutique metrics-and-logs dashboard.

Prometheus and Loki each request a 10 GiB `ReadWriteOnce` persistent volume.
The default configuration is intended for development and modest demo traffic.
Loki uses a single filesystem-backed replica; use object storage and an
appropriately scaled Loki deployment for production log volumes.

## Deploy

The application and NATS can be deployed before or after this stack. A default
dynamic `StorageClass` is required.

```sh
kubectl apply -k kubernetes-manifests/observability

kubectl -n observability rollout status statefulset/prometheus --timeout=5m
kubectl -n observability rollout status statefulset/loki --timeout=5m
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
the home page. It combines service and NATS metrics with namespace/pod-filtered
logs. Grafana Explore can be used for ad hoc PromQL and LogQL queries.

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

Check Prometheus discovery and a Loki label after the pods are ready:

```sh
kubectl -n observability port-forward service/prometheus 9090:9090
```

In another terminal:

```sh
curl --fail 'http://localhost:9090/api/v1/query?query=up'

kubectl -n observability port-forward service/loki 3100:3100
curl --fail 'http://localhost:3100/loki/api/v1/labels'
```

Configuration updates to Prometheus, Loki, or Alloy require a rollout restart
of the corresponding workload. Grafana polls its provisioned dashboard files
every 30 seconds.
