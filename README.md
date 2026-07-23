# Online Boutique with NATS

This repository runs the Online Boutique microservices demo on any conformant
Kubernetes cluster. It uses Kubernetes resources, a container registry, and
NATS for the event-driven workflows; no provider-specific services are
required.

The goal of the project is to explore how NATS can be used for event-driven architecture. The user-facing functinallity is practically identical to the [original project's](https://github.com/GoogleCloudPlatform/microservices-demo).

## Architecture

Online Boutique is composed of microservices written in Go, C#, Node.js,
Python, and Java. Deployed business interactions use NATS commands, events,
bounded queries, storefront projections, and durable checkout processing. HTTP
is used at the frontend edge and for pod-local health/metrics only.

```mermaid
flowchart LR
    User[Browser or API client] -->|HTTP| FE[frontend]
    Load[loadgenerator] -->|HTTP| FE

    FE -->|Core NATS projected queries| Projection[storefront projection]
    FE -->|JetStream cart/order commands| NATS[(NATS JetStream)]
    FE -->|Core NATS tokenization query| Payment[payment]

    Catalog[product catalog] -->|owner snapshots| NATS
    Currency[currency] -->|owner snapshots| NATS
    NATS --> Cart[cart]
    NATS --> Recommendation[recommendation]
    NATS --> Ad[ad]
    NATS --> Checkout[checkout process manager]
    NATS --> Shipping[shipping]
    NATS --> Payment
    NATS --> Email[email]
    NATS --> Projection
    Cart -->|Redis protocol| Redis[(redis-cart)]

    Apps[All domain workloads] -.->|HTTP health and metrics :8080| Prometheus
    Kubernetes[Kubernetes pod logs] -.->|Kubernetes API| Alloy
    Alloy --> Loki
    Prometheus --> Grafana
    Loki --> Grafana
    Operator[Operator browser] --> Grafana
```


| Service | Language | Description |
| --- | --- | --- |
| [frontend](src/frontend) | Go | Browser-facing web application. |
| [cartservice](src/cartservice) | C# | Shopping cart storage and cart commands. |
| [productcatalogservice](src/productcatalogservice) | Go | Product catalogue. |
| [currencyservice](src/currencyservice) | Node.js | Currency conversion. |
| [paymentservice](src/paymentservice) | Node.js | Mock payment processing. |
| [shippingservice](src/shippingservice) | Go | Mock shipping quotes and fulfilment. |
| [emailservice](src/emailservice) | Python | Order notifications. |
| [checkoutservice](src/checkoutservice) | Go | Checkout orchestration. |
| [recommendationservice](src/recommendationservice) | Python | Product recommendations. |
| [adservice](src/adservice) | Java | Contextual advertisements. |
| [storefrontprojectionservice](src/storefrontprojectionservice) | Go | NATS-backed storefront read model. |

## Quickstart

Requirements:

- A Kubernetes cluster and `kubectl` context with cluster-admin access.
- A default dynamic `StorageClass`. The NATS and application PVCs deliberately
  do not contain node names, host paths, or static PV references.
- Docker and access to a container registry for application images.

Clone this repository:

```sh
git clone https://github.com/Matslo1234/microservices-demo-nats.git
cd microservices-demo-nats
```

### 1. Provide dynamic storage

If `kubectl get storageclass` shows no default class, install a provisioner. For
a development or test cluster, Rancher's local-path provisioner is a simple
option:

```sh
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.36/deploy/local-path-storage.yaml

kubectl annotate storageclass local-path \
  storageclass.kubernetes.io/is-default-class=true \
  --overwrite

kubectl get storageclass
```

Local-path storage is tied to the selected node and is appropriate for
development or testing. For durable multi-node storage, install the CSI driver
for your storage platform and mark its StorageClass as the default instead.

### 2. Install NATS

Apply NATS before the application. The setup Deployment creates missing broker
TLS, auth, and JetStream-encryption Secrets without rotating existing values.

```sh
kubectl apply -k kubernetes-manifests/nats/fresh-cluster
kubectl -n nats rollout status deployment/nats-setup --timeout=5m
kubectl -n nats rollout status statefulset/nats --timeout=10m
kubectl -n nats wait --for=condition=complete job/nats-bootstrap --timeout=5m
```

Optionally run the NATS acceptance checks:

```sh
bash scripts/nats/verify.sh
```

### 3. Deploy the application


Note: the application requires a working NATS setup. Verify that NATS is running before you deploy the application.

You can use the release manifest (replace its image prefix/tag when publishing
your own build):

```sh
kubectl apply -f release/kubernetes-manifests.yaml
```

Check the status of the deploy. Wait for all pods to start.
```sh
kubectl get pods
```

Once the deploy finishes you can access the application via the ip of the LoadBalancer. Use CLUSTER-IP if running the cluster locally or EXTERNAL-IP if running on a remote cluster.
```sh
kubectl get service frontend-external
```

### 4. Add logs and metrics

An optional, standalone observability stack captures Kubernetes container logs
and scrapes the application's existing metrics endpoints. It runs entirely in
the `observability` namespace and provides a preconfigured Grafana frontend:

```sh
kubectl apply -k kubernetes-manifests/observability
```

After the deploy finishes you can connect to the cluster ip / extrnal ip of grafana
```sh
kubectl get service -n observability grafana
```

Open <http://localhost:3000>. Deployment, storage, access, and production notes
are in the [observability stack documentation](kubernetes-manifests/observability/README.md).

## Development

For local development you will need to build your own Docker images and publish them to your own Docker image repository. You can use [build-and-push-images.sh](scripts/build-and-push-images.sh) to build all of the images but you must provide your own username and image tag.
