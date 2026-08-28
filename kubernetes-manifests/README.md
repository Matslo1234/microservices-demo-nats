# ./kubernetes-manifests

:warning: Kubernetes manifests provided in this directory are not directly
deployable to a cluster. They are meant to be used with `skaffold` command to
insert the correct `image:` tags.

Use the manifests in [/release](/release) directory which are configured with
pre-built public images.

The standalone [observability](observability) kustomization is an exception: it
uses pinned upstream images and can be applied directly with `kubectl apply -k`.

Regional deployment entry points live in
[`../regional-manifests/regions`](../regional-manifests/regions). NATS has an
explicit standalone overlay for the current local cluster and supercluster
templates under [`nats/overlays`](nats/overlays). Prefer
`scripts/nats/deploy.sh --context <context> --region <region> --role
<primary|secondary> --application`; it validates the inventory and rendered
role boundary, then applies to exactly one context.

For a resource-constrained development/test cluster, the
[`nats/single-worker`](nats/single-worker) entry point runs one NATS server and
uses one JetStream copy while inheriting the standalone configuration's
encryption, persistence, resource, security, monitoring, and backup settings.
It provides no NATS server-failure tolerance.

The local overlay is intentionally WAN-free. A supercluster overlay must have
real ordinal gateway DNS, gateway certificate SANs, shared gateway trust, and
allowlisted TCP `7222` CIDRs before it is selected in the inventory.
