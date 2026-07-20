# Online Boutique NetworkPolicies

NetworkPolicies are mandatory in the Phase 6 base. This compatibility
component is intentionally resource-free so existing overlays that still list
it continue to render without duplicating those policies.

The base applies default-deny ingress and egress, then permits only:

- HTTP `8080` ingress to application pods for frontend traffic, probes, and
  metrics;
- DNS and NATS TLS `4222` egress from named application workloads;
- optional OTLP `4317` egress to the in-namespace collector;
- cartservice to Redis `6379`; and
- loadgenerator to frontend `8080`.

No action is required to enable the policies. Render or apply the normal base:

```sh
kubectl kustomize kustomize
kubectl apply -k kustomize
kubectl get networkpolicy
```

The cluster CNI must enforce NetworkPolicy. GKE Dataplane V2 and Calico are
examples; verify enforcement for the selected platform before relying on these
rules as a security boundary.
