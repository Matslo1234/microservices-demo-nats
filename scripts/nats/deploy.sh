#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required to deploy the NATS platform" >&2
  exit 1
fi

kubectl apply -f "${repo_root}/kubernetes-manifests/nats/base/namespace.yaml"

# Jobs are immutable and the bootstrap is intentionally rerun on each deploy so
# declarative stream changes are reconciled.
kubectl --namespace nats delete job nats-bootstrap --ignore-not-found --wait=true
kubectl apply -k "${repo_root}/kubernetes-manifests/nats/fresh-cluster"
# ConfigMaps do not change StatefulSet/Deployment pod templates, so explicitly
# roll the setup controller and workloads to pick up reconciled configuration.
kubectl --namespace nats rollout restart deployment/nats-setup
kubectl --namespace nats rollout status deployment/nats-setup --timeout=5m
kubectl --namespace nats rollout restart statefulset/nats deployment/nats-advisory-watcher
kubectl --namespace nats rollout status statefulset/nats --timeout=10m
kubectl --namespace nats wait --for=condition=complete job/nats-bootstrap --timeout=5m
kubectl --namespace nats rollout status deployment/nats-advisory-watcher --timeout=5m

# nats-setup may have synchronized Secret-backed environment variables after a
# nats namespace recreation or credential rotation. Restart only deployments
# that consume those credentials so they refresh both passwords and the CA.
application_deployments=(
  frontend
  storefrontprojectionservice
  productcatalogservice
  currencyservice
  cartservice
  recommendationservice
  adservice
  checkoutservice
  shippingservice
  paymentservice
  emailservice
)
restart_targets=()
for deployment in "${application_deployments[@]}"; do
  if kubectl --namespace default get deployment "${deployment}" >/dev/null 2>&1; then
    restart_targets+=("deployment/${deployment}")
  fi
done

if ((${#restart_targets[@]} > 0)); then
  kubectl --namespace default rollout restart "${restart_targets[@]}"
  for deployment in "${restart_targets[@]}"; do
    kubectl --namespace default rollout status "${deployment}" --timeout=5m
  done
fi

echo "NATS Phase 1 platform deployed."
