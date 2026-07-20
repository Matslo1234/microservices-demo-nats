#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
projection_manifest="${repo_root}/kubernetes-manifests/nats/phase6-projection-rebuild.yaml"
restore_manifest="${repo_root}/kubernetes-manifests/nats/phase6-isolated-restore.yaml"

if [[ "${PHASE6_ALLOW_PROJECTION_REBUILD:-false}" != "true" ]]; then
  echo "Set PHASE6_ALLOW_PROJECTION_REBUILD=true to authorize deleting and replaying the rebuildable storefront projection." >&2
  exit 2
fi

frontend_replicas="$(kubectl --namespace default get deployment frontend -o jsonpath='{.spec.replicas}')"
projection_replicas="$(kubectl --namespace default get deployment storefrontprojectionservice -o jsonpath='{.spec.replicas}')"
restored=false

restore_workloads() {
  if [[ "${restored}" == "false" ]]; then
    kubectl --namespace default scale deployment/storefrontprojectionservice --replicas="${projection_replicas}" >/dev/null || true
    kubectl --namespace default scale deployment/frontend --replicas="${frontend_replicas}" >/dev/null || true
  fi
  kubectl --namespace nats delete -f "${restore_manifest}" --ignore-not-found --wait=true >/dev/null || true
  kubectl --namespace nats delete job \
    nats-phase6-projection-rebuild nats-phase6-replay-verification nats-phase6-backup \
    --ignore-not-found --wait=true >/dev/null || true
}
trap restore_workloads EXIT

kubectl --namespace default scale deployment/frontend deployment/storefrontprojectionservice --replicas=0
kubectl --namespace default rollout status deployment/frontend --timeout=3m
kubectl --namespace default rollout status deployment/storefrontprojectionservice --timeout=3m

kubectl --namespace nats delete job nats-phase6-projection-rebuild nats-phase6-replay-verification --ignore-not-found --wait=true
kubectl apply -f "${projection_manifest}" --selector='phase6.online-boutique.io/step=rebuild'
kubectl --namespace nats wait --for=condition=complete job/nats-phase6-projection-rebuild --timeout=5m
kubectl --namespace nats logs job/nats-phase6-projection-rebuild

kubectl --namespace default scale deployment/storefrontprojectionservice --replicas="${projection_replicas}"
kubectl --namespace default rollout status deployment/storefrontprojectionservice --timeout=10m
kubectl apply -f "${projection_manifest}" --selector='phase6.online-boutique.io/step=replay'
kubectl --namespace nats wait --for=condition=complete job/nats-phase6-replay-verification --timeout=6m
kubectl --namespace nats logs job/nats-phase6-replay-verification

kubectl --namespace default scale deployment/frontend --replicas="${frontend_replicas}"
kubectl --namespace default rollout status deployment/frontend --timeout=5m
restored=true

kubectl --namespace nats delete job nats-phase6-backup --ignore-not-found --wait=true
kubectl --namespace nats create job --from=cronjob/nats-backup nats-phase6-backup
kubectl --namespace nats wait --for=condition=complete job/nats-phase6-backup --timeout=15m
kubectl --namespace nats logs job/nats-phase6-backup

kubectl apply -f "${restore_manifest}" --selector='phase6.online-boutique.io/step=infrastructure'
kubectl --namespace nats rollout status statefulset/nats-phase6-restore-dr --timeout=5m
kubectl apply -f "${restore_manifest}" --selector='phase6.online-boutique.io/step=restore'
kubectl --namespace nats wait --for=condition=complete job/nats-phase6-restore-dr --timeout=15m
kubectl --namespace nats logs job/nats-phase6-restore-dr

echo "Phase 6 storefront replay and isolated JetStream restore verification passed."
