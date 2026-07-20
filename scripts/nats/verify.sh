#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

kubectl --namespace nats delete job nats-phase1-verification \
  --ignore-not-found --wait=true
kubectl apply -f "${repo_root}/kubernetes-manifests/nats/verification-job.yaml"
kubectl --namespace nats wait --for=condition=complete \
  job/nats-phase1-verification --timeout=5m
kubectl --namespace nats logs job/nats-phase1-verification
