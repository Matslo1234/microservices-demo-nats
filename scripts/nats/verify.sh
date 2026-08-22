#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
context=""

usage() {
  echo "usage: $0 [--context CONTEXT]" >&2
}

while (($#)); do
  case "$1" in
    --context) context="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

kubectl_args=()
if [[ -n "${context}" ]]; then
  kubectl config get-contexts "${context}" >/dev/null 2>&1 || {
    echo "kubectl context ${context@Q} does not exist" >&2
    exit 1
  }
  kubectl_args=(--context "${context}")
fi

kubectl "${kubectl_args[@]}" --namespace nats delete job nats-phase1-verification \
  --ignore-not-found --wait=true
kubectl "${kubectl_args[@]}" apply -f "${repo_root}/kubernetes-manifests/nats/verification-job.yaml"
kubectl "${kubectl_args[@]}" --namespace nats wait --for=condition=complete \
  job/nats-phase1-verification --timeout=5m
kubectl "${kubectl_args[@]}" --namespace nats logs job/nats-phase1-verification
