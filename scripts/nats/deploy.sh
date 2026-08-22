#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
inventory="${repo_root}/kubernetes-manifests/regions/inventory.yaml"
context=""
region=""
role=""
deploy_application=false

usage() {
  echo "usage: $0 --context CONTEXT --region REGION --role primary|secondary [--inventory PATH] [--application]" >&2
}

while (($#)); do
  case "$1" in
    --context) context="${2:-}"; shift 2 ;;
    --region) region="${2:-}"; shift 2 ;;
    --role) role="${2:-}"; shift 2 ;;
    --inventory) inventory="${2:-}"; shift 2 ;;
    --application) deploy_application=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "${context}" || -z "${region}" || -z "${role}" ]]; then
  usage
  exit 2
fi
if [[ ! -f "${inventory}" ]]; then
  echo "inventory ${inventory@Q} does not exist" >&2
  exit 1
fi
for command in kubectl python3; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "${command} is required" >&2
    exit 1
  fi
done

python3 "${repo_root}/scripts/nats/validate-regions.py" \
  --inventory "${inventory}" --region "${region}" --context "${context}" --role "${role}"
nats_mode="$(python3 "${repo_root}/scripts/nats/validate-regions.py" \
  --inventory "${inventory}" --region "${region}" --print-mode)"

if ! kubectl config get-contexts "${context}" >/dev/null 2>&1; then
  echo "kubectl context ${context@Q} does not exist" >&2
  exit 1
fi

if [[ "${nats_mode}" == "standalone" ]]; then
  nats_overlay="${repo_root}/kubernetes-manifests/nats/overlays/standalone"
else
  nats_overlay="${repo_root}/kubernetes-manifests/nats/overlays/supercluster/${region}"
fi
application_overlay="${repo_root}/regional-manifests/regions/${region}"
if [[ ! -d "${nats_overlay}" ]]; then
  echo "missing NATS overlay ${nats_overlay}" >&2
  exit 1
fi
if [[ "${deploy_application}" == true && ! -d "${application_overlay}" ]]; then
  echo "missing application overlay ${application_overlay}" >&2
  exit 1
fi

render_dir="$(mktemp -d)"
trap 'rm -rf "${render_dir}"' EXIT
kubectl kustomize "${nats_overlay}" >"${render_dir}/nats.yaml"
python3 "${repo_root}/scripts/nats/validate-rendered.py" \
  --manifest "${render_dir}/nats.yaml" --region "${region}" --role "${role}"
if [[ "${deploy_application}" == true ]]; then
  kubectl kustomize "${application_overlay}" >"${render_dir}/application.yaml"
  python3 "${repo_root}/scripts/nats/validate-rendered.py" \
    --manifest "${render_dir}/application.yaml" --region "${region}" --role "${role}" --application
fi

# One explicit context is used for every mutation. This script never loops over
# contexts or deploys a second region as a side effect.
kubectl --context "${context}" apply -f "${repo_root}/kubernetes-manifests/nats/base/namespace.yaml"
kubectl --context "${context}" --namespace nats delete job \
  nats-global-bootstrap nats-regional-bootstrap --ignore-not-found --wait=true
kubectl --context "${context}" apply -f "${render_dir}/nats.yaml"
kubectl --context "${context}" --namespace nats rollout restart deployment/nats-setup
kubectl --context "${context}" --namespace nats rollout status deployment/nats-setup --timeout=5m
kubectl --context "${context}" --namespace nats rollout restart statefulset/nats
kubectl --context "${context}" --namespace nats rollout status statefulset/nats --timeout=10m
if [[ "${role}" == primary ]]; then
  kubectl --context "${context}" --namespace nats wait --for=condition=complete job/nats-global-bootstrap --timeout=10m
fi
kubectl --context "${context}" --namespace nats wait --for=condition=complete job/nats-regional-bootstrap --timeout=10m

if [[ "${deploy_application}" == true ]]; then
  kubectl --context "${context}" apply -f "${render_dir}/application.yaml"
fi

application_deployments=(frontend storefrontprojectionservice paymentservice)
if [[ "${role}" == primary ]]; then
  application_deployments+=(
    benchmarkservice messageoperationsservice productcatalogservice
    currencyservice cartservice recommendationservice adservice
    checkoutservice shippingservice emailservice
  )
fi
restart_targets=()
for deployment in "${application_deployments[@]}"; do
  if kubectl --context "${context}" --namespace default get deployment "${deployment}" >/dev/null 2>&1; then
    restart_targets+=("deployment/${deployment}")
  fi
done
if ((${#restart_targets[@]} > 0)); then
  kubectl --context "${context}" --namespace default rollout restart "${restart_targets[@]}"
  for deployment in "${restart_targets[@]}"; do
    kubectl --context "${context}" --namespace default rollout status "${deployment}" --timeout=5m
  done
fi

echo "Deployed ${role} region ${region} to the single explicit context ${context}."
