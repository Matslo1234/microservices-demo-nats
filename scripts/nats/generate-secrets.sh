#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
context=""
region=""
rotate=false

usage() {
  echo "usage: $0 --context CONTEXT --region REGION [--rotate]" >&2
}

while (($#)); do
  case "$1" in
    --context) context="${2:-}"; shift 2 ;;
    --region) region="${2:-}"; shift 2 ;;
    --rotate) rotate=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "${context}" || -z "${region}" ]]; then
  usage
  exit 2
fi

kc() {
  kubectl --context "${context}" "$@"
}

for command in kubectl openssl python3; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "${command} is required" >&2
    exit 1
  fi
done

if ! kc config get-contexts "${context}" >/dev/null 2>&1; then
  echo "kubectl context ${context@Q} does not exist" >&2
  exit 1
fi

python3 "${repo_root}/scripts/nats/validate-regions.py" \
  --inventory "${repo_root}/kubernetes-manifests/regions/inventory.yaml" \
  --region "${region}" --context "${context}"
cluster_role="$(python3 "${repo_root}/scripts/nats/validate-regions.py" \
  --inventory "${repo_root}/kubernetes-manifests/regions/inventory.yaml" \
  --region "${region}" --print-role)"
cluster_mode="$(python3 "${repo_root}/scripts/nats/validate-regions.py" \
  --inventory "${repo_root}/kubernetes-manifests/regions/inventory.yaml" \
  --region "${region}" --print-mode)"

if [[ "${cluster_role}" == secondary ]]; then
  if ! kc --namespace default get secret global-application-secrets >/dev/null 2>&1; then
    echo "secondary region ${region} requires a pre-provisioned global-application-secrets Secret" >&2
    exit 1
  fi
fi
if [[ "${cluster_mode}" == supercluster ]] && \
   ! kc --namespace nats get secret nats-gateway-tls >/dev/null 2>&1; then
  echo "supercluster region ${region} requires a pre-provisioned nats-gateway-tls Secret" >&2
  exit 1
fi

if [[ "${rotate}" == false ]] && \
   kc --namespace nats get secret nats-server-auth >/dev/null 2>&1 && \
   kc --namespace nats get secret nats-server-tls >/dev/null 2>&1 && \
   kc --namespace nats get secret nats-admin-credentials >/dev/null 2>&1 && \
   kc --namespace nats get secret nats-messageoperations-credentials >/dev/null 2>&1 && \
   kc --namespace default get secret messageoperations-admin-api >/dev/null 2>&1 && \
   kc --namespace default get secret global-application-secrets >/dev/null 2>&1 && \
   kc --namespace default get configmap nats-ca >/dev/null 2>&1; then
  echo "NATS secrets already exist; use --rotate to replace workload credentials."
  exit 0
fi

kc apply -f "${repo_root}/kubernetes-manifests/nats/base/namespace.yaml"

secret_dir="$(mktemp -d)"
trap 'rm -rf "${secret_dir}"' EXIT
umask 077

random_secret() {
  printf 's%s\n' "$(openssl rand -hex 32)"
}

# Credential rotation must not silently make existing encrypted JetStream data
# unreadable or split route trust during a rolling restart. Preserve the data
# encryption key and TLS material. Global cookie/payment/shipping keys are not
# regional broker credentials and are never rotated by this script.
if [[ "${rotate}" == true ]] && \
   kc --namespace nats get secret nats-server-auth >/dev/null 2>&1 && \
   kc --namespace nats get secret nats-server-tls >/dev/null 2>&1; then
  kc --namespace nats get secret nats-server-tls \
    -o jsonpath='{.data.ca\.crt}' | base64 --decode >"${secret_dir}/ca.crt"
  kc --namespace nats get secret nats-server-tls \
    -o jsonpath='{.data.tls\.crt}' | base64 --decode >"${secret_dir}/tls.crt"
  kc --namespace nats get secret nats-server-tls \
    -o jsonpath='{.data.tls\.key}' | base64 --decode >"${secret_dir}/tls.key"
  jetstream_encryption_key="$(kc --namespace nats get secret nats-server-auth \
    -o jsonpath='{.data.JETSTREAM_ENCRYPTION_KEY}' | base64 --decode)"
else
  openssl req -x509 -newkey rsa:3072 -sha256 -nodes -days 3650 \
    -subj "/CN=Online Boutique NATS CA" \
    -keyout "${secret_dir}/ca.key" \
    -out "${secret_dir}/ca.crt" >/dev/null 2>&1

  openssl req -newkey rsa:3072 -sha256 -nodes \
    -subj "/CN=nats.nats.svc.cluster.local" \
    -keyout "${secret_dir}/tls.key" \
    -out "${secret_dir}/tls.csr" >/dev/null 2>&1

  openssl x509 -req -sha256 -days 825 \
    -in "${secret_dir}/tls.csr" \
    -CA "${secret_dir}/ca.crt" \
    -CAkey "${secret_dir}/ca.key" \
    -CAcreateserial \
    -extfile <(printf '%s\n' \
      'subjectAltName=DNS:nats,DNS:nats.nats,DNS:nats.nats.svc,DNS:nats.nats.svc.cluster.local,DNS:nats-headless,DNS:nats-headless.nats.svc.cluster.local,DNS:*.nats-headless.nats.svc.cluster.local' \
      'extendedKeyUsage=serverAuth,clientAuth' \
      'keyUsage=digitalSignature,keyEncipherment') \
    -out "${secret_dir}/tls.crt" >/dev/null 2>&1
  jetstream_encryption_key="$(random_secret)"
fi

declare -A passwords
services=(
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
  benchmarkservice
  messageoperationsservice
)

for service in "${services[@]}"; do
  passwords["${service}"]="$(random_secret)"
done

cat >"${secret_dir}/server.env" <<EOF
JETSTREAM_ENCRYPTION_KEY=${jetstream_encryption_key}
SYS_PASSWORD=$(random_secret)
ADMIN_PASSWORD=$(random_secret)
FRONTEND_PASSWORD=${passwords[frontend]}
STOREFRONTPROJECTIONSERVICE_PASSWORD=${passwords[storefrontprojectionservice]}
PRODUCTCATALOGSERVICE_PASSWORD=${passwords[productcatalogservice]}
CURRENCYSERVICE_PASSWORD=${passwords[currencyservice]}
CARTSERVICE_PASSWORD=${passwords[cartservice]}
RECOMMENDATIONSERVICE_PASSWORD=${passwords[recommendationservice]}
ADSERVICE_PASSWORD=${passwords[adservice]}
CHECKOUTSERVICE_PASSWORD=${passwords[checkoutservice]}
SHIPPINGSERVICE_PASSWORD=${passwords[shippingservice]}
PAYMENTSERVICE_PASSWORD=${passwords[paymentservice]}
EMAILSERVICE_PASSWORD=${passwords[emailservice]}
BENCHMARKSERVICE_PASSWORD=${passwords[benchmarkservice]}
MESSAGEOPERATIONSSERVICE_PASSWORD=${passwords[messageoperationsservice]}
EOF

kc --namespace nats create secret generic nats-server-auth \
  --from-env-file="${secret_dir}/server.env" \
  --dry-run=client -o yaml | kc apply -f -

kc --namespace nats create secret generic nats-server-tls \
  --from-file=ca.crt="${secret_dir}/ca.crt" \
  --from-file=tls.crt="${secret_dir}/tls.crt" \
  --from-file=tls.key="${secret_dir}/tls.key" \
  --dry-run=client -o yaml | kc apply -f -

# These values are global business identity. Preserve an externally
# distributed Secret when it already exists; standalone development creates it
# once for convenience.
if ! kc --namespace default get secret global-application-secrets >/dev/null 2>&1; then
  if [[ "${cluster_role}" == secondary ]]; then
    echo "secondary region ${region} requires a pre-provisioned global-application-secrets Secret" >&2
    exit 1
  fi
  kc --namespace default create secret generic global-application-secrets \
    --from-literal=FRONTEND_COOKIE_KEY="$(random_secret)" \
    --from-literal=PAYMENT_SIGNING_KEY="$(random_secret)" \
    --from-literal=PAYMENT_VERIFICATION_KEYS='{}' \
    --from-literal=SHIPPING_PROVIDER_SECRET="$(random_secret)" \
    --dry-run=client -o yaml | kc apply -f -
else
  echo "Preserving global application business keys."
fi

kc --namespace nats create configmap nats-ca \
  --from-file=ca.crt="${secret_dir}/ca.crt" \
  --dry-run=client -o yaml | kc apply -f -
kc --namespace default create configmap nats-ca \
  --from-file=ca.crt="${secret_dir}/ca.crt" \
  --dry-run=client -o yaml | kc apply -f -

cat >"${secret_dir}/client.env" <<EOF
NATS_URL=tls://nats.nats.svc.cluster.local:4222
NATS_USER=admin
NATS_PASSWORD=$(awk -F= '$1 == "ADMIN_PASSWORD" {print $2}' "${secret_dir}/server.env")
EOF
kc --namespace nats create secret generic nats-admin-credentials \
  --from-env-file="${secret_dir}/client.env" \
  --dry-run=client -o yaml | kc apply -f -

for service in "${services[@]}"; do
  {
    cat <<EOF
NATS_URL=tls://nats.nats.svc.cluster.local:4222
NATS_USER=${service}
NATS_PASSWORD=${passwords[${service}]}
EOF
  } >"${secret_dir}/client.env"
  kc --namespace default create secret generic "nats-credentials-${service}" \
    --from-env-file="${secret_dir}/client.env" \
    --dry-run=client -o yaml | kc apply -f -
done

kc --namespace nats create secret generic nats-messageoperations-credentials \
  --from-literal=NATS_URL=tls://nats.nats.svc.cluster.local:4222 \
  --from-literal=NATS_USER=messageoperationsservice \
  --from-literal=NATS_PASSWORD="${passwords[messageoperationsservice]}" \
  --dry-run=client -o yaml | kc apply -f -

if [[ "${rotate}" == false ]] && \
   kc --namespace default get secret messageoperations-admin-api >/dev/null 2>&1; then
  echo "Preserving existing messageoperations administrator API token."
else
  kc --namespace default create secret generic messageoperations-admin-api \
    --from-literal=ADMIN_USER=admin \
    --from-literal=ADMIN_API_TOKEN="$(random_secret)" \
    --dry-run=client -o yaml | kc apply -f -
fi

echo "Generated regional NATS credentials without writing private material to the repository; global application keys, TLS, and JetStream encryption were preserved during regional credential rotation."
