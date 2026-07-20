#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
rotate="${1:-}"

if [[ "${rotate}" != "--rotate" ]] && \
   kubectl --namespace nats get secret nats-server-auth >/dev/null 2>&1 && \
   kubectl --namespace nats get secret nats-server-tls >/dev/null 2>&1 && \
   kubectl --namespace nats get secret nats-admin-credentials >/dev/null 2>&1 && \
   kubectl --namespace nats get secret nats-messageoperations-credentials >/dev/null 2>&1 && \
   kubectl --namespace default get configmap nats-ca >/dev/null 2>&1; then
  echo "NATS secrets already exist; use --rotate to replace workload credentials."
  exit 0
fi

for command in kubectl openssl; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "${command} is required" >&2
    exit 1
  fi
done

kubectl apply -f "${repo_root}/kubernetes-manifests/nats/base/namespace.yaml"

secret_dir="$(mktemp -d)"
trap 'rm -rf "${secret_dir}"' EXIT
umask 077

random_secret() {
  printf 's%s\n' "$(openssl rand -hex 32)"
}

# Credential rotation must not silently make existing encrypted JetStream data
# unreadable or split route trust during a rolling restart. Preserve the data
# encryption key and TLS material; their rotation is a separate restore/rekey
# maintenance procedure documented in the runbook.
if [[ "${rotate}" == "--rotate" ]] && \
   kubectl --namespace nats get secret nats-server-auth >/dev/null 2>&1 && \
   kubectl --namespace nats get secret nats-server-tls >/dev/null 2>&1; then
  kubectl --namespace nats get secret nats-server-tls \
    -o jsonpath='{.data.ca\.crt}' | base64 --decode >"${secret_dir}/ca.crt"
  kubectl --namespace nats get secret nats-server-tls \
    -o jsonpath='{.data.tls\.crt}' | base64 --decode >"${secret_dir}/tls.crt"
  kubectl --namespace nats get secret nats-server-tls \
    -o jsonpath='{.data.tls\.key}' | base64 --decode >"${secret_dir}/tls.key"
  jetstream_encryption_key="$(kubectl --namespace nats get secret nats-server-auth \
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
MESSAGEOPERATIONSSERVICE_PASSWORD=${passwords[messageoperationsservice]}
EOF

kubectl --namespace nats create secret generic nats-server-auth \
  --from-env-file="${secret_dir}/server.env" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl --namespace nats create secret generic nats-server-tls \
  --from-file=ca.crt="${secret_dir}/ca.crt" \
  --from-file=tls.crt="${secret_dir}/tls.crt" \
  --from-file=tls.key="${secret_dir}/tls.key" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl --namespace nats create configmap nats-ca \
  --from-file=ca.crt="${secret_dir}/ca.crt" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl --namespace default create configmap nats-ca \
  --from-file=ca.crt="${secret_dir}/ca.crt" \
  --dry-run=client -o yaml | kubectl apply -f -

cat >"${secret_dir}/client.env" <<EOF
NATS_URL=tls://nats.nats.svc.cluster.local:4222
NATS_USER=admin
NATS_PASSWORD=$(awk -F= '$1 == "ADMIN_PASSWORD" {print $2}' "${secret_dir}/server.env")
EOF
kubectl --namespace nats create secret generic nats-admin-credentials \
  --from-env-file="${secret_dir}/client.env" \
  --dry-run=client -o yaml | kubectl apply -f -

for service in "${services[@]}"; do
  cat >"${secret_dir}/client.env" <<EOF
NATS_URL=tls://nats.nats.svc.cluster.local:4222
NATS_USER=${service}
NATS_PASSWORD=${passwords[${service}]}
EOF
  kubectl --namespace default create secret generic "nats-credentials-${service}" \
    --from-env-file="${secret_dir}/client.env" \
    --dry-run=client -o yaml | kubectl apply -f -
done

kubectl --namespace nats create secret generic nats-messageoperations-credentials \
  --from-literal=NATS_URL=tls://nats.nats.svc.cluster.local:4222 \
  --from-literal=NATS_USER=messageoperationsservice \
  --from-literal=NATS_PASSWORD="${passwords[messageoperationsservice]}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "Generated NATS admin and per-workload credentials without writing private material to the repository; existing TLS and JetStream encryption keys were preserved during credential rotation."
