#!/usr/bin/env bash

set -euo pipefail

ca_context=""
target_context=""
dns_names=""

usage() {
  echo "usage: $0 --ca-context CONTEXT --target-context CONTEXT --dns-names DNS1,DNS2[,DNS3]" >&2
}

while (($#)); do
  case "$1" in
    --ca-context) ca_context="${2:-}"; shift 2 ;;
    --target-context) target_context="${2:-}"; shift 2 ;;
    --dns-names) dns_names="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "${ca_context}" || -z "${target_context}" || -z "${dns_names}" ]]; then
  usage
  exit 2
fi
if [[ ! "${dns_names}" =~ ^[A-Za-z0-9.-]+(,[A-Za-z0-9.-]+)*$ ]]; then
  echo "--dns-names must be a comma-separated DNS name list" >&2
  exit 2
fi
for command in base64 kubectl openssl; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "${command} is required" >&2
    exit 1
  }
done
for context in "${ca_context}" "${target_context}"; do
  kubectl config get-contexts "${context}" >/dev/null 2>&1 || {
    echo "kubectl context ${context@Q} does not exist" >&2
    exit 1
  }
  kubectl --context "${context}" get namespace nats >/dev/null
done

pki_dir="$(mktemp -d)"
signer_name=""
cleanup() {
  if [[ -n "${signer_name}" ]]; then
    kubectl --context "${ca_context}" --namespace nats delete job "${signer_name}" \
      --ignore-not-found --wait=false >/dev/null 2>&1 || true
    kubectl --context "${ca_context}" --namespace nats delete configmap "${signer_name}" \
      --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  find "${pki_dir}" -type f -delete
  rmdir "${pki_dir}"
}
trap cleanup EXIT
umask 077

if kubectl --context "${ca_context}" --namespace nats get secret nats-gateway-ca >/dev/null 2>&1; then
  kubectl --context "${ca_context}" --namespace nats get secret nats-gateway-ca \
    -o jsonpath='{.data.ca\.crt}' | base64 --decode >"${pki_dir}/ca.crt"
else
  openssl req -x509 -newkey rsa:3072 -sha256 -nodes -days 3650 \
    -subj "/CN=Online Boutique NATS Gateway CA" \
    -keyout "${pki_dir}/ca.key" -out "${pki_dir}/ca.crt" >/dev/null 2>&1
  kubectl --context "${ca_context}" --namespace nats create secret generic nats-gateway-ca \
    --from-file=ca.crt="${pki_dir}/ca.crt" \
    --from-file=ca.key="${pki_dir}/ca.key" \
    --dry-run=client -o yaml | \
    kubectl --context "${ca_context}" apply -f -
fi

san_list="DNS:${dns_names//,/,DNS:}"
{
  echo "basicConstraints=critical,CA:FALSE"
  echo "keyUsage=critical,digitalSignature,keyEncipherment"
  echo "extendedKeyUsage=serverAuth,clientAuth"
  echo "subjectAltName=${san_list}"
} >"${pki_dir}/leaf.ext"

openssl req -newkey rsa:3072 -sha256 -nodes \
  -subj "/CN=Online Boutique NATS Gateway" \
  -keyout "${pki_dir}/tls.key" -out "${pki_dir}/tls.csr" >/dev/null 2>&1

# Sign inside the CA cluster so an existing CA private key is never exported
# through kubectl or written to the operator's filesystem.
signer_name="nats-gateway-signer-$(date +%s)-$$"
kubectl --context "${ca_context}" --namespace nats create configmap "${signer_name}" \
  --from-file=tls.csr="${pki_dir}/tls.csr" \
  --from-file=leaf.ext="${pki_dir}/leaf.ext" \
  --dry-run=client -o yaml | \
  kubectl --context "${ca_context}" apply -f - >/dev/null

kubectl --context "${ca_context}" --namespace nats apply -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${signer_name}
  namespace: nats
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 300
  template:
    metadata:
      labels:
        app.kubernetes.io/name: nats-gateway-signer
        app.kubernetes.io/part-of: online-boutique
    spec:
      automountServiceAccountToken: false
      restartPolicy: Never
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
      - name: signer
        image: alpine/openssl:3.3.3@sha256:0cb8b9948f86d73f196cef2f828ca0b60e40c7c74d8edf9e60d9990cbb7b8b76
        imagePullPolicy: IfNotPresent
        command: ["/bin/sh", "-ec"]
        args:
        - |
          openssl x509 -req -sha256 -days 825 \
            -in /request/tls.csr \
            -CA /ca/ca.crt -CAkey /ca/ca.key \
            -set_serial "0x\$(openssl rand -hex 16)" \
            -extfile /request/leaf.ext -out /tmp/tls.crt >/dev/null 2>&1
          base64 /tmp/tls.crt | tr -d '\n'
        resources:
          requests:
            cpu: 10m
            memory: 16Mi
          limits:
            cpu: 100m
            memory: 64Mi
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop: ["ALL"]
          readOnlyRootFilesystem: true
        volumeMounts:
        - name: ca
          mountPath: /ca
          readOnly: true
        - name: request
          mountPath: /request
          readOnly: true
        - name: tmp
          mountPath: /tmp
      volumes:
      - name: ca
        secret:
          secretName: nats-gateway-ca
      - name: request
        configMap:
          name: ${signer_name}
      - name: tmp
        emptyDir: {}
EOF
kubectl --context "${ca_context}" --namespace nats wait \
  --for=condition=complete "job/${signer_name}" --timeout=2m >/dev/null
kubectl --context "${ca_context}" --namespace nats logs "job/${signer_name}" | \
  base64 --decode >"${pki_dir}/tls.crt"
openssl verify -CAfile "${pki_dir}/ca.crt" "${pki_dir}/tls.crt" >/dev/null

kubectl --context "${target_context}" --namespace nats create secret generic nats-gateway-tls \
  --from-file=ca.crt="${pki_dir}/ca.crt" \
  --from-file=tls.crt="${pki_dir}/tls.crt" \
  --from-file=tls.key="${pki_dir}/tls.key" \
  --dry-run=client -o yaml | \
  kubectl --context "${target_context}" apply -f -

echo "Issued nats-gateway-tls in ${target_context@Q} from the CA retained in ${ca_context@Q}; the CA private key remained inside its Kubernetes cluster."
