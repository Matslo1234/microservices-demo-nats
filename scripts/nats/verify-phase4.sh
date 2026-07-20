#!/usr/bin/env bash

set -euo pipefail

namespace="${NAMESPACE:-default}"
local_port="${PHASE4_FRONTEND_PORT:-18080}"
product_id="OLJCESPC7Z"
run_id="$(date +%s%N)"
temp_dir="$(mktemp -d)"
port_forward_pid=""
original_cart_replicas=""

for command in kubectl curl; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "${command} is required" >&2
    exit 1
  fi
done

restore() {
  if [[ -n "${port_forward_pid}" ]]; then
    kill "${port_forward_pid}" >/dev/null 2>&1 || true
    wait "${port_forward_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${original_cart_replicas}" ]]; then
    kubectl --namespace "${namespace}" scale deployment/cartservice \
      --replicas="${original_cart_replicas}" >/dev/null 2>&1 || true
  fi
  rm -rf "${temp_dir}"
}
trap restore EXIT

kubectl --namespace "${namespace}" port-forward service/frontend "${local_port}:80" \
  >"${temp_dir}/port-forward.log" 2>&1 &
port_forward_pid="$!"
for _ in $(seq 1 60); do
  if curl --silent --fail --max-time 1 "http://127.0.0.1:${local_port}/_readyz" >/dev/null; then
    break
  fi
  sleep 0.25
done
curl --silent --fail --max-time 2 "http://127.0.0.1:${local_port}/_readyz" >/dev/null

post_cart() {
  local user_id="$1"
  local idempotency_key="$2"
  local quantity="$3"
  curl --silent --show-error --max-time 5 --request POST \
    --header "Cookie: shop_session-id=${user_id}" \
    --header "Idempotency-Key: ${idempotency_key}" \
    --header "Accept: application/json" \
    --data-urlencode "product_id=${product_id}" \
    --data-urlencode "quantity=${quantity}" \
    --dump-header "${temp_dir}/headers" \
    --output "${temp_dir}/body" \
    --write-out '%{http_code}' \
    "http://127.0.0.1:${local_port}/cart"
}

post_cart_html() {
  local user_id="$1"
  local idempotency_key="$2"
  local quantity="$3"
  curl --silent --show-error --max-time 5 --request POST \
    --header "Cookie: shop_session-id=${user_id}" \
    --header "Idempotency-Key: ${idempotency_key}" \
    --header "Accept: text/html,application/xhtml+xml" \
    --data-urlencode "product_id=${product_id}" \
    --data-urlencode "quantity=${quantity}" \
    --dump-header "${temp_dir}/headers" \
    --output "${temp_dir}/body" \
    --write-out '%{http_code}' \
    "http://127.0.0.1:${local_port}/cart"
}

post_clear() {
  local user_id="$1"
  local idempotency_key="$2"
  curl --silent --show-error --max-time 5 --request POST \
    --header "Cookie: shop_session-id=${user_id}" \
    --header "Idempotency-Key: ${idempotency_key}" \
    --header "Accept: application/json" \
    --dump-header "${temp_dir}/headers" \
    --output "${temp_dir}/body" \
    --write-out '%{http_code}' \
    "http://127.0.0.1:${local_port}/cart/empty"
}

operation_id() {
  awk 'tolower($1) == "x-operation-id:" {print $2}' "${temp_dir}/headers" | tr -d '\r' | tail -1
}

wait_for_status() {
  local user_id="$1"
  local operation="$2"
  local expected="$3"
  for _ in $(seq 1 200); do
    code="$(curl --silent --show-error --max-time 2 \
      --header "Cookie: shop_session-id=${user_id}" \
      --header "Accept: application/json" \
      --output "${temp_dir}/operation" --write-out '%{http_code}' \
      "http://127.0.0.1:${local_port}/operations/${operation}")"
    if [[ "${code}" == "200" ]] && grep -q "\"status\":\"${expected}\"" "${temp_dir}/operation"; then
      return 0
    fi
    sleep 0.1
  done
  echo "operation ${operation} did not reach ${expected}" >&2
  cat "${temp_dir}/operation" >&2 || true
  return 1
}

normal_user="phase4-idempotency-${run_id}"
normal_key="phase4-idempotency-${run_id}"
normal_status="$(post_cart "${normal_user}" "${normal_key}" 2)"
if [[ "${normal_status}" != "302" && "${normal_status}" != "202" ]]; then
  echo "first cart command returned ${normal_status}, expected 302 or 202" >&2
  cat "${temp_dir}/body" >&2
  exit 1
fi
normal_operation="$(operation_id)"
wait_for_status "${normal_user}" "${normal_operation}" "SUCCEEDED"

retry_status="$(post_cart "${normal_user}" "${normal_key}" 2)"
if [[ "${retry_status}" != "302" ]]; then
  echo "completed idempotent retry returned ${retry_status}, expected immediate 302" >&2
  cat "${temp_dir}/body" >&2
  exit 1
fi
cart_html="$(curl --silent --fail --max-time 3 \
  --header "Cookie: shop_session-id=${normal_user}" \
  "http://127.0.0.1:${local_port}/cart")"
if [[ "${cart_html}" != *"SKU #${product_id}"* || "${cart_html}" != *"Quantity: 2"* ]]; then
  echo "idempotent retry changed the cart more than once" >&2
  exit 1
fi

clear_status="$(post_clear "${normal_user}" "phase4-clear-${run_id}")"
if [[ "${clear_status}" != "302" && "${clear_status}" != "202" ]]; then
  echo "cart clear returned ${clear_status}, expected 302 or 202" >&2
  cat "${temp_dir}/body" >&2
  exit 1
fi
clear_operation="$(operation_id)"
wait_for_status "${normal_user}" "${clear_operation}" "SUCCEEDED"
empty_cart_html="$(curl --silent --fail --max-time 3 \
  --header "Cookie: shop_session-id=${normal_user}" \
  "http://127.0.0.1:${local_port}/cart")"
if [[ "${empty_cart_html}" != *"Your shopping cart is empty!"* ]]; then
  echo "completed clear command did not empty the projected cart" >&2
  exit 1
fi

if [[ "${PHASE4_ALLOW_OUTAGE:-false}" != "true" ]]; then
  echo "add_status=${normal_status} idempotent_operation=${normal_operation} retry_status=${retry_status} cart_quantity=2"
  echo "clear_status=${clear_status} clear_operation=${clear_operation} cart_empty=true"
  echo "Phase 4 non-disruptive cart-command verification passed."
  echo "Set PHASE4_ALLOW_OUTAGE=true only with explicit authorization to run worker-outage recovery checks."
  exit 0
fi

original_cart_replicas="$(kubectl --namespace "${namespace}" get deployment/cartservice -o jsonpath='{.spec.replicas}')"
if [[ "${original_cart_replicas}" == "0" ]]; then
  echo "cartservice must be running before Phase 4 verification" >&2
  exit 1
fi
kubectl --namespace "${namespace}" scale deployment/cartservice --replicas=0
kubectl --namespace "${namespace}" wait --for=delete pod --selector=app=cartservice --timeout=2m || true

browser_user="phase4-browser-${run_id}"
browser_status="$(post_cart_html "${browser_user}" "phase4-browser-${run_id}" 1)"
if [[ "${browser_status}" != "202" ]]; then
  echo "browser cart worker outage returned ${browser_status}, expected 202" >&2
  cat "${temp_dir}/body" >&2
  exit 1
fi
if ! awk 'tolower($1) == "content-type:" {print tolower($2)}' "${temp_dir}/headers" | grep -q '^text/html'; then
  echo "browser 202 response did not use an HTML content type" >&2
  cat "${temp_dir}/headers" >&2
  exit 1
fi
if ! grep -q 'Updating your cart' "${temp_dir}/body"; then
  echo "browser 202 response did not render the cart progress page" >&2
  cat "${temp_dir}/body" >&2
  exit 1
fi
browser_operation="$(operation_id)"

outage_user="phase4-outage-${run_id}"
first_outage_status="$(post_cart "${outage_user}" "phase4-outage-first-${run_id}" 3)"
if [[ "${first_outage_status}" != "202" ]]; then
  echo "cart worker outage returned ${first_outage_status}, expected 202" >&2
  cat "${temp_dir}/body" >&2
  exit 1
fi
first_outage_operation="$(operation_id)"
wait_for_status "${outage_user}" "${first_outage_operation}" "QUEUED"

second_outage_status="$(post_cart "${outage_user}" "phase4-outage-second-${run_id}" 7)"
if [[ "${second_outage_status}" != "202" ]]; then
  echo "second queued command returned ${second_outage_status}, expected 202" >&2
  cat "${temp_dir}/body" >&2
  exit 1
fi
second_outage_operation="$(operation_id)"
wait_for_status "${outage_user}" "${second_outage_operation}" "QUEUED"

kubectl --namespace "${namespace}" scale deployment/cartservice --replicas="${original_cart_replicas}"
kubectl --namespace "${namespace}" rollout status deployment/cartservice --timeout=5m
wait_for_status "${browser_user}" "${browser_operation}" "SUCCEEDED"
wait_for_status "${outage_user}" "${first_outage_operation}" "SUCCEEDED"
wait_for_status "${outage_user}" "${second_outage_operation}" "REJECTED"
if ! grep -q '"failure_code":"CART_VERSION_CONFLICT"' "${temp_dir}/operation"; then
  echo "stale queued command was not rejected with CART_VERSION_CONFLICT" >&2
  cat "${temp_dir}/operation" >&2
  exit 1
fi

outage_cart_html="$(curl --silent --fail --max-time 3 \
  --header "Cookie: shop_session-id=${outage_user}" \
  "http://127.0.0.1:${local_port}/cart")"
if [[ "${outage_cart_html}" != *"SKU #${product_id}"* || "${outage_cart_html}" != *"Quantity: 3"* ]]; then
  echo "queued commands did not preserve optimistic cart semantics" >&2
  exit 1
fi

echo "add_status=${normal_status} idempotent_operation=${normal_operation} retry_status=${retry_status} cart_quantity=2"
echo "clear_status=${clear_status} clear_operation=${clear_operation} cart_empty=true"
echo "browser_fallback=202:text/html browser_recovered=SUCCEEDED"
echo "outage_statuses=202,202 recovered=SUCCEEDED,REJECTED cart_quantity=3"
echo "Phase 4 durable cart-command verification passed."
