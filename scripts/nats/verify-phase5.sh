#!/usr/bin/env bash

set -euo pipefail

namespace="${NAMESPACE:-default}"
local_port="${PHASE5_FRONTEND_PORT:-18085}"
run_id="$(date +%s%N)"
temp_dir="$(mktemp -d)"
port_forward_pid=""
cart_replicas=""

restore() {
  if [[ -n "${port_forward_pid}" ]]; then
    kill "${port_forward_pid}" >/dev/null 2>&1 || true
    wait "${port_forward_pid}" >/dev/null 2>&1 || true
  fi
  if [[ "${PHASE5_ALLOW_FAILURE_INJECTION:-false}" == "true" ]]; then
    kubectl --namespace "${namespace}" set env deployment/paymentservice PAYMENT_FAILURE_MODE- >/dev/null 2>&1 || true
    kubectl --namespace "${namespace}" set env deployment/shippingservice SHIPPING_FAILURE_MODE- >/dev/null 2>&1 || true
    kubectl --namespace "${namespace}" set env deployment/emailservice EMAIL_FAILURE_MODE- >/dev/null 2>&1 || true
    if [[ -n "${cart_replicas}" ]]; then
      kubectl --namespace "${namespace}" scale deployment/cartservice --replicas="${cart_replicas}" >/dev/null 2>&1 || true
    fi
  fi
  rm -rf "${temp_dir}"
}
trap restore EXIT

for command in kubectl curl; do
  command -v "${command}" >/dev/null 2>&1 || { echo "${command} is required" >&2; exit 1; }
done

kubectl --namespace "${namespace}" port-forward service/frontend "${local_port}:80" >"${temp_dir}/port-forward.log" 2>&1 &
port_forward_pid="$!"
for _ in $(seq 1 80); do
  curl --silent --fail --max-time 1 "http://127.0.0.1:${local_port}/_readyz" >/dev/null && break
  sleep 0.25
done
curl --silent --fail --max-time 2 "http://127.0.0.1:${local_port}/_readyz" >/dev/null

wait_operation() {
  local user_id="$1" operation_id="$2"
  for _ in $(seq 1 200); do
    code="$(curl --silent --max-time 2 --header "Cookie: shop_session-id=${user_id}" --header 'Accept: application/json' \
      --output "${temp_dir}/operation" --write-out '%{http_code}' \
      "http://127.0.0.1:${local_port}/operations/${operation_id}")"
    if [[ "${code}" == "200" ]] && grep -q '"status":"SUCCEEDED"' "${temp_dir}/operation"; then return 0; fi
    sleep 0.1
  done
  echo "cart operation ${operation_id} did not succeed" >&2; return 1
}

add_cart() {
  local user_id="$1" key="$2"
  status="$(curl --silent --show-error --max-time 5 --request POST \
    --header "Cookie: shop_session-id=${user_id}" --header "Idempotency-Key: ${key}" --header 'Accept: application/json' \
    --data-urlencode 'product_id=OLJCESPC7Z' --data-urlencode 'quantity=1' \
    --dump-header "${temp_dir}/cart-headers" --output "${temp_dir}/cart-body" --write-out '%{http_code}' \
    "http://127.0.0.1:${local_port}/cart")"
  [[ "${status}" == "302" || "${status}" == "202" ]] || { cat "${temp_dir}/cart-body" >&2; return 1; }
  operation_id="$(awk 'tolower($1) == "x-operation-id:" {print $2}' "${temp_dir}/cart-headers" | tr -d '\r' | tail -1)"
  wait_operation "${user_id}" "${operation_id}"
}

post_checkout() {
  local user_id="$1" key="$2"
  curl --silent --show-error --max-time 7 --request POST \
    --header "Cookie: shop_session-id=${user_id}" --header "Idempotency-Key: ${key}" --header 'Accept: application/json' \
    --data-urlencode 'email=phase5@example.com' --data-urlencode 'street_address=1 Main Street' \
    --data-urlencode 'zip_code=1000' --data-urlencode 'city=Ljubljana' --data-urlencode 'state=SI' \
    --data-urlencode 'country=Slovenia' --data-urlencode 'credit_card_number=4432801561520454' \
    --data-urlencode 'credit_card_expiration_month=12' --data-urlencode 'credit_card_expiration_year=2099' \
    --data-urlencode 'credit_card_cvv=672' --dump-header "${temp_dir}/order-headers" \
    --output "${temp_dir}/order-body" --write-out '%{http_code}' \
    "http://127.0.0.1:${local_port}/cart/checkout"
}

order_id() { awk 'tolower($1) == "x-order-id:" {print $2}' "${temp_dir}/order-headers" | tr -d '\r' | tail -1; }

wait_order() {
  local user_id="$1" id="$2" expected="$3"
  for _ in $(seq 1 400); do
    code="$(curl --silent --max-time 2 --header "Cookie: shop_session-id=${user_id}" --header 'Accept: application/json' \
      --output "${temp_dir}/order-status" --write-out '%{http_code}' "http://127.0.0.1:${local_port}/orders/${id}")"
    if [[ "${code}" == "200" ]] && grep -q "\"status\":\"${expected}\"" "${temp_dir}/order-status"; then return 0; fi
    sleep 0.1
  done
  echo "order ${id} did not reach ${expected}" >&2; cat "${temp_dir}/order-status" >&2 || true; return 1
}

normal_user="phase5-success-${run_id}"; normal_key="phase5-success-${run_id}"
add_cart "${normal_user}" "cart-${normal_key}"
[[ "$(post_checkout "${normal_user}" "${normal_key}")" == "202" ]] || { cat "${temp_dir}/order-body" >&2; exit 1; }
normal_order="$(order_id)"; wait_order "${normal_user}" "${normal_order}" "COMPLETED"
[[ "$(post_checkout "${normal_user}" "${normal_key}")" == "202" ]] || exit 1
[[ "$(order_id)" == "${normal_order}" ]] || { echo "idempotent retry changed order ID" >&2; exit 1; }
for deployment in frontend checkoutservice paymentservice shippingservice emailservice; do
  if kubectl --namespace "${namespace}" logs "deployment/${deployment}" --since=10m 2>/dev/null | grep -qE '4432801561520454|credit_card_cvv[^a-z]'; then
    echo "sensitive card data appeared in ${deployment} logs" >&2
    exit 1
  fi
done

if [[ "${PHASE5_ALLOW_FAILURE_INJECTION:-false}" != "true" ]]; then
  echo "success_order=${normal_order} retry_order=${normal_order} status=COMPLETED"
  echo "Phase 5 non-disruptive checkout verification passed."
  echo "Set PHASE5_ALLOW_FAILURE_INJECTION=true only with explicit authorization for rolling provider failure checks."
  exit 0
fi

cart_replicas="$(kubectl --namespace "${namespace}" get deployment/cartservice -o jsonpath='{.spec.replicas}')"

set_mode() {
  local deployment="$1" variable="$2" value="$3"
  kubectl --namespace "${namespace}" set env "deployment/${deployment}" "${variable}=${value}" >/dev/null
  kubectl --namespace "${namespace}" rollout status "deployment/${deployment}" --timeout=5m >/dev/null
}

clear_mode() {
  local deployment="$1" variable="$2"
  kubectl --namespace "${namespace}" set env "deployment/${deployment}" "${variable}-" >/dev/null
  kubectl --namespace "${namespace}" rollout status "deployment/${deployment}" --timeout=5m >/dev/null
}

failure_order() {
  local label="$1" expected="$2"
  local user_id="phase5-${label}-${run_id}" key="phase5-${label}-${run_id}"
  add_cart "${user_id}" "cart-${key}"
  [[ "$(post_checkout "${user_id}" "${key}")" == "202" ]] || return 1
  id="$(order_id)"; wait_order "${user_id}" "${id}" "${expected}"
  echo "${label}_order=${id} status=${expected}"
}

set_mode shippingservice SHIPPING_FAILURE_MODE quote
failure_order quote-failure CANCELLED
clear_mode shippingservice SHIPPING_FAILURE_MODE

set_mode paymentservice PAYMENT_FAILURE_MODE authorization_declined
failure_order authorization-decline CANCELLED
clear_mode paymentservice PAYMENT_FAILURE_MODE

set_mode shippingservice SHIPPING_FAILURE_MODE shipment
failure_order shipment-failure CANCELLED
clear_mode shippingservice SHIPPING_FAILURE_MODE

set_mode paymentservice PAYMENT_FAILURE_MODE capture_failed
failure_order capture-failure CANCELLED
clear_mode paymentservice PAYMENT_FAILURE_MODE

set_mode paymentservice PAYMENT_FAILURE_MODE capture_failed
set_mode shippingservice SHIPPING_FAILURE_MODE cancel
failure_order compensation-failure MANUAL_REVIEW
clear_mode paymentservice PAYMENT_FAILURE_MODE
clear_mode shippingservice SHIPPING_FAILURE_MODE

set_mode emailservice EMAIL_FAILURE_MODE failed
email_user="phase5-email-failure-${run_id}"; email_key="phase5-email-failure-${run_id}"
add_cart "${email_user}" "cart-${email_key}"; [[ "$(post_checkout "${email_user}" "${email_key}")" == "202" ]]
email_order="$(order_id)"; wait_order "${email_user}" "${email_order}" COMPLETED
for _ in $(seq 1 100); do
  grep -q '"notification_status":"FAILED"' "${temp_dir}/order-status" && break
  sleep 0.1; wait_order "${email_user}" "${email_order}" COMPLETED
done
grep -q '"notification_status":"FAILED"' "${temp_dir}/order-status"
clear_mode emailservice EMAIL_FAILURE_MODE

cart_user="phase5-cart-outage-${run_id}"; cart_key="phase5-cart-outage-${run_id}"
# Seed the cart before stopping its command worker.
add_cart "${cart_user}" "cart-${cart_key}"
kubectl --namespace "${namespace}" scale deployment/cartservice --replicas=0 >/dev/null
[[ "$(post_checkout "${cart_user}" "${cart_key}")" == "202" ]]
cart_order="$(order_id)"; wait_order "${cart_user}" "${cart_order}" COMPLETED
kubectl --namespace "${namespace}" scale deployment/cartservice --replicas="${cart_replicas}" >/dev/null
kubectl --namespace "${namespace}" rollout status deployment/cartservice --timeout=5m >/dev/null

echo "email_failure_order=${email_order} status=COMPLETED notification=FAILED"
echo "cart_clear_outage_order=${cart_order} status=COMPLETED"
echo "Phase 5 checkout and failure-injection verification passed."
