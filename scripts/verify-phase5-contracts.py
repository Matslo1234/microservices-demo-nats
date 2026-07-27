#!/usr/bin/env python3
"""Static guardrails for the Phase 5 checkout saga cutover."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def source(*parts: str) -> tuple[Path, str]:
    path = ROOT.joinpath(*parts)
    return path, path.read_text()


def require(path: Path, text: str, *needles: str) -> None:
    for needle in needles:
        if needle not in text:
            raise SystemExit(f"{path.relative_to(ROOT)} is missing {needle!r}")


def forbid(path: Path, text: str, *needles: str) -> None:
    for needle in needles:
        if needle in text:
            raise SystemExit(f"{path.relative_to(ROOT)} still contains forbidden Phase 5 dependency {needle!r}")


frontend_files = list((ROOT / "src/frontend").glob("*.go"))
frontend = "\n".join(item.read_text() for item in frontend_files)
forbid(ROOT / "src/frontend", frontend, "NewCheckoutServiceClient", "checkoutSvcConn", "checkoutSvcAddr")
require(ROOT / "src/frontend", frontend,
        "boutique.qry.payment.tokenize.v1", "boutique.cmd.order.submit.v1",
        '"/orders/{id}"', "http.StatusAccepted", "PaymentToken:")

checkout_files = [
    item for item in (ROOT / "src/checkoutservice").glob("*.go")
    if not item.name.endswith("_test.go")
]
checkout = "\n".join(item.read_text() for item in checkout_files)
forbid(ROOT / "src/checkoutservice", checkout,
       "NewCartServiceClient", "NewProductCatalogServiceClient", "NewCurrencyServiceClient",
       "NewShippingServiceClient", "NewPaymentServiceClient", "NewEmailServiceClient",
       "relayOutbox", "RemoveOutboxBatch", "TxPipelined", ".Watch(",
       'key("revision")', 'key("outbox")')
require(ROOT / "src/checkoutservice", checkout,
        "checkout-order-commands-v1", "CHECKOUT_REDIS_ADDR", "commitOrderScript",
        "ApplyOrder", "resultJournalRetention", "NewResultEnvelope",
        "PublishMsg", "Nats-Msg-Id", "RedisLeaseStore", "DueDeadlines",
        "checkoutDeadlineShards", "LoadOrderProjections",
        "WAITING_FOR_QUOTE", "WAITING_FOR_AUTHORIZATION", "WAITING_FOR_SHIPMENT",
        "WAITING_FOR_CAPTURE", "COMPENSATING", "MANUAL_REVIEW",
        "boutique.cmd.payment.release-authorization.v1",
        "boutique.cmd.shipping.cancel-shipment.v1",
        "boutique.evt.order.step-timed-out.v1",
        "boutique.cmd.cart.clear.v1")

payment_path, payment = source("src", "paymentservice", "nats_worker.js")
require(payment_path, payment,
        "boutique.qry.payment.tokenize.v1", "payment-commands-v1",
        "INVALID_OR_EXPIRED_TOKEN", "authorization_declined", "capture_failed", "release_failed",
        "ptok_v1", "pauth_v1", "PAYMENT_SIGNING_KEY", "PAYMENT_SIGNING_KEY_ID",
        "PAYMENT_VERIFICATION_KEYS", "createSigningKeyring",
        "crypto.hkdfSync", "crypto.timingSafeEqual", "Nats-Msg-Id")
forbid(payment_path, payment,
       "credit_card_number:", "credit_card_cvv:", "PaymentState", "PAYMENT_STORE_PATH",
       "provider-state.json")
server_path, payment_server = source("src", "paymentservice", "server.js")
forbid(server_path, payment_server, "JSON.stringify(call.request)")

shipping_path = ROOT / "src" / "shippingservice"
shipping = "\n".join(item.read_text() for item in shipping_path.glob("*.go"))
require(shipping_path, shipping,
        "shipping-commands-v1", "calculate-order-quote", "create-shipment", "cancel-shipment",
        "SHIPPING_FAILURE_MODE", "SHIPPING_PROVIDER_SECRET", "newShippingProvider")
forbid(shipping_path, shipping,
       "openShippingProviderStore", "shippingProviderStore", "SHIPPING_STORE_PATH",
       "provider-state.json")

email_path, email = source("src", "emailservice", "nats_worker.py")
require(email_path, email,
        "email-order-completed-v1", "boutique.evt.order.completed.v1",
        "order-confirmation-sent", "order-confirmation-failed", "EMAIL_FAILURE_MODE",
        "_mask_recipient")

projection_files = list((ROOT / "src/storefrontprojectionservice").glob("*.go"))
projection = "\n".join(item.read_text() for item in projection_files)
require(ROOT / "src/storefrontprojectionservice", projection,
        'js.KeyValue("STOREFRONT_ORDERS")', '"boutique.evt.order.completed.v1"',
        '"boutique.evt.order.manual-review-required.v1"', '"order":', "orderQuery")

checkout_manifest_path, checkout_manifest = source(
    "kubernetes-manifests", "checkoutservice.yaml")
require(checkout_manifest_path, checkout_manifest,
        "CHECKOUT_REDIS_ADDR", "redis-checkout-cluster:6379", "CHECKOUT_REDIS_MODE",
        "checkout:v2", "CHECKOUT_DEADLINE_LEASE",
        "type: RollingUpdate", "maxUnavailable: 0",
        "nats-client-config", "nats-ca")
forbid(checkout_manifest_path, checkout_manifest,
       "CHECKOUT_STORE_PATH", "redis-checkout:6379", "redis-checkout-data",
       "name: redis-checkout\n")

payment_manifest_path, payment_manifest = source(
    "kubernetes-manifests", "paymentservice.yaml")
require(payment_manifest_path, payment_manifest,
        "type: RollingUpdate", "maxUnavailable: 0", "PAYMENT_SIGNING_KEY_ID",
        "nats-client-config", "nats-ca")
forbid(payment_manifest_path, payment_manifest,
       "PAYMENT_STORE_PATH", "payment-data", "PersistentVolumeClaim", "type: Recreate")

setup_path, setup = source("kubernetes-manifests", "nats", "base", "setup.yaml")
require(setup_path, setup,
        "PAYMENT_SIGNING_KEY", '[ "${service}" = paymentservice ]',
        "SHIPPING_PROVIDER_SECRET", '[ "${service}" = shippingservice ]',
        'sync_from_file secrets default "nats-credentials-${service}"')

for manifest_name, store_path, claim in (
    ("shippingservice.yaml", "SHIPPING_STORE_PATH", "shipping-data"),
    ("emailservice.yaml", "EMAIL_STORE_PATH", "email-data"),
):
    manifest_path, manifest = source("kubernetes-manifests", manifest_name)
    require(manifest_path, manifest,
            "type: RollingUpdate", "maxUnavailable: 0",
            "nats-client-config", "nats-ca")
    forbid(manifest_path, manifest,
           store_path, claim, "PersistentVolumeClaim", "type: Recreate")

frontend_manifest_path, frontend_manifest = source("kubernetes-manifests", "frontend.yaml")
forbid(frontend_manifest_path, frontend_manifest, "CHECKOUT_SERVICE_ADDR")
require(frontend_manifest_path, frontend_manifest,
        "boutique.qry.storefront.order.v1", "boutique.qry.payment.tokenize.v1")

loadgen_path, loadgen = source("src", "loadgenerator", "locustfile.py")
require(loadgen_path, loadgen,
        'name="/cart/checkout"', 'name="/orders/[id]"', "Idempotency-Key",
        "order.get('status') == 'COMPLETED'", "MANUAL_REVIEW")

commands_path, commands = source("protos", "commands", "v1", "commands.proto")
require(commands_path, commands, "string payment_token = 11;")
forbid(commands_path, commands, "credit_card_number", "credit_card_cvv")

print("Phase 5 static checkout-saga cutover guardrails passed.")
