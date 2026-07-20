#!/usr/bin/env python3
"""Static guardrails for the Phase 4 cart command cutover."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(text: str, needle: str, source: Path) -> None:
    if needle not in text:
        raise SystemExit(f"{source.relative_to(ROOT)} is missing {needle!r}")


def forbid(text: str, needle: str, source: Path) -> None:
    if needle in text:
        raise SystemExit(f"{source.relative_to(ROOT)} still contains forbidden cart write dependency {needle!r}")


frontend_dir = ROOT / "src" / "frontend"
frontend = "\n".join(path.read_text() for path in frontend_dir.glob("*.go"))
for forbidden in ("NewCartServiceClient", "cartSvcAddr", "cartSvcConn"):
    forbid(frontend, forbidden, frontend_dir)
for required in (
    "boutique.cmd.cart.add-item.v1",
    "boutique.cmd.cart.clear.v1",
    "boutique.evt.storefront.operation-accepted.v1",
    "http.StatusAccepted",
    '"/operations/{id}"',
    'r.Header.Get("Idempotency-Key")',
    '"text/html; charset=utf-8"',
):
    require(frontend, required, frontend_dir)

frontend_manifest = ROOT / "kubernetes-manifests" / "frontend.yaml"
manifest = frontend_manifest.read_text()
forbid(manifest, "CART_SERVICE_ADDR", frontend_manifest)
require(manifest, "boutique.qry.storefront.operation.v1", frontend_manifest)

cart_dir = ROOT / "src" / "cartservice" / "src"
cart = "\n".join(path.read_text() for path in cart_dir.rglob("*.cs"))
for required in (
    'new ConsumerConfig("cart-commands-v1")',
    'FilterSubject = "boutique.cmd.cart.>"',
    "ExpectedCartVersion",
    "Condition.KeyNotExists(inboxKey)",
    "CartCommandRejectedEvent",
    "AckAsync",
):
    require(cart, required, cart_dir)

projection_dir = ROOT / "src" / "storefrontprojectionservice"
projection = "\n".join(path.read_text() for path in projection_dir.glob("*.go"))
for required in (
    'js.KeyValue("STOREFRONT_OPERATIONS")',
    '"boutique.evt.storefront.operation-accepted.v1"',
    '"boutique.evt.cart.command-rejected.v1"',
    '"operation":',
):
    require(projection, required, projection_dir)

progress_page = ROOT / "src" / "frontend" / "templates" / "cart-operation.html"
progress = progress_page.read_text()
for required in ("Updating your cart", "data-operation-url", "window.location.replace"):
    require(progress, required, progress_page)

loadgenerator = ROOT / "src" / "loadgenerator" / "locustfile.py"
loadgen = loadgenerator.read_text()
for required in ("Idempotency-Key", "application/json", "allow_redirects=False", "response.status_code != 202", "/operations/[id]"):
    require(loadgen, required, loadgenerator)

print("Phase 4 static cart-command cutover guardrails passed.")
