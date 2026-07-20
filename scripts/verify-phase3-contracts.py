#!/usr/bin/env python3
"""Static guardrails for the Phase 3 storefront read cutover."""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(text: str, needle: str, source: Path) -> None:
    if needle not in text:
        raise SystemExit(f"{source.relative_to(ROOT)} is missing {needle!r}")


def forbid(text: str, needle: str, source: Path) -> None:
    if needle in text:
        raise SystemExit(f"{source.relative_to(ROOT)} still contains forbidden read dependency {needle!r}")


def go_function(text: str, name: str, source: Path) -> str:
    match = re.search(rf"^func \([^\n]+\) {re.escape(name)}\([^\n]*\) \{{", text, re.MULTILINE)
    if not match:
        raise SystemExit(f"{source.relative_to(ROOT)} is missing handler {name}")
    following = text.find("\nfunc ", match.end())
    return text[match.start() : following if following >= 0 else len(text)]


frontend_dir = ROOT / "src" / "frontend"
frontend = "\n".join(path.read_text() for path in frontend_dir.glob("*.go"))
for forbidden in (
    "NewProductCatalogServiceClient",
    "NewCurrencyServiceClient",
    "NewRecommendationServiceClient",
    "NewAdServiceClient",
    "NewShippingServiceClient",
):
    forbid(frontend, forbidden, frontend_dir)

handlers = (frontend_dir / "handlers.go").read_text()
route_handlers = {
    "homeHandler": "home",
    "productHandler": "product",
    "viewCartHandler": "cart",
    "getProductByID": "product-meta",
}
for handler, view in route_handlers.items():
    body = go_function(handlers, handler, frontend_dir / "handlers.go")
    if body.count(".storefrontQuery(") != 1:
        raise SystemExit(f"{handler} must make exactly one storefront query")
    require(body, f'"{view}"', frontend_dir / "handlers.go")
require(frontend, "publishPageView", frontend_dir)

projection = (ROOT / "src" / "storefrontprojectionservice" / "queries.go").read_text()
require(projection, "micro.AddService", ROOT / "src" / "storefrontprojectionservice" / "queries.go")
for view in ("home", "product", "cart", "currencies", "product-meta"):
    require(projection, f'"{view}"', ROOT / "src" / "storefrontprojectionservice" / "queries.go")

manifest = (ROOT / "kubernetes-manifests" / "frontend.yaml").read_text()
for forbidden_env in (
    "PRODUCT_CATALOG_SERVICE_ADDR",
    "CURRENCY_SERVICE_ADDR",
    "RECOMMENDATION_SERVICE_ADDR",
    "AD_SERVICE_ADDR",
    "SHIPPING_SERVICE_ADDR",
):
    forbid(manifest, forbidden_env, ROOT / "kubernetes-manifests" / "frontend.yaml")

reactors = {
    "recommendationservice": "recommendation-page-views-v1",
    "adservice": "ad-page-views-v1",
    "shippingservice": "shipping-cart-quotes-v1",
}
for service, durable in reactors.items():
    service_manifest = ROOT / "kubernetes-manifests" / f"{service}.yaml"
    content = service_manifest.read_text()
    require(content, 'name: NATS_REQUIRED', service_manifest)
    require(content, durable, service_manifest)

print("Phase 3 static read-cutover guardrails passed.")
