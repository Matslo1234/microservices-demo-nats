#!/usr/bin/env python3
"""Static guardrails for the Phase 6 NATS-only runtime boundary."""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def read(*parts: str) -> tuple[Path, str]:
    path = ROOT.joinpath(*parts)
    return path, path.read_text()


def require(path: Path, text: str, *needles: str) -> None:
    for needle in needles:
        if needle not in text:
            raise SystemExit(f"{path.relative_to(ROOT)} is missing {needle!r}")


def forbid(path: Path, text: str, *needles: str) -> None:
    for needle in needles:
        if needle in text:
            raise SystemExit(f"{path.relative_to(ROOT)} still contains forbidden Phase 6 text {needle!r}")


canonical_path, canonical = read("protos", "demo.proto")
forbid(canonical_path, canonical, "service ")

cart_proto_path, cart_proto = read("src", "cartservice", "src", "protos", "Cart.proto")
forbid(cart_proto_path, cart_proto, "service ")

cart_settings_path, cart_settings = read("src", "cartservice", "src", "appsettings.json")
require(cart_settings_path, cart_settings, '"Protocols": "Http1"')

removed_stubs = (
    "protos/hipstershop/demo_grpc.pb.go",
    "src/checkoutservice/genproto/demo_grpc.pb.go",
    "src/frontend/genproto/demo_grpc.pb.go",
    "src/productcatalogservice/genproto/demo_grpc.pb.go",
    "src/shippingservice/genproto/demo_grpc.pb.go",
    "src/recommendationservice/demo_pb2_grpc.py",
    "src/recommendationservice/client.py",
    "src/adservice/src/main/java/hipstershop/AdServiceClient.java",
    "src/cartservice/src/services/CartService.cs",
    "src/paymentservice/charge.js",
    "src/currencyservice/client.js",
)
for relative in removed_stubs:
    if ROOT.joinpath(relative).exists():
        raise SystemExit(f"{relative} should have been removed after the rollback window")

business_service_addresses = (
    "AD_SERVICE_ADDR",
    "CART_SERVICE_ADDR",
    "CHECKOUT_SERVICE_ADDR",
    "CURRENCY_SERVICE_ADDR",
    "EMAIL_SERVICE_ADDR",
    "PAYMENT_SERVICE_ADDR",
    "PRODUCT_CATALOG_SERVICE_ADDR",
    "RECOMMENDATION_SERVICE_ADDR",
    "SHIPPING_SERVICE_ADDR",
)

manifest_files = sorted((ROOT / "kubernetes-manifests").glob("*service.yaml"))
for path in manifest_files:
    text = path.read_text()
    forbid(path, text, *business_service_addresses, "grpc.health.v1.Health/Check")
    if path.name != "frontend.yaml":
        require(path, text, "containerPort: 8080", "path: /healthz", "path: /readyz", "prometheus.io/scrape")

release_path, release = read("release", "kubernetes-manifests.yaml")
forbid(
    release_path,
    release,
    *business_service_addresses,
    "SHOPPING_ASSISTANT_SERVICE_ADDR",
    "PACKAGING_SERVICE_URL",
    "grpc.health.v1.Health/Check",
    'user: "shoppingassistantservice"',
    'user: "packagingservice"',
)

for backend in (
    "adservice",
    "cartservice",
    "checkoutservice",
    "currencyservice",
    "emailservice",
    "paymentservice",
    "productcatalogservice",
    "recommendationservice",
    "shippingservice",
    "storefrontprojectionservice",
):
    forbid(release_path, release, f"kind: Service\nmetadata:\n  name: {backend}\n")

network_path, network = read("kubernetes-manifests", "network-policies.yaml")
require(
    network_path,
    network,
    "name: default-deny",
    "name: application-health-and-nats",
    "port: 4222",
    "name: cart-to-redis",
    "name: loadgenerator-to-frontend",
)

nats_policy_path, nats_policy = read("kubernetes-manifests", "nats", "base", "network-policy.yaml")
require(nats_policy_path, nats_policy, "operator: In", "storefrontprojectionservice")
forbid(nats_policy_path, nats_policy, "shoppingassistantservice", "packagingservice")

nats_config_path, nats_config = read("kubernetes-manifests", "nats", "base", "config.yaml")
forbid(nats_config_path, nats_config, 'user: "shoppingassistantservice"', 'user: "packagingservice"')
require(
    nats_config_path,
    nats_config,
    '"boutique.cmd.cart.add-item.v1"',
    '"boutique.cmd.order.submit.v1"',
    '"boutique.evt.order.completed.v1"',
)

setup_path, setup = read("kubernetes-manifests", "nats", "base", "setup.yaml")
require(
    setup_path,
    setup,
    'verbs: ["get", "create", "patch"]',
    "api_patch()",
    'sync_from_file configmaps "${namespace}" nats-ca',
    'sync_from_file secrets default "nats-credentials-${service}"',
)

deploy_path, deploy = read("scripts", "nats", "deploy.sh")
require(
    deploy_path,
    deploy,
    "application_deployments=(",
    'kubectl --namespace default rollout restart "${restart_targets[@]}"',
)

monitoring_path, monitoring = read("kubernetes-manifests", "nats", "base", "monitoring.yaml")
require(
    monitoring_path,
    monitoring,
    "NatsJetStreamConsumerLag",
    "NatsJetStreamAckBacklog",
    "BoutiqueDependencyUnavailable",
    "boutique_dependency_ready",
)

load_path, loadgen = read("src", "loadgenerator", "locustfile.py")
require(
    load_path,
    loadgen,
    'name="/product-meta/[ids]"',
    "Idempotency-Key",
    "checkout retry returned a different order resource",
    "cart retry returned a different operation resource",
)

dr_path, dr = read("scripts", "nats", "verify-phase6-dr.sh")
require(
    dr_path,
    dr,
    "PHASE6_ALLOW_PROJECTION_REBUILD",
    "nats-phase6-projection-rebuild",
    "nats-phase6-backup",
    "nats-phase6-restore-dr",
)

restore_path, restore = read("kubernetes-manifests", "nats", "phase6-isolated-restore.yaml")
server_path, server = read("kubernetes-manifests", "nats", "base", "statefulset.yaml")
restore_image = re.search(r"image:\s+(nats:[^\s]+)", restore)
server_image = re.search(r"image:\s+(nats:[^\s]+)", server)
if not restore_image or not server_image or restore_image.group(1) != server_image.group(1):
    raise SystemExit(
        f"{restore_path.relative_to(ROOT)} must use the production NATS server image "
        f"from {server_path.relative_to(ROOT)}"
    )
require(restore_path, restore, "--timeout=10m")

backup_path, backup = read("kubernetes-manifests", "nats", "base", "backup.yaml")
require(backup_path, backup, "kind: PersistentVolumeClaim", "name: nats-backups", "--consumers --check")

plan_path, plan = read("docs", "nats-event-driven-upgrade-plan.md")
require(plan_path, plan, "Phase 6 implementation note", "Point 1 was intentionally not")

phase_path = ROOT / "docs" / "development" / "Phase6.md"
if not phase_path.exists():
    raise SystemExit("docs/development/Phase6.md is required")

for obsolete_release in ("dev-kubernetes-mainfests.yaml", "nats-kubernetes-manifests.yaml"):
    if (ROOT / "release" / obsolete_release).exists():
        raise SystemExit(f"release/{obsolete_release} is an obsolete gRPC-era bundle")

helm_path = ROOT / "helm-chart"
if helm_path.exists() and any(path.is_file() for path in helm_path.rglob("*")):
    raise SystemExit("helm-chart is an obsolete gRPC-only deployment path")
if ROOT.joinpath("docs/releasing/make-helm-chart.sh").exists():
    raise SystemExit("docs/releasing/make-helm-chart.sh is an obsolete gRPC-only release path")

print("Phase 6 static NATS-only cleanup guardrails passed.")
