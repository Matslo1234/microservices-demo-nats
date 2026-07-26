#!/usr/bin/env python3
"""Verify stateless Phase 2 shared views, bootstrap, and replica contracts."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROGRESS = ROOT / "docs" / "development" / "stateless-progress" / "Phase2.md"


class VerificationError(RuntimeError):
    pass


def run(
    command: list[str],
    *,
    cwd: Path = ROOT,
    environment: dict[str, str] | None = None,
) -> str:
    merged = os.environ.copy()
    if environment:
        merged.update(environment)
    result = subprocess.run(
        command,
        cwd=cwd,
        env=merged,
        text=True,
        capture_output=True,
    )
    if result.returncode:
        details = "\n".join(
            value.strip()
            for value in (result.stdout, result.stderr)
            if value.strip()
        )
        raise VerificationError(
            f"command failed: {' '.join(command)}\n{details}"
        )
    return result.stdout


def require_all(path: Path, required: tuple[str, ...]) -> str:
    content = path.read_text()
    missing = [value for value in required if value not in content]
    if missing:
        raise VerificationError(
            f"{path.relative_to(ROOT)} is missing {missing}"
        )
    return content


def verify_storefront_sources() -> None:
    projection = require_all(
        ROOT / "src" / "storefrontprojectionservice" / "projection.go",
        (
            "ensureProjectionConsumer",
            "wrong last sequence",
            "SourceEventID",
            "staleEventSkips",
            "projectionBackoff",
        ),
    )
    queries = require_all(
        ROOT / "src" / "storefrontprojectionservice" / "queries.go",
        (
            "getJSONWithRevision",
            "QueryRevision",
            "observeQueryRevision",
        ),
    )
    combined = projection + queries
    forbidden = (
        "orderViews",
        "operationViews",
        "cartViews",
        "cachedOrder",
        "cachedOperation",
        "cachedCart",
        "DeleteConsumer(",
        "sync.Map",
    )
    present = [value for value in forbidden if value in combined]
    if present:
        raise VerificationError(
            f"storefront still has replica-local/delete-on-start paths: {present}"
        )
    views = require_all(
        ROOT
        / "src"
        / "storefrontprojectionservice"
        / "internal"
        / "storefront"
        / "views.go",
        ("source_event_id", "source_version", "ProjectionMetadata"),
    )
    del views
    require_all(
        ROOT / "src" / "storefrontprojectionservice" / "main.go",
        (
            "initializeProjectionRuntime",
            "dependencies are unavailable; retrying",
            "boutique_projection_kv_conflict_retries_total",
            "boutique_projection_stale_events_total",
            "boutique_storefront_query_revision",
            "boutique_projection_age_seconds",
        ),
    )


def verify_recommendation_sources() -> None:
    worker = require_all(
        ROOT / "src" / "recommendationservice" / "nats_events.py",
        (
            'CATALOG_BUCKET = "RECOMMENDATION_CATALOG"',
            "catalog_candidates(",
            "apply_product(",
            "apply_snapshot(",
            "MODEL_REVISION",
        ),
    )
    for forbidden in ("_products = set()", "_products_lock", "_bootstrap_catalog"):
        if forbidden in worker:
            raise VerificationError(
                f"recommendation still has local catalog path {forbidden!r}"
            )
    require_all(
        ROOT / "src" / "recommendationservice" / "catalog_kv.py",
        (
            "CatalogConflict",
            "source_event_id",
            "product_version",
            "catalog_revision",
            "await store.update",
        ),
    )
    require_all(
        ROOT / "src" / "recommendationservice" / "nats_events.py",
        ("while not _stop.is_set()", "unavailable; retrying startup"),
    )


def verify_replica_state_sources() -> None:
    ad = require_all(
        ROOT
        / "src"
        / "adservice"
        / "src"
        / "main"
        / "java"
        / "hipstershop"
        / "NatsEventWorker.java",
        (
            "CONFIG_REVISION",
            "source.getOccurredAt()",
            "source.getMessageId()",
            "consumer is unavailable; retrying",
        ),
    )
    if "Instant.now()" in ad:
        raise VerificationError("ad retries still depend on replica wall clock")
    frontend = require_all(
        ROOT / "src" / "frontend" / "middleware.go",
        (
            "signSessionCookie",
            "verifySessionCookie",
            "hmac.Equal",
            "NATS_PASSWORD",
        ),
    )
    del frontend
    payment = require_all(
        ROOT / "src" / "paymentservice" / "nats_worker.js",
        (
            "deriveSigningKey",
            "verifyPaymentToken",
            "stableID",
            "PAYMENT_SIGNING_KEY",
        ),
    )
    if re.search(
        r"(?i)(provider|token|payment)(Outcome|Registry|Cache)\s*=\s*new Map",
        payment,
    ):
        raise VerificationError("payment has a process-local outcome registry")
    require_all(
        ROOT / "src" / "paymentservice" / "nats_worker.test.js",
        (
            "replicaAKey",
            "replicaBKey",
            "a second replica could not verify the issued token",
        ),
    )


def verify_nats_resources(rendered_nats: str) -> None:
    required = (
        "ensure_kv RECOMMENDATION_CATALOG 5 0s 268435456",
        "ensure_kv BOOTSTRAP_CLAIMS 5 0s 67108864",
        '"$KV.RECOMMENDATION_CATALOG.>"',
        '"$KV.BOOTSTRAP_CLAIMS.>"',
        'user: "productcatalogservice"',
        'user: "currencyservice"',
        'user: "recommendationservice"',
    )
    missing = [value for value in required if value not in rendered_nats]
    if missing:
        raise VerificationError(
            f"NATS Phase 2 resources or permissions are missing: {missing}"
        )
    require_all(
        ROOT / "src" / "productcatalogservice" / "nats_events.go",
        (
            'KeyValue("BOOTSTRAP_CLAIMS")',
            "NewKVLeaseStore",
            "ErrLeaseComplete",
            "renewBootstrapClaim",
        ),
    )
    require_all(
        ROOT / "src" / "currencyservice" / "nats_events.js",
        (
            "new Kvm(nc).open('BOOTSTRAP_CLAIMS')",
            "ensureBootstrap",
            "bootstrap.currency",
        ),
    )
    require_all(
        ROOT / "src" / "currencyservice" / "server.js",
        (
            "healthServer.listen",
            "currency NATS bootstrap is unavailable; retrying",
        ),
    )
    require_all(
        ROOT / "src" / "productcatalogservice" / "server.go",
        (
            "initializeCatalogPublisher",
            "catalog NATS bootstrap is unavailable; retrying",
        ),
    )


def run_tests() -> None:
    run([sys.executable, "scripts/verify-stateless-phase1.py"])
    go_modules = (
        ROOT / "src" / "storefrontprojectionservice",
        ROOT / "src" / "productcatalogservice",
        ROOT / "src" / "frontend",
    )
    with tempfile.TemporaryDirectory(prefix="stateless-phase2-") as temp:
        temp_root = Path(temp)
        for module in go_modules:
            run(
                ["go", "test", "./..."],
                cwd=module,
                environment={
                    "GOCACHE": str(
                        temp_root / f"go-build-{module.name}"
                    ),
                },
            )
    run(
        [sys.executable, "-m", "unittest", "catalog_kv_test.py", "-v"],
        cwd=ROOT / "src" / "recommendationservice",
    )
    run(["npm", "test"], cwd=ROOT / "src" / "currencyservice")
    run(["npm", "test"], cwd=ROOT / "src" / "paymentservice")


def main() -> int:
    try:
        run_tests()
        verify_storefront_sources()
        verify_recommendation_sources()
        verify_replica_state_sources()
        kubectl = shutil.which("kubectl")
        if not kubectl:
            raise VerificationError("kubectl with built-in kustomize is required")
        run([kubectl, "kustomize", "kubernetes-manifests"])
        rendered_nats = run(
            [kubectl, "kustomize", "kubernetes-manifests/nats"]
        )
        verify_nats_resources(rendered_nats)
        if not PROGRESS.is_file():
            raise VerificationError(
                "docs/development/stateless-progress/Phase2.md has not been written"
            )
        run(
            [
                "git",
                "check-ignore",
                "--quiet",
                str(PROGRESS.relative_to(ROOT)),
            ]
        )
    except (OSError, VerificationError) as error:
        print(f"Stateless Phase 2 verification failed: {error}", file=sys.stderr)
        return 1
    print(
        "Stateless Phase 2 shared projections, recommendation KV, concurrent "
        "bootstrap claims, replica-local state contracts, and ignored progress "
        "documentation verified"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
