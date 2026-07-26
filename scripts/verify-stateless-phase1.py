#!/usr/bin/env python3
"""Verify stateless Phase 1 backing services, libraries, tests, and dashboards."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
GO_LIBRARY = ROOT / "src" / "shared" / "stateless" / "go"
DOTNET_TESTS = (
    ROOT
    / "src"
    / "shared"
    / "stateless"
    / "dotnet"
    / "Boutique.Stateless.Tests"
    / "Boutique.Stateless.Tests.csproj"
)
PROGRESS = (
    ROOT / "docs" / "development" / "stateless-progress" / "Phase1.md"
)


class VerificationError(RuntimeError):
    pass


def run(
    command: list[str],
    *,
    cwd: Path = ROOT,
    environment: dict[str, str] | None = None,
) -> str:
    merged_environment = os.environ.copy()
    if environment:
        merged_environment.update(environment)
    result = subprocess.run(
        command,
        cwd=cwd,
        env=merged_environment,
        text=True,
        capture_output=True,
    )
    if result.returncode:
        details = "\n".join(
            part.strip() for part in (result.stdout, result.stderr) if part.strip()
        )
        raise VerificationError(
            f"command failed: {' '.join(command)}\n{details}"
        )
    return result.stdout


def document(rendered: str, kind: str, name: str) -> str:
    for candidate in re.split(r"(?m)^---\s*$", rendered):
        if not re.search(rf"(?m)^kind:\s*{re.escape(kind)}\s*$", candidate):
            continue
        if re.search(
            rf"(?m)^metadata:\s*\n(?:^[ \t].*\n)*?^  name:\s*{re.escape(name)}\s*$",
            candidate,
        ):
            return candidate
    raise VerificationError(f"rendered manifest is missing {kind}/{name}")


def verify_redis_clusters(rendered: str) -> None:
    for owner in ("cart", "checkout"):
        name = f"redis-{owner}-cluster"
        stateful_set = document(rendered, "StatefulSet", name)
        required = (
            "replicas: 6",
            "--cluster-enabled yes",
            "--appendonly yes",
            "--appendfsync always",
            "--cluster-preferred-endpoint-type hostname",
            "topologySpreadConstraints:",
            "volumeClaimTemplates:",
        )
        missing = [value for value in required if value not in stateful_set]
        if missing:
            raise VerificationError(
                f"StatefulSet/{name} is missing {', '.join(missing)}"
            )
        service = document(rendered, "Service", name)
        if "port: 6379" not in service:
            raise VerificationError(f"Service/{name} does not expose Redis")
        bootstrap = document(rendered, "Job", f"{name}-bootstrap")
        if "redis-cluster-bootstrap" not in bootstrap:
            raise VerificationError(f"Job/{name}-bootstrap has no bootstrap script")
        disruption_budget = document(rendered, "PodDisruptionBudget", name)
        if "minAvailable: 5" not in disruption_budget:
            raise VerificationError(
                f"PodDisruptionBudget/{name} does not preserve every shard during voluntary disruption"
            )
        network_policy = document(rendered, "NetworkPolicy", name)
        if "port: 16379" not in network_policy:
            raise VerificationError(
                f"NetworkPolicy/{name} does not permit the Redis cluster bus"
            )


def verify_benchmark_stores(rendered_nats: str, rendered_app: str) -> None:
    for required in (
        "ensure_kv BENCHMARK_RUNS 10 0s 536870912",
        "ensure_object BENCHMARK_ARTIFACTS",
        'user: "benchmarkservice", password: $BENCHMARKSERVICE_PASSWORD',
        '"$KV.BENCHMARK_RUNS.>"',
        '"$O.BENCHMARK_ARTIFACTS.>"',
        "benchmarkservice messageoperationsservice",
        '"nats-credentials-${service}"',
    ):
        if required not in rendered_nats:
            raise VerificationError(
                f"NATS manifests are missing benchmark-store contract {required!r}"
            )
    benchmark = document(rendered_app, "Deployment", "benchmarkservice")
    for required in (
        "nats-client-config",
        "nats-credentials-benchmarkservice",
        "mountPath: /etc/nats-ca",
    ):
        if required not in benchmark:
            raise VerificationError(
                f"benchmarkservice is missing shared-store client input {required!r}"
            )
    benchmark_policy = document(
        rendered_app, "NetworkPolicy", "benchmarkservice-access-and-collection"
    )
    if "port: 4222" not in benchmark_policy:
        raise VerificationError("benchmarkservice cannot reach the NATS stores")


def dashboard_json() -> dict:
    path = (
        ROOT
        / "kubernetes-manifests"
        / "observability"
        / "stateless-dashboard.yaml"
    )
    lines = path.read_text().splitlines()
    try:
        marker = lines.index("  stateless-handlers.json: |")
    except ValueError as error:
        raise VerificationError("stateless Grafana dashboard data is missing") from error
    payload_lines: list[str] = []
    for line in lines[marker + 1 :]:
        if line and not line.startswith("    "):
            break
        payload_lines.append(line[4:] if line.startswith("    ") else "")
    try:
        parsed = json.loads("\n".join(payload_lines))
    except json.JSONDecodeError as error:
        raise VerificationError(f"stateless Grafana dashboard is invalid JSON: {error}") from error
    if not isinstance(parsed, dict):
        raise VerificationError("stateless Grafana dashboard must be a JSON object")
    return parsed


def verify_dashboard(rendered: str) -> None:
    dashboard = dashboard_json()
    serialized = json.dumps(dashboard, sort_keys=True)
    required_metrics = (
        "boutique_handler_outcomes_total",
        "boutique_state_conflicts_total",
        "boutique_handler_redeliveries_total",
        "boutique_result_republishes_total",
        "boutique_claim_outcomes_total",
        "boutique_claim_lease_age_seconds",
        "jetstream_consumer_num_pending",
        "jetstream_consumer_num_ack_pending",
    )
    missing = [metric for metric in required_metrics if metric not in serialized]
    if missing:
        raise VerificationError(
            f"stateless dashboard is missing metrics: {', '.join(missing)}"
        )
    grafana = document(rendered, "Deployment", "grafana")
    if "grafana-stateless-dashboard" not in grafana:
        raise VerificationError("Grafana does not mount the stateless dashboard")


def verify_dotnet_sources() -> None:
    required_sources = (
        DOTNET_TESTS,
        DOTNET_TESTS.parent / "PrimitivesTests.cs",
        DOTNET_TESTS.parent.parent / "Boutique.Stateless" / "ResultMessages.cs",
        DOTNET_TESTS.parent.parent
        / "Boutique.Stateless"
        / "RedisAtomicAggregateStore.cs",
        DOTNET_TESTS.parent.parent / "Boutique.Stateless" / "Retry.cs",
        DOTNET_TESTS.parent.parent / "Boutique.Stateless" / "MetricNames.cs",
    )
    missing = [path.relative_to(ROOT) for path in required_sources if not path.is_file()]
    if missing:
        raise VerificationError(f"missing .NET shared-library sources: {missing}")
    combined = "\n".join(path.read_text() for path in required_sources)
    for required in (
        "br1_cfipIWfz73yXKiJIc0nF-uV6vnx5kWbMUG2_o-ukd50",
        "br1_BipmFE_ifI2JqRb67NFrgisjZYeejPTlkKhojRP1Mz8",
        'redis.call("SET", KEYS[3], ARGV[3], "PX", ARGV[4])',
        "AggregateConflictException",
        "RetryClass",
    ):
        if required not in combined:
            raise VerificationError(
                f".NET shared primitives are missing contract {required!r}"
            )


def main() -> int:
    try:
        run([sys.executable, "scripts/verify-stateless-phase0.py"])
        with tempfile.TemporaryDirectory(prefix="stateless-phase1-") as temp:
            run(
                ["go", "test", "./..."],
                cwd=GO_LIBRARY,
                environment={
                    "GOCACHE": str(Path(temp) / "go-build"),
                    "GOMODCACHE": os.environ.get(
                        "GOMODCACHE", "/tmp/stateless-phase1-gomodcache"
                    ),
                },
            )
        verify_dotnet_sources()
        dotnet = shutil.which("dotnet")
        if dotnet:
            run([dotnet, "test", str(DOTNET_TESTS), "--nologo"])
        elif os.environ.get("REQUIRE_DOTNET") == "1":
            raise VerificationError("dotnet is required in this environment")
        else:
            print(
                "warning: dotnet not found; validated .NET source contracts "
                "without compiling",
                file=sys.stderr,
            )

        kubectl = shutil.which("kubectl")
        if not kubectl:
            raise VerificationError("kubectl with built-in kustomize is required")
        rendered_app = run([kubectl, "kustomize", "kubernetes-manifests"])
        rendered_nats = run([kubectl, "kustomize", "kubernetes-manifests/nats"])
        rendered_observability = run(
            [kubectl, "kustomize", "kubernetes-manifests/observability"]
        )
        verify_redis_clusters(rendered_app)
        verify_benchmark_stores(rendered_nats, rendered_app)
        verify_dashboard(rendered_observability)

        if not PROGRESS.is_file():
            raise VerificationError(
                "docs/development/stateless-progress/Phase1.md has not been written"
            )
        ignored = run(
            ["git", "check-ignore", "--quiet", str(PROGRESS.relative_to(ROOT))]
        )
        del ignored
    except (OSError, VerificationError) as error:
        print(f"Stateless Phase 1 verification failed: {error}", file=sys.stderr)
        return 1
    print(
        "Stateless Phase 1 Redis clusters, Go/.NET primitives, leases, "
        "benchmark stores, dashboards, and progress-document ignore rule verified"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
