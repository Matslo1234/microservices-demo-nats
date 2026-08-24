#!/usr/bin/env python3
"""Verify stateless Phase 5 checkout storage, publication, and deadline contracts."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHECKOUT = ROOT / "src" / "checkoutservice"
PROGRESS = ROOT / "docs" / "development" / "stateless-progress" / "Phase5.md"


class VerificationError(RuntimeError):
    pass


def run(command: list[str], *, cwd: Path = ROOT) -> str:
    environment = os.environ.copy()
    environment.setdefault("GOCACHE", "/tmp/checkout-phase5-go-cache")
    result = subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if result.returncode:
        raise VerificationError(
            f"{' '.join(command)} failed in "
            f"{cwd.relative_to(ROOT) or Path('.')}:\n{result.stdout.rstrip()}"
        )
    return result.stdout


def require(content: str, *values: str) -> None:
    missing = [value for value in values if value not in content]
    if missing:
        raise VerificationError(f"missing Phase 5 contracts: {missing}")


def forbid(content: str, *values: str) -> None:
    retained = [value for value in values if value in content]
    if retained:
        raise VerificationError(f"retained forbidden Phase 5 paths: {retained}")


def production_sources() -> str:
    return "\n".join(
        path.read_text()
        for path in sorted(CHECKOUT.glob("*.go"))
        if not path.name.endswith("_test.go")
    )


def verify_sources() -> None:
    source = production_sources()
    require(
        source,
        "commitOrderScript",
        "checkoutDeadlineShards  = 64",
        'baseKey + ":saga"',
        'baseKey + ":accepted"',
        'baseKey + ":results"',
        "LoadOrderProjections",
        "NewResultEnvelope",
        "MarshalEnvelope",
        "PublishMsgAsync(",
        'message.Header.Set("Nats-Msg-Id"',
        "RedisLeaseStore",
        "DueDeadlines",
        "stableID(\"checkout-deadline\"",
        "deadlineLeaseRecoveries",
        "boutique_checkout_transition_conflicts_total",
        "boutique_checkout_result_publications_total",
        "boutique_checkout_deadline_oldest_age_seconds",
        "redis.NewClusterClient",
        "MaxRedirects: 16",
        "checkout dependency initialization failed",
    )
    forbid(
        source,
        "relayOutbox",
        "RemoveOutboxBatch",
        "outboxPublishBatchSize",
        "HGetAll(",
        "TxPipelined",
        ".Watch(",
        'key("revision")',
        'key("orders")',
        'key("outbox")',
        "log.Fatal(",
    )

    tests = (CHECKOUT / "store_test.go").read_text() + (
        CHECKOUT / "saga_test.go"
    ).read_text()
    require(
        tests,
        "TestConcurrentDuplicateReturnsExactStoredJournal",
        "TestUnrelatedOrdersDoNotConflictOnOneRevision",
        "TestAcceptedProjectionIsImmutable",
        "TestDeadlineLeaseCanBeRecoveredByAnotherReplica",
        "TestDeadlineResultsRepublishAfterPublishBeforeLeaseCompletion",
        "TestLiveRedisClusterContinuesAfterReplicaTakeover",
        "TestSagaTransitionsAcrossRandomReplicasAtScale",
        "[]int{1, 3, 10}",
        "TestFullFailureAndCompensationMatrix",
        "TestCrashBoundariesReplayStoredResults",
        '"CLUSTER", "FAILOVER", "TAKEOVER"',
    )
    fixture = (
        ROOT / "scripts" / "testing" / "checkout-redis-cluster-entrypoint.sh"
    ).read_text()
    require(
        fixture,
        "--cluster-enabled yes",
        "--cluster-replicas 1",
        "127.0.0.1:7105",
    )


def manifest_documents(rendered: str) -> list[str]:
    return re.split(r"(?m)^---[ \t]*\r?\n", rendered)


def document(rendered: str, kind: str, name: str) -> str:
    for candidate in manifest_documents(rendered):
        if not re.search(rf"(?m)^kind:\s*{re.escape(kind)}\s*$", candidate):
            continue
        metadata = re.search(
            r"(?ms)^metadata:\s*\n(?P<body>(?:^[ \t]+.*\n?)*)",
            candidate,
        )
        if metadata and re.search(
            rf"(?m)^[ \t]+name:\s*{re.escape(name)}\s*$",
            metadata.group("body"),
        ):
            return candidate
    raise VerificationError(f"rendered manifest is missing {kind}/{name}")


def absent(rendered: str, kind: str, name: str) -> None:
    try:
        document(rendered, kind, name)
    except VerificationError:
        return
    raise VerificationError(f"legacy {kind}/{name} still renders")


def verify_manifests() -> None:
    kubectl = shutil.which("kubectl")
    if not kubectl:
        raise VerificationError("kubectl with built-in kustomize is required")
    rendered = run([kubectl, "kustomize", "kubernetes-manifests"])
    checkout = document(rendered, "Deployment", "checkoutservice")
    require(
        checkout,
        "type: RollingUpdate",
        "maxUnavailable: 0",
        "maxSurge: 1",
        "value: redis-checkout-cluster:6379",
        "name: CHECKOUT_REDIS_MODE",
        "value: cluster",
        "value: checkout:v2",
        "name: CHECKOUT_DEADLINE_LEASE",
    )
    forbid(checkout, "value: redis-checkout:6379")
    absent(rendered, "Deployment", "redis-checkout")
    absent(rendered, "Service", "redis-checkout")
    absent(rendered, "PersistentVolumeClaim", "redis-checkout-data")

    cluster = document(rendered, "StatefulSet", "redis-checkout-cluster")
    require(
        cluster,
        "name: repair-topology",
        "command:",
        "- /config/repair-topology.sh",
        "name: redis-cluster-bootstrap",
    )
    bootstrap = document(rendered, "ConfigMap", "redis-cluster-bootstrap")
    require(
        bootstrap,
        "repair-topology.sh",
        'getent hosts "${hostname}"',
        'if [ -z "${hostname}" ]',
        'chmod 600 "${repaired}"',
        'mv "${repaired}" "${topology}"',
    )
    require(
        cluster,
        "replicas: 6",
        "--cluster-enabled yes",
        "--cluster-preferred-endpoint-type hostname",
        "--appendfsync always",
        "--save 3600 1",
        "memory: 4Gi",
    )
    policy = document(rendered, "NetworkPolicy", "checkout-to-redis")
    require(policy, "app: redis-checkout-cluster")
    if re.search(r"(?m)^[ \t]+app:\s*redis-checkout\s*$", policy):
        raise VerificationError("checkout egress still permits singleton Redis")


def main() -> int:
    try:
        run([sys.executable, "scripts/verify-stateless-phase4.py"])
        run(["go", "test", "./..."], cwd=CHECKOUT)
        run([sys.executable, "scripts/verify-phase5-contracts.py"])
        verify_sources()
        verify_manifests()
        if not PROGRESS.is_file():
            raise VerificationError(
                "docs/development/stateless-progress/Phase5.md has not been written"
            )
        run(["git", "check-ignore", "--quiet", str(PROGRESS.relative_to(ROOT))])
    except (OSError, VerificationError) as error:
        print(f"Stateless Phase 5 verification failed: {error}", file=sys.stderr)
        return 1
    print(
        "Stateless Phase 5 order-local commits, handler-owned publication, "
        "leased deadlines, replica/failure matrix, clustered deployment, "
        "and ignored progress documentation verified"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
