#!/usr/bin/env python3
"""Verify stateless Phase 4 cart storage, publication, and replica contracts."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROGRESS = ROOT / "docs" / "development" / "stateless-progress" / "Phase4.md"
CART_ROOT = ROOT / "src" / "cartservice"
SDK_IMAGE = (
    "mcr.microsoft.com/dotnet/sdk:10.0.100-noble@"
    "sha256:c7445f141c04f1a6b454181bd098dcfa606c61ba0bd213d0a702489e5bd4cd71"
)


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
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if result.returncode:
        raise VerificationError(
            f"{' '.join(command)} failed in "
            f"{cwd.relative_to(ROOT) or Path('.')}:\n{result.stdout.rstrip()}"
        )
    return result.stdout


def require_all(path: Path, values: tuple[str, ...]) -> str:
    content = path.read_text()
    missing = [value for value in values if value not in content]
    if missing:
        raise VerificationError(
            f"{path.relative_to(ROOT)} is missing Phase 4 contracts: {missing}"
        )
    return content


def cart_sources() -> str:
    return "\n".join(
        path.read_text()
        for path in sorted((CART_ROOT / "src").rglob("*.cs"))
    )


def verify_sources() -> None:
    source = cart_sources()
    for forbidden in (
        "NatsOutboxRelay",
        "RedisOutboxCartStore",
        "IEventOutboxCartStore",
        "cart:outbox:pending",
        "cart:outbox:messages",
        "SortedSetRangeByRankAsync",
        "HashSetAsync(OutboxKey",
        "Guid.CreateVersion7",
        "DateTime.UtcNow",
        "AddDistributedMemoryCache",
        "SpannerCartStore",
        "AlloyDBCartStore",
    ):
        if forbidden in source:
            raise VerificationError(
                f"cartservice retains forbidden replica/outbox path {forbidden!r}"
            )

    for required in (
        "RedisAtomicAggregateStore",
        '"cart:v1"',
        "CartResultJournal.Serialize",
        "ResultEnvelopes.CreateMetadata",
        'ResultSlot = "cart.mutation"',
        "ResultRetention = TimeSpan.FromDays(8)",
        "commit.Duplicate",
        "RedisCatalogProjection",
        'new ConsumerConfig("cart-catalog-v1")',
        'FilterSubject = "boutique.evt.catalog.>"',
        "MaxAckPending = 1",
        '"PRODUCT_NOT_FOUND"',
        "PublishResultAsync",
        "NatsJSPubOpts { MsgId = result.MessageId }",
        "await message.AckAsync",
        "await message.NakAsync",
        "Cart NATS initialization interrupted; retrying",
        "boutique_cart_command_duration_seconds",
        "boutique_cart_inbox_hits_total",
        "boutique_cart_redis_retries_total",
    ):
        if required not in source:
            raise VerificationError(
                f"cartservice is missing Phase 4 contract {required!r}"
            )

    shared = require_all(
        ROOT
        / "src"
        / "shared"
        / "stateless"
        / "dotnet"
        / "Boutique.Stateless"
        / "RedisAtomicAggregateStore.cs",
        (
            "ForAggregate",
            "LoadScript",
            'if ARGV[5] == "1" then',
            'redis.call("SET", KEYS[3], ARGV[3], "PX", ARGV[4])',
            "AdvanceVersion",
        ),
    )
    del shared

    tests = require_all(
        CART_ROOT / "tests" / "CartServiceTests.cs",
        (
            "DuplicateAfterPublishTimeoutRepublishesExactStoredResult",
            "ConcurrentAddCommandsBothCommitAfterInternalConflictRetry",
            "ProductMissingFromCartCatalogIsRejectedWithoutMutation",
            "LegacyExpectedVersionOnAddIsIgnored",
            "AmbiguousRedisFailoverRecoversTheCommittedJournal",
            "LiveRedisClusterContinuesAfterReplicaTakeover",
            'ExecuteAsync("CLUSTER", "FAILOVER", "TAKEOVER")',
            "ReplicaMatrixPreservesOneLogicalTransitionUnderLoad",
            "[InlineData(1)]",
            "[InlineData(3)]",
            "[InlineData(10)]",
            "FailAfterCommitOnce",
        ),
    )
    del tests
    require_all(
        ROOT / "scripts" / "testing" / "cart-redis-cluster-entrypoint.sh",
        (
            "--cluster-enabled yes",
            "--cluster-replicas 1",
            "127.0.0.1:7005",
        ),
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


def verify_manifests() -> None:
    kubectl = shutil.which("kubectl")
    if not kubectl:
        raise VerificationError("kubectl with built-in kustomize is required")
    rendered = run([kubectl, "kustomize", "kubernetes-manifests"])
    cart = document(rendered, "Deployment", "cartservice")
    for required in (
        "type: RollingUpdate",
        "maxUnavailable: 0",
        "maxSurge: 1",
        "value: redis-cart-cluster:6379",
    ):
        if required not in cart:
            raise VerificationError(
                f"Deployment/cartservice is missing {required!r}"
            )
    if "value: redis-cart:6379" in cart:
        raise VerificationError("cartservice still targets the singleton Redis store")
    try:
        document(rendered, "Deployment", "redis-cart")
    except VerificationError:
        pass
    else:
        raise VerificationError("legacy Deployment/redis-cart still renders")
    try:
        document(rendered, "PersistentVolumeClaim", "redis-cart-data")
    except VerificationError:
        pass
    else:
        raise VerificationError("legacy PersistentVolumeClaim/redis-cart-data still renders")

    cluster = document(rendered, "StatefulSet", "redis-cart-cluster")
    for required in (
        "name: repair-topology",
        "- /config/repair-topology.sh",
        "name: redis-cluster-bootstrap",
    ):
        if required not in cluster:
            raise VerificationError(
                f"StatefulSet/redis-cart-cluster is missing {required!r}"
            )
    bootstrap = document(rendered, "ConfigMap", "redis-cluster-bootstrap")
    for required in (
        "repair-topology.sh",
        'getent hosts "${hostname}"',
        'chmod 600 "${repaired}"',
        'mv "${repaired}" "${topology}"',
    ):
        if required not in bootstrap:
            raise VerificationError(
                f"ConfigMap/redis-cluster-bootstrap is missing {required!r}"
            )
    for required in (
        "replicas: 6",
        "--cluster-enabled yes",
        "--cluster-preferred-endpoint-type hostname",
        "--appendfsync always",
    ):
        if required not in cluster:
            raise VerificationError(
                f"StatefulSet/redis-cart-cluster is missing {required!r}"
            )
    policy = document(rendered, "NetworkPolicy", "cart-to-redis")
    if "app: redis-cart-cluster" not in policy or re.search(
        r"(?m)^[ \t]+app:\s*redis-cart\s*$", policy
    ):
        raise VerificationError(
            "cart Redis egress is not restricted to redis-cart-cluster"
        )


def run_dotnet_tests() -> None:
    dotnet = shutil.which("dotnet")
    projects = (
        ROOT / "src" / "cartservice" / "cartservice.sln",
        ROOT
        / "src"
        / "shared"
        / "stateless"
        / "dotnet"
        / "Boutique.Stateless.Tests"
        / "Boutique.Stateless.Tests.csproj",
    )
    if dotnet:
        for project in projects:
            run([dotnet, "test", str(project), "--nologo"])
        return
    docker = shutil.which("docker")
    if not docker:
        raise VerificationError("dotnet or docker is required for Phase 4 tests")
    for project in projects:
        run(
            [
                docker,
                "run",
                "--rm",
                "-v",
                f"{ROOT}:/workspace",
                "-w",
                "/workspace",
                SDK_IMAGE,
                "dotnet",
                "test",
                f"/workspace/{project.relative_to(ROOT)}",
                "--nologo",
            ]
        )


def main() -> int:
    try:
        run([sys.executable, "scripts/verify-stateless-phase3.py"])
        run_dotnet_tests()
        verify_sources()
        verify_manifests()
        run([sys.executable, "scripts/verify-phase4-contracts.py"])
        if not PROGRESS.is_file():
            raise VerificationError(
                "docs/development/stateless-progress/Phase4.md has not been written"
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
        print(f"Stateless Phase 4 verification failed: {error}", file=sys.stderr)
        return 1
    print(
        "Stateless Phase 4 aggregate-local cart commits, handler-owned "
        "publication, outbox removal, replica/failover tests, clustered "
        "deployment, and ignored progress documentation verified"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
