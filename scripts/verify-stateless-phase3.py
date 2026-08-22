#!/usr/bin/env python3
"""Verify stateless Phase 3 provider behavior and deployment contracts."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROGRESS = ROOT / "docs" / "development" / "stateless-progress" / "Phase3.md"
PROVIDER_MANIFESTS = (
    ROOT / "kubernetes-manifests" / "emailservice.yaml",
    ROOT / "kubernetes-manifests" / "shippingservice.yaml",
    ROOT / "release" / "kubernetes-manifests.yaml",
    ROOT / "release" / "kubernetes-manifests-no-loadgenerator.yaml",
    ROOT / "benchmark" / "benchmark-nats.yaml",
    ROOT / "benchmark" / "benchmark-nats-multiple-replicas.yaml",
    ROOT / "benchmark" / "benchmark-nats-hpa.yaml",
    ROOT / "benchmark" / "benchmark-nats-with-delay.yaml",
)


class VerificationError(RuntimeError):
    pass


def run(
    command: list[str],
    *,
    cwd: Path = ROOT,
    environment: dict[str, str] | None = None,
) -> str:
    env = os.environ.copy()
    if environment:
        env.update(environment)
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if result.returncode:
        rendered = " ".join(command)
        raise VerificationError(
            f"{rendered} failed in {cwd.relative_to(ROOT) or Path('.')}:\n"
            f"{result.stdout.rstrip()}"
        )
    return result.stdout


def require_all(path: Path, values: tuple[str, ...]) -> str:
    content = path.read_text()
    missing = [value for value in values if value not in content]
    if missing:
        raise VerificationError(
            f"{path.relative_to(ROOT)} is missing Phase 3 contracts: {missing}"
        )
    return content


def manifest_documents(path: Path) -> list[str]:
    return re.split(r"(?m)^---[ \t]*\r?\n", path.read_text())


def deployment(path: Path, name: str) -> str:
    for document in manifest_documents(path):
        if not re.search(r"(?m)^kind:\s*Deployment\s*$", document):
            continue
        match = re.search(
            r"(?ms)^metadata:\s*\n(?P<body>(?:^[ \t]+.*\n?)*)",
            document,
        )
        if match and re.search(
            rf"(?m)^[ \t]+name:\s*{re.escape(name)}\s*$",
            match.group("body"),
        ):
            return document
    raise VerificationError(
        f"{path.relative_to(ROOT)} has no Deployment/{name}"
    )


def verify_sources() -> None:
    shipping_files = sorted((ROOT / "src" / "shippingservice").glob("*.go"))
    shipping = "\n".join(path.read_text() for path in shipping_files)
    for forbidden in (
        "shippingProviderStore",
        "openShippingProviderStore",
        "SHIPPING_STORE_PATH",
        "provider-state.json",
        "os.OpenFile",
        "os.ReadFile",
        "math/rand",
    ):
        if forbidden in shipping:
            raise VerificationError(
                f"shipping retains local provider state or randomness: {forbidden}"
            )
    for required in (
        "SHIPPING_PROVIDER_SECRET",
        "newShippingProvider",
        "shippingCreateShipmentSlot",
        "validateShippingInput",
        "inputTime.Add(15 * time.Minute)",
        "stateless.NewResultEnvelope",
        "shipping NATS initialization interrupted; retrying",
    ):
        if required not in shipping:
            raise VerificationError(
                f"shipping deterministic provider contract is missing {required!r}"
            )

    email_path = ROOT / "src" / "emailservice" / "nats_worker.py"
    email = require_all(
        email_path,
        (
            "_provider_idempotency_key",
            'NOTIFICATION_TYPE = "order-confirmation"',
            "_result_message_id",
            "result.occurred_at.CopyFrom(envelope.occurred_at)",
            "SerializeToString(deterministic=True)",
            "Email NATS initialization interrupted; retrying",
        ),
    )
    for forbidden in (
        "import sqlite3",
        "class _State",
        "EMAIL_STORE_PATH",
        ".sqlite3",
        "inbox.json",
    ):
        if forbidden in email:
            raise VerificationError(
                f"email retains local provider state: {forbidden}"
            )

    payment_path = ROOT / "src" / "paymentservice" / "nats_worker.js"
    payment = require_all(
        payment_path,
        (
            "PAYMENT_SIGNING_KEY_ID",
            "PAYMENT_VERIFICATION_KEYS",
            "createSigningKeyring",
            "signing_key_set_fingerprint",
            "verifiedToken.keyID",
            "deriveResultMessageID",
        ),
    )
    if re.search(
        r"(?i)(provider|token|payment)(Outcome|Registry|Cache)\s*=\s*new Map",
        payment,
    ):
        raise VerificationError("payment has a process-local provider registry")
    require_all(
        ROOT / "src" / "paymentservice" / "nats_worker.test.js",
        (
            "overlapKeyring",
            "key rotation changed a retried authorization outcome",
            "payment token omitted its signing key ID",
        ),
    )


def verify_manifests() -> None:
    forbidden = (
        "EMAIL_STORE_PATH",
        "SHIPPING_STORE_PATH",
        "email-data",
        "shipping-data",
    )
    for path in PROVIDER_MANIFESTS:
        content = path.read_text()
        remaining = [value for value in forbidden if value in content]
        if remaining:
            raise VerificationError(
                f"{path.relative_to(ROOT)} retains provider storage: {remaining}"
            )
        services = (
            (path.stem,)
            if path.parent == ROOT / "kubernetes-manifests"
            else ("emailservice", "shippingservice")
        )
        for service in services:
            workload = deployment(path, service)
            for required in (
                "type: RollingUpdate",
                "maxUnavailable: 0",
                "maxSurge: 1",
            ):
                if required not in workload:
                    raise VerificationError(
                        f"{path.relative_to(ROOT)} Deployment/{service} "
                        f"is missing {required!r}"
                    )
            if "persistentVolumeClaim:" in workload or "type: Recreate" in workload:
                raise VerificationError(
                    f"{path.relative_to(ROOT)} Deployment/{service} is not stateless"
                )

    payment_manifests = (
        ROOT / "kubernetes-manifests" / "paymentservice.yaml",
        ROOT / "release" / "kubernetes-manifests.yaml",
        ROOT / "release" / "kubernetes-manifests-no-loadgenerator.yaml",
        ROOT / "benchmark" / "benchmark-nats.yaml",
        ROOT / "benchmark" / "benchmark-nats-multiple-replicas.yaml",
        ROOT / "benchmark" / "benchmark-nats-hpa.yaml",
        ROOT / "benchmark" / "benchmark-nats-with-delay.yaml",
    )
    for path in payment_manifests:
        if "PAYMENT_SIGNING_KEY_ID" not in deployment(path, "paymentservice"):
            raise VerificationError(
                f"{path.relative_to(ROOT)} Deployment/paymentservice does not "
                "configure an explicit active signing key ID"
            )

    setup = require_all(
        ROOT / "kubernetes-manifests" / "nats" / "base" / "setup.yaml",
        (
            "SHIPPING_PROVIDER_SECRET",
            "global-application-secrets",
            "PAYMENT_SIGNING_KEY",
        ),
    )
    del setup


def run_tests() -> None:
    run([sys.executable, "scripts/verify-stateless-phase2.py"])
    with tempfile.TemporaryDirectory(prefix="stateless-phase3-") as temp:
        run(
            ["go", "test", "./..."],
            cwd=ROOT / "src" / "shippingservice",
            environment={"GOCACHE": str(Path(temp) / "go-build")},
        )
    try:
        run(
            [sys.executable, "-m", "unittest", "nats_worker_test.py", "-v"],
            cwd=ROOT / "src" / "emailservice",
        )
    except VerificationError as host_error:
        docker = shutil.which("docker")
        if not docker:
            raise host_error
        image = "microservices-demo-emailservice-phase3-test"
        run(
            [
                docker,
                "build",
                "-f",
                "src/emailservice/Dockerfile",
                "-t",
                image,
                ".",
            ]
        )
        run(
            [
                docker,
                "run",
                "--rm",
                "--entrypoint",
                "python",
                image,
                "-m",
                "unittest",
                "nats_worker_test.py",
                "-v",
            ]
        )
    run(["npm", "test"], cwd=ROOT / "src" / "paymentservice")


def main() -> int:
    try:
        run_tests()
        verify_sources()
        verify_manifests()
        kubectl = shutil.which("kubectl")
        if not kubectl:
            raise VerificationError("kubectl with built-in kustomize is required")
        run([kubectl, "kustomize", "kubernetes-manifests"])
        rendered_nats = run(
            [kubectl, "kustomize", "kubernetes-manifests/nats"]
        )
        if "SHIPPING_PROVIDER_SECRET" not in rendered_nats:
            raise VerificationError(
                "rendered NATS setup omits the shipping provider secret"
            )
        if not PROGRESS.is_file():
            raise VerificationError(
                "docs/development/stateless-progress/Phase3.md has not been written"
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
        print(f"Stateless Phase 3 verification failed: {error}", file=sys.stderr)
        return 1
    print(
        "Stateless Phase 3 deterministic shipping/email providers, payment "
        "key rotation, provider PVC removal, rolling updates, and ignored "
        "progress documentation verified"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
