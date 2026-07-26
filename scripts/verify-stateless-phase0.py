#!/usr/bin/env python3
"""Verify stateless Phase 0 contracts, tests, and shrink-only guardrails."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import re
import struct
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "protos" / "stateless-contracts-v1.json"
MESSAGE_REGISTRY_PATH = ROOT / "protos" / "contracts-v1.json"
BASELINE_PATH = ROOT / "scripts" / "stateless-guardrails-baseline.json"
BOOTSTRAP_PATH = ROOT / "kubernetes-manifests" / "nats" / "base" / "bootstrap.yaml"

RESULT_SLOT_PATTERN = re.compile(r"^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*$")
DATABASE_IMPORT_PATTERN = re.compile(
    r"""(?ix)
    ^\s*(?:
        import\s+sqlite3\b |
        from\s+sqlite3\b |
        .*go\.etcd\.io/(?:bbolt|bolt)\b |
        .*Microsoft\.Data\.Sqlite\b |
        .*System\.Data\.SQLite\b |
        .*better-sqlite3\b |
        .*require\(\s*['"]sqlite3['"]\s*\)
    )
    """
)
JOURNAL_PATH_PATTERN = re.compile(
    r"""(?ix)
    \b[A-Z][A-Z0-9_]*(?:STORE|JOURNAL|STATE|DATABASE|DB)_PATH\b |
    (?:^|["'\s])/(?:var/lib|tmp|data)/[^"'\s]+\.(?:jsonl?|db|sqlite3?|bolt|bbolt)["']? |
    ["'][^"']*\.sqlite3?["']
    """
)
SINGLE_WRITER_PATTERN = re.compile(r"(?i)\bsingle[- ]writer\b")
SOURCE_SUFFIXES = {
    ".cs",
    ".fs",
    ".go",
    ".java",
    ".js",
    ".mjs",
    ".py",
    ".ts",
}
MANIFEST_ROOTS = (
    ROOT / "kubernetes-manifests",
    ROOT / "release",
    ROOT / "benchmark",
)


class VerificationError(RuntimeError):
    pass


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise VerificationError(f"cannot read {path.relative_to(ROOT)}: {error}") from error
    if not isinstance(value, dict):
        raise VerificationError(f"{path.relative_to(ROOT)} must contain a JSON object")
    return value


def derive_result_message_id(input_message_id: str, result_slot: str) -> str:
    encoded_input = input_message_id.encode("utf-8")
    encoded_slot = result_slot.encode("utf-8")
    digest = hashlib.sha256(
        b"boutique.result.v1\0"
        + struct.pack(">I", len(encoded_input))
        + encoded_input
        + struct.pack(">I", len(encoded_slot))
        + encoded_slot
    ).digest()
    return "br1_" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def validate_retention(contract: dict) -> None:
    retention = contract.get("retention")
    if not isinstance(retention, dict):
        raise VerificationError("stateless contract is missing retention")
    streams = retention.get("streams")
    if not isinstance(streams, list) or not streams:
        raise VerificationError("stateless contract retention must list streams")

    bootstrap = BOOTSTRAP_PATH.read_text()
    names: set[str] = set()
    for stream in streams:
        if not isinstance(stream, dict):
            raise VerificationError("retention stream entries must be objects")
        try:
            name = str(stream["name"])
            max_age = int(stream["max_age_seconds"])
            ack_wait = int(stream["ack_wait_seconds"])
            max_deliver = int(stream["max_deliver"])
            actual_minimum = int(stream["minimum_record_retention_seconds"])
        except (KeyError, TypeError, ValueError) as error:
            raise VerificationError(f"invalid retention entry: {stream!r}") from error
        if name in names:
            raise VerificationError(f"duplicate retention stream {name}")
        names.add(name)
        expected_minimum = max(max_age, ack_wait * max_deliver) + max(
            86400, math.ceil(max_age / 10)
        )
        if actual_minimum != expected_minimum:
            raise VerificationError(
                f"{name} minimum record retention is {actual_minimum}, "
                f"expected {expected_minimum}"
            )
        configured = re.search(
            rf'"name":\s*"{re.escape(name)}".*?"max_age":\s*(\d+)',
            bootstrap,
            re.DOTALL,
        )
        if configured is None:
            raise VerificationError(f"{name} is absent from NATS bootstrap")
        configured_seconds = int(configured.group(1)) // 1_000_000_000
        if configured_seconds != max_age:
            raise VerificationError(
                f"{name} contract max age {max_age}s differs from "
                f"bootstrap {configured_seconds}s"
            )


def validate_stateless_contract() -> None:
    contract = read_json(CONTRACT_PATH)
    registry = read_json(MESSAGE_REGISTRY_PATH)
    if contract.get("schema_version") != 1:
        raise VerificationError("stateless contract schema_version must be 1")

    derivation = contract.get("result_message_id")
    expected_derivation = {
        "algorithm": "sha256-length-prefixed-v1",
        "domain": "boutique.result.v1",
        "encoding": "br1_<base64url-without-padding>",
        "inputs": ["input_message_id", "result_slot"],
    }
    if not isinstance(derivation, dict):
        raise VerificationError("stateless contract is missing result_message_id")
    for key, expected in expected_derivation.items():
        if derivation.get(key) != expected:
            raise VerificationError(
                f"result_message_id.{key} must be {expected!r}"
            )
    vectors = derivation.get("test_vectors")
    if not isinstance(vectors, list) or len(vectors) < 2:
        raise VerificationError("result message ID derivation needs at least two vectors")
    for vector in vectors:
        try:
            actual = derive_result_message_id(
                vector["input_message_id"], vector["result_slot"]
            )
            expected = vector["message_id"]
        except (KeyError, TypeError) as error:
            raise VerificationError(f"invalid result ID vector: {vector!r}") from error
        if actual != expected:
            raise VerificationError(
                f"result ID vector for {vector['input_message_id']!r} is stale"
            )

    registered_commands = {
        entry["subject"] for entry in registry.get("commands", [])
    }
    registered_subjects = registered_commands | {
        entry["subject"] for entry in registry.get("events", [])
    }
    command_results = contract.get("command_results")
    if not isinstance(command_results, list):
        raise VerificationError("stateless contract command_results must be a list")
    mapped_commands: set[str] = set()
    for command in command_results:
        try:
            input_subject = command["input_subject"]
            handler_service = command["handler_service"]
            aggregate_key = command["aggregate_key"]
            results = command["results"]
        except (KeyError, TypeError) as error:
            raise VerificationError(f"invalid command result entry: {command!r}") from error
        if input_subject in mapped_commands:
            raise VerificationError(f"duplicate command result mapping {input_subject}")
        mapped_commands.add(input_subject)
        if not handler_service or not aggregate_key:
            raise VerificationError(f"{input_subject} has an incomplete owner contract")
        if not isinstance(results, list) or not results:
            raise VerificationError(f"{input_subject} has no result slots")
        slots: set[str] = set()
        for result in results:
            slot = result.get("slot", "")
            subjects = result.get("subjects")
            if not RESULT_SLOT_PATTERN.fullmatch(slot):
                raise VerificationError(
                    f"{input_subject} has invalid result slot {slot!r}"
                )
            if slot in slots:
                raise VerificationError(
                    f"{input_subject} repeats result slot {slot!r}"
                )
            slots.add(slot)
            if not isinstance(subjects, list) or not subjects:
                raise VerificationError(
                    f"{input_subject} result slot {slot!r} has no subjects"
                )
            unknown = set(subjects) - registered_subjects
            if unknown:
                raise VerificationError(
                    f"{input_subject} result slot {slot!r} has unknown subjects "
                    f"{sorted(unknown)}"
                )
    if mapped_commands != registered_commands:
        raise VerificationError(
            "command result inventory differs from registered commands: "
            f"missing={sorted(registered_commands - mapped_commands)}, "
            f"extra={sorted(mapped_commands - registered_commands)}"
        )

    inventories = contract.get("service_state_inventory")
    if not isinstance(inventories, list):
        raise VerificationError(
            "stateless contract service_state_inventory must be a list"
        )
    inventory_names: set[str] = set()
    required_fields = {
        "service",
        "current_correctness_state",
        "target_store",
        "idempotency_contract",
    }
    for inventory in inventories:
        if not isinstance(inventory, dict):
            raise VerificationError("service state inventory entries must be objects")
        missing = [
            field
            for field in required_fields
            if not isinstance(inventory.get(field), str) or not inventory[field].strip()
        ]
        if missing:
            raise VerificationError(
                f"service state inventory entry is missing {missing}: {inventory!r}"
            )
        service = inventory["service"]
        if service in inventory_names:
            raise VerificationError(f"duplicate state inventory for {service}")
        inventory_names.add(service)
    source_services = {
        path.name
        for path in (ROOT / "src").iterdir()
        if path.is_dir() and not path.name.startswith(".") and path.name != "shared"
    }
    if inventory_names != source_services:
        raise VerificationError(
            "state inventory differs from src services: "
            f"missing={sorted(source_services - inventory_names)}, "
            f"extra={sorted(inventory_names - source_services)}"
        )
    owners = {entry["handler_service"] for entry in command_results}
    if not owners <= inventory_names:
        raise VerificationError(
            f"command owners missing state inventories: {sorted(owners - inventory_names)}"
        )
    validate_retention(contract)


def relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def yaml_documents(path: Path) -> Iterable[tuple[str, str, str]]:
    for document in re.split(r"(?m)^---\s*$", path.read_text()):
        kind_match = re.search(r"(?m)^kind:\s*([A-Za-z0-9]+)\s*$", document)
        name = ""
        in_metadata = False
        for line in document.splitlines():
            if line == "metadata:":
                in_metadata = True
                continue
            if not in_metadata:
                continue
            if line and not line[0].isspace():
                break
            name_match = re.fullmatch(r"  name:\s*([A-Za-z0-9.-]+)\s*", line)
            if name_match:
                name = name_match.group(1)
                break
        if kind_match and name:
            yield kind_match.group(1), name, document


def manifest_files() -> list[Path]:
    files: list[Path] = []
    for root in MANIFEST_ROOTS:
        files.extend(root.rglob("*.yaml"))
        files.extend(root.rglob("*.yml"))
    return sorted(set(files))


def repeated_line_findings(
    category: str, paths: Iterable[Path], pattern: re.Pattern[str]
) -> list[str]:
    findings: list[str] = []
    for path in paths:
        counts: Counter[str] = Counter()
        for line in path.read_text(errors="replace").splitlines():
            normalized = " ".join(line.strip().split())
            if not normalized or not pattern.search(line):
                continue
            counts[normalized] += 1
            findings.append(
                f"{category}::{relative(path)}::{normalized}#{counts[normalized]}"
            )
    return findings


def collect_guardrail_findings() -> dict[str, list[str]]:
    manifests = manifest_files()
    application_services = {
        path.name
        for path in (ROOT / "src").iterdir()
        if path.is_dir() and not path.name.startswith(".") and path.name != "shared"
    }
    pvc_findings: list[str] = []
    for path in manifests:
        for kind, name, document in yaml_documents(path):
            if kind != "Deployment" or name not in application_services:
                continue
            claims = re.findall(
                r"(?m)^[ \t]+claimName:\s*([A-Za-z0-9.-]+)\s*$", document
            )
            for claim, count in Counter(claims).items():
                for occurrence in range(1, count + 1):
                    pvc_findings.append(
                        f"application_pvc::{relative(path)}::Deployment/"
                        f"{name}::{claim}#{occurrence}"
                    )

    production_sources = sorted(
        path
        for path in (ROOT / "src").rglob("*")
        if path.is_file()
        and path.suffix.lower() in SOURCE_SUFFIXES
        and "test" not in path.name.lower()
        and "node_modules" not in path.parts
        and "bin" not in path.parts
        and "obj" not in path.parts
    )
    path_sources = production_sources + manifests
    findings = {
        "application_pvc": sorted(pvc_findings),
        "database_import": sorted(
            repeated_line_findings(
                "database_import", production_sources, DATABASE_IMPORT_PATTERN
            )
        ),
        "local_journal_path": sorted(
            repeated_line_findings(
                "local_journal_path", path_sources, JOURNAL_PATH_PATTERN
            )
        ),
        "single_writer_comment": sorted(
            repeated_line_findings(
                "single_writer_comment", manifests, SINGLE_WRITER_PATTERN
            )
        ),
    }
    return findings


def validate_guardrail_baseline(findings: dict[str, list[str]]) -> None:
    baseline = read_json(BASELINE_PATH)
    if baseline.get("schema_version") != 1:
        raise VerificationError("stateless guardrail baseline schema_version must be 1")
    expected = baseline.get("accepted_findings")
    if not isinstance(expected, dict):
        raise VerificationError("guardrail baseline is missing accepted_findings")
    errors: list[str] = []
    for category, actual_entries in findings.items():
        expected_entries = expected.get(category)
        if not isinstance(expected_entries, list):
            errors.append(f"baseline category {category} must be a list")
            continue
        actual = set(actual_entries)
        accepted = set(expected_entries)
        new = sorted(actual - accepted)
        stale = sorted(accepted - actual)
        if new:
            errors.append(
                f"new {category} findings are forbidden:\n  " + "\n  ".join(new)
            )
        if stale:
            errors.append(
                f"removed {category} findings require shrinking the baseline:\n  "
                + "\n  ".join(stale)
            )
    unknown_categories = set(expected) - set(findings)
    if unknown_categories:
        errors.append(
            f"baseline has unknown categories: {sorted(unknown_categories)}"
        )
    if errors:
        raise VerificationError("\n".join(errors))


def run_contract_tests() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "unittest",
            "scripts.tests.test_stateless_handler_contract",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    if result.returncode:
        details = "\n".join(
            part.strip() for part in (result.stdout, result.stderr) if part.strip()
        )
        raise VerificationError(f"stateless handler contract tests failed:\n{details}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--print-baseline",
        action="store_true",
        help="print current static findings as a reviewed shrink-only baseline",
    )
    args = parser.parse_args()
    try:
        findings = collect_guardrail_findings()
        if args.print_baseline:
            print(
                json.dumps(
                    {
                        "schema_version": 1,
                        "accepted_findings": findings,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        validate_stateless_contract()
        validate_guardrail_baseline(findings)
        run_contract_tests()
    except (OSError, VerificationError) as error:
        print(f"Stateless Phase 0 verification failed: {error}", file=sys.stderr)
        return 1
    print(
        "Stateless Phase 0 contracts, inventories, retention, handler tests, "
        "and pod-state guardrails verified"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
