#!/usr/bin/env python3
"""Compile and compatibility-check the Phase 0 NATS contracts. This is strictly a test and not needed for building the project."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterator


ROOT = Path(__file__).resolve().parents[1]
PROTO_ROOT = ROOT / "protos"
LOCK_PATH = PROTO_ROOT / "compatibility" / "v1" / "schema-lock.json"
REGISTRY_PATH = PROTO_ROOT / "contracts-v1.json"
HTTP_BASELINE_PATH = ROOT / "docs" / "development" / "baselines" / "http-responses.json"
GRPC_BASELINE_PATH = ROOT / "docs" / "development" / "baselines" / "grpc-traffic.json"
CONTRACT_PREFIXES = ("protos/common/", "protos/commands/", "protos/events/")
SUBJECT_PATTERN = re.compile(r"^boutique\.(cmd|evt)\.[a-z0-9-]+(?:\.[a-z0-9-]+)*\.v[1-9][0-9]*$")
MESSAGE_TYPE_PATTERN = re.compile(r"^boutique\.[a-z][A-Za-z0-9.]+\.v[1-9][0-9]*$")


class ContractError(RuntimeError):
    pass


def run(command: list[str], *, cwd: Path = ROOT) -> None:
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True)
    if result.returncode:
        details = "\n".join(part for part in (result.stdout, result.stderr) if part)
        raise ContractError(f"command failed: {' '.join(command)}\n{details}")


def read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(data):
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
        if shift >= 70:
            break
    raise ContractError("invalid varint in compiled protobuf descriptor")


def wire_fields(data: bytes) -> Iterator[tuple[int, int, Any]]:
    offset = 0
    while offset < len(data):
        tag, offset = read_varint(data, offset)
        number, wire_type = tag >> 3, tag & 7
        if number == 0:
            raise ContractError("invalid zero field in compiled protobuf descriptor")
        if wire_type == 0:
            value, offset = read_varint(data, offset)
        elif wire_type == 1:
            value = data[offset : offset + 8]
            offset += 8
        elif wire_type == 2:
            length, offset = read_varint(data, offset)
            value = data[offset : offset + length]
            offset += length
        elif wire_type == 5:
            value = data[offset : offset + 4]
            offset += 4
        else:
            raise ContractError(f"unsupported wire type {wire_type} in descriptor")
        if offset > len(data):
            raise ContractError("truncated compiled protobuf descriptor")
        yield number, wire_type, value


def values(data: bytes, field_number: int, wire_type: int | None = None) -> list[Any]:
    return [
        value
        for number, actual_wire_type, value in wire_fields(data)
        if number == field_number and (wire_type is None or wire_type == actual_wire_type)
    ]


def text_value(data: bytes, field_number: int, default: str = "") -> str:
    found = values(data, field_number, 2)
    return found[0].decode("utf-8") if found else default


def int_value(data: bytes, field_number: int, default: int = 0) -> int:
    found = values(data, field_number, 0)
    return int(found[0]) if found else default


def parse_enum(data: bytes, prefix: str) -> tuple[str, dict[str, int]]:
    name = text_value(data, 1)
    enum_values: dict[str, int] = {}
    for raw_value in values(data, 2, 2):
        value_name = text_value(raw_value, 1)
        value_number = int_value(raw_value, 2)
        enum_values[value_name] = value_number
    return f"{prefix}.{name}", enum_values


def parse_message(
    data: bytes,
    prefix: str,
    messages: dict[str, Any],
    enums: dict[str, Any],
) -> None:
    name = text_value(data, 1)
    full_name = f"{prefix}.{name}"
    fields: dict[str, Any] = {}
    for raw_field in values(data, 2, 2):
        number = int_value(raw_field, 3)
        fields[str(number)] = [
            text_value(raw_field, 1),
            int_value(raw_field, 4),
            int_value(raw_field, 5),
            text_value(raw_field, 6),
            bool(int_value(raw_field, 17)),
        ]
    messages[full_name] = fields
    for raw_nested in values(data, 3, 2):
        parse_message(raw_nested, full_name, messages, enums)
    for raw_enum in values(data, 4, 2):
        enum_name, enum_values = parse_enum(raw_enum, full_name)
        enums[enum_name] = enum_values


def parse_descriptor_set(data: bytes) -> dict[str, Any]:
    parsed_files: dict[str, Any] = {}
    for raw_file in values(data, 1, 2):
        name = text_value(raw_file, 1)
        if not name.startswith(CONTRACT_PREFIXES):
            continue
        package = text_value(raw_file, 2)
        messages: dict[str, Any] = {}
        enums: dict[str, Any] = {}
        for raw_message in values(raw_file, 4, 2):
            parse_message(raw_message, package, messages, enums)
        for raw_enum in values(raw_file, 5, 2):
            enum_name, enum_values = parse_enum(raw_enum, package)
            enums[enum_name] = enum_values

        raw_options = values(raw_file, 8, 2)
        options = raw_options[0] if raw_options else b""
        parsed_files[name] = {
            "package": package,
            "language_options": {
                "java_package": text_value(options, 1),
                "java_multiple_files": bool(int_value(options, 10)),
                "go_package": text_value(options, 11),
                "csharp_namespace": text_value(options, 37),
            },
            "messages": messages,
            "enums": enums,
        }
    return {"lock_version": 1, "files": parsed_files}


def contract_sources() -> list[str]:
    files: list[Path] = []
    for directory in (PROTO_ROOT / "common", PROTO_ROOT / "commands", PROTO_ROOT / "events"):
        files.extend(directory.rglob("*.proto"))
    return [path.relative_to(ROOT).as_posix() for path in sorted(files)]


def compile_contracts(temp_dir: Path) -> dict[str, Any]:
    protoc = shutil.which("protoc")
    if not protoc:
        raise ContractError("protoc is required; install protobuf-compiler")

    sources = contract_sources()
    if not sources:
        raise ContractError("no Phase 0 protobuf contracts found")

    descriptor_path = temp_dir / "contracts.pb"
    base = [protoc, f"--proto_path={ROOT}", "--fatal_warnings"]
    run(base + [f"--descriptor_set_out={descriptor_path}", "--include_imports", *sources])

    # Exercise every repository language target. JavaScript services load .proto
    # files dynamically, so successful descriptor compilation is their compile
    # check. Go generation runs when protoc-gen-go is available; its package
    # option is always checked below.
    for language, flag in (("java", "java_out"), ("csharp", "csharp_out"), ("python", "python_out")):
        output = temp_dir / language
        output.mkdir()
        run(base + [f"--{flag}={output}", *sources])

    go_plugin = shutil.which("protoc-gen-go")
    if not go_plugin and shutil.which("go"):
        go_path = subprocess.run(
            ["go", "env", "GOPATH"],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )
        if go_path.returncode == 0:
            candidate = Path(go_path.stdout.strip()) / "bin" / "protoc-gen-go"
            if candidate.is_file():
                go_plugin = str(candidate)
    if go_plugin:
        output = temp_dir / "go"
        output.mkdir()
        run(base + [f"--plugin=protoc-gen-go={go_plugin}", f"--go_out={output}", "--go_opt=paths=source_relative", *sources])
    else:
        if os.environ.get("REQUIRE_PROTOC_GEN_GO") == "1":
            raise ContractError("protoc-gen-go is required in this environment")
        print("warning: protoc-gen-go not found; validated Go package descriptors without generating source", file=sys.stderr)

    current = parse_descriptor_set(descriptor_path.read_bytes())
    expected_files = set(sources)
    actual_files = set(current["files"])
    if actual_files != expected_files:
        raise ContractError(f"compiled contract files differ: expected {sorted(expected_files)}, got {sorted(actual_files)}")
    for name, descriptor in current["files"].items():
        options = descriptor["language_options"]
        missing = [key for key, value in options.items() if not value]
        if missing:
            raise ContractError(f"{name} is missing language options: {', '.join(missing)}")
    return current


def compare_with_lock(current: dict[str, Any], locked: dict[str, Any]) -> None:
    errors: list[str] = []
    for file_name, old_file in locked.get("files", {}).items():
        new_file = current.get("files", {}).get(file_name)
        if new_file is None:
            errors.append(f"removed file {file_name}")
            continue
        if old_file["package"] != new_file["package"]:
            errors.append(f"changed package for {file_name}")
        if old_file.get("language_options") != new_file.get("language_options"):
            errors.append(f"changed language binding options for {file_name}")
        for message_name, old_message in old_file.get("messages", {}).items():
            new_message = new_file.get("messages", {}).get(message_name)
            if new_message is None:
                errors.append(f"removed message {message_name}")
                continue
            for number, old_field in old_message.items():
                new_field = new_message.get(number)
                if new_field is None:
                    errors.append(f"removed field {message_name}.{old_field[0]} = {number}")
                elif old_field != new_field:
                    errors.append(
                        f"changed field {message_name} number {number}: "
                        f"{old_field!r} -> {new_field!r}"
                    )
        for enum_name, old_enum in old_file.get("enums", {}).items():
            new_enum = new_file.get("enums", {}).get(enum_name)
            if new_enum is None:
                errors.append(f"removed enum {enum_name}")
                continue
            for value_name, number in old_enum.items():
                if new_enum.get(value_name) != number:
                    errors.append(f"removed or renumbered enum value {enum_name}.{value_name} = {number}")
    if errors:
        raise ContractError("breaking protobuf changes detected:\n- " + "\n- ".join(errors))
    if current != locked:
        raise ContractError(
            "protobuf changes are compatible but the golden schema lock is stale; "
            "review the additions and refresh schema-lock.json with --print-lock"
        )


def all_message_names(current: dict[str, Any]) -> set[str]:
    return {
        message_name
        for descriptor in current["files"].values()
        for message_name in descriptor["messages"]
    }


def validate_registry(current: dict[str, Any]) -> None:
    registry = json.loads(REGISTRY_PATH.read_text())
    if registry.get("schema_version") != 1:
        raise ContractError("contracts-v1.json must have schema_version 1")
    if registry.get("envelope") != "boutique.common.v1.MessageEnvelope":
        raise ContractError("contracts-v1.json has an unexpected envelope")

    known_messages = all_message_names(current)
    registered_payloads: list[str] = []
    subjects: list[str] = []
    message_types: list[str] = []
    for kind in ("commands", "events"):
        expected_token = "cmd" if kind == "commands" else "evt"
        expected_suffix = "Command" if kind == "commands" else "Event"
        for contract in registry.get(kind, []):
            subject = contract.get("subject", "")
            message_type = contract.get("message_type", "")
            payload = contract.get("payload", "")
            if not SUBJECT_PATTERN.fullmatch(subject) or subject.split(".")[1] != expected_token:
                raise ContractError(f"invalid {kind[:-1]} subject: {subject}")
            if not MESSAGE_TYPE_PATTERN.fullmatch(message_type):
                raise ContractError(f"invalid message_type: {message_type}")
            if payload not in known_messages:
                raise ContractError(f"registry payload does not exist: {payload}")
            if not payload.endswith(expected_suffix):
                raise ContractError(f"registry payload has wrong kind: {payload}")
            subjects.append(subject)
            message_types.append(message_type)
            registered_payloads.append(payload)

    for label, entries in (("subject", subjects), ("message_type", message_types), ("payload", registered_payloads)):
        duplicates = sorted({value for value in entries if entries.count(value) > 1})
        if duplicates:
            raise ContractError(f"duplicate {label} values: {duplicates}")

    contract_payloads = {
        name
        for name in known_messages
        if name.startswith("boutique.commands.") and name.endswith("Command")
        or name.startswith("boutique.events.") and name.endswith("Event")
    }
    missing = sorted(contract_payloads - set(registered_payloads))
    if missing:
        raise ContractError(f"command/event payloads missing from registry: {missing}")

    envelope = next(
        descriptor["messages"]["boutique.common.v1.MessageEnvelope"]
        for descriptor in current["files"].values()
        if "boutique.common.v1.MessageEnvelope" in descriptor["messages"]
    )
    expected_envelope_fields = {
        "message_id", "message_type", "schema_version", "occurred_at", "producer",
        "aggregate_type", "aggregate_id", "aggregate_version", "correlation_id",
        "causation_id", "traceparent", "tracestate", "data",
    }
    actual_envelope_fields = {field[0] for field in envelope.values()}
    if actual_envelope_fields != expected_envelope_fields:
        raise ContractError("MessageEnvelope does not contain the required metadata fields")

    forbidden = re.compile(r"(^|_)(pan|cvv|card_number|credit_card_number)($|_)")
    for descriptor in current["files"].values():
        if not descriptor["package"].startswith("boutique.events"):
            continue
        for message_name, message in descriptor["messages"].items():
            for field in message.values():
                if forbidden.search(field[0]):
                    raise ContractError(f"sensitive card field forbidden in event {message_name}.{field[0]}")


def validate_baselines() -> None:
    http = json.loads(HTTP_BASELINE_PATH.read_text())
    responses = http.get("responses", [])
    if not responses:
        raise ContractError("HTTP baseline has no responses")
    required_http = {"method", "path", "scenario", "status", "body", "target"}
    for index, response in enumerate(responses):
        missing = required_http - response.keys()
        if missing:
            raise ContractError(f"HTTP baseline response {index} is missing {sorted(missing)}")

    grpc = json.loads(GRPC_BASELINE_PATH.read_text())
    interactions = grpc.get("interactions", [])
    if not interactions:
        raise ContractError("gRPC baseline has no interactions")
    required_grpc = {"caller", "callee", "rpc", "cardinality", "target_kind", "target"}
    for index, interaction in enumerate(interactions):
        missing = required_grpc - interaction.keys()
        if missing:
            raise ContractError(f"gRPC baseline interaction {index} is missing {sorted(missing)}")
    if not grpc.get("retained_non_grpc") or not grpc.get("exposed_unused"):
        raise ContractError("baseline must map retained non-gRPC and exposed-unused interactions")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--print-lock",
        action="store_true",
        help="print the current compiled schema lock instead of checking it",
    )
    args = parser.parse_args()
    try:
        with tempfile.TemporaryDirectory(prefix="boutique-phase0-") as raw_temp_dir:
            current = compile_contracts(Path(raw_temp_dir))
        validate_registry(current)
        validate_baselines()
        if args.print_lock:
            print(json.dumps(current, separators=(",", ":"), sort_keys=True))
            return 0
        if not LOCK_PATH.exists():
            raise ContractError(f"golden schema lock is missing: {LOCK_PATH}")
        compare_with_lock(current, json.loads(LOCK_PATH.read_text()))
    except (ContractError, json.JSONDecodeError, OSError) as error:
        print(f"Phase 0 contract verification failed: {error}", file=sys.stderr)
        return 1
    print("Phase 0 contracts, language targets, registry, baselines, and golden lock verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
