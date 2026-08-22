# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import io
import json
import shutil
import zipfile
from pathlib import Path
from typing import Any


def ready_object(run_id: str, index: int) -> str:
    return f"{run_id}/workers/{index:04d}/ready.json"


def result_object(run_id: str, index: int) -> str:
    return f"{run_id}/workers/{index:04d}/result.zip"


def start_object(run_id: str) -> str:
    return f"{run_id}/workers/start.json"


def saturation_progress_object(
    run_id: str, rung: int, worker_index: int
) -> str:
    return (
        f"{run_id}/saturation/{rung:04d}/"
        f"worker-{worker_index:04d}.json"
    )


def saturation_decision_object(run_id: str, rung: int) -> str:
    return f"{run_id}/saturation/{rung:04d}/decision.json"


def archive_directory(directory: Path) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(
        output, "w", compression=zipfile.ZIP_DEFLATED
    ) as archive:
        for path in sorted(directory.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(directory))
    return output.getvalue()


def extract_archive(data: bytes, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for member in archive.infolist():
            path = Path(member.filename)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(
                    f"worker archive contains unsafe path {member.filename!r}"
                )
        archive.extractall(destination)


def _read_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                # Locust can be terminated between writing and flushing the
                # final record. Other complete records remain usable.
                if line_number > 1:
                    continue
                raise
            if isinstance(value, dict):
                records.append(value)
    return records


def _write_records(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as target:
        for record in records:
            target.write(json.dumps(record, sort_keys=True) + "\n")


def _timestamp(record: dict[str, Any]) -> float:
    return float(record.get("timestamp", 0))


def merge_worker_outputs(
    run_directory: Path, worker_directories: list[Path]
) -> list[dict[str, Any]]:
    business = sorted(
        (
            record
            for directory in worker_directories
            for record in _read_records(directory / "business.jsonl")
        ),
        key=_timestamp,
    )
    _write_records(run_directory / "business.jsonl", business)

    outstanding = sorted(
        (
            record
            for directory in worker_directories
            for record in _read_records(directory / "outstanding.jsonl")
        ),
        key=_timestamp,
    )
    total_outstanding = 0
    for record in outstanding:
        if record.get("event") == "accepted":
            total_outstanding += 1
        else:
            total_outstanding = max(0, total_outstanding - 1)
        record["outstanding"] = total_outstanding
    _write_records(run_directory / "outstanding.jsonl", outstanding)

    for shared_artifact in ("resources.jsonl", "saturation.jsonl"):
        source = worker_directories[0] / shared_artifact
        if source.exists():
            shutil.copyfile(source, run_directory / shared_artifact)

    statuses: list[dict[str, Any]] = []
    with (run_directory / "runner.log").open("wb") as combined_log:
        for index, directory in enumerate(worker_directories):
            status_path = directory / "worker-status.json"
            if status_path.exists():
                with status_path.open(encoding="utf-8") as source:
                    status = json.load(source)
                if isinstance(status, dict):
                    statuses.append(status)

            combined_log.write(
                f"===== worker {index:04d} =====\n".encode()
            )
            log_path = directory / "runner.log"
            if log_path.exists():
                combined_log.write(log_path.read_bytes())
            combined_log.write(b"\n")

            for csv_path in sorted(directory.glob("locust_*.csv")):
                suffix = csv_path.name.removeprefix("locust")
                shutil.copyfile(
                    csv_path,
                    run_directory
                    / f"locust_worker-{index:04d}{suffix}",
                )

    with (run_directory / "workers.json").open(
        "w", encoding="utf-8"
    ) as target:
        json.dump(statuses, target, indent=2, sort_keys=True)
        target.write("\n")
    return statuses
