# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import heapq
import io
import json
import shutil
import zipfile
from collections.abc import Iterator
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


def archive_directory_to_file(directory: Path, destination: Path) -> None:
    """Archive a worker directory without retaining the ZIP in memory."""
    with zipfile.ZipFile(
        destination, "w", compression=zipfile.ZIP_DEFLATED
    ) as archive:
        for path in sorted(directory.rglob("*")):
            if path.is_file() and path != destination:
                archive.write(path, path.relative_to(directory))


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


def extract_archive_file(source: Path, destination: Path) -> None:
    """Extract a worker archive directly from disk."""
    destination.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(source) as archive:
        for member in archive.infolist():
            path = Path(member.filename)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(
                    f"worker archive contains unsafe path {member.filename!r}"
                )
        archive.extractall(destination)


def _read_records(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
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
                yield value


def _write_records(path: Path, records: Iterator[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as target:
        for record in records:
            target.write(json.dumps(record, sort_keys=True) + "\n")


def _timestamp(record: dict[str, Any]) -> float:
    return float(record.get("timestamp", 0))


def merge_worker_outputs(
    run_directory: Path, worker_directories: list[Path]
) -> list[dict[str, Any]]:
    business = heapq.merge(
        *(
            _read_records(directory / "business.jsonl")
            for directory in worker_directories
        ),
        key=_timestamp,
    )
    _write_records(run_directory / "business.jsonl", business)

    outstanding = heapq.merge(
        *(
            _read_records(directory / "outstanding.jsonl")
            for directory in worker_directories
        ),
        key=_timestamp,
    )
    total_outstanding = 0

    def rebase_outstanding() -> Iterator[dict[str, Any]]:
        nonlocal total_outstanding
        for record in outstanding:
            if record.get("event") == "accepted":
                total_outstanding += 1
            else:
                total_outstanding = max(0, total_outstanding - 1)
            record["outstanding"] = total_outstanding
            yield record

    _write_records(
        run_directory / "outstanding.jsonl", rebase_outstanding()
    )

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
                with log_path.open("rb") as worker_log:
                    shutil.copyfileobj(worker_log, combined_log)
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
