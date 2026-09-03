#!/usr/bin/env python3
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

"""Rebuild completed benchmark summaries from cluster artifact archives."""

from __future__ import annotations

import argparse
import json
import shutil
import stat
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_SOURCE = REPOSITORY_ROOT / "src" / "benchmarkservice"
sys.path.insert(0, str(BENCHMARK_SOURCE))

from reporting import build_report  # noqa: E402


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download every completed benchmark artifact archive from the "
            "cluster and rebuild its summary.json locally."
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPOSITORY_ROOT / "benchmark-results" / "recovery",
        help="directory for recovered RUN_ID-summary.json files",
    )
    parser.add_argument(
        "--url",
        help=(
            "benchmark service base URL; by default it is discovered from "
            "the active kubectl context"
        ),
    )
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--service", default="benchmarkservice-external")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="replace summaries already present in the output directory",
    )
    return parser.parse_args()


def discover_service_url(namespace: str, service: str) -> str:
    command = [
        "kubectl",
        "get",
        "service",
        service,
        "--namespace",
        namespace,
        "--output",
        "json",
    ]
    try:
        completed = subprocess.run(
            command, check=True, capture_output=True, text=True
        )
    except FileNotFoundError as error:
        raise RuntimeError("kubectl is not installed or is not on PATH") from error
    except subprocess.CalledProcessError as error:
        message = error.stderr.strip() or error.stdout.strip()
        raise RuntimeError(f"kubectl could not read service {service}: {message}")

    value = json.loads(completed.stdout)
    ingress = value.get("status", {}).get("loadBalancer", {}).get("ingress", [])
    if not ingress:
        raise RuntimeError(
            f"service {namespace}/{service} has no external address; "
            "use --url with a reachable benchmark service URL"
        )
    address = ingress[0].get("hostname") or ingress[0].get("ip")
    if not address:
        raise RuntimeError(f"service {namespace}/{service} has an empty address")
    port = int(value.get("spec", {}).get("ports", [{}])[0].get("port", 80))
    suffix = "" if port == 80 else f":{port}"
    return f"http://{address}{suffix}"


def api_url(base_url: str, *parts: str) -> str:
    encoded = "/".join(urllib.parse.quote(part, safe="") for part in parts)
    return f"{base_url.rstrip('/')}/{encoded}"


def read_json(url: str, timeout: float) -> Any:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def download(url: str, destination: Path, timeout: float) -> None:
    request = urllib.request.Request(url, headers={"Accept": "application/zip"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        with destination.open("wb") as target:
            shutil.copyfileobj(response, target)


def extract_safely(archive_path: Path, destination: Path) -> None:
    with zipfile.ZipFile(archive_path) as archive:
        for member in archive.infolist():
            path = PurePosixPath(member.filename.replace("\\", "/"))
            if path.is_absolute() or not path.parts or ".." in path.parts:
                raise RuntimeError(f"unsafe path in artifact archive: {member.filename}")
            mode = member.external_attr >> 16
            if stat.S_ISLNK(mode):
                raise RuntimeError(f"symlink in artifact archive: {member.filename}")
            target = destination.joinpath(*path.parts)
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output)


def recover_run(
    base_url: str,
    run_id: str,
    output: Path,
    timeout: float,
) -> Path:
    with tempfile.TemporaryDirectory(prefix=f"benchmark-{run_id}-") as temporary:
        work = Path(temporary)
        archive = work / "artifacts.zip"
        run_directory = work / "run"
        run_directory.mkdir()
        download(
            api_url(base_url, "api", "runs", run_id, "artifacts.zip"),
            archive,
            timeout,
        )
        extract_safely(archive, run_directory)
        for required in ("config.json", "business.jsonl", "resources.jsonl"):
            if not (run_directory / required).is_file():
                raise RuntimeError(f"{run_id}: archive is missing {required}")
        summary = build_report(run_directory)
        destination = output / f"{run_id}-summary.json"
        temporary_destination = output / f".{run_id}-summary.json.tmp"
        temporary_destination.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_destination.replace(destination)
        return destination


def main() -> int:
    options = arguments()
    base_url = options.url or discover_service_url(
        options.namespace, options.service
    )
    runs = read_json(api_url(base_url, "api", "runs"), options.timeout)
    if not isinstance(runs, list):
        raise RuntimeError("benchmark service returned an invalid run list")

    recoverable = [
        run
        for run in runs
        if isinstance(run, dict)
        and run.get("state") == "completed"
        and run.get("artifacts_available") is True
        and isinstance(run.get("run_id"), str)
    ]
    options.output.mkdir(parents=True, exist_ok=True)
    recovered = 0
    skipped = 0
    failed = 0
    print(f"Benchmark service: {base_url}")
    print(f"Recoverable completed runs: {len(recoverable)}")
    for run in recoverable:
        run_id = run["run_id"]
        destination = options.output / f"{run_id}-summary.json"
        if destination.exists() and not options.overwrite:
            print(f"SKIP {run_id}: {destination} already exists")
            skipped += 1
            continue
        try:
            recovered_path = recover_run(
                base_url, run_id, options.output, options.timeout
            )
        except (OSError, RuntimeError, urllib.error.URLError, zipfile.BadZipFile) as error:
            print(f"FAIL {run_id}: {error}", file=sys.stderr)
            failed += 1
            continue
        print(f"OK   {run_id}: {recovered_path}")
        recovered += 1

    print(f"Recovered: {recovered}; skipped: {skipped}; failed: {failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, urllib.error.URLError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
