#!/usr/bin/env python3
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import BenchmarkConfig, ConfigError
from parallel import merge_worker_outputs
from reporting import build_report


SOURCE_DIRECTORY = Path(__file__).resolve().parent


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Run an Online Boutique benchmark without the benchmarkservice "
            "API or a Kubernetes cluster."
        )
    )
    result.add_argument("--url", dest="target_url", required=True)
    result.add_argument("--metrics-url")
    result.add_argument(
        "--application-type", choices=("GRPC", "NATS"), required=True
    )
    result.add_argument("--workload", choices=("closed", "open"), default="closed")
    result.add_argument("--warmup-seconds", type=int, default=30)
    result.add_argument("--duration-seconds", type=int, default=120)
    result.add_argument("--drain-seconds", type=int, default=60)
    result.add_argument("--users", type=int, default=10)
    result.add_argument("--spawn-rate", type=float, default=1.0)
    result.add_argument("--arrival-rate", type=float, default=1.0)
    result.add_argument("--outcome-timeout-seconds", type=float, default=30.0)
    result.add_argument(
        "--settlement-timeout-seconds", type=float, default=60.0
    )
    result.add_argument(
        "--resource-sample-interval-seconds", type=float, default=5.0
    )
    result.add_argument("--seed", type=int, default=1)
    result.add_argument(
        "--collect-resources",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    result.add_argument(
        "--collect-nats-metrics",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    result.add_argument(
        "--output", type=Path, default=Path("benchmark-results")
    )
    return result


def config_from_args(arguments: argparse.Namespace) -> BenchmarkConfig:
    values = vars(arguments).copy()
    values["collect_nats_metrics"] = (
        arguments.collect_nats_metrics
        if arguments.application_type == "NATS"
        else False
    )
    return BenchmarkConfig.from_request(values, arguments.application_type)


def write_json(path: Path, value: Any) -> None:
    with path.open("w", encoding="utf-8") as target:
        json.dump(value, target, indent=2, sort_keys=True)
        target.write("\n")


def locust_command(
    config: BenchmarkConfig, run_directory: Path
) -> list[str]:
    users = config.users if config.workload == "closed" else 1
    spawn_rate = config.spawn_rate if config.workload == "closed" else 1
    user_class = (
        "ClosedLoopUser"
        if config.workload == "closed"
        else "OpenLoopDriver"
    )
    return [
        sys.executable,
        "-m",
        "locust",
        "-f",
        str(SOURCE_DIRECTORY / "locustfile.py"),
        "--headless",
        "--only-summary",
        "--host",
        config.target_url,
        "--users",
        str(users),
        "--spawn-rate",
        str(spawn_rate),
        "--run-time",
        f"{config.run_seconds}s",
        "--stop-timeout",
        str(config.drain_seconds),
        "--csv",
        str(run_directory / "locust"),
        "--csv-full-history",
        "--exit-code-on-error",
        "0",
        "--loglevel",
        "WARNING",
        user_class,
    ]


def create_archive(run_directory: Path, run_id: str) -> None:
    destination = run_directory / f"{run_id}.zip"
    with zipfile.ZipFile(
        destination, "w", compression=zipfile.ZIP_DEFLATED
    ) as archive:
        for path in sorted(run_directory.rglob("*")):
            if path.is_file() and path != destination:
                archive.write(path, path.relative_to(run_directory))


def run(config: BenchmarkConfig, output_root: Path) -> tuple[int, Path]:
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    run_directory = output_root.resolve() / run_id
    run_directory.mkdir(parents=True, mode=0o750, exist_ok=False)
    write_json(run_directory / "config.json", config.as_dict())

    processes: list[subprocess.Popen[bytes]] = []
    logs: list[Any] = []
    statuses: list[dict[str, Any]] = []

    def request_stop(signum: int, frame: Any) -> None:
        for process in processes:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGINT)

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    start_epoch = time.time() + (5 if config.worker_count > 1 else 0)
    worker_directories: list[Path] = []
    try:
        for index in range(config.worker_count):
            worker_config = config.for_worker(index)
            worker_directory = (
                run_directory
                if config.worker_count == 1
                else run_directory / f"worker-{index:04d}"
            )
            worker_directory.mkdir(
                parents=True,
                mode=0o750,
                exist_ok=config.worker_count == 1,
            )
            worker_directories.append(worker_directory)
            config_path = worker_directory / "worker-config.json"
            write_json(config_path, worker_config.as_dict())
            environment = os.environ.copy()
            environment.update(
                {
                    "BENCHMARK_CONFIG_FILE": str(config_path),
                    "BENCHMARK_OUTPUT_DIR": str(worker_directory),
                    "BENCHMARK_START_EPOCH": str(start_epoch),
                }
            )
            log = (worker_directory / "runner.log").open("wb")
            logs.append(log)
            processes.append(
                subprocess.Popen(
                    locust_command(worker_config, worker_directory),
                    cwd=SOURCE_DIRECTORY,
                    env=environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            )

        for index, process in enumerate(processes):
            return_code = process.wait()
            status = {
                "worker_index": index,
                "worker_count": config.worker_count,
                "state": "completed" if return_code == 0 else "failed",
                "ended_at": datetime.now(timezone.utc).isoformat(),
                "exit_code": return_code,
                "config": config.for_worker(index).as_dict(),
            }
            statuses.append(status)
            write_json(
                worker_directories[index] / "worker-status.json", status
            )
    finally:
        for log in logs:
            log.close()
        for process in processes:
            if process.poll() is None:
                process.terminate()

    if config.worker_count > 1:
        statuses = merge_worker_outputs(run_directory, worker_directories)
    failed = [status for status in statuses if status["state"] != "completed"]
    state = "failed" if failed else "completed"
    message = None
    summary = None
    try:
        summary = build_report(run_directory)
    except Exception as error:
        state = "failed"
        message = f"report generation failed: {error}"
    write_json(
        run_directory / "status.json",
        {
            "run_id": run_id,
            "state": state,
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "worker_count": config.worker_count,
            "message": message,
            "summary_available": summary is not None,
        },
    )
    create_archive(run_directory, run_id)
    return (0 if state == "completed" else 1), run_directory


def main() -> int:
    arguments = parser().parse_args()
    try:
        config = config_from_args(arguments)
    except ConfigError as error:
        parser().error(str(error))
    return_code, run_directory = run(config, arguments.output)
    print(run_directory)
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
