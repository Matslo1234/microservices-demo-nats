# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
import zipfile
from pathlib import Path
from typing import Any, Callable

from config import BenchmarkConfig
from control import LEASE_KEY, RUN_PREFIX, RunConflict, utc_now
from reporting import build_report
from shared_store import (
    NatsSharedStore,
    RecordNotFound,
    RevisionConflict,
)


SOURCE_DIRECTORY = Path(__file__).resolve().parent
OUTPUT_ROOT = Path(os.environ.get("BENCHMARK_WORK_DIR", "/work"))
RUN_ID = os.environ["BENCHMARK_RUN_ID"]
LEASE_SECONDS = 90
HEARTBEAT_SECONDS = 30


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as target:
        json.dump(value, target, indent=2, sort_keys=True)
        target.write("\n")
    os.replace(temporary, path)


def mutate(
    store: NatsSharedStore,
    change: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    key = RUN_PREFIX + RUN_ID
    for _ in range(12):
        current = store.get(key)
        updated = dict(current.value)
        updated["status"] = dict(current.value["status"])
        updated["artifacts"] = dict(current.value.get("artifacts", {}))
        change(updated)
        updated["updated_at"] = utc_now()
        try:
            store.update(key, updated, current.revision)
            return updated
        except RevisionConflict:
            continue
    raise RunConflict(f"run {RUN_ID} is being updated concurrently")


def renew_lease(store: NatsSharedStore) -> float:
    lease_until = time.time() + LEASE_SECONDS
    for _ in range(12):
        current = store.get(LEASE_KEY)
        if current.value.get("run_id") != RUN_ID:
            raise RunConflict("benchmark lease belongs to another run")
        lease = dict(current.value)
        lease.update(
            {
                "owner": f"job:{os.environ.get('POD_NAME', 'unknown')}",
                "lease_until": lease_until,
                "updated_at": utc_now(),
            }
        )
        try:
            store.update(LEASE_KEY, lease, current.revision)
            return lease_until
        except RevisionConflict:
            continue
    raise RunConflict("benchmark lease renewal remained contended")


def release_lease(store: NatsSharedStore) -> None:
    for _ in range(12):
        try:
            current = store.get(LEASE_KEY)
        except RecordNotFound:
            return
        if current.value.get("run_id") != RUN_ID:
            return
        lease = dict(current.value)
        lease.update(
            {"lease_until": 0, "released": True, "updated_at": utc_now()}
        )
        try:
            store.update(LEASE_KEY, lease, current.revision)
            return
        except RevisionConflict:
            continue


def locust_command(config: BenchmarkConfig, run_directory: Path) -> list[str]:
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


def create_archive(run_directory: Path) -> Path:
    archive = run_directory / f"{RUN_ID}.zip"
    with zipfile.ZipFile(
        archive, "w", compression=zipfile.ZIP_DEFLATED
    ) as target:
        for artifact in sorted(run_directory.iterdir()):
            if artifact.is_file() and artifact != archive:
                target.write(artifact, artifact.name)
    return archive


def content_type(path: Path) -> str:
    return {
        ".csv": "text/csv",
        ".json": "application/json",
        ".jsonl": "application/x-ndjson",
        ".log": "text/plain",
        ".zip": "application/zip",
    }.get(path.suffix, "application/octet-stream")


def upload_artifacts(
    store: NatsSharedStore, run_directory: Path
) -> dict[str, dict[str, Any]]:
    artifacts: dict[str, dict[str, Any]] = {}
    for path in sorted(run_directory.iterdir()):
        if not path.is_file():
            continue
        object_name = f"{RUN_ID}/{path.name}"
        data = path.read_bytes()
        store.put_object(object_name, data)
        artifacts[path.name] = {
            "object": object_name,
            "content_type": content_type(path),
            "size": len(data),
        }
        if path.name == f"{RUN_ID}.zip":
            artifacts["artifacts.zip"] = dict(artifacts[path.name])
    return artifacts


def main() -> int:
    store = NatsSharedStore(operation_timeout=30)
    run_directory = OUTPUT_ROOT / RUN_ID
    run_directory.mkdir(parents=True, mode=0o750, exist_ok=False)
    process: subprocess.Popen[bytes] | None = None
    stop_requested = threading.Event()
    heartbeat_stop = threading.Event()

    def request_stop(signum: int, frame: Any) -> None:
        stop_requested.set()
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGINT)

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    try:
        record = store.get(RUN_PREFIX + RUN_ID).value
        config = BenchmarkConfig.from_dict(record["config"])
        write_json(run_directory / "config.json", config.as_dict())
        lease_until = renew_lease(store)
        mutate(
            store,
            lambda value: value["status"].update(
                {
                    "state": "starting",
                    "started_at": utc_now(),
                    "pod_name": os.environ.get("POD_NAME"),
                    "lease_until": lease_until,
                }
            ),
        )

        def heartbeat() -> None:
            while not heartbeat_stop.wait(HEARTBEAT_SECONDS):
                try:
                    current_lease = renew_lease(store)
                    mutate(
                        store,
                        lambda value: value["status"].update(
                            {
                                "heartbeat_at": utc_now(),
                                "lease_until": current_lease,
                            }
                        ),
                    )
                except Exception:
                    stop_requested.set()
                    if process is not None and process.poll() is None:
                        os.killpg(process.pid, signal.SIGINT)
                    return

        heartbeat_thread = threading.Thread(
            target=heartbeat, name="benchmark-lease", daemon=True
        )
        heartbeat_thread.start()

        environment = os.environ.copy()
        environment["BENCHMARK_CONFIG_FILE"] = str(
            run_directory / "config.json"
        )
        environment["BENCHMARK_OUTPUT_DIR"] = str(run_directory)
        with (run_directory / "runner.log").open("wb") as log:
            process = subprocess.Popen(
                locust_command(config, run_directory),
                cwd=SOURCE_DIRECTORY,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            mutate(
                store,
                lambda value: value["status"].update(
                    {"state": "running", "pid": process.pid}
                ),
            )
            return_code = process.wait()

        heartbeat_stop.set()
        heartbeat_thread.join(timeout=5)
        state = (
            "stopped"
            if stop_requested.is_set()
            else ("completed" if return_code == 0 else "failed")
        )
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
                "run_id": RUN_ID,
                "state": state,
                "ended_at": utc_now(),
                "exit_code": return_code,
                "message": message,
            },
        )
        create_archive(run_directory)
        artifacts = upload_artifacts(store, run_directory)

        def finish(value: dict[str, Any]) -> None:
            requested = bool(value["status"].get("stop_requested"))
            value["status"].update(
                {
                    "state": "stopped" if requested else state,
                    "ended_at": utc_now(),
                    "exit_code": return_code,
                }
            )
            value["status"].pop("lease_until", None)
            if message:
                value["status"]["message"] = message
            value["summary"] = summary
            value["artifacts"] = artifacts

        mutate(store, finish)
        release_lease(store)
        return 0 if state in {"completed", "stopped"} else 1
    except Exception as error:
        try:
            mutate(
                store,
                lambda value: value["status"].update(
                    {
                        "state": "failed",
                        "ended_at": utc_now(),
                        "message": str(error),
                    }
                ),
            )
            release_lease(store)
        except Exception:
            pass
        print(f"benchmark Job failed: {error}", file=sys.stderr, flush=True)
        return 1
    finally:
        heartbeat_stop.set()
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
