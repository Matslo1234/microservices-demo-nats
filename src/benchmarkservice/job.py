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
from parallel import (
    archive_directory,
    extract_archive,
    merge_worker_outputs,
    ready_object,
    result_object,
    start_object,
)
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
COORDINATION_TIMEOUT_SECONDS = 240
START_DELAY_SECONDS = 5


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
        if current.value.get("released"):
            raise RunConflict("benchmark lease was already released")
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


def worker_identity(config: BenchmarkConfig) -> tuple[int, int]:
    worker_count = int(
        os.environ.get("BENCHMARK_WORKER_COUNT", "1") or "1"
    )
    raw_index = os.environ.get("BENCHMARK_WORKER_INDEX", "")
    worker_index = int(raw_index) if raw_index else 0
    if worker_count != config.worker_count:
        raise RuntimeError(
            "Kubernetes worker count does not match benchmark configuration "
            f"({worker_count} != {config.worker_count})"
        )
    if worker_index < 0 or worker_index >= worker_count:
        raise RuntimeError(
            f"invalid worker index {worker_index} for {worker_count} workers"
        )
    return worker_index, worker_count


def wait_for_object(
    store: NatsSharedStore,
    name: str,
    stop_requested: threading.Event,
    deadline: float,
) -> bytes:
    while True:
        if stop_requested.is_set():
            raise InterruptedError("benchmark stop was requested")
        try:
            return store.get_object(name)
        except RecordNotFound:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for {name}")
            stop_requested.wait(0.5)


def coordinate_start(
    store: NatsSharedStore,
    worker_index: int,
    worker_count: int,
    stop_requested: threading.Event,
) -> float:
    store.put_object(
        ready_object(RUN_ID, worker_index),
        json.dumps(
            {
                "worker_index": worker_index,
                "pod_name": os.environ.get("POD_NAME"),
                "ready_at": utc_now(),
            },
            sort_keys=True,
        ).encode(),
    )
    deadline = time.monotonic() + COORDINATION_TIMEOUT_SECONDS
    if worker_index == 0:
        for index in range(worker_count):
            wait_for_object(
                store,
                ready_object(RUN_ID, index),
                stop_requested,
                deadline,
            )
        started_at = time.time() + START_DELAY_SECONDS
        store.put_object(
            start_object(RUN_ID),
            json.dumps({"start_epoch": started_at}, sort_keys=True).encode(),
        )
        return started_at

    data = wait_for_object(
        store, start_object(RUN_ID), stop_requested, deadline
    )
    value = json.loads(data)
    return float(value["start_epoch"])


def delete_coordination_objects(
    store: NatsSharedStore, worker_count: int
) -> None:
    names = [
        start_object(RUN_ID),
        *(
            name
            for index in range(worker_count)
            for name in (
                ready_object(RUN_ID, index),
                result_object(RUN_ID, index),
            )
        ),
    ]
    for name in names:
        try:
            store.delete_object(name)
        except Exception:
            # The merged run artifacts are already durable. Temporary worker
            # cleanup must not change an otherwise completed benchmark.
            continue


def create_archive(run_directory: Path) -> Path:
    archive = run_directory / f"{RUN_ID}.zip"
    with zipfile.ZipFile(
        archive, "w", compression=zipfile.ZIP_DEFLATED
    ) as target:
        for artifact in sorted(run_directory.rglob("*")):
            if artifact.is_file() and artifact != archive:
                target.write(artifact, artifact.relative_to(run_directory))
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
    heartbeat_thread: threading.Thread | None = None
    worker_directory: Path | None = None
    worker_config: BenchmarkConfig | None = None
    worker_count = int(
        os.environ.get("BENCHMARK_WORKER_COUNT", "1") or "1"
    )
    worker_index = int(
        os.environ.get("BENCHMARK_WORKER_INDEX", "") or "0"
    )
    coordinator = worker_index == 0

    def request_stop(signum: int, frame: Any) -> None:
        stop_requested.set()
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGINT)

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    try:
        record = store.get(RUN_PREFIX + RUN_ID).value
        config = BenchmarkConfig.from_dict(record["config"])
        worker_index, worker_count = worker_identity(config)
        coordinator = worker_index == 0
        worker_config = config.for_worker(worker_index)
        worker_directory = (
            run_directory
            if worker_count == 1
            else run_directory / f"worker-{worker_index:04d}"
        )
        worker_directory.mkdir(
            parents=True, mode=0o750, exist_ok=worker_count == 1
        )
        write_json(run_directory / "config.json", config.as_dict())
        worker_config_path = worker_directory / "worker-config.json"
        write_json(worker_config_path, worker_config.as_dict())
        if coordinator:
            lease_until = renew_lease(store)
            mutate(
                store,
                lambda value: value["status"].update(
                    {
                        "state": "starting",
                        "started_at": utc_now(),
                        "pod_name": os.environ.get("POD_NAME"),
                        "worker_count": worker_count,
                        "lease_until": lease_until,
                    }
                ),
            )

        def heartbeat() -> None:
            while not heartbeat_stop.wait(HEARTBEAT_SECONDS):
                try:
                    current_lease = renew_lease(store)
                    if heartbeat_stop.is_set():
                        return
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

        if coordinator:
            heartbeat_thread = threading.Thread(
                target=heartbeat, name="benchmark-lease", daemon=True
            )
            heartbeat_thread.start()

        start_epoch = (
            coordinate_start(
                store, worker_index, worker_count, stop_requested
            )
            if worker_count > 1
            else time.time()
        )

        environment = os.environ.copy()
        environment["BENCHMARK_CONFIG_FILE"] = str(worker_config_path)
        environment["BENCHMARK_OUTPUT_DIR"] = str(worker_directory)
        environment["BENCHMARK_START_EPOCH"] = str(start_epoch)
        with (worker_directory / "runner.log").open("wb") as log:
            process = subprocess.Popen(
                locust_command(worker_config, worker_directory),
                cwd=SOURCE_DIRECTORY,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            if coordinator:
                mutate(
                    store,
                    lambda value: value["status"].update(
                        {"state": "running", "pid": process.pid}
                    ),
                )
            return_code = process.wait()

        worker_state = (
            "stopped"
            if stop_requested.is_set()
            else ("completed" if return_code == 0 else "failed")
        )
        write_json(
            worker_directory / "worker-status.json",
            {
                "worker_index": worker_index,
                "worker_count": worker_count,
                "pod_name": os.environ.get("POD_NAME"),
                "state": worker_state,
                "ended_at": utc_now(),
                "exit_code": return_code,
                "config": worker_config.as_dict(),
            },
        )
        if worker_count > 1:
            store.put_object(
                result_object(RUN_ID, worker_index),
                archive_directory(worker_directory),
            )
        if not coordinator:
            # The coordinator owns run finalization. Returning success keeps a
            # recorded Locust failure from aborting the Indexed Job before the
            # coordinator can merge and publish every worker's diagnostics.
            return 0

        statuses = [
            {
                "worker_index": 0,
                "state": worker_state,
                "exit_code": return_code,
            }
        ]
        if worker_count > 1:
            deadline = time.monotonic() + COORDINATION_TIMEOUT_SECONDS
            worker_directories: list[Path] = []
            for index in range(worker_count):
                directory = run_directory / f"worker-{index:04d}"
                if index != worker_index:
                    data = wait_for_object(
                        store,
                        result_object(RUN_ID, index),
                        stop_requested,
                        deadline,
                    )
                    extract_archive(data, directory)
                worker_directories.append(directory)
            statuses = merge_worker_outputs(
                run_directory, worker_directories
            )
            if len(statuses) != worker_count:
                raise RuntimeError(
                    f"received {len(statuses)} worker statuses for "
                    f"{worker_count} workers"
                )

        failed_workers = [
            status
            for status in statuses
            if status.get("state") not in {"completed", "stopped"}
        ]
        stopped_workers = [
            status
            for status in statuses
            if status.get("state") == "stopped"
        ]
        state = (
            "stopped"
            if stop_requested.is_set() or stopped_workers
            else ("failed" if failed_workers else "completed")
        )
        exit_code = next(
            (
                int(status.get("exit_code", 1))
                for status in failed_workers
            ),
            0,
        )
        message = None
        if failed_workers:
            details = ", ".join(
                f"{int(status.get('worker_index', -1)):04d} "
                f"(exit {status.get('exit_code')})"
                for status in failed_workers
            )
            message = f"benchmark workers failed: {details}"
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
                "exit_code": exit_code,
                "worker_count": worker_count,
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
                    "exit_code": exit_code,
                }
            )
            value["status"].pop("lease_until", None)
            if message:
                value["status"]["message"] = message
            value["summary"] = summary
            value["artifacts"] = artifacts

        heartbeat_stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=5)
        mutate(store, finish)
        release_lease(store)
        if worker_count > 1:
            delete_coordination_objects(store, worker_count)
        return 0 if state in {"completed", "stopped"} else 1
    except Exception as error:
        if not coordinator and worker_directory is not None:
            try:
                write_json(
                    worker_directory / "worker-status.json",
                    {
                        "worker_index": worker_index,
                        "worker_count": worker_count,
                        "pod_name": os.environ.get("POD_NAME"),
                        "state": "failed",
                        "ended_at": utc_now(),
                        "exit_code": 1,
                        "message": str(error),
                        "config": worker_config.as_dict()
                        if worker_config is not None
                        else None,
                    },
                )
                store.put_object(
                    result_object(RUN_ID, worker_index),
                    archive_directory(worker_directory),
                )
                print(
                    f"benchmark worker failed: {error}",
                    file=sys.stderr,
                    flush=True,
                )
                return 0
            except Exception:
                pass
        if coordinator:
            heartbeat_stop.set()
            if heartbeat_thread is not None:
                heartbeat_thread.join(timeout=5)
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
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=5)
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
