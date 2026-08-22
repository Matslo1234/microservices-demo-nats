# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from shared_store import RecordNotFound


APPLICATION_STREAMS = {"BOUTIQUE_COMMANDS", "BOUTIQUE_EVENTS"}
RAPID_PENDING_GROWTH_PER_SECOND = 10.0
COORDINATION_TIMEOUT_SECONDS = 60.0


def consumer_pending_total(metrics: list[dict[str, Any]]) -> float | None:
    """Return one de-duplicated total for application consumer pending."""
    consumers: dict[str, float] = {}
    found = False
    for metric in metrics:
        if metric.get("name") != "jetstream_consumer_num_pending":
            continue
        labels = metric.get("labels", {})
        stream = labels.get("stream_name") or labels.get("stream")
        consumer = labels.get("consumer_name") or labels.get("consumer")
        if stream not in APPLICATION_STREAMS or not consumer:
            continue
        found = True
        key = f"{stream}/{consumer}"
        value = float(metric.get("value", 0))
        consumers[key] = max(value, consumers.get(key, 0.0))
    return sum(consumers.values()) if found else None


def evaluate_rung(
    *,
    application_type: str,
    target_rate: float,
    duration_seconds: float,
    completed: int,
    previous_goodput: float | None,
    pending_start: float | None,
    pending_end: float | None,
    final_rung: bool,
    maximum_rate_reached: bool,
) -> dict[str, Any]:
    duration = max(float(duration_seconds), 0.001)
    goodput = completed / duration
    pending_growth = (
        pending_end - pending_start
        if pending_start is not None and pending_end is not None
        else None
    )
    pending_growth_per_second = (
        pending_growth / duration if pending_growth is not None else None
    )

    saturated = False
    saturation_reason: str | None = None
    if (
        application_type == "NATS"
        and pending_growth_per_second is not None
        and pending_growth_per_second >= RAPID_PENDING_GROWTH_PER_SECOND
    ):
        saturated = True
        saturation_reason = "nats_pending_increasing_rapidly"
    elif previous_goodput is not None and goodput <= previous_goodput:
        saturated = True
        saturation_reason = "goodput_stopped_increasing"

    # Backlog growth and declining goodput are measurements, not control
    # signals. Keep submitting through the configured steady interval, holding
    # at the maximum rate when necessary, and only stop after its final rung.
    stop = final_rung
    stop_reason = "maximum_duration_reached" if final_rung else None

    return {
        "target_requests_per_second": target_rate,
        "duration_seconds": round(duration, 6),
        "completed_during_rung": completed,
        "observed_goodput_orders_per_second": round(goodput, 6),
        "previous_goodput_orders_per_second": (
            round(previous_goodput, 6)
            if previous_goodput is not None
            else None
        ),
        "pending_start": pending_start,
        "pending_end": pending_end,
        "pending_growth": pending_growth,
        "pending_growth_per_second": (
            round(pending_growth_per_second, 6)
            if pending_growth_per_second is not None
            else None
        ),
        "rapid_pending_growth_threshold_per_second": (
            RAPID_PENDING_GROWTH_PER_SECOND
        ),
        "maximum_rate_reached": maximum_rate_reached,
        "stop": stop,
        "saturated": saturated,
        "saturation_reason": saturation_reason,
        "stop_reason": stop_reason,
    }


class _CoordinationBackend:
    def put(self, name: str, value: dict[str, Any]) -> None:
        raise NotImplementedError

    def get(self, name: str) -> dict[str, Any]:
        raise NotImplementedError

    def close(self) -> None:
        pass


class _FileBackend(_CoordinationBackend):
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)

    def _path(self, name: str) -> Path:
        return self.directory / name.replace("/", "-")

    def put(self, name: str, value: dict[str, Any]) -> None:
        destination = self._path(name)
        temporary = destination.with_name(
            destination.name + f".tmp-{os.getpid()}-{uuid.uuid4().hex}"
        )
        with temporary.open("w", encoding="utf-8") as target:
            json.dump(value, target, sort_keys=True)
            target.write("\n")
        os.replace(temporary, destination)

    def get(self, name: str) -> dict[str, Any]:
        path = self._path(name)
        try:
            with path.open(encoding="utf-8") as source:
                value = json.load(source)
        except FileNotFoundError as error:
            raise RecordNotFound(name) from error
        if not isinstance(value, dict):
            raise ValueError(f"coordination value {name} is not an object")
        return value


class _NatsBackend(_CoordinationBackend):
    def __init__(self) -> None:
        # Locust monkey-patches sockets and threads with gevent before this
        # backend is constructed.  The asyncio NATS client cannot reliably
        # complete a TLS connection in that mixed runtime, so keep it in a
        # small, unpatched child process and use a line-oriented JSON protocol.
        bridge = Path(__file__).with_name(
            "saturation_nats_bridge.py"
        ).resolve()
        self.process = subprocess.Popen(
            [sys.executable, "-u", str(bridge)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        self.request_id = 0

    def _request(
        self, operation: str, **arguments: Any
    ) -> dict[str, Any]:
        if self.process.poll() is not None:
            raise RuntimeError(
                "saturation NATS coordination process exited "
                f"with code {self.process.returncode}"
            )
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError(
                "saturation NATS coordination pipes are unavailable"
            )

        self.request_id += 1
        request = {
            "id": self.request_id,
            "operation": operation,
            **arguments,
        }
        try:
            self.process.stdin.write(
                json.dumps(request, separators=(",", ":")) + "\n"
            )
            self.process.stdin.flush()
            line = self.process.stdout.readline()
        except (BrokenPipeError, OSError) as error:
            raise RuntimeError(
                "lost saturation NATS coordination process"
            ) from error
        if not line:
            raise RuntimeError(
                "saturation NATS coordination process closed its output"
            )
        try:
            response = json.loads(line)
        except json.JSONDecodeError as error:
            raise RuntimeError(
                "saturation NATS coordination process returned invalid JSON"
            ) from error
        if not isinstance(response, dict) or response.get("id") != self.request_id:
            raise RuntimeError(
                "saturation NATS coordination process returned an "
                "unexpected response"
            )
        if response.get("ok") is not True:
            if response.get("error_type") == "RecordNotFound":
                raise RecordNotFound(arguments.get("name", ""))
            detail = str(response.get("error", "unknown error"))
            raise RuntimeError(
                f"saturation NATS coordination {operation} failed: {detail}"
            )
        return response

    def put(self, name: str, value: dict[str, Any]) -> None:
        self._request("put", name=name, value=value)

    def get(self, name: str) -> dict[str, Any]:
        value = self._request("get", name=name).get("value")
        if not isinstance(value, dict):
            raise ValueError(f"coordination value {name} is not an object")
        return value

    def close(self) -> None:
        if self.process.poll() is None:
            try:
                self._request("close")
            except Exception:
                self.process.terminate()
        if self.process.stdin is not None:
            self.process.stdin.close()
        if self.process.stdout is not None:
            self.process.stdout.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)


class SaturationCoordinator:
    """Synchronize rung observations and decisions across Locust workers."""

    def __init__(
        self,
        *,
        application_type: str,
        output_directory: Path,
        worker_index: int,
        worker_count: int,
    ) -> None:
        self.application_type = application_type
        self.output_directory = output_directory
        self.worker_index = worker_index
        self.worker_count = worker_count
        self.run_id = os.environ.get("BENCHMARK_RUN_ID", "standalone")
        self.previous_goodput: float | None = None
        self.backend: _CoordinationBackend | None = None
        if worker_count > 1:
            local_directory = os.environ.get(
                "BENCHMARK_SATURATION_COORDINATION_DIR"
            )
            self.backend = (
                _FileBackend(Path(local_directory))
                if local_directory
                else _NatsBackend()
            )

    def _name(self, rung: int, suffix: str) -> str:
        return f"{self.run_id}/saturation/{rung:04d}/{suffix}.json"

    def _wait(self, name: str) -> dict[str, Any]:
        from gevent import sleep

        assert self.backend is not None
        deadline = time.monotonic() + COORDINATION_TIMEOUT_SECONDS
        while True:
            try:
                return self.backend.get(name)
            except RecordNotFound:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"timed out waiting for saturation coordination {name}"
                    )
                sleep(0.05)

    def finish_rung(
        self,
        *,
        rung: int,
        target_rate: float,
        started_elapsed_seconds: float,
        ended_elapsed_seconds: float,
        completed_before: int,
        completed_after: int,
        pending_start: float | None,
        pending_end: float | None,
        final_rung: bool,
        maximum_rate_reached: bool,
    ) -> dict[str, Any]:
        progress = {
            "worker_index": self.worker_index,
            "completed": max(0, completed_after - completed_before),
            "pending_start": pending_start,
            "pending_end": pending_end,
        }
        if self.backend is None:
            progresses = [progress]
        else:
            self.backend.put(
                self._name(rung, f"worker-{self.worker_index:04d}"),
                progress,
            )
            if self.worker_index == 0:
                progresses = [
                    self._wait(self._name(rung, f"worker-{index:04d}"))
                    for index in range(self.worker_count)
                ]
            else:
                return self._wait(self._name(rung, "decision"))

        duration = max(0.001, ended_elapsed_seconds - started_elapsed_seconds)
        decision = evaluate_rung(
            application_type=self.application_type,
            target_rate=target_rate,
            duration_seconds=duration,
            completed=sum(int(item.get("completed", 0)) for item in progresses),
            previous_goodput=self.previous_goodput,
            pending_start=pending_start,
            pending_end=pending_end,
            final_rung=final_rung,
            maximum_rate_reached=maximum_rate_reached,
        )
        decision.update(
            {
                "rung": rung,
                "started_elapsed_seconds": round(
                    started_elapsed_seconds, 6
                ),
                "ended_elapsed_seconds": round(ended_elapsed_seconds, 6),
                "worker_count": self.worker_count,
            }
        )
        self.previous_goodput = float(
            decision["observed_goodput_orders_per_second"]
        )
        if self.backend is not None:
            self.backend.put(self._name(rung, "decision"), decision)
        with (self.output_directory / "saturation.jsonl").open(
            "a", encoding="utf-8"
        ) as target:
            target.write(json.dumps(decision, sort_keys=True) + "\n")
        return decision

    def close(self) -> None:
        if self.backend is not None:
            self.backend.close()
