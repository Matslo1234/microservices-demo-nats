# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import gevent
from gevent.event import Event

from config import BenchmarkConfig


CONFIG_FILE = Path(os.environ["BENCHMARK_CONFIG_FILE"])
OUTPUT_DIRECTORY = Path(os.environ["BENCHMARK_OUTPUT_DIR"])
with CONFIG_FILE.open(encoding="utf-8") as source:
    CONFIG = BenchmarkConfig.from_worker_dict(json.load(source))

TEST_STARTED_MONOTONIC = 0.0
TEST_STARTED_EPOCH = 0.0


def start_clock() -> None:
    global TEST_STARTED_MONOTONIC, TEST_STARTED_EPOCH
    synchronized_start = float(
        os.environ.get("BENCHMARK_START_EPOCH", "0") or 0
    )
    delay = synchronized_start - time.time()
    if delay > 0:
        gevent.sleep(delay)
    TEST_STARTED_MONOTONIC = time.monotonic()
    TEST_STARTED_EPOCH = time.time()


def elapsed_now() -> float:
    if not TEST_STARTED_MONOTONIC:
        return 0.0
    return time.monotonic() - TEST_STARTED_MONOTONIC


def phase_for_elapsed(elapsed: float) -> str:
    if elapsed < CONFIG.warmup_seconds:
        return "warmup"
    if elapsed < CONFIG.submission_seconds:
        return "steady"
    if elapsed < CONFIG.run_seconds:
        return "drain"
    return "after"


def phase_now() -> str:
    return phase_for_elapsed(elapsed_now())


def drain_deadline() -> float:
    return TEST_STARTED_MONOTONIC + CONFIG.run_seconds


def submission_deadline() -> float:
    return TEST_STARTED_MONOTONIC + CONFIG.submission_seconds


class ArtifactRecorder:
    def __init__(self) -> None:
        self._business: Any = None
        self._outstanding: Any = None
        self._lock = threading.Lock()
        self._outstanding_count = 0

    def open(self) -> None:
        OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)
        self._business = (OUTPUT_DIRECTORY / "business.jsonl").open(
            "a", encoding="utf-8"
        )
        self._outstanding = (OUTPUT_DIRECTORY / "outstanding.jsonl").open(
            "a", encoding="utf-8"
        )

    def close(self) -> None:
        with self._lock:
            for target in (self._business, self._outstanding):
                if target is not None:
                    target.flush()
                    target.close()
            self._business = None
            self._outstanding = None

    def request(
        self,
        request_type: str,
        name: str,
        response_time: float,
        exception: Exception | None,
        start_time: float | None,
        context: dict[str, Any] | None,
    ) -> None:
        if request_type != "BUSINESS" or self._business is None:
            return
        safe_context = {
            str(key): value
            for key, value in (context or {}).items()
            if not str(key).startswith("_")
        }
        timestamp = start_time or time.time()
        phase = safe_context.get(
            "phase", phase_for_elapsed(max(0.0, timestamp - TEST_STARTED_EPOCH))
        )
        record = {
            "timestamp": timestamp,
            "phase": phase,
            "request_type": request_type,
            "name": name,
            "response_time_ms": round(float(response_time), 3),
            "success": exception is None,
            "error": str(exception) if exception else None,
            "context": safe_context,
        }
        with self._lock:
            self._business.write(json.dumps(record, sort_keys=True) + "\n")

    def accepted(self, transaction_id: str, phase: str) -> None:
        with self._lock:
            self._outstanding_count += 1
            self._write_outstanding("accepted", transaction_id, phase)

    def terminal(self, transaction_id: str, outcome: str, phase: str) -> None:
        with self._lock:
            self._outstanding_count = max(0, self._outstanding_count - 1)
            self._write_outstanding(outcome.lower(), transaction_id, phase)

    def _write_outstanding(
        self, event: str, transaction_id: str, phase: str
    ) -> None:
        if self._outstanding is None:
            return
        self._outstanding.write(
            json.dumps(
                {
                    "timestamp": time.time(),
                    "elapsed_seconds": round(elapsed_now(), 6),
                    "phase": phase,
                    "event": event,
                    "transaction_id": transaction_id,
                    "outstanding": self._outstanding_count,
                },
                sort_keys=True,
            )
            + "\n"
        )


RECORDER = ArtifactRecorder()


class RemoteMetricsClient:
    def __init__(self, url: str) -> None:
        self.url = url
        self.token = os.environ.get("BENCHMARK_METRICS_TOKEN", "")

    def sample(self) -> dict[str, Any]:
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = Request(self.url, headers=headers)
        with urlopen(request, timeout=5) as response:
            value = json.load(response)
        if not isinstance(value, dict):
            raise RuntimeError("metrics endpoint returned non-object JSON")
        pods = value.get("pods", {})
        nats_metrics = value.get("nats_metrics", [])
        errors = value.get("errors", [])
        if not isinstance(pods, dict):
            raise RuntimeError("metrics endpoint returned invalid pods")
        if not isinstance(nats_metrics, list) or not isinstance(errors, list):
            raise RuntimeError("metrics endpoint returned invalid metric lists")
        return {
            "pods": pods,
            "nats_metrics": nats_metrics,
            "errors": [str(error) for error in errors],
        }


class ResourceSampler:
    def __init__(self) -> None:
        self._stop = Event()
        self._greenlet: gevent.Greenlet | None = None
        self._target: Any = None

    def start(self) -> None:
        if not (CONFIG.collect_resources or CONFIG.collect_nats_metrics):
            return
        self._target = (OUTPUT_DIRECTORY / "resources.jsonl").open(
            "a", encoding="utf-8"
        )
        self._greenlet = gevent.spawn(self._run)

    def stop(self) -> None:
        if self._greenlet is None:
            return
        self._stop.set()
        self._greenlet.join(timeout=5)
        if not self._greenlet.ready():
            self._greenlet.kill(block=True)
        if self._target is not None:
            self._target.flush()
            self._target.close()
        self._target = None
        self._greenlet = None

    def _run(self) -> None:
        assert CONFIG.metrics_url is not None
        metrics = RemoteMetricsClient(CONFIG.metrics_url)

        while True:
            record: dict[str, Any] = {
                "timestamp": time.time(),
                "elapsed_seconds": round(elapsed_now(), 6),
                "phase": phase_now(),
                "pods": {},
                "nats_metrics": [],
                "errors": [],
            }
            try:
                snapshot = metrics.sample()
                if CONFIG.collect_resources:
                    record["pods"] = snapshot["pods"]
                if CONFIG.collect_nats_metrics:
                    record["nats_metrics"] = snapshot["nats_metrics"]
                record["errors"].extend(snapshot["errors"])
            except Exception as exc:
                record["errors"].append(f"metrics endpoint: {exc}")
            if self._target is not None:
                self._target.write(json.dumps(record, sort_keys=True) + "\n")
                self._target.flush()
            if self._stop.wait(
                timeout=CONFIG.resource_sample_interval_seconds
            ):
                break


RESOURCE_SAMPLER = ResourceSampler()
