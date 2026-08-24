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

from cluster_metrics import ClusterMetricsCollector
from config import BenchmarkConfig, LOCAL_CLUSTER
from saturation import consumer_pending_total


CONFIG_FILE = Path(os.environ["BENCHMARK_CONFIG_FILE"])
OUTPUT_DIRECTORY = Path(os.environ["BENCHMARK_OUTPUT_DIR"])
with CONFIG_FILE.open(encoding="utf-8") as source:
    CONFIG = BenchmarkConfig.from_worker_dict(json.load(source))

TEST_STARTED_MONOTONIC = 0.0
TEST_STARTED_EPOCH = 0.0
EARLY_DRAIN_DEADLINE: float | None = None
ACTIVE_SATURATION_RUNG: int | None = None
ACTIVE_SATURATION_RATE: float | None = None


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
    if (
        EARLY_DRAIN_DEADLINE is not None
        and time.monotonic() >= EARLY_DRAIN_DEADLINE - CONFIG.drain_seconds
    ):
        return "drain"
    if ACTIVE_SATURATION_RUNG is not None:
        return "steady"
    return phase_for_elapsed(elapsed_now())


def drain_deadline() -> float:
    configured = TEST_STARTED_MONOTONIC + CONFIG.run_seconds
    return min(configured, EARLY_DRAIN_DEADLINE or configured)


def begin_early_drain() -> float:
    global EARLY_DRAIN_DEADLINE
    global ACTIVE_SATURATION_RATE, ACTIVE_SATURATION_RUNG
    EARLY_DRAIN_DEADLINE = time.monotonic() + CONFIG.drain_seconds
    ACTIVE_SATURATION_RUNG = None
    ACTIVE_SATURATION_RATE = None
    return EARLY_DRAIN_DEADLINE


def set_saturation_rung(rung: int, target_rate: float) -> None:
    global ACTIVE_SATURATION_RATE, ACTIVE_SATURATION_RUNG
    ACTIVE_SATURATION_RUNG = rung
    ACTIVE_SATURATION_RATE = target_rate


def submission_deadline() -> float:
    return TEST_STARTED_MONOTONIC + CONFIG.submission_seconds


class ArtifactRecorder:
    def __init__(self) -> None:
        self._business: Any = None
        self._outstanding: Any = None
        self._lock = threading.Lock()
        self._outstanding_count = 0
        self._completed_count = 0

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
        recorded_at = time.time()
        phase = safe_context.get(
            "phase", phase_for_elapsed(max(0.0, timestamp - TEST_STARTED_EPOCH))
        )
        record = {
            "timestamp": timestamp,
            "recorded_at": recorded_at,
            "recorded_elapsed_seconds": round(
                max(0.0, recorded_at - TEST_STARTED_EPOCH), 6
            ),
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
            if (
                name == "checkout_to_outcome"
                and safe_context.get("outcome") == "COMPLETED"
            ):
                self._completed_count += 1

    def completed_count(self) -> int:
        with self._lock:
            return self._completed_count

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
        nats_micro_endpoints = value.get("nats_micro_endpoints", [])
        errors = value.get("errors", [])
        if not isinstance(pods, dict):
            raise RuntimeError("metrics endpoint returned invalid pods")
        if (
            not isinstance(nats_metrics, list)
            or not isinstance(nats_micro_endpoints, list)
            or not isinstance(errors, list)
        ):
            raise RuntimeError("metrics endpoint returned invalid metric lists")
        return {
            "pods": pods,
            "nats_metrics": nats_metrics,
            "nats_micro_endpoints": nats_micro_endpoints,
            "errors": [str(error) for error in errors],
        }


class LocalMetricsClient:
    def __init__(self, application_type: str, namespace: str) -> None:
        self.application_type = application_type
        self.namespace = namespace
        self.collector: ClusterMetricsCollector | None = None

    def sample(self) -> dict[str, Any]:
        if self.collector is None:
            self.collector = ClusterMetricsCollector(
                self.application_type,
                application_namespace=self.namespace,
            )
        return self.collector.snapshot()


class ResourceSampler:
    def __init__(self) -> None:
        self._stop = Event()
        self._greenlet: gevent.Greenlet | None = None
        self._target: Any = None
        self._latest_pending: float | None = None

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
        if CONFIG.metrics_url == LOCAL_CLUSTER:
            metrics = LocalMetricsClient(
                CONFIG.application_type,
                os.environ.get("POD_NAMESPACE", "default"),
            )
        else:
            metrics = RemoteMetricsClient(CONFIG.metrics_url)

        while True:
            record: dict[str, Any] = {
                "timestamp": time.time(),
                "elapsed_seconds": round(elapsed_now(), 6),
                "phase": phase_now(),
                "pods": {},
                "nats_metrics": [],
                "nats_micro_endpoints": [],
                "errors": [],
            }
            if CONFIG.workload == "saturation":
                record.update(
                    {
                        "saturation_rung": ACTIVE_SATURATION_RUNG,
                        "target_requests_per_second": (
                            ACTIVE_SATURATION_RATE
                        ),
                    }
                )
            try:
                snapshot = metrics.sample()
                if CONFIG.collect_resources:
                    record["pods"] = snapshot["pods"]
                if CONFIG.collect_nats_metrics:
                    record["nats_metrics"] = snapshot["nats_metrics"]
                    record["nats_micro_endpoints"] = snapshot[
                        "nats_micro_endpoints"
                    ]
                    self._latest_pending = consumer_pending_total(
                        snapshot["nats_metrics"]
                    )
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

    def latest_pending(self) -> float | None:
        return self._latest_pending


RESOURCE_SAMPLER = ResourceSampler()
