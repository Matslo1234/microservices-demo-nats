# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import hmac
import json
import os
import signal
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from cluster_metrics import ClusterMetricsCollector


class SnapshotCache:
    def __init__(self, collector: ClusterMetricsCollector) -> None:
        self.collector = collector
        self._lock = threading.Lock()
        self._values: dict[str, dict[str, Any]] = {}
        self._stop = threading.Event()
        self._nats_interval = float(
            os.environ.get("NATS_METRICS_CACHE_INTERVAL_SECONDS", "1")
        )
        self._kubernetes_interval = float(
            os.environ.get("KUBERNETES_METRICS_CACHE_INTERVAL_SECONDS", "5")
        )
        sources = [
            (
                "kubernetes",
                self._kubernetes_interval,
                self.collector._collect_kubernetes,
            )
        ]
        if collector.application_type == "NATS":
            sources.append(
                ("nats", self._nats_interval, self.collector._collect_nats)
            )
            sources.append(
                (
                    "nats_raft",
                    self._nats_interval,
                    self.collector._collect_nats_raft,
                )
            )
        self._threads = [
            threading.Thread(
                target=self._refresh_source,
                args=(source, interval, collect),
                name=f"benchmark-{source}-metrics-cache",
                daemon=True,
            )
            for source, interval, collect in sources
        ]
        for thread in self._threads:
            thread.start()

    def _refresh_source(self, source: str, interval: float, collect: Any) -> None:
        next_refresh = time.monotonic()
        while not self._stop.is_set():
            try:
                value = collect()
            except Exception as error:
                value = {
                    "errors": [f"cache refresh: {error}"],
                    "sample": {
                        "requested_at": time.time(),
                        "collected_at": time.time(),
                        "duration_seconds": 0,
                        "partial": True,
                    },
                }
            with self._lock:
                self._values[source] = value
            next_refresh += interval
            now = time.monotonic()
            if next_refresh <= now:
                skipped = int((now - next_refresh) // interval) + 1
                next_refresh += skipped * interval
            self._stop.wait(max(0.0, next_refresh - time.monotonic()))

    def get(self) -> dict[str, Any]:
        with self._lock:
            values = {
                source: dict(value) for source, value in self._values.items()
            }
        result: dict[str, Any] = {
            "pods": {},
            "nats_metrics": [],
            "nats_micro_endpoints": [],
            "nats_order_completed_observer": None,
            "nats_raft_groups": [],
            "errors": [],
            "source_samples": {},
        }
        expected = {"kubernetes"}
        if self.collector.application_type == "NATS":
            expected.update({"nats", "nats_raft"})
        for source in sorted(expected):
            value = values.get(source)
            if value is None:
                result["errors"].append(f"{source}: metrics cache is warming")
                continue
            sample = value.pop("sample", None)
            if isinstance(sample, dict):
                result["source_samples"][source] = sample
            for error in value.pop("errors", []):
                result["errors"].append(f"{source}: {error}")
            result.update(value)
        now = time.time()
        result["snapshot"] = {
            "requested_at": now,
            "completed_at": now,
            "duration_seconds": 0,
            "cached": True,
            "source_ages_seconds": {
                source: round(
                    max(0.0, now - float(sample["collected_at"])), 6
                )
                for source, sample in result["source_samples"].items()
                if isinstance(sample.get("collected_at"), (int, float))
            },
        }
        return result

    def close(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=2)
        self.collector.close()


class MetricsHandler(BaseHTTPRequestHandler):
    cache: SnapshotCache
    token = os.environ.get("BENCHMARK_METRICS_TOKEN", "")
    server_version = "benchmarkmetrics/1.0"

    def log_message(self, format_string: str, *args: Any) -> None:
        print(
            json.dumps(
                {
                    "severity": "INFO",
                    "message": format_string % args,
                    "client": self.client_address[0],
                }
            ),
            flush=True,
        )

    def _json(self, status: HTTPStatus, value: Any) -> None:
        body = json.dumps(value, sort_keys=True).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self) -> bool:
        if not self.token:
            return True
        authorization = self.headers.get("Authorization", "")
        scheme, separator, supplied = authorization.partition(" ")
        return (
            bool(separator)
            and scheme.lower() == "bearer"
            and hmac.compare_digest(supplied, self.token)
        )

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/healthz":
            self._json(HTTPStatus.OK, {"status": "ok"})
            return
        if path not in {"/snapshot", "/nats-order-completed-observer"}:
            self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        if not self._authorized():
            self._json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
            return
        try:
            value = (
                {
                    "nats_order_completed_observer": (
                        self.cache.get().get(
                            "nats_order_completed_observer"
                        )
                    )
                }
                if path == "/nats-order-completed-observer"
                else self.cache.get()
            )
            self._json(HTTPStatus.OK, value)
        except Exception as error:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": str(error)})


def main() -> None:
    MetricsHandler.cache = SnapshotCache(ClusterMetricsCollector())
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), MetricsHandler)

    def request_shutdown(signum: int, frame: Any) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        MetricsHandler.cache.close()


if __name__ == "__main__":
    main()
