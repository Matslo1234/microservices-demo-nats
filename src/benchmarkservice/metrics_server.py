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
        self._value: dict[str, Any] | None = None
        self._created = 0.0

    def get(self) -> dict[str, Any]:
        with self._lock:
            now = time.monotonic()
            if self._value is None or now - self._created >= 1.0:
                self._value = self.collector.snapshot()
                self._created = now
            return self._value


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
                        self.cache.collector.nats_order_completed_sample()
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


if __name__ == "__main__":
    main()
