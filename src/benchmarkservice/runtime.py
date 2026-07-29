# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import json
import os
import re
import ssl
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


DEFAULT_SERVICES = (
    "storefrontprojectionservice",
    "productcatalogservice",
    "recommendationservice",
    "checkoutservice",
    "currencyservice",
    "shippingservice",
    "paymentservice",
    "emailservice",
    "cartservice",
    "adservice",
    "redis-cart",
    "frontend",
)


def service_for_pod(namespace: str, pod_name: str) -> str | None:
    if namespace == "nats":
        if re.fullmatch(r"nats-[0-9]+", pod_name):
            return "nats"
        return None
    if namespace != "default" or pod_name.startswith("benchmarkservice-"):
        return None
    for service in DEFAULT_SERVICES:
        if pod_name == service or pod_name.startswith(service + "-"):
            if (
                service == "storefrontprojectionservice"
                and CONFIG.application_type == "GRPC"
            ):
                return None
            return service
    return None


class KubernetesSummaryClient:
    def __init__(self) -> None:
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not host:
            raise RuntimeError("Kubernetes service environment is unavailable")
        self.base_url = f"https://{host}:{port}"
        token_path = Path(
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
        )
        ca_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
        self.token = token_path.read_text(encoding="utf-8").strip()
        self.context = ssl.create_default_context(cafile=str(ca_path))
        self.nodes = self._node_names()

    def _get(self, path: str) -> dict[str, Any]:
        request = Request(
            self.base_url + path,
            headers={"Authorization": f"Bearer {self.token}"},
        )
        with urlopen(request, timeout=3, context=self.context) as response:
            value = json.load(response)
        if not isinstance(value, dict):
            raise RuntimeError(f"Kubernetes endpoint {path} returned non-object JSON")
        return value

    def _node_names(self) -> list[str]:
        value = self._get("/api/v1/nodes")
        return [
            str(item.get("metadata", {}).get("name"))
            for item in value.get("items", [])
            if item.get("metadata", {}).get("name")
        ]

    def sample(self) -> dict[str, dict[str, Any]]:
        pods: dict[str, dict[str, Any]] = {}
        for node in self.nodes:
            summary = self._get(f"/api/v1/nodes/{node}/proxy/stats/summary")
            for pod in summary.get("pods", []):
                reference = pod.get("podRef", {})
                namespace = str(reference.get("namespace", ""))
                name = str(reference.get("name", ""))
                service = service_for_pod(namespace, name)
                if service is None:
                    continue
                network = pod.get("network", {})
                pods[f"{namespace}/{name}"] = {
                    "service": service,
                    "node": node,
                    "cpu_usage_core_nanoseconds": int(
                        pod.get("cpu", {}).get("usageCoreNanoSeconds", 0)
                    ),
                    "memory_working_set_bytes": int(
                        pod.get("memory", {}).get("workingSetBytes", 0)
                    ),
                    "network_rx_bytes": int(network.get("rxBytes", 0)),
                    "network_tx_bytes": int(network.get("txBytes", 0)),
                }
        return pods


PROMETHEUS_LINE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>.*)\})?\s+"
    r"(?P<value>[-+]?(?:[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?|Inf|NaN))$"
)
PROMETHEUS_LABEL = re.compile(
    r'(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)="(?P<value>(?:\\.|[^"])*)"(?:,|$)'
)
NATS_METRICS = {
    "jetstream_consumer_num_pending",
    "jetstream_consumer_num_ack_pending",
    "jetstream_consumer_num_redelivered",
    "gnatsd_varz_jetstream_stats_storage",
}


def parse_labels(raw: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    for match in PROMETHEUS_LABEL.finditer(raw):
        value = match.group("value")
        value = (
            value.replace(r"\\", "\\")
            .replace(r"\"", '"')
            .replace(r"\n", "\n")
        )
        labels[match.group("name")] = value
    return labels


def scrape_prometheus(url: str) -> list[dict[str, Any]]:
    request = Request(url, headers={"Accept": "text/plain"})
    with urlopen(request, timeout=3) as response:
        body = response.read().decode("utf-8", errors="replace")
    metrics: list[dict[str, Any]] = []
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        match = PROMETHEUS_LINE.match(line)
        if match is None or match.group("name") not in NATS_METRICS:
            continue
        value = match.group("value")
        try:
            number = float(value)
        except ValueError:
            continue
        metrics.append(
            {
                "name": match.group("name"),
                "labels": parse_labels(match.group("labels") or ""),
                "value": number,
                "endpoint": url,
            }
        )
    return metrics


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
        kubernetes: KubernetesSummaryClient | None = None
        kubernetes_error: str | None = None
        if CONFIG.collect_resources:
            try:
                kubernetes = KubernetesSummaryClient()
            except Exception as exc:
                kubernetes_error = str(exc)
        urls = [
            value.strip()
            for value in os.environ.get(
                "NATS_METRICS_URLS",
                ",".join(
                    f"http://nats-{index}.nats-headless.nats.svc.cluster.local:7777/metrics"
                    for index in range(3)
                ),
            ).split(",")
            if value.strip()
        ]

        while True:
            record: dict[str, Any] = {
                "timestamp": time.time(),
                "elapsed_seconds": round(elapsed_now(), 6),
                "phase": phase_now(),
                "pods": {},
                "nats_metrics": [],
                "errors": [],
            }
            if kubernetes is not None:
                try:
                    record["pods"] = kubernetes.sample()
                except Exception as exc:
                    record["errors"].append(f"kubernetes: {exc}")
            elif kubernetes_error:
                record["errors"].append(f"kubernetes: {kubernetes_error}")

            if CONFIG.collect_nats_metrics:
                for url in urls:
                    try:
                        record["nats_metrics"].extend(scrape_prometheus(url))
                    except Exception as exc:
                        record["errors"].append(f"{url}: {exc}")
            if self._target is not None:
                self._target.write(json.dumps(record, sort_keys=True) + "\n")
                self._target.flush()
            if self._stop.wait(
                timeout=CONFIG.resource_sample_interval_seconds
            ):
                break


RESOURCE_SAMPLER = ResourceSampler()
