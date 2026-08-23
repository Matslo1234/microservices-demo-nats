# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import ssl
import threading
from concurrent.futures import Future
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


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
    "redis-checkout",
    "frontend",
)
PROMETHEUS_LINE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>.*)\})?\s+"
    r"(?P<value>[-+]?(?:[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?|Inf|NaN))"
    r"(?:\s+[0-9]+(?:\.[0-9]+)?)?\s*$"
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
CADVISOR_METRICS = {
    "container_cpu_cfs_periods_total": "cpu_cfs_periods_total",
    "container_cpu_cfs_throttled_periods_total": (
        "cpu_cfs_throttled_periods_total"
    ),
    "container_cpu_cfs_throttled_seconds_total": (
        "cpu_cfs_throttled_seconds_total"
    ),
    "container_fs_reads_total": "disk_reads_completed_total",
    "container_fs_writes_total": "disk_writes_completed_total",
    "container_fs_io_time_seconds_total": "disk_io_time_seconds_total",
    "container_fs_io_time_weighted_seconds_total": (
        "disk_io_time_weighted_seconds_total"
    ),
}
INTEGER_CADVISOR_FIELDS = {
    "cpu_cfs_periods_total",
    "cpu_cfs_throttled_periods_total",
    "disk_reads_completed_total",
    "disk_writes_completed_total",
}


def service_for_pod(
    namespace: str,
    pod_name: str,
    application_type: str,
    application_namespace: str = "default",
    nats_namespace: str = "nats",
) -> str | None:
    if namespace == nats_namespace:
        if re.fullmatch(r"nats-[0-9]+", pod_name):
            return "nats"
        return None
    if namespace != application_namespace or pod_name.startswith(
        "benchmarkservice-"
    ):
        return None
    for service in DEFAULT_SERVICES:
        if pod_name == service or pod_name.startswith(service + "-"):
            if (
                service == "storefrontprojectionservice"
                and application_type == "GRPC"
            ):
                return None
            return service
    return None


class KubernetesSummaryClient:
    def __init__(
        self,
        application_type: str,
        application_namespace: str = "default",
        nats_namespace: str = "nats",
    ) -> None:
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not host:
            raise RuntimeError("Kubernetes service environment is unavailable")
        self.base_url = f"https://{host}:{port}"
        self.application_type = application_type
        self.application_namespace = application_namespace
        self.nats_namespace = nats_namespace
        token_path = Path(
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
        )
        ca_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
        self.token = token_path.read_text(encoding="utf-8").strip()
        self.context = ssl.create_default_context(cafile=str(ca_path))
        self.nodes = self._node_names()
        self.errors: list[str] = []

    def _request(self, path: str) -> Any:
        request = Request(
            self.base_url + path,
            headers={"Authorization": f"Bearer {self.token}"},
        )
        return urlopen(request, timeout=3, context=self.context)

    def _get(self, path: str) -> dict[str, Any]:
        with self._request(path) as response:
            value = json.load(response)
        if not isinstance(value, dict):
            raise RuntimeError(
                f"Kubernetes endpoint {path} returned non-object JSON"
            )
        return value

    def _get_text(self, path: str) -> str:
        with self._request(path) as response:
            return response.read().decode("utf-8", errors="replace")

    def _node_names(self) -> list[str]:
        value = self._get("/api/v1/nodes")
        return [
            str(item.get("metadata", {}).get("name"))
            for item in value.get("items", [])
            if item.get("metadata", {}).get("name")
        ]

    def sample(self) -> dict[str, dict[str, Any]]:
        pods: dict[str, dict[str, Any]] = {}
        self.errors = []
        for node in self.nodes:
            summary = self._get(
                f"/api/v1/nodes/{node}/proxy/stats/summary"
            )
            for pod in summary.get("pods", []):
                reference = pod.get("podRef", {})
                namespace = str(reference.get("namespace", ""))
                name = str(reference.get("name", ""))
                service = service_for_pod(
                    namespace,
                    name,
                    self.application_type,
                    self.application_namespace,
                    self.nats_namespace,
                )
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
            try:
                body = self._get_text(
                    f"/api/v1/nodes/{node}/proxy/metrics/cadvisor"
                )
                add_cadvisor_metrics(pods, body, node)
            except Exception as error:
                self.errors.append(f"{node} cAdvisor: {error}")
        return pods


def parse_labels(raw: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    for match in PROMETHEUS_LABEL.finditer(raw):
        value = match.group("value")
        value = (
            value.replace(r"\\", "\\")
            .replace(r'\"', '"')
            .replace(r"\n", "\n")
        )
        labels[match.group("name")] = value
    return labels


def parse_prometheus(
    body: str, names: set[str], endpoint: str
) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        match = PROMETHEUS_LINE.match(line)
        if match is None or match.group("name") not in names:
            continue
        try:
            number = float(match.group("value"))
        except ValueError:
            continue
        if not math.isfinite(number):
            continue
        metrics.append(
            {
                "name": match.group("name"),
                "labels": parse_labels(match.group("labels") or ""),
                "value": number,
                "endpoint": endpoint,
            }
        )
    return metrics


def scrape_prometheus(url: str) -> list[dict[str, Any]]:
    request = Request(url, headers={"Accept": "text/plain"})
    with urlopen(request, timeout=3) as response:
        body = response.read().decode("utf-8", errors="replace")
    return parse_prometheus(body, NATS_METRICS, url)


def add_cadvisor_metrics(
    pods: dict[str, dict[str, Any]], body: str, node: str
) -> None:
    """Add cumulative throttling and block-I/O counters to pod samples."""
    values: dict[str, dict[str, float]] = {}
    pod_fallbacks: dict[str, dict[str, float]] = {}
    for metric in parse_prometheus(body, set(CADVISOR_METRICS), node):
        labels = metric["labels"]
        namespace = labels.get("namespace", "")
        pod_name = labels.get("pod", "")
        container = labels.get("container", "")
        pod_key = f"{namespace}/{pod_name}"
        if pod_key not in pods or pods[pod_key].get("node") != node:
            continue
        field = CADVISOR_METRICS[metric["name"]]
        if not container:
            if field.startswith("disk_io_time_"):
                fallback = pod_fallbacks.setdefault(pod_key, {})
                fallback[field] = fallback.get(field, 0.0) + float(
                    metric["value"]
                )
            continue
        if container == "POD":
            continue
        pod_values = values.setdefault(pod_key, {})
        pod_values[field] = pod_values.get(field, 0.0) + float(
            metric["value"]
        )

    for pod_key, pod_values in values.items():
        for field, value in pod_fallbacks.get(pod_key, {}).items():
            pod_values.setdefault(field, value)
        for field, value in pod_values.items():
            pods[pod_key][field] = (
                int(value) if field in INTEGER_CADVISOR_FIELDS else value
            )
        reads = int(pod_values.get("disk_reads_completed_total", 0))
        writes = int(pod_values.get("disk_writes_completed_total", 0))
        if reads or writes:
            pods[pod_key]["disk_io_operations_total"] = reads + writes


def normalize_nats_micro_stats(value: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten one NATS micro STATS response into stable endpoint records."""
    service_name = str(value.get("name", ""))
    service_id = str(value.get("id", ""))
    if not service_name or not service_id:
        return []

    result: list[dict[str, Any]] = []
    for endpoint in value.get("endpoints", []):
        if not isinstance(endpoint, dict):
            continue
        data = endpoint.get("data", {})
        if not isinstance(data, dict):
            data = {}
        pending: int | None = None
        for source in (endpoint, data):
            for name in ("pending_requests", "num_pending", "pending"):
                candidate = source.get(name)
                if candidate is not None:
                    try:
                        pending = max(0, int(candidate))
                    except (TypeError, ValueError):
                        continue
                    else:
                        break
            if pending is not None:
                break
        result.append(
            {
                "service_name": service_name,
                "service_id": service_id,
                "version": str(value.get("version", "")),
                "endpoint_name": str(endpoint.get("name", "")),
                "subject": str(endpoint.get("subject", "")),
                "queue_group": str(
                    endpoint.get("queue_group", endpoint.get("queue", ""))
                ),
                "num_requests": int(endpoint.get("num_requests", 0)),
                "num_errors": int(endpoint.get("num_errors", 0)),
                "processing_time_nanoseconds": int(
                    endpoint.get("processing_time", 0)
                ),
                "average_processing_time_nanoseconds": int(
                    endpoint.get("average_processing_time", 0)
                ),
                "pending_requests": pending,
            }
        )
    return result


class NatsMicroStatsClient:
    """Synchronous facade over a persistent NATS client for $SRV.STATS."""

    def __init__(self) -> None:
        self.response_timeout = float(
            os.environ.get("NATS_MICRO_STATS_TIMEOUT_SECONDS", "0.25")
        )
        self.connect_timeout = float(
            os.environ.get("NATS_CONNECT_TIMEOUT", "2s").rstrip("s")
        )
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="benchmark-nats-micro-stats",
            daemon=True,
        )
        self._thread.start()
        self._connection: Any = None

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _ensure_connected(self) -> Any:
        if self._connection is not None and not self._connection.is_closed:
            return self._connection

        import nats

        ca_file = os.environ.get("NATS_CA_FILE")
        tls_context = (
            ssl.create_default_context(cafile=ca_file) if ca_file else None
        )
        if tls_context is not None and hasattr(ssl, "VERIFY_X509_STRICT"):
            tls_context.verify_flags &= ~ssl.VERIFY_X509_STRICT
        self._connection = await nats.connect(
            servers=[os.environ["NATS_URL"]],
            user=os.environ.get("NATS_USER") or None,
            password=os.environ.get("NATS_PASSWORD") or None,
            name="benchmarkmetrics/micro-stats",
            tls=tls_context,
            connect_timeout=self.connect_timeout,
            allow_reconnect=True,
            max_reconnect_attempts=-1,
        )
        return self._connection

    async def _sample(self) -> list[dict[str, Any]]:
        connection = await self._ensure_connected()
        inbox = connection.new_inbox()
        subscription = await connection.subscribe(inbox)
        responses: list[dict[str, Any]] = []
        try:
            await connection.publish("$SRV.STATS", b"", reply=inbox)
            await connection.flush(timeout=self.connect_timeout)
            deadline = self._loop.time() + self.response_timeout
            while True:
                remaining = deadline - self._loop.time()
                if remaining <= 0:
                    break
                try:
                    message = await asyncio.wait_for(
                        subscription.next_msg(), remaining
                    )
                except TimeoutError:
                    break
                try:
                    value = json.loads(message.data)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if isinstance(value, dict):
                    responses.extend(normalize_nats_micro_stats(value))
        finally:
            await subscription.unsubscribe()
        return sorted(
            responses,
            key=lambda item: (
                item["service_name"],
                item["service_id"],
                item["endpoint_name"],
            ),
        )

    def sample(self) -> list[dict[str, Any]]:
        result: Future[list[dict[str, Any]]] = asyncio.run_coroutine_threadsafe(
            self._sample(), self._loop
        )
        return result.result(
            timeout=self.connect_timeout + self.response_timeout + 1
        )


class ClusterMetricsCollector:
    def __init__(self) -> None:
        application_type = os.environ.get("APPLICATION_TYPE", "").upper()
        if application_type not in {"GRPC", "NATS"}:
            raise RuntimeError("APPLICATION_TYPE must be GRPC or NATS")
        self.application_type = application_type
        self.kubernetes = KubernetesSummaryClient(
            application_type,
            os.environ.get("APPLICATION_NAMESPACE", "default"),
            os.environ.get("NATS_NAMESPACE", "nats"),
        )
        self.nats_urls = [
            value.strip()
            for value in os.environ.get(
                "NATS_METRICS_URLS",
                ",".join(
                    "http://nats-"
                    f"{index}.nats-headless.nats.svc.cluster.local:7777/metrics"
                    for index in range(3)
                ),
            ).split(",")
            if value.strip()
        ]
        self.nats_micro = (
            NatsMicroStatsClient()
            if self.application_type == "NATS" and os.environ.get("NATS_URL")
            else None
        )

    def snapshot(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "pods": {},
            "nats_metrics": [],
            "nats_micro_endpoints": [],
            "errors": [],
        }
        try:
            result["pods"] = self.kubernetes.sample()
            result["errors"].extend(
                f"kubernetes: {error}"
                for error in getattr(self.kubernetes, "errors", [])
            )
        except Exception as error:
            result["errors"].append(f"kubernetes: {error}")
        if self.application_type == "NATS":
            for url in self.nats_urls:
                try:
                    result["nats_metrics"].extend(scrape_prometheus(url))
                except Exception as error:
                    result["errors"].append(f"{url}: {error}")
            if self.nats_micro is not None:
                try:
                    result["nats_micro_endpoints"] = self.nats_micro.sample()
                except Exception as error:
                    result["errors"].append(f"nats micro stats: {error}")
        return result
