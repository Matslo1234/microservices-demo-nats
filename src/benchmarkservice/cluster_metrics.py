# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import asyncio
import copy
import json
import math
import os
import re
import ssl
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, wait
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
APPLICATION_STREAMS = {"BOUTIQUE_COMMANDS", "BOUTIQUE_EVENTS"}
NATS_METRICS = {
    "jetstream_consumer_num_pending",
    "jetstream_consumer_num_ack_pending",
    "jetstream_consumer_num_redelivered",
    "jetstream_consumer_num_waiting",
    "jetstream_consumer_ack_floor_consumer_seq",
    "jetstream_consumer_ack_floor_stream_seq",
    "jetstream_consumer_delivered_consumer_seq",
    "jetstream_consumer_delivered_stream_seq",
    "jetstream_stream_consumer_count",
    "jetstream_stream_first_seq",
    "jetstream_stream_last_seq",
    "jetstream_stream_subject_count",
    "jetstream_stream_total_bytes",
    "jetstream_stream_total_messages",
    "gnatsd_varz_in_bytes",
    "gnatsd_varz_in_msgs",
    "gnatsd_varz_out_bytes",
    "gnatsd_varz_out_msgs",
    "gnatsd_varz_cpu",
    "gnatsd_varz_mem",
    "gnatsd_varz_jetstream_config_max_memory",
    "gnatsd_varz_jetstream_config_max_storage",
    "gnatsd_varz_jetstream_config_sync_interval",
    "gnatsd_varz_jetstream_meta_cluster_size",
    "gnatsd_varz_jetstream_meta_leader",
    "gnatsd_varz_jetstream_meta_pending",
    "gnatsd_varz_jetstream_meta_pending_infos",
    "gnatsd_varz_jetstream_meta_pending_requests",
    "gnatsd_varz_jetstream_meta_snapshot_last_duration",
    "gnatsd_varz_jetstream_meta_snapshot_pending_entries",
    "gnatsd_varz_jetstream_meta_snapshot_pending_size",
    "gnatsd_varz_jetstream_stats_api_errors",
    "gnatsd_varz_jetstream_stats_api_total",
    "gnatsd_varz_jetstream_stats_ha_assets",
    "gnatsd_varz_jetstream_stats_memory",
    "gnatsd_varz_jetstream_stats_reserved_memory",
    "gnatsd_varz_jetstream_stats_reserved_storage",
    "gnatsd_varz_jetstream_stats_storage",
    "gnatsd_varz_slow_consumers",
    "gnatsd_varz_stale_connections",
    "gnatsd_varz_stalled_clients",
    "jetstream_account_storage_used",
}
ORDER_COMPLETED_SUBJECT = "boutique.evt.order.completed.v1"
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
        self.request_timeout = float(
            os.environ.get("KUBERNETES_METRICS_REQUEST_TIMEOUT_SECONDS", "2")
        )
        self.snapshot_deadline = float(
            os.environ.get("KUBERNETES_METRICS_SNAPSHOT_DEADLINE_SECONDS", "4")
        )
        workers = int(os.environ.get("KUBERNETES_METRICS_WORKERS", "8"))
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, min(workers, 32)),
            thread_name_prefix="benchmark-kube-metrics",
        )
        self._cached_nodes: list[str] = []
        self.cadvisor_interval = float(
            os.environ.get("CADVISOR_SAMPLE_INTERVAL_SECONDS", "30")
        )
        self.cadvisor_enabled = os.environ.get(
            "COLLECT_CADVISOR_METRICS", "true"
        ).lower() not in {"0", "false", "no"}
        self._last_cadvisor = 0.0
        self._cadvisor_cache: dict[str, dict[str, int | float]] = {}
        self.errors: list[str] = []
        self.diagnostics: dict[str, Any] = {}

    def _request(self, path: str) -> Any:
        request = Request(
            self.base_url + path,
            headers={"Authorization": f"Bearer {self.token}"},
        )
        return urlopen(
            request, timeout=self.request_timeout, context=self.context
        )

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

    def _relevant_nodes(self) -> list[str]:
        value = self._get("/api/v1/pods")
        nodes: set[str] = set()
        for item in value.get("items", []):
            metadata = item.get("metadata", {})
            namespace = str(metadata.get("namespace", ""))
            name = str(metadata.get("name", ""))
            if service_for_pod(
                namespace,
                name,
                self.application_type,
                self.application_namespace,
                self.nats_namespace,
            ) is None:
                continue
            node = str(item.get("spec", {}).get("nodeName", ""))
            if node:
                nodes.add(node)
        discovered = sorted(nodes)
        if discovered:
            self._cached_nodes = discovered
        return discovered or list(self._cached_nodes)

    def _sample_node(
        self, node: str, collect_cadvisor: bool
    ) -> tuple[dict[str, dict[str, Any]], list[str]]:
        pods: dict[str, dict[str, Any]] = {}
        errors: list[str] = []
        summary = self._get(f"/api/v1/nodes/{node}/proxy/stats/summary")
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
        if collect_cadvisor:
            try:
                body = self._get_text(
                    f"/api/v1/nodes/{node}/proxy/metrics/cadvisor"
                )
                add_cadvisor_metrics(pods, body, node)
            except Exception as error:
                errors.append(f"{node} cAdvisor: {error}")
        return pods, errors

    def sample(self) -> dict[str, dict[str, Any]]:
        started = time.monotonic()
        pods: dict[str, dict[str, Any]] = {}
        self.errors = []
        try:
            nodes = self._relevant_nodes()
        except Exception as error:
            nodes = list(self._cached_nodes)
            self.errors.append(f"pod discovery: {error}")
        collect_cadvisor = self.cadvisor_enabled and (
            started - self._last_cadvisor >= self.cadvisor_interval
        )
        if collect_cadvisor:
            self._last_cadvisor = started
        futures = {
            self._executor.submit(self._sample_node, node, collect_cadvisor): node
            for node in nodes
        }
        remaining = max(0.0, self.snapshot_deadline - (time.monotonic() - started))
        completed, unfinished = wait(futures, timeout=remaining)
        for future in completed:
            node = futures[future]
            try:
                node_pods, errors = future.result()
                pods.update(node_pods)
                self.errors.extend(errors)
            except Exception as error:
                self.errors.append(f"{node}: {error}")
        for future in unfinished:
            future.cancel()
            self.errors.append(
                f"{futures[future]}: snapshot deadline exceeded"
            )
        base_fields = {
            "service",
            "node",
            "cpu_usage_core_nanoseconds",
            "memory_working_set_bytes",
            "network_rx_bytes",
            "network_tx_bytes",
        }
        if collect_cadvisor:
            for pod_key, pod in pods.items():
                counters = {
                    key: value
                    for key, value in pod.items()
                    if key not in base_fields
                    and isinstance(value, (int, float))
                    and not isinstance(value, bool)
                }
                if counters:
                    self._cadvisor_cache[pod_key] = counters
        for pod_key, pod in pods.items():
            for key, value in self._cadvisor_cache.get(pod_key, {}).items():
                pod.setdefault(key, value)
        duration = time.monotonic() - started
        self.diagnostics = {
            "duration_seconds": round(duration, 6),
            "nodes_requested": len(nodes),
            "nodes_completed": len(completed),
            "nodes_timed_out": len(unfinished),
            "cadvisor_collected": collect_cadvisor,
            "partial": bool(self.errors),
        }
        return pods

    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)


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


def scrape_prometheus(
    url: str, timeout: float | None = None
) -> list[dict[str, Any]]:
    request = Request(url, headers={"Accept": "text/plain"})
    with urlopen(
        request,
        timeout=(
            timeout
            if timeout is not None
            else float(os.environ.get("NATS_METRICS_REQUEST_TIMEOUT_SECONDS", "2"))
        ),
    ) as response:
        body = response.read().decode("utf-8", errors="replace")
    return parse_prometheus(body, NATS_METRICS, url)


def scrape_nats_raft(
    url: str, timeout: float | None = None
) -> list[dict[str, Any]]:
    """Return compact per-group Raft/WAL observations from one NATS server."""
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(
        request,
        timeout=(
            timeout
            if timeout is not None
            else float(os.environ.get("NATS_RAFT_REQUEST_TIMEOUT_SECONDS", "1"))
        ),
    ) as response:
        value = json.load(response)
    if not isinstance(value, dict):
        raise RuntimeError(f"NATS Raft endpoint {url} returned non-object JSON")

    result: list[dict[str, Any]] = []
    for account, groups in value.items():
        if not isinstance(groups, dict):
            continue
        for group_name, group in groups.items():
            if not isinstance(group, dict):
                continue
            wal = group.get("wal", {})
            if not isinstance(wal, dict):
                wal = {}

            def number(source: dict[str, Any], name: str) -> int:
                try:
                    return int(source.get(name, 0))
                except (TypeError, ValueError):
                    return 0

            result.append(
                {
                    "endpoint": url,
                    "account": str(account),
                    "group": str(group_name),
                    "kind": (
                        "meta"
                        if group_name == "_meta_"
                        else "stream"
                        if str(group_name).startswith("S-")
                        else "consumer"
                        if str(group_name).startswith("C-")
                        else "other"
                    ),
                    "id": str(group.get("id", "")),
                    "state": str(group.get("state", "")),
                    "leader": str(group.get("leader", "")),
                    "size": number(group, "size"),
                    "quorum_needed": number(group, "quorum_needed"),
                    "committed": number(group, "committed"),
                    "applied": number(group, "applied"),
                    "term": number(group, "term"),
                    "pterm": number(group, "pterm"),
                    "pindex": number(group, "pindex"),
                    "proposal_queue": number(group, "ipq_proposal_len"),
                    "entry_queue": number(group, "ipq_entry_len"),
                    "response_queue": number(group, "ipq_resp_len"),
                    "apply_queue": number(group, "ipq_apply_len"),
                    "wal_messages": number(wal, "messages"),
                    "wal_bytes": number(wal, "bytes"),
                    "wal_first_seq": number(wal, "first_seq"),
                    "wal_last_seq": number(wal, "last_seq"),
                }
            )
    return result


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
    """Persistent NATS client for micro stats and completion observations."""

    def __init__(self) -> None:
        self.response_timeout = float(
            os.environ.get("NATS_MICRO_STATS_TIMEOUT_SECONDS", "0.5")
        )
        self.connect_timeout = float(
            os.environ.get("NATS_CONNECT_TIMEOUT", "4s").rstrip("s")
        )
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="benchmark-nats-micro-stats",
            daemon=True,
        )
        self._thread.start()
        self._connection: Any = None
        self._order_completed_observer_id = uuid.uuid4().hex
        self._order_completed_total = 0
        self._order_completed_error: str | None = None

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

        async def completion(_message: Any) -> None:
            self._order_completed_total += 1

        async def disconnected() -> None:
            self._order_completed_observer_id = uuid.uuid4().hex
            self._order_completed_total = 0

        async def observer_error(error: Exception) -> None:
            self._order_completed_error = str(error)

        self._connection = await nats.connect(
            servers=[os.environ["NATS_URL"]],
            user=os.environ.get("NATS_USER") or None,
            password=os.environ.get("NATS_PASSWORD") or None,
            name="benchmarkmetrics/micro-stats",
            tls=tls_context,
            connect_timeout=self.connect_timeout,
            allow_reconnect=True,
            max_reconnect_attempts=-1,
            disconnected_cb=disconnected,
            error_cb=observer_error,
        )
        await self._connection.subscribe(
            ORDER_COMPLETED_SUBJECT,
            cb=completion,
        )
        await self._connection.flush(timeout=self.connect_timeout)
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

    async def _order_completed_sample(self) -> dict[str, Any]:
        connection = await self._ensure_connected()
        await connection.flush(timeout=self.connect_timeout)
        if self._order_completed_error is not None:
            raise RuntimeError(self._order_completed_error)
        return {
            "observer_id": self._order_completed_observer_id,
            "total": self._order_completed_total,
        }

    def order_completed_sample(self) -> dict[str, Any]:
        result: Future[dict[str, Any]] = asyncio.run_coroutine_threadsafe(
            self._order_completed_sample(), self._loop
        )
        return result.result(timeout=self.connect_timeout + 1)

    async def _close(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.drain()

    def close(self) -> None:
        try:
            result: Future[None] = asyncio.run_coroutine_threadsafe(
                self._close(), self._loop
            )
            result.result(timeout=self.connect_timeout + 1)
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)


class ClusterMetricsCollector:
    def __init__(
        self,
        application_type: str | None = None,
        application_namespace: str | None = None,
        nats_namespace: str | None = None,
    ) -> None:
        application_type = (
            application_type
            if application_type is not None
            else os.environ.get("APPLICATION_TYPE", "")
        ).upper()
        if application_type not in {"GRPC", "NATS"}:
            raise RuntimeError("APPLICATION_TYPE must be GRPC or NATS")
        self.application_type = application_type
        self.kubernetes = KubernetesSummaryClient(
            application_type,
            application_namespace
            or os.environ.get("APPLICATION_NAMESPACE")
            or os.environ.get("POD_NAMESPACE", "default"),
            nats_namespace or os.environ.get("NATS_NAMESPACE", "nats"),
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
        self.nats_raft_urls = [
            value.strip()
            for value in os.environ.get(
                "NATS_RAFT_URLS",
                ",".join(
                    "http://nats-"
                    f"{index}.nats-headless.nats.svc.cluster.local:8222/"
                    "raftz?acc=BOUTIQUE"
                    for index in range(3)
                ),
            ).split(",")
            if value.strip()
        ]
        self._application_stream_raft_groups: dict[str, str] = {}
        self._raft_group_lock = threading.Lock()
        self.nats_micro = (
            NatsMicroStatsClient()
            if self.application_type == "NATS" and os.environ.get("NATS_URL")
            else None
        )
        self.snapshot_deadline = float(
            os.environ.get("METRICS_SNAPSHOT_DEADLINE_SECONDS", "5")
        )
        self.nats_deadline = float(
            os.environ.get("NATS_METRICS_SNAPSHOT_DEADLINE_SECONDS", "3")
        )
        self._source_executor = ThreadPoolExecutor(
            max_workers=3, thread_name_prefix="benchmark-metrics-source"
        )
        self._nats_executor = ThreadPoolExecutor(
            max_workers=max(1, len(self.nats_urls) + 2),
            thread_name_prefix="benchmark-nats-exporter",
        )
        self._raft_executor = ThreadPoolExecutor(
            max_workers=max(1, len(self.nats_raft_urls)),
            thread_name_prefix="benchmark-nats-raft",
        )

    def _collect_kubernetes(self) -> dict[str, Any]:
        started_at = time.time()
        started = time.monotonic()
        value: dict[str, Any] = {"pods": {}, "errors": []}
        try:
            value["pods"] = self.kubernetes.sample()
            value["errors"].extend(
                str(error) for error in getattr(self.kubernetes, "errors", [])
            )
        except Exception as error:
            value["errors"].append(str(error))
        value["sample"] = {
            "requested_at": started_at,
            "collected_at": time.time(),
            "duration_seconds": round(time.monotonic() - started, 6),
            "diagnostics": copy.deepcopy(
                getattr(self.kubernetes, "diagnostics", {})
            ),
            "partial": bool(value["errors"]),
        }
        return value

    def _collect_nats(self) -> dict[str, Any]:
        started_at = time.time()
        started = time.monotonic()
        value: dict[str, Any] = {
            "nats_metrics": [],
            "nats_micro_endpoints": [],
            "nats_order_completed_observer": None,
            "errors": [],
        }
        futures: dict[Future[Any], tuple[str, str]] = {
            self._nats_executor.submit(scrape_prometheus, url): (
                "exporter",
                url,
            )
            for url in self.nats_urls
        }
        if self.nats_micro is not None:
            futures[self._nats_executor.submit(self.nats_micro.sample)] = (
                "micro",
                "micro stats",
            )
            futures[
                self._nats_executor.submit(
                    self.nats_micro.order_completed_sample
                )
            ] = ("observer", "completed-order count")
        completed, unfinished = wait(futures, timeout=self.nats_deadline)
        for future in completed:
            kind, endpoint = futures[future]
            try:
                response = future.result()
                if kind == "exporter":
                    value["nats_metrics"].extend(response)
                elif kind == "micro":
                    value["nats_micro_endpoints"] = response
                else:
                    value["nats_order_completed_observer"] = response
            except Exception as error:
                value["errors"].append(f"{endpoint}: {error}")
        for future in unfinished:
            future.cancel()
            value["errors"].append(
                f"{futures[future][1]}: NATS snapshot deadline exceeded"
            )
        raft_groups: dict[str, str] = {}
        for metric in value["nats_metrics"]:
            labels = metric.get("labels", {})
            stream = labels.get("stream_name") or labels.get("stream")
            group = labels.get("stream_raft_group")
            if stream in APPLICATION_STREAMS and group:
                raft_groups[str(group)] = str(stream)
        if raft_groups:
            with self._raft_group_lock:
                self._application_stream_raft_groups.update(raft_groups)
        value["sample"] = {
            "requested_at": started_at,
            "collected_at": time.time(),
            "duration_seconds": round(time.monotonic() - started, 6),
            "requests": len(futures),
            "requests_completed": len(completed),
            "requests_timed_out": len(unfinished),
            "partial": bool(value["errors"]),
        }
        return value

    def _collect_nats_raft(self) -> dict[str, Any]:
        started_at = time.time()
        started = time.monotonic()
        value: dict[str, Any] = {"nats_raft_groups": [], "errors": []}
        futures = {
            self._raft_executor.submit(scrape_nats_raft, url): url
            for url in self.nats_raft_urls
        }
        completed, unfinished = wait(futures, timeout=self.nats_deadline)
        for future in completed:
            endpoint = futures[future]
            try:
                value["nats_raft_groups"].extend(future.result())
            except Exception as error:
                value["errors"].append(f"{endpoint}: {error}")
        for future in unfinished:
            future.cancel()
            value["errors"].append(
                f"{futures[future]}: NATS Raft snapshot deadline exceeded"
            )

        with self._raft_group_lock:
            stream_groups = dict(self._application_stream_raft_groups)
        selected: list[dict[str, Any]] = []
        for group in value["nats_raft_groups"]:
            group_name = str(group.get("group", ""))
            if group.get("kind") not in {"meta", "consumer"} and (
                group_name not in stream_groups
            ):
                continue
            if group_name in stream_groups:
                group["stream_name"] = stream_groups[group_name]
            selected.append(group)
        value["nats_raft_groups"] = selected
        value["sample"] = {
            "requested_at": started_at,
            "collected_at": time.time(),
            "duration_seconds": round(time.monotonic() - started, 6),
            "requests": len(futures),
            "requests_completed": len(completed),
            "requests_timed_out": len(unfinished),
            "groups": len(selected),
            "partial": bool(value["errors"]),
        }
        return value

    def snapshot(self) -> dict[str, Any]:
        requested_at = time.time()
        started = time.monotonic()
        result: dict[str, Any] = {
            "pods": {},
            "nats_metrics": [],
            "nats_micro_endpoints": [],
            "nats_order_completed_observer": None,
            "nats_raft_groups": [],
            "errors": [],
            "source_samples": {},
        }
        futures = {
            self._source_executor.submit(self._collect_kubernetes): "kubernetes"
        }
        if self.application_type == "NATS":
            futures[self._source_executor.submit(self._collect_nats)] = "nats"
            futures[
                self._source_executor.submit(self._collect_nats_raft)
            ] = "nats_raft"
        completed, unfinished = wait(futures, timeout=self.snapshot_deadline)
        for future in completed:
            source = futures[future]
            try:
                value = future.result()
            except Exception as error:
                result["errors"].append(f"{source}: {error}")
                continue
            result["source_samples"][source] = value.pop("sample")
            for error in value.pop("errors", []):
                result["errors"].append(f"{source}: {error}")
            result.update(value)
        for future in unfinished:
            source = futures[future]
            future.cancel()
            result["errors"].append(
                f"{source}: whole-snapshot deadline exceeded"
            )
        result["snapshot"] = {
            "requested_at": requested_at,
            "completed_at": time.time(),
            "duration_seconds": round(time.monotonic() - started, 6),
            "sources_completed": sorted(futures[future] for future in completed),
            "sources_timed_out": sorted(futures[future] for future in unfinished),
        }
        return result

    def nats_order_completed_sample(self) -> dict[str, Any] | None:
        if self.nats_micro is None:
            return None
        return self.nats_micro.order_completed_sample()

    def close(self) -> None:
        self.kubernetes.close()
        self._source_executor.shutdown(wait=False, cancel_futures=True)
        self._nats_executor.shutdown(wait=False, cancel_futures=True)
        self._raft_executor.shutdown(wait=False, cancel_futures=True)
        if self.nats_micro is not None:
            self.nats_micro.close()
