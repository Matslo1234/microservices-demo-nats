# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import json
import os
import re
import ssl
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
    "frontend",
)
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

    def _get(self, path: str) -> dict[str, Any]:
        request = Request(
            self.base_url + path,
            headers={"Authorization": f"Bearer {self.token}"},
        )
        with urlopen(request, timeout=3, context=self.context) as response:
            value = json.load(response)
        if not isinstance(value, dict):
            raise RuntimeError(
                f"Kubernetes endpoint {path} returned non-object JSON"
            )
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
        try:
            number = float(match.group("value"))
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

    def snapshot(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "pods": {},
            "nats_metrics": [],
            "errors": [],
        }
        try:
            result["pods"] = self.kubernetes.sample()
        except Exception as error:
            result["errors"].append(f"kubernetes: {error}")
        if self.application_type == "NATS":
            for url in self.nats_urls:
                try:
                    result["nats_metrics"].extend(scrape_prometheus(url))
                except Exception as error:
                    result["errors"].append(f"{url}: {error}")
        return result
