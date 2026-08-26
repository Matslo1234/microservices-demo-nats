# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import asyncio
import os
import time
import unittest
from unittest import mock

from cluster_metrics import (
    ClusterMetricsCollector,
    KubernetesSummaryClient,
    NatsMicroStatsClient,
    add_cadvisor_metrics,
    normalize_nats_micro_stats,
    parse_labels,
    scrape_prometheus,
    service_for_pod,
)
from nats_order_observer_bridge import dispatch as dispatch_order_observer


class _FakeCompletedOrderObserver:
    def order_completed_sample(self) -> dict[str, object]:
        return {"observer_id": "observer-a", "total": 12_345}


class ClusterMetricsTest(unittest.TestCase):
    def test_prometheus_scrape_uses_bounded_default_timeout(self) -> None:
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b""
        with mock.patch(
            "cluster_metrics.urlopen", return_value=response
        ) as urlopen:
            self.assertEqual([], scrape_prometheus("http://nats/metrics"))

        urlopen.assert_called_once()
        self.assertEqual(2, urlopen.call_args.kwargs["timeout"])

    def test_nats_micro_stats_uses_doubled_default_timeouts(self) -> None:
        thread = mock.MagicMock()
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch("cluster_metrics.asyncio.new_event_loop"),
            mock.patch("cluster_metrics.threading.Thread", return_value=thread),
        ):
            client = NatsMicroStatsClient()

        self.assertEqual(0.5, client.response_timeout)
        self.assertEqual(4.0, client.connect_timeout)
        thread.start.assert_called_once_with()

    def test_completed_order_bridge_returns_observer_sample(self) -> None:
        response, should_close = dispatch_order_observer(
            _FakeCompletedOrderObserver(),
            {"id": 7, "operation": "sample"},
        )

        self.assertEqual(
            {
                "id": 7,
                "ok": True,
                "value": {
                    "observer_id": "observer-a",
                    "total": 12_345,
                },
            },
            response,
        )
        self.assertFalse(should_close)

    def test_completed_order_observer_reports_continuous_live_count(self) -> None:
        connection = mock.Mock()
        connection.flush = mock.AsyncMock()
        client = NatsMicroStatsClient.__new__(NatsMicroStatsClient)
        client._ensure_connected = mock.AsyncMock(return_value=connection)
        client.connect_timeout = 2
        client._order_completed_observer_id = "observer-a"
        client._order_completed_total = 12_345
        client._order_completed_error = None

        sample = asyncio.run(client._order_completed_sample())

        self.assertEqual(
            {"observer_id": "observer-a", "total": 12_345},
            sample,
        )
        connection.flush.assert_awaited_once_with(timeout=2)

    def test_collector_accepts_the_runner_application_and_namespace(self) -> None:
        with mock.patch(
            "cluster_metrics.KubernetesSummaryClient"
        ) as kubernetes:
            collector = ClusterMetricsCollector(
                "grpc",
                application_namespace="shop",
                nats_namespace="messaging",
            )
            collector.close()

        self.assertEqual("GRPC", collector.application_type)
        kubernetes.assert_called_once_with("GRPC", "shop", "messaging")

    def test_kubernetes_sampling_has_whole_snapshot_deadline(self) -> None:
        client = KubernetesSummaryClient.__new__(KubernetesSummaryClient)
        client.snapshot_deadline = 0.03
        client.cadvisor_enabled = False
        client.cadvisor_interval = 30
        client._last_cadvisor = 0
        client._cached_nodes = ["node-a", "node-b"]
        client.errors = []
        client.diagnostics = {}
        from concurrent.futures import ThreadPoolExecutor

        client._executor = ThreadPoolExecutor(max_workers=2)
        client._relevant_nodes = mock.Mock(return_value=["node-a", "node-b"])

        def slow_node(node: str, collect_cadvisor: bool):
            time.sleep(0.2)
            return {}, []

        client._sample_node = slow_node
        started = time.monotonic()
        pods = client.sample()
        duration = time.monotonic() - started
        client.close()

        self.assertEqual({}, pods)
        self.assertLess(duration, 0.15)
        self.assertEqual(2, client.diagnostics["nodes_timed_out"])
        self.assertTrue(client.diagnostics["partial"])

    def test_relevant_nodes_exclude_unmeasured_pods(self) -> None:
        client = KubernetesSummaryClient.__new__(KubernetesSummaryClient)
        client.application_type = "NATS"
        client.application_namespace = "shop"
        client.nats_namespace = "nats"
        client._cached_nodes = []
        client._get = mock.Mock(
            return_value={
                "items": [
                    {
                        "metadata": {"namespace": "shop", "name": "frontend-a"},
                        "spec": {"nodeName": "node-a"},
                    },
                    {
                        "metadata": {"namespace": "kube-system", "name": "coredns-a"},
                        "spec": {"nodeName": "node-b"},
                    },
                    {
                        "metadata": {"namespace": "nats", "name": "nats-0"},
                        "spec": {"nodeName": "node-c"},
                    },
                ]
            }
        )

        self.assertEqual(["node-a", "node-c"], client._relevant_nodes())

    def test_cadvisor_counters_are_carried_between_slower_refreshes(self) -> None:
        client = KubernetesSummaryClient.__new__(KubernetesSummaryClient)
        client.snapshot_deadline = 1
        client.cadvisor_enabled = True
        client.cadvisor_interval = 30
        client._last_cadvisor = 0
        client._cached_nodes = ["node-a"]
        client._cadvisor_cache = {}
        client.errors = []
        client.diagnostics = {}
        from concurrent.futures import ThreadPoolExecutor

        client._executor = ThreadPoolExecutor(max_workers=1)
        client._relevant_nodes = mock.Mock(return_value=["node-a"])

        def node_sample(node: str, collect_cadvisor: bool):
            pod = {
                "service": "frontend",
                "node": node,
                "cpu_usage_core_nanoseconds": 1,
                "memory_working_set_bytes": 2,
                "network_rx_bytes": 3,
                "network_tx_bytes": 4,
            }
            if collect_cadvisor:
                pod["cpu_cfs_periods_total"] = 10
            return {"shop/frontend-a": pod}, []

        client._sample_node = node_sample
        first = client.sample()
        second = client.sample()
        client.close()

        self.assertEqual(10, first["shop/frontend-a"]["cpu_cfs_periods_total"])
        self.assertEqual(10, second["shop/frontend-a"]["cpu_cfs_periods_total"])

    def test_only_target_application_and_nats_pods_are_reported(self) -> None:
        self.assertEqual(
            "frontend",
            service_for_pod("shop", "frontend-abc", "NATS", "shop"),
        )
        self.assertEqual(
            "nats",
            service_for_pod("messaging", "nats-2", "NATS", "shop", "messaging"),
        )
        self.assertIsNone(
            service_for_pod("load", "frontend-abc", "NATS", "shop")
        )
        self.assertIsNone(
            service_for_pod(
                "shop", "storefrontprojectionservice-abc", "GRPC", "shop"
            )
        )
        self.assertEqual(
            "redis-checkout",
            service_for_pod(
                "shop", "redis-checkout-cluster-2", "NATS", "shop"
            ),
        )

    def test_prometheus_labels_are_unescaped(self) -> None:
        self.assertEqual(
            {"stream_name": "BOUTIQUE_EVENTS", "description": 'a"b'},
            parse_labels(
                'stream_name="BOUTIQUE_EVENTS",description="a\\"b"'
            ),
        )

    def test_cadvisor_metrics_are_aggregated_for_target_containers(self) -> None:
        pods = {
            "shop/redis-checkout-cluster-0": {
                "service": "redis-checkout",
                "node": "node-a",
            }
        }
        body = """
container_cpu_cfs_periods_total{namespace="shop",pod="redis-checkout-cluster-0",container="redis"} 100 1787487637785
container_cpu_cfs_throttled_periods_total{namespace="shop",pod="redis-checkout-cluster-0",container="redis"} 7
container_cpu_cfs_throttled_seconds_total{namespace="shop",pod="redis-checkout-cluster-0",container="redis"} 1.25
container_fs_reads_total{namespace="shop",pod="redis-checkout-cluster-0",container="redis",device="sda"} 12
container_fs_writes_total{namespace="shop",pod="redis-checkout-cluster-0",container="redis",device="sda"} 8
container_fs_io_time_seconds_total{namespace="shop",pod="redis-checkout-cluster-0",container="redis",device="sda"} 2.5
container_fs_io_time_weighted_seconds_total{namespace="shop",pod="redis-checkout-cluster-0",container="redis",device="sda"} 0.75
container_fs_reads_total{namespace="shop",pod="redis-checkout-cluster-0",container="POD",device="sda"} 999
container_fs_reads_total{namespace="other",pod="redis-checkout-cluster-0",container="redis",device="sda"} 999
"""

        add_cadvisor_metrics(pods, body, "node-a")

        pod = pods["shop/redis-checkout-cluster-0"]
        self.assertEqual(100, pod["cpu_cfs_periods_total"])
        self.assertEqual(7, pod["cpu_cfs_throttled_periods_total"])
        self.assertEqual(1.25, pod["cpu_cfs_throttled_seconds_total"])
        self.assertEqual(20, pod["disk_io_operations_total"])
        self.assertEqual(2.5, pod["disk_io_time_seconds_total"])
        self.assertEqual(0.75, pod["disk_io_time_weighted_seconds_total"])

    def test_nats_micro_stats_are_flattened_with_custom_pending(self) -> None:
        endpoints = normalize_nats_micro_stats(
            {
                "name": "PaymentTokenization",
                "id": "instance-a",
                "version": "1.0.0",
                "endpoints": [
                    {
                        "name": "tokenize",
                        "subject": "boutique.qry.payment.tokenize.v1",
                        "queue_group": "payment-tokenize-v1",
                        "num_requests": 12,
                        "num_errors": 1,
                        "processing_time": 9000,
                        "average_processing_time": 750,
                        "data": {"pending_requests": 3},
                    }
                ],
            }
        )

        self.assertEqual(1, len(endpoints))
        self.assertEqual(3, endpoints[0]["pending_requests"])
        self.assertEqual(9000, endpoints[0]["processing_time_nanoseconds"])
        self.assertEqual(
            "boutique.qry.payment.tokenize.v1", endpoints[0]["subject"]
        )


if __name__ == "__main__":
    unittest.main()
