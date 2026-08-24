# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest
from unittest import mock

from cluster_metrics import (
    ClusterMetricsCollector,
    add_cadvisor_metrics,
    normalize_nats_micro_stats,
    parse_labels,
    service_for_pod,
)


class ClusterMetricsTest(unittest.TestCase):
    def test_collector_accepts_the_runner_application_and_namespace(self) -> None:
        with mock.patch(
            "cluster_metrics.KubernetesSummaryClient"
        ) as kubernetes:
            collector = ClusterMetricsCollector(
                "grpc",
                application_namespace="shop",
                nats_namespace="messaging",
            )

        self.assertEqual("GRPC", collector.application_type)
        kubernetes.assert_called_once_with("GRPC", "shop", "messaging")

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
