# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from cluster_metrics import parse_labels, service_for_pod


class ClusterMetricsTest(unittest.TestCase):
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

    def test_prometheus_labels_are_unescaped(self) -> None:
        self.assertEqual(
            {"stream_name": "BOUTIQUE_EVENTS", "description": 'a"b'},
            parse_labels(
                'stream_name="BOUTIQUE_EVENTS",description="a\\"b"'
            ),
        )


if __name__ == "__main__":
    unittest.main()
