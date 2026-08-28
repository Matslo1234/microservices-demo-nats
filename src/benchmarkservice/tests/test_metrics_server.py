# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import threading
import time
import unittest

from metrics_server import SnapshotCache


class _IndependentCollector:
    application_type = "NATS"

    def __init__(self) -> None:
        self.kubernetes_started = threading.Event()
        self.release_kubernetes = threading.Event()
        self.nats_calls = 0
        self.closed = False

    def _collect_kubernetes(self):
        self.kubernetes_started.set()
        self.release_kubernetes.wait(1)
        return {
            "pods": {"default/frontend-a": {"service": "frontend"}},
            "errors": [],
            "sample": {"collected_at": time.time()},
        }

    def _collect_nats(self):
        self.nats_calls += 1
        return {
            "nats_metrics": [{"name": "pending", "value": self.nats_calls}],
            "nats_micro_endpoints": [],
            "nats_order_completed_observer": None,
            "errors": [],
            "sample": {"collected_at": time.time()},
        }

    def _collect_nats_raft(self):
        return {
            "nats_raft_groups": [
                {"account": "BOUTIQUE", "group": "_meta_"}
            ],
            "errors": [],
            "sample": {"collected_at": time.time()},
        }

    def close(self) -> None:
        self.closed = True


class SnapshotCacheTest(unittest.TestCase):
    def test_slow_kubernetes_refresh_does_not_block_nats_or_get(self) -> None:
        collector = _IndependentCollector()
        cache = SnapshotCache(collector)
        self.assertTrue(collector.kubernetes_started.wait(0.2))
        deadline = time.monotonic() + 0.5
        value = cache.get()
        while not value["nats_metrics"] and time.monotonic() < deadline:
            time.sleep(0.01)
            value = cache.get()

        started = time.monotonic()
        value = cache.get()
        duration = time.monotonic() - started
        collector.release_kubernetes.set()
        cache.close()

        self.assertLess(duration, 0.05)
        self.assertTrue(value["nats_metrics"])
        self.assertTrue(value["nats_raft_groups"])
        self.assertIn("nats", value["source_samples"])
        self.assertIn("nats_raft", value["source_samples"])
        self.assertTrue(collector.closed)


if __name__ == "__main__":
    unittest.main()
