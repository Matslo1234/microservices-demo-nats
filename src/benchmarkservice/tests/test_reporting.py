# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import json
import tempfile
import unittest
from pathlib import Path

from config import BenchmarkConfig
from reporting import build_report, nats_summary, percentile


class ReportingTest(unittest.TestCase):
    def test_percentile_interpolates_and_handles_empty_input(self) -> None:
        self.assertIsNone(percentile([], 0.95))
        self.assertEqual(2.5, percentile([1, 2, 3, 4], 0.5))

    def test_build_report_excludes_warmup_and_calculates_resources(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary)
            config = BenchmarkConfig.from_request(
                {
                    "warmup_seconds": 1,
                    "duration_seconds": 10,
                    "drain_seconds": 2,
                },
                "NATS",
                "frontend:80",
            )
            (run / "config.json").write_text(
                json.dumps(config.as_dict()), encoding="utf-8"
            )
            business = [
                self._business("warmup", "COMPLETED", 50, True, True),
                self._business("steady", "COMPLETED", 100, True, True),
                self._business("steady", "REJECTED", 30, False, True),
                {
                    **self._business("steady", "ACCEPTED", 10, True, True),
                    "name": "checkout_acceptance",
                },
            ]
            (run / "business.jsonl").write_text(
                "".join(json.dumps(record) + "\n" for record in business),
                encoding="utf-8",
            )
            resources = [
                self._resource(2, 1_000_000_000, 100, 10, 20),
                self._resource(7, 3_000_000_000, 300, 40, 80),
            ]
            (run / "resources.jsonl").write_text(
                "".join(json.dumps(record) + "\n" for record in resources),
                encoding="utf-8",
            )

            summary = build_report(run)

            self.assertEqual(2, summary["business"]["submitted"])
            self.assertEqual(1, summary["business"]["completed"])
            self.assertEqual(0.1, summary["business"]["goodput_orders_per_second"])
            self.assertEqual(
                100.0, summary["business"]["checkout_to_outcome"]["p95_ms"]
            )
            self.assertEqual(2.0, summary["resources"]["cpu_seconds"])
            self.assertEqual(1_000.0, summary["resources"]["memory_byte_seconds"])
            self.assertEqual(30, summary["resources"]["network_rx_bytes"])
            self.assertTrue((run / "business.csv").exists())
            self.assertTrue((run / "summary.json").exists())

    def test_nats_summary_deduplicates_replicated_consumer_series(self) -> None:
        first = {
            "phase": "steady",
            "nats_metrics": [
                self._nats_metric(
                    "jetstream_consumer_num_pending", 3, "orders", "checkout"
                ),
                self._nats_metric(
                    "jetstream_consumer_num_pending", 3, "orders", "checkout"
                ),
                self._nats_metric(
                    "gnatsd_varz_jetstream_stats_storage", 10
                ),
                self._nats_metric(
                    "gnatsd_varz_jetstream_stats_storage", 20
                ),
            ],
        }
        second = {
            "phase": "steady",
            "nats_metrics": [
                self._nats_metric(
                    "jetstream_consumer_num_pending", 1, "orders", "checkout"
                ),
                self._nats_metric(
                    "jetstream_consumer_num_pending", 1, "orders", "checkout"
                ),
                self._nats_metric(
                    "gnatsd_varz_jetstream_stats_storage", 15
                ),
                self._nats_metric(
                    "gnatsd_varz_jetstream_stats_storage", 25
                ),
            ],
        }

        summary = nats_summary([first, second])

        self.assertEqual(3, summary["consumer_pending"]["first"])
        self.assertEqual(-2, summary["consumer_pending"]["change"])
        self.assertEqual(30, summary["storage_bytes"]["first"])
        self.assertEqual(40, summary["storage_bytes"]["last"])

    @staticmethod
    def _business(
        phase: str,
        outcome: str,
        response_time: float,
        success: bool,
        accepted: bool,
    ) -> dict:
        return {
            "timestamp": 1,
            "phase": phase,
            "request_type": "BUSINESS",
            "name": "checkout_to_outcome",
            "response_time_ms": response_time,
            "success": success,
            "error": None if success else outcome,
            "context": {"outcome": outcome, "accepted": accepted},
        }

    @staticmethod
    def _resource(
        timestamp: float,
        cpu: int,
        memory: int,
        received: int,
        transmitted: int,
    ) -> dict:
        return {
            "timestamp": timestamp,
            "phase": "steady",
            "pods": {
                "default/frontend-1": {
                    "service": "frontend",
                    "cpu_usage_core_nanoseconds": cpu,
                    "memory_working_set_bytes": memory,
                    "network_rx_bytes": received,
                    "network_tx_bytes": transmitted,
                }
            },
            "nats_metrics": [],
        }

    @staticmethod
    def _nats_metric(
        name: str,
        value: float,
        stream: str | None = None,
        consumer: str | None = None,
    ) -> dict:
        labels = {}
        if stream is not None:
            labels["stream_name"] = stream
        if consumer is not None:
            labels["consumer_name"] = consumer
        return {"name": name, "value": value, "labels": labels}


if __name__ == "__main__":
    unittest.main()
