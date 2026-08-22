# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import json
import tempfile
import unittest
from pathlib import Path

from config import BenchmarkConfig
from reporting import (
    build_report,
    business_summary,
    capacity_assessment,
    nats_summary,
    percentile,
)


class ReportingTest(unittest.TestCase):
    def test_percentile_interpolates_and_handles_empty_input(self) -> None:
        self.assertIsNone(percentile([], 0.95))
        self.assertEqual(2.5, percentile([1, 2, 3, 4], 0.5))

    def test_build_report_excludes_warmup_and_calculates_resources(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary)
            config = BenchmarkConfig.from_request(
                {
                    "target_url": "http://frontend",
                    "metrics_url": "http://benchmarkmetrics/snapshot",
                    "warmup_seconds": 1,
                    "duration_seconds": 10,
                    "drain_seconds": 2,
                },
                "NATS",
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
            self.assertEqual(1, summary["worker_count"])
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
                    "jetstream_consumer_num_pending",
                    3,
                    "BOUTIQUE_EVENTS",
                    "checkout",
                ),
                self._nats_metric(
                    "jetstream_consumer_num_pending",
                    3,
                    "BOUTIQUE_EVENTS",
                    "checkout",
                ),
                self._nats_metric(
                    "jetstream_consumer_num_pending",
                    100,
                    "KV_BENCHMARK_RUNS",
                    "controller",
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
                    "jetstream_consumer_num_pending",
                    1,
                    "BOUTIQUE_EVENTS",
                    "checkout",
                ),
                self._nats_metric(
                    "jetstream_consumer_num_pending",
                    1,
                    "BOUTIQUE_EVENTS",
                    "checkout",
                ),
                self._nats_metric(
                    "jetstream_consumer_num_pending",
                    200,
                    "KV_BENCHMARK_RUNS",
                    "controller",
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

    def test_generator_saturation_is_reported_as_unsustainable(self) -> None:
        saturated = self._business(
            "steady", "GENERATOR_SATURATED", 0, False, False
        )
        saturated["context"]["scheduled_at"] = 1
        business = business_summary([saturated], 10)
        config = BenchmarkConfig.from_request(
            {
                "target_url": "http://frontend",
                "metrics_url": "http://benchmarkmetrics/snapshot",
                "workload": "open",
            },
            "GRPC",
        )

        assessment = capacity_assessment(
            config,
            business,
            {"final": 0},
            {"available": False},
        )

        self.assertEqual(1, business["scheduled_open_loop"])
        self.assertEqual(1, business["outcomes"]["GENERATOR_SATURATED"])
        self.assertFalse(assessment["sustainable"])
        self.assertIn(
            "load generator concurrency limit was reached 1 times",
            assessment["reasons"],
        )

    def test_saturation_report_keeps_per_rung_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            run = Path(temporary)
            config = BenchmarkConfig.from_request(
                {
                    "target_url": "http://frontend",
                    "metrics_url": "http://benchmarkmetrics/snapshot",
                    "workload": "saturation",
                    "warmup_seconds": 0,
                    "duration_seconds": 30,
                    "drain_seconds": 2,
                },
                "NATS",
            )
            (run / "config.json").write_text(
                json.dumps(config.as_dict()), encoding="utf-8"
            )
            business = []
            for rung, completed in ((0, 1), (1, 2), (2, 1)):
                for _ in range(completed):
                    record = self._business(
                        "steady", "COMPLETED", 100 + rung, True, True
                    )
                    record["context"].update(
                        {
                            "saturation_rung": rung,
                            "target_requests_per_second": 10 + rung * 10,
                            "scheduled_at": 1,
                        }
                    )
                    business.append(record)
            (run / "business.jsonl").write_text(
                "".join(json.dumps(record) + "\n" for record in business),
                encoding="utf-8",
            )
            decisions = [
                self._saturation_decision(0, 10, 0, 10, 1, False, None),
                self._saturation_decision(
                    1,
                    20,
                    10,
                    20,
                    2,
                    True,
                    "goodput_stopped_increasing",
                ),
                self._saturation_decision(
                    2,
                    30,
                    20,
                    30,
                    1,
                    False,
                    None,
                    stop=True,
                    stop_reason="maximum_duration_reached",
                ),
            ]
            (run / "saturation.jsonl").write_text(
                "".join(json.dumps(record) + "\n" for record in decisions),
                encoding="utf-8",
            )
            resources = [
                self._resource(1, 1_000_000_000, 100, 10, 20),
                self._resource(9, 2_000_000_000, 100, 20, 30),
                self._resource(11, 3_000_000_000, 100, 30, 40),
                self._resource(19, 4_000_000_000, 100, 40, 50),
            ]
            for elapsed, record in zip((1, 9, 11, 19), resources):
                record["elapsed_seconds"] = elapsed
            (run / "resources.jsonl").write_text(
                "".join(json.dumps(record) + "\n" for record in resources),
                encoding="utf-8",
            )

            summary = build_report(run)

            saturation = summary["saturation"]
            self.assertTrue(saturation["saturated"])
            self.assertEqual(20, saturation["saturation_requests_per_second"])
            self.assertEqual(
                "goodput_stopped_increasing",
                saturation["saturation_reason"],
            )
            self.assertEqual(
                "maximum_duration_reached", saturation["stop_reason"]
            )
            self.assertEqual(
                10, saturation["highest_sustainable_requests_per_second"]
            )
            self.assertEqual(3, len(saturation["rungs"]))
            self.assertEqual(
                2, saturation["rungs"][1]["business"]["completed"]
            )
            self.assertTrue(
                saturation["rungs"][0]["resources"]["available"]
            )

    @staticmethod
    def _saturation_decision(
        rung: int,
        rate: int,
        started: int,
        ended: int,
        completed: int,
        saturated: bool,
        reason: str | None,
        *,
        stop: bool = False,
        stop_reason: str | None = None,
    ) -> dict:
        return {
            "rung": rung,
            "target_requests_per_second": rate,
            "started_elapsed_seconds": started,
            "ended_elapsed_seconds": ended,
            "duration_seconds": ended - started,
            "completed_during_rung": completed,
            "observed_goodput_orders_per_second": completed / (ended - started),
            "pending_start": 0,
            "pending_end": 0,
            "pending_growth": 0,
            "pending_growth_per_second": 0,
            "stop": stop,
            "saturated": saturated,
            "saturation_reason": reason,
            "stop_reason": stop_reason,
        }

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
