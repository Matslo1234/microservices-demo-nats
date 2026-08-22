# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from saturation import consumer_pending_total, evaluate_rung


class SaturationTest(unittest.TestCase):
    def test_goodput_plateau_stops_the_ladder(self) -> None:
        increasing = evaluate_rung(
            application_type="GRPC",
            target_rate=20,
            duration_seconds=10,
            completed=100,
            previous_goodput=9,
            pending_start=None,
            pending_end=None,
            final_rung=False,
            maximum_rate_reached=False,
        )
        plateau = evaluate_rung(
            application_type="GRPC",
            target_rate=30,
            duration_seconds=10,
            completed=100,
            previous_goodput=10,
            pending_start=None,
            pending_end=None,
            final_rung=False,
            maximum_rate_reached=False,
        )

        self.assertFalse(increasing["stop"])
        self.assertTrue(plateau["stop"])
        self.assertTrue(plateau["saturated"])
        self.assertEqual(
            "goodput_stopped_increasing", plateau["stop_reason"]
        )

    def test_nats_rapid_pending_growth_stops_the_ladder(self) -> None:
        decision = evaluate_rung(
            application_type="NATS",
            target_rate=20,
            duration_seconds=10,
            completed=150,
            previous_goodput=10,
            pending_start=5,
            pending_end=105,
            final_rung=False,
            maximum_rate_reached=False,
        )

        self.assertTrue(decision["stop"])
        self.assertEqual(
            "nats_pending_increasing_rapidly", decision["stop_reason"]
        )
        self.assertEqual(10, decision["pending_growth_per_second"])

    def test_pending_total_deduplicates_exporter_replicas(self) -> None:
        metrics = [
            self._pending("BOUTIQUE_EVENTS", "checkout", 3),
            self._pending("BOUTIQUE_EVENTS", "checkout", 3),
            self._pending("BOUTIQUE_COMMANDS", "cart", 2),
            self._pending("KV_BENCHMARK_RUNS", "controller", 100),
        ]

        self.assertEqual(5, consumer_pending_total(metrics))

    @staticmethod
    def _pending(stream: str, consumer: str, value: float) -> dict:
        return {
            "name": "jetstream_consumer_num_pending",
            "labels": {
                "stream_name": stream,
                "consumer_name": consumer,
            },
            "value": value,
        }


if __name__ == "__main__":
    unittest.main()
