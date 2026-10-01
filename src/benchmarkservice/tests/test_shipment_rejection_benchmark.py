# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from shipment_rejection_benchmark import (
    ARRIVAL_RATE,
    DURATION_SECONDS,
    config_from_args,
    monitored_outcomes,
    parser,
)


class ShipmentRejectionBenchmarkTest(unittest.TestCase):
    def test_uses_fixed_open_loop_workload(self) -> None:
        arguments = parser().parse_args(
            [
                "--url",
                "https://shop.example",
                "--metrics-url",
                "https://metrics.example/snapshot",
            ]
        )

        config = config_from_args(arguments)

        self.assertEqual("NATS", config.application_type)
        self.assertEqual("open", config.workload)
        self.assertEqual(0, config.warmup_seconds)
        self.assertEqual(DURATION_SECONDS, config.duration_seconds)
        self.assertEqual(ARRIVAL_RATE, config.arrival_rate)
        self.assertEqual(1, config.worker_count)
        self.assertTrue(config.collect_nats_metrics)

    def test_reports_the_four_requested_outcomes(self) -> None:
        outcomes = monitored_outcomes(
            {
                "workflow_events": {
                    "available": True,
                    "successful_orders": 1_200,
                    "shipments_rejected": 300,
                    "payment_authorizations_released": 300,
                    "orders_in_manual_review": 0,
                }
            }
        )

        self.assertEqual(1_200, outcomes["successful_orders"])
        self.assertEqual(300, outcomes["shipments_rejected"])
        self.assertEqual(
            300, outcomes["payment_authorizations_released"]
        )
        self.assertEqual(0, outcomes["orders_in_manual_review"])
        self.assertTrue(outcomes["monitoring_available"])


if __name__ == "__main__":
    unittest.main()
