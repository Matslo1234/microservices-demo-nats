# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from config import BenchmarkConfig, ConfigError, normalize_target_url


class BenchmarkConfigTest(unittest.TestCase):
    def test_defaults_are_valid_for_nats(self) -> None:
        config = BenchmarkConfig.from_request({}, "nats", "frontend:80")

        self.assertEqual("NATS", config.application_type)
        self.assertEqual("http://frontend:80", config.target_url)
        self.assertEqual("closed", config.workload)
        self.assertTrue(config.collect_nats_metrics)
        self.assertEqual(
            config.warmup_seconds
            + config.duration_seconds
            + config.drain_seconds,
            config.run_seconds,
        )

    def test_grpc_never_collects_nats_metrics(self) -> None:
        config = BenchmarkConfig.from_request(
            {"collect_nats_metrics": True}, "GRPC", "http://frontend"
        )
        self.assertFalse(config.collect_nats_metrics)

    def test_rejects_invalid_application_and_workload(self) -> None:
        with self.assertRaises(ConfigError):
            BenchmarkConfig.from_request({}, "HTTP", "frontend")
        with self.assertRaises(ConfigError):
            BenchmarkConfig.from_request(
                {"workload": "burst"}, "NATS", "frontend"
            )

    def test_rejects_out_of_range_and_boolean_integer(self) -> None:
        with self.assertRaises(ConfigError):
            BenchmarkConfig.from_request(
                {"duration_seconds": 0}, "NATS", "frontend"
            )
        with self.assertRaises(ConfigError):
            BenchmarkConfig.from_request(
                {"users": True}, "NATS", "frontend"
            )

    def test_target_normalization_rejects_credentials(self) -> None:
        self.assertEqual(
            "https://frontend.example", normalize_target_url("https://frontend.example/")
        )
        with self.assertRaises(ConfigError):
            normalize_target_url("http://user:password@frontend")

    def test_open_workload_is_split_at_100_requests_per_second(self):
        config = BenchmarkConfig.from_request(
            {"workload": "open", "arrival_rate": 250},
            "NATS",
            "frontend",
        )

        workers = [
            config.for_worker(index) for index in range(config.worker_count)
        ]

        self.assertEqual(3, config.worker_count)
        self.assertAlmostEqual(
            250, sum(worker.arrival_rate for worker in workers)
        )
        self.assertEqual(
            [100.0, 100.0, 50.0],
            [worker.arrival_rate for worker in workers],
        )
        self.assertTrue(
            all(worker.arrival_rate <= 100 for worker in workers)
        )
        self.assertTrue(workers[0].collect_resources)
        self.assertFalse(workers[1].collect_resources)
        self.assertTrue(workers[0].collect_nats_metrics)
        self.assertFalse(workers[1].collect_nats_metrics)

    def test_closed_workload_splits_users_and_divides_spawn_rate(self):
        config = BenchmarkConfig.from_request(
            {"workload": "closed", "users": 2_501, "spawn_rate": 90},
            "GRPC",
            "frontend",
        )

        workers = [
            config.for_worker(index) for index in range(config.worker_count)
        ]

        self.assertEqual(3, config.worker_count)
        self.assertEqual(2_501, sum(worker.users for worker in workers))
        self.assertTrue(all(worker.users <= 1_000 for worker in workers))
        self.assertEqual([30.0, 30.0, 30.0], [
            worker.spawn_rate for worker in workers
        ])

    def test_threshold_values_keep_a_single_worker(self):
        open_config = BenchmarkConfig.from_request(
            {"workload": "open", "arrival_rate": 100},
            "GRPC",
            "frontend",
        )
        closed_config = BenchmarkConfig.from_request(
            {"workload": "closed", "users": 1_000},
            "GRPC",
            "frontend",
        )

        self.assertEqual(1, open_config.worker_count)
        self.assertEqual(1, closed_config.worker_count)

    def test_minimum_spawn_rate_remains_valid_after_ten_way_split(self):
        config = BenchmarkConfig.from_request(
            {
                "workload": "closed",
                "users": 10_000,
                "spawn_rate": 0.01,
            },
            "GRPC",
            "frontend",
        )

        worker = config.for_worker(9)
        restored = BenchmarkConfig.from_worker_dict(worker.as_dict())

        self.assertEqual(0.001, restored.spawn_rate)


if __name__ == "__main__":
    unittest.main()
