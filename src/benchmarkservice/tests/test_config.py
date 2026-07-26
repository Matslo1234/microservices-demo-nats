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


if __name__ == "__main__":
    unittest.main()
