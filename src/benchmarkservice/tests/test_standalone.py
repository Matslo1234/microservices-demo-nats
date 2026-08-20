# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from config import ConfigError
from standalone import config_from_args, parser


class StandaloneTest(unittest.TestCase):
    def test_required_urls_are_used_by_workers(self) -> None:
        arguments = parser().parse_args(
            [
                "--url",
                "https://shop.example",
                "--metrics-url",
                "https://metrics.example/snapshot",
                "--application-type",
                "NATS",
                "--workload",
                "open",
                "--arrival-rate",
                "250",
            ]
        )

        config = config_from_args(arguments)

        self.assertEqual("https://shop.example", config.target_url)
        self.assertEqual(
            "https://metrics.example/snapshot", config.metrics_url
        )
        self.assertEqual(3, config.worker_count)
        self.assertEqual(
            config.metrics_url, config.for_worker(2).metrics_url
        )

    def test_metrics_url_can_only_be_omitted_when_collection_is_off(self) -> None:
        arguments = parser().parse_args(
            [
                "--url",
                "https://shop.example",
                "--application-type",
                "GRPC",
            ]
        )
        with self.assertRaisesRegex(ConfigError, "metrics_url is required"):
            config_from_args(arguments)

        arguments.collect_resources = False
        self.assertIsNone(config_from_args(arguments).metrics_url)


if __name__ == "__main__":
    unittest.main()
