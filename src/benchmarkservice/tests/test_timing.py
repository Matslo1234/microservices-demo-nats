# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from timing import outcome_latency_ms


class TimingTest(unittest.TestCase):
    def test_outcome_latency_uses_terminal_timestamp(self) -> None:
        self.assertAlmostEqual(
            125.25,
            outcome_latency_ms(
                1_785_024_000.0, "2026-07-26T00:00:00.125250Z"
            ),
            places=3,
        )

    def test_outcome_latency_accepts_rfc3339_offset(self) -> None:
        self.assertAlmostEqual(
            250.0,
            outcome_latency_ms(
                1_785_024_000.0, "2026-07-26T02:00:00.250+02:00"
            ),
            places=3,
        )

    def test_outcome_latency_requires_timestamp_and_timezone(self) -> None:
        for value in (None, "", "not-a-time", "2026-07-26T00:00:00"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    outcome_latency_ms(1_785_024_000.0, value)


if __name__ == "__main__":
    unittest.main()
