# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from timing import received_latency_ms


class TimingTest(unittest.TestCase):
    def test_received_latency_uses_local_event_receipt(self) -> None:
        self.assertAlmostEqual(
            125.25,
            received_latency_ms(100.0, 100.12525),
            places=3,
        )

    def test_received_latency_rejects_reversed_clock_values(self) -> None:
        with self.assertRaises(ValueError):
            received_latency_ms(100.0, 99.0)

if __name__ == "__main__":
    unittest.main()
