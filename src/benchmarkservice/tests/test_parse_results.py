# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from parse_results import ResultError, main, process_folder


class ParseResultsTest(unittest.TestCase):
    def test_groups_closed_runs_and_writes_latex_table(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            self._write_result(folder / "run-a.json", "closed", 2, 10, 9, 100)
            self._write_result(folder / "run-b.json", "closed", 2, 30, 21, 300)
            self._write_result(folder / "run-c.json", "closed", 1, 5, 5, 50)
            # Config JSON is an artifact, not a result summary.
            (folder / "config.json").write_text(
                json.dumps({"workload": "closed"}), encoding="utf-8"
            )

            outputs = process_folder(folder)

            self.assertEqual([folder / "closed_results.tex"], outputs)
            table = outputs[0].read_text(encoding="utf-8")
            self.assertIn("Users & Orders & Success rate & P95 & std", table)
            self.assertIn(r"1000 & 5 & 100.00\% & 50.000 & 0.000", table)
            self.assertIn(r"2000 & 40 & 75.00\% & 200.000 & 100.000", table)

    def test_writes_independent_saturation_graphs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            grpc = self._saturation_summary("GRPC")
            nats = self._saturation_summary("NATS")
            (folder / "grpc-summary.json").write_text(
                json.dumps(grpc), encoding="utf-8"
            )
            nested = folder / "nested"
            nested.mkdir()
            (nested / "nats-summary.json").write_text(
                json.dumps(nats), encoding="utf-8"
            )

            outputs = process_folder(folder)

            self.assertEqual(
                {
                    folder / "grpc-summary_goodput.svg",
                    nested / "nats-summary_goodput.svg",
                    nested / "nats-summary_max_pending_events.svg",
                },
                set(outputs),
            )
            goodput = outputs[0].read_text(encoding="utf-8")
            self.assertIn("Goodput (orders/s)", goodput)
            pending = (nested / "nats-summary_max_pending_events.svg").read_text(
                encoding="utf-8"
            )
            self.assertIn("Maximum pending events", pending)

    def test_unsupported_workload_has_required_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            self._write_result(folder / "open.json", "open", 1, 1, 1, 2)
            stderr = io.StringIO()

            with contextlib.redirect_stderr(stderr):
                exit_code = main([str(folder)])

            self.assertEqual(1, exit_code)
            self.assertEqual("unsupported workload open\n", stderr.getvalue())

    def test_rejects_folder_without_result_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            (folder / "config.json").write_text(
                json.dumps({"workload": "closed"}), encoding="utf-8"
            )

            with self.assertRaisesRegex(
                ResultError, "no benchmark result JSON files"
            ):
                process_folder(folder)

    @staticmethod
    def _write_result(
        path: Path,
        workload: str,
        worker_count: int,
        submitted: int,
        completed: int,
        p95: float,
    ) -> None:
        path.write_text(
            json.dumps(
                {
                    "application_type": "GRPC",
                    "workload": workload,
                    "worker_count": worker_count,
                    "business": {
                        "submitted": submitted,
                        "completed": completed,
                        "checkout_to_outcome": {"p95_ms": p95},
                    },
                }
            ),
            encoding="utf-8",
        )

    @staticmethod
    def _saturation_summary(application_type: str) -> dict:
        return {
            "application_type": application_type,
            "workload": "saturation",
            "worker_count": 1,
            "business": {"submitted": 30, "completed": 28},
            "saturation": {
                "rungs": [
                    {
                        "target_requests_per_second": 10,
                        "business": {"goodput_orders_per_second": 9.5},
                        "nats": {"consumer_pending": {"max": 2}},
                    },
                    {
                        "target_requests_per_second": 20,
                        "business": {"goodput_orders_per_second": 18.5},
                        "nats": {"consumer_pending": {"max": 7}},
                    },
                ]
            },
        }


if __name__ == "__main__":
    unittest.main()
