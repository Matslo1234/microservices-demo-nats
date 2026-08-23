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
            self._write_result(folder / "run-a.json", "closed", 1_500, 10, 9, 100)
            self._write_result(folder / "run-b.json", "closed", 1_500, 30, 21, 300)
            self._write_result(folder / "run-c.json", "closed", 10, 5, 5, 50)
            # Config JSON is an artifact, not a result summary.
            (folder / "config.json").write_text(
                json.dumps({"workload": "closed"}), encoding="utf-8"
            )

            outputs = process_folder(folder)

            self.assertEqual([folder / "closed_results.tex"], outputs)
            table = outputs[0].read_text(encoding="utf-8")
            self.assertIn("Users & Orders & Success rate & P95 & std", table)
            self.assertIn(r"10 & 5 & 100.00\% & 50.000 & 0.000", table)
            self.assertIn(r"1500 & 40 & 75.00\% & 200.000 & 100.000", table)

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
                    folder / "grpc-summary_p95_latency.svg",
                    nested / "nats-summary_goodput.svg",
                    nested / "nats-summary_p95_latency.svg",
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
            latency = (nested / "nats-summary_p95_latency.svg").read_text(
                encoding="utf-8"
            )
            self.assertIn("P95 outcome latency (ms)", latency)
            self.assertIn(">125<", latency)
            self.assertIn(">250<", latency)

    def test_unsupported_workload_has_required_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            self._write_result(folder / "open.json", "open", 1, 1, 1, 2)
            stderr = io.StringIO()

            with contextlib.redirect_stderr(stderr):
                exit_code = main([str(folder)])

            self.assertEqual(1, exit_code)
            self.assertEqual("unsupported workload open\n", stderr.getvalue())

    def test_writes_fault_tolerance_graphs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            grpc_path = folder / "grpc-fault.json"
            grpc_path.write_text(
                json.dumps(self._fault_tolerance_summary("GRPC")),
                encoding="utf-8",
            )
            nats_path = folder / "nats-fault.json"
            nats_path.write_text(
                json.dumps(self._fault_tolerance_summary("NATS")),
                encoding="utf-8",
            )

            outputs = process_folder(folder)

            self.assertEqual(
                {
                    folder / "grpc-fault_successful_requests.svg",
                    folder / "nats-fault_successful_requests.svg",
                    folder / "nats-fault_queued_events.svg",
                },
                set(outputs),
            )
            successful = (
                folder / "nats-fault_successful_requests.svg"
            ).read_text(encoding="utf-8")
            self.assertIn("successfully processed requests", successful)
            self.assertIn("Successful requests/s", successful)
            self.assertIn("Elapsed time (s)", successful)
            self.assertIn(">7<", successful)
            queued = (folder / "nats-fault_queued_events.svg").read_text(
                encoding="utf-8"
            )
            self.assertIn("queued NATS events", queued)
            self.assertIn("Queued events", queued)
            self.assertIn(">4<", queued)

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
        users: int,
        submitted: int,
        completed: int,
        p95: float,
    ) -> None:
        path.write_text(
            json.dumps(
                {
                    "application_type": "GRPC",
                    "workload": workload,
                    "users": users,
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
                        "business": {
                            "goodput_orders_per_second": 9.5,
                            "checkout_to_outcome": {"p95_ms": 125},
                        },
                        "nats": {"consumer_pending": {"max": 2}},
                    },
                    {
                        "target_requests_per_second": 20,
                        "business": {
                            "goodput_orders_per_second": 18.5,
                            "checkout_to_outcome": {"p95_ms": 250},
                        },
                        "nats": {"consumer_pending": {"max": 7}},
                    },
                ]
            },
        }

    @staticmethod
    def _fault_tolerance_summary(application_type: str) -> dict:
        return {
            "application_type": application_type,
            "workload": "fault_tolerance",
            "business": {"submitted": 20, "completed": 17},
            "fault_tolerance": {
                "per_second": [
                    {
                        "elapsed_seconds": 0,
                        "successfully_processed": 10,
                        "nats_waiting_events": None,
                    },
                    {
                        "elapsed_seconds": 1,
                        "successfully_processed": 7,
                        "nats_waiting_events": 4,
                    },
                ]
            },
        }


if __name__ == "__main__":
    unittest.main()
