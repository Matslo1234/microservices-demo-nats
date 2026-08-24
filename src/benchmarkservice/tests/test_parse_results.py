# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from parse_results import (
    Result,
    ResultError,
    collect_closed_nats_waiting_series,
    find_results,
    main,
    process_folder,
    saturation_points,
    write_png_chart,
)


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

            self.assertEqual(
                [
                    folder / "closed_results.tex",
                    folder / "resource-usage.tex",
                    folder / "closed_p95_latency.png",
                ],
                outputs,
            )
            table = outputs[0].read_text(encoding="utf-8")
            self.assertIn("Users & Orders & Success rate & P95 & std", table)
            self.assertIn(r"10 & 5 & 100.00\% & 50.000 & 0.000", table)
            self.assertIn(r"1500 & 20 & 75.00\% & 200.000 & 100.000", table)
            self.assert_png(folder / "closed_p95_latency.png")

    def test_writes_closed_nats_waiting_series_per_user_count(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            self._write_result(
                folder / "users-10-a.json",
                "closed",
                10,
                10,
                10,
                100,
                application_type="NATS",
                waiting_events=[(0, 2), (1, 4)],
            )
            self._write_result(
                folder / "users-10-b.json",
                "closed",
                10,
                10,
                10,
                120,
                application_type="NATS",
                waiting_events=[(0, 4), (1, 8)],
            )
            self._write_result(
                folder / "users-20.json",
                "closed",
                20,
                10,
                10,
                200,
                application_type="NATS",
                waiting_events=[(0, 5), (1, 10)],
            )

            outputs = process_folder(folder)

            waiting_path = folder / "closed_nats_waiting_events.png"
            self.assertIn(waiting_path, outputs)
            self.assert_png(waiting_path)

    def test_reads_closed_nats_waiting_series_from_resource_samples(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            self._write_result(
                folder / "summary.json",
                "closed",
                10,
                10,
                10,
                100,
                application_type="NATS",
            )
            records = [
                self._nats_resource_sample(0.2, 2),
                self._nats_resource_sample(2.2, 6),
            ]
            (folder / "resources.jsonl").write_text(
                "".join(json.dumps(record) + "\n" for record in records),
                encoding="utf-8",
            )

            series = collect_closed_nats_waiting_series(find_results(folder))

            self.assertEqual(
                {10: [(0.0, 2.0), (1.0, 2.0), (2.0, 6.0)]},
                series,
            )

    def test_uses_outstanding_order_maxima_without_pending_series(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            path = folder / "summary.json"
            self._write_result(
                path,
                "closed",
                10,
                10,
                10,
                100,
                application_type="NATS",
            )
            summary = json.loads(path.read_text(encoding="utf-8"))
            summary.update(
                {
                    "nats": {"consumer_pending": {"max": 8}},
                    "outstanding_orders": {
                        "series": [
                            {"elapsed_seconds": 0, "max_outstanding": 3},
                            {"elapsed_seconds": 2, "max_outstanding": 7},
                        ]
                    },
                }
            )
            path.write_text(json.dumps(summary), encoding="utf-8")

            series = collect_closed_nats_waiting_series(find_results(folder))

            self.assertEqual(
                {10: [(0.0, 3.0), (1.0, 3.0), (2.0, 7.0)]},
                series,
            )

    def test_writes_resource_table_for_each_closed_user_count(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            self._write_resource_result(
                folder / "users-10-a.json",
                10,
                10,
                {
                    "frontend": (20, 100_000_000),
                    "checkout_service": (10, 50_000_000),
                },
            )
            self._write_resource_result(
                folder / "users-10-b.json",
                10,
                20,
                {
                    "frontend": (60, 120_000_000),
                    "checkout_service": (40, 70_000_000),
                },
            )
            self._write_resource_result(
                folder / "users-20.json",
                20,
                10,
                {"frontend": (10, 80_000_000)},
            )

            outputs = process_folder(folder)

            self.assertIn(folder / "resource-usage.tex", outputs)
            table = (folder / "resource-usage.tex").read_text(
                encoding="utf-8"
            )
            self.assertEqual(2, table.count(r"\begin{table}"))
            self.assertIn(r"\caption{Resource usage for 10 users}", table)
            self.assertIn(r"\caption{Resource usage for 20 users}", table)
            self.assertIn("Avg memory MB & Avg memory MB std.", table)
            self.assertIn(
                r"checkout\_service & 1.500000 & 0.500000 & 60.000 & 10.000",
                table,
            )
            self.assertIn(
                r"frontend & 2.500000 & 0.500000 & 110.000 & 10.000",
                table,
            )
            self.assertIn(
                r"Total & 4.000000 & 1.000000 & 170.000 & 20.000",
                table,
            )

    def test_closed_order_average_can_be_fractional(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            submitted_per_run = (1_100, 1_100, 1_100, 1_100, 1_099)
            for run, submitted in enumerate(submitted_per_run):
                self._write_result(
                    folder / f"run-{run}.json",
                    "closed",
                    100,
                    submitted,
                    submitted,
                    10,
                )

            outputs = process_folder(folder)

            table = outputs[0].read_text(encoding="utf-8")
            self.assertIn(r"100 & 1099.8 & 100.00\%", table)

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
                    folder / "grpc-summary_goodput.png",
                    folder / "grpc-summary_p95_latency.png",
                    nested / "nats-summary_goodput.png",
                    nested / "nats-summary_p95_latency.png",
                    nested / "nats-summary_max_pending_events.png",
                },
                set(outputs),
            )
            for output in outputs:
                self.assert_png(output)

    def test_uses_observed_goodput_for_saturation_graph(self) -> None:
        summary = self._saturation_summary("GRPC")

        goodput, _, _ = saturation_points(
            Result(path=Path("summary.json"), summary=summary)
        )

        self.assertEqual([(10.0, 9.0), (20.0, 17.0)], goodput)

    def test_writes_single_rung_nats_saturation_pngs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            summary = self._saturation_summary("NATS")
            summary["saturation"]["rungs"] = summary["saturation"]["rungs"][:1]
            (folder / "summary.json").write_text(
                json.dumps(summary), encoding="utf-8"
            )

            outputs = process_folder(folder)

            self.assertEqual(3, len(outputs))
            for output in outputs:
                self.assert_png(output)

    def test_writes_png_with_fixed_dimensions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            chart_path = Path(temporary) / "chart.png"
            write_png_chart(
                chart_path,
                [(0.0, 0.5), (1_000_000.0, 1_000_000.0)],
                title="Chart",
                y_label="Values",
            )

            self.assert_png(chart_path)

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
                    folder / "grpc-fault_successful_requests.png",
                    folder / "nats-fault_successful_requests.png",
                    folder / "nats-fault_queued_events.png",
                },
                set(outputs),
            )
            for output in outputs:
                self.assert_png(output)

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

    def assert_png(self, path: Path) -> None:
        image = path.read_bytes()
        self.assertEqual(b"\x89PNG\r\n\x1a\n", image[:8])
        self.assertEqual(
            (960, 600),
            tuple(
                int.from_bytes(image[offset : offset + 4], "big")
                for offset in (16, 20)
            ),
        )

    @staticmethod
    def _write_result(
        path: Path,
        workload: str,
        users: int,
        submitted: int,
        completed: int,
        p95: float,
        *,
        application_type: str = "GRPC",
        waiting_events: list[tuple[int, float]] | None = None,
    ) -> None:
        nats = (
            {
                "consumer_pending": {
                    "series": [
                        {
                            "elapsed_seconds": second,
                            "waiting_events": waiting,
                        }
                        for second, waiting in waiting_events
                    ]
                }
            }
            if waiting_events is not None
            else None
        )
        path.write_text(
            json.dumps(
                {
                    "application_type": application_type,
                    "workload": workload,
                    "users": users,
                    "business": {
                        "submitted": submitted,
                        "completed": completed,
                        "checkout_to_outcome": {"p95_ms": p95},
                    },
                    **({"nats": nats} if nats is not None else {}),
                }
            ),
            encoding="utf-8",
        )

    @staticmethod
    def _write_resource_result(
        path: Path,
        users: int,
        completed: int,
        services: dict[str, tuple[float, int]],
    ) -> None:
        path.write_text(
            json.dumps(
                {
                    "application_type": "GRPC",
                    "workload": "closed",
                    "users": users,
                    "business": {
                        "submitted": completed,
                        "completed": completed,
                        "checkout_to_outcome": {"p95_ms": 10},
                    },
                    "resources": {
                        "available": True,
                        "by_service": {
                            service: {
                                "cpu_seconds": cpu_seconds,
                                "average_memory_bytes": average_memory,
                            }
                            for service, (
                                cpu_seconds,
                                average_memory,
                            ) in services.items()
                        },
                    },
                }
            ),
            encoding="utf-8",
        )

    @staticmethod
    def _nats_resource_sample(elapsed: float, waiting: float) -> dict:
        return {
            "elapsed_seconds": elapsed,
            "phase": "steady",
            "nats_metrics": [
                {
                    "name": "jetstream_consumer_num_pending",
                    "value": waiting,
                    "labels": {
                        "stream_name": "BOUTIQUE_EVENTS",
                        "consumer_name": "checkout-v1",
                    },
                },
                # Replicated exporter series must not be double counted.
                {
                    "name": "jetstream_consumer_num_pending",
                    "value": waiting,
                    "labels": {
                        "stream_name": "BOUTIQUE_EVENTS",
                        "consumer_name": "checkout-v1",
                    },
                },
            ],
        }

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
                        "observed_goodput_orders_per_second": 9.0,
                        "business": {
                            "goodput_orders_per_second": 9.5,
                            "checkout_to_outcome": {"p95_ms": 125},
                        },
                        "nats": {"consumer_pending": {"max": 2}},
                    },
                    {
                        "target_requests_per_second": 20,
                        "observed_goodput_orders_per_second": 17.0,
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
