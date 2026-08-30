# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from parse_results import (
    _axis_limit,
    Result,
    ResultError,
    average_interpolated_saturation_pending_points,
    average_saturation_points,
    collect_closed_nats_waiting_series,
    collect_closed_resource_measurements,
    fault_tolerance_markers,
    find_results,
    main,
    process_folder,
    saturation_interpolated_pending_points,
    saturation_latency_timeout_points,
    saturation_points,
    write_average_saturation_graphs,
    write_fault_tolerance_graphs,
    write_png_chart,
    write_png_multi_series_chart,
    write_resource_usage_tables,
    write_saturation_graphs,
)


class ParseResultsTest(unittest.TestCase):
    def test_axis_limit_produces_round_tick_intervals(self) -> None:
        self.assertEqual(150.0, _axis_limit(120.0))
        self.assertEqual(35_000.0, _axis_limit(30_000.0, headroom=1.1))

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
                    folder / "closed_completed_orders.png",
                ],
                outputs,
            )
            table = outputs[0].read_text(encoding="utf-8")
            self.assertIn("Users & Orders & Success rate & P95 & std", table)
            self.assertIn(r"10 & 5 & 100.00\% & 50.000 & 0.000", table)
            self.assertIn(r"1500 & 20 & 75.00\% & 200.000 & 100.000", table)
            self.assert_png(folder / "closed_p95_latency.png")
            self.assert_png(folder / "closed_completed_orders.png")

    def test_writes_closed_latency_comparison_graph(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            folder = root / "current"
            comparison = root / "comparison"
            folder.mkdir()
            comparison.mkdir()
            self._write_result(
                folder / "run-a.json", "closed", 10, 10, 8, 100
            )
            self._write_result(
                folder / "run-b.json", "closed", 10, 10, 10, 120
            )
            self._write_result(
                comparison / "run-a.json", "closed", 10, 20, 14, 200
            )
            self._write_result(
                comparison / "run-b.json", "closed", 10, 20, 18, 240
            )

            with mock.patch(
                "parse_results.write_png_multi_series_chart",
                wraps=write_png_multi_series_chart,
            ) as write_chart:
                outputs = process_folder(
                    folder, closed_compare=comparison
                )

            comparison_path = folder / "closed_p95_latency_comparison.png"
            completed_path = (
                folder / "closed_completed_orders_comparison.png"
            )
            self.assertIn(comparison_path, outputs)
            self.assertIn(completed_path, outputs)
            self.assert_png(comparison_path)
            self.assert_png(completed_path)
            latency_call, completed_call = write_chart.call_args_list
            self.assertEqual(
                [
                    ("Prvotna aplikacija", [(10.0, 110.0)]),
                    ("Predelana aplikacija", [(10.0, 220.0)]),
                ],
                latency_call.args[1],
            )
            self.assertEqual(
                [[(10.0, 10.0)], [(10.0, 20.0)]],
                latency_call.kwargs["error_bars"],
            )
            self.assertEqual(
                [
                    ("Prvotna aplikacija", [(10.0, 9.0)]),
                    ("Predelana aplikacija", [(10.0, 16.0)]),
                ],
                completed_call.args[1],
            )
            self.assertEqual(
                [[(10.0, 1.0)], [(10.0, 2.0)]],
                completed_call.kwargs["error_bars"],
            )

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
                steady_seconds=3,
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
                steady_seconds=3,
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
                steady_seconds=3,
            )

            with mock.patch(
                "parse_results.write_png_multi_series_chart",
                wraps=write_png_multi_series_chart,
            ) as write_chart:
                outputs = process_folder(folder, closed_infer_end=True)

            waiting_path = folder / "closed_nats_waiting_events.png"
            self.assertIn(waiting_path, outputs)
            self.assert_png(waiting_path)
            self.assertEqual(
                [
                    [(0.0, 1.0), (1.0, 2.0), (2.0, 2.0)],
                    [(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)],
                ],
                write_chart.call_args.kwargs["deviation_bands"],
            )
            self.assertEqual(3, write_chart.call_args.kwargs["x_limit"])

    def test_closed_end_inference_is_opt_in(self) -> None:
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
                waiting_events=[(0, 3), (2, 7)],
                steady_seconds=5,
            )
            results = find_results(folder)

            observed = collect_closed_nats_waiting_series(results)
            inferred = collect_closed_nats_waiting_series(
                results, infer_end=True
            )

            self.assertEqual((2.0, 7.0), observed[10][-1])
            self.assertEqual(
                [(2.0, 7.0), (3.0, 7.0), (4.0, 7.0)],
                inferred[10][-3:],
            )

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

    def test_rebases_waiting_series_and_excludes_drain(self) -> None:
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
                waiting_events=[
                    (30, 1_683),
                    (35, 1_197),
                    (39, 500),
                    (40, 400),
                    (45, 1),
                ],
            )
            summary = json.loads(path.read_text(encoding="utf-8"))
            summary["warmup_seconds"] = 30
            summary["steady_seconds"] = 10
            path.write_text(json.dumps(summary), encoding="utf-8")

            series = collect_closed_nats_waiting_series(find_results(folder))

            self.assertEqual(
                {
                    10: [
                        (0.0, 1_683.0),
                        (1.0, 1_683.0),
                        (2.0, 1_683.0),
                        (3.0, 1_683.0),
                        (4.0, 1_683.0),
                        (5.0, 1_197.0),
                        (6.0, 1_197.0),
                        (7.0, 1_197.0),
                        (8.0, 1_197.0),
                        (9.0, 500.0),
                    ]
                },
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
                            {
                                "elapsed_seconds": 0,
                                "phase": "steady",
                                "max_outstanding": 3,
                            },
                            {
                                "elapsed_seconds": 2,
                                "phase": "steady",
                                "max_outstanding": 7,
                            },
                            {
                                "elapsed_seconds": 3,
                                "phase": "drain",
                                "max_outstanding": 1,
                            },
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
                    "nats": (100, 1_000_000_000),
                },
            )
            self._write_resource_result(
                folder / "users-10-b.json",
                10,
                20,
                {
                    "frontend": (60, 120_000_000),
                    "checkout_service": (40, 70_000_000),
                    "nats": (200, 2_000_000_000),
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
            self.assertNotIn("nats &", table)

    def test_combines_nats_storefront_resource_services(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            self._write_resource_result(
                folder / "summary.json",
                10,
                10,
                {
                    "storefrontprojectionservice": (20, 100_000_000),
                    "storefrontqueryservice": (10, 50_000_000),
                    "nats": (40, 200_000_000),
                },
                application_type="NATS",
            )

            destination = folder / "resource-usage.tex"
            write_resource_usage_tables(
                destination,
                collect_closed_resource_measurements(find_results(folder)),
            )

            table = destination.read_text(encoding="utf-8")
            self.assertIn(
                "storefrontprojectionservice & 3.000000 & 0.000000 & "
                "150.000 & 0.000",
                table,
            )
            self.assertIn(
                "nats & 4.000000 & 0.000000 & 200.000 & 0.000",
                table,
            )
            self.assertIn(
                "Total & 7.000000 & 0.000000 & 350.000 & 0.000",
                table,
            )
            self.assertNotIn("storefrontqueryservice", table)

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
                    nested
                    / "nats-summary_max_pending_events_interpolated.png",
                },
                set(outputs),
            )
            for output in outputs:
                self.assert_png(output)

    def test_limits_saturation_data_by_requested_rate(self) -> None:
        summary = self._saturation_summary("NATS")
        excluded_rung = summary["saturation"]["rungs"][1]
        excluded_rung["processing_goodput_orders_per_second"] = "invalid"
        excluded_rung["business"]["goodput_orders_per_second"] = "invalid"
        excluded_rung["business"]["checkout_to_outcome"]["p95_ms"] = (
            "invalid"
        )
        excluded_rung["nats"]["consumer_pending"]["max"] = "invalid"
        result = Result(path=Path("summary.json"), summary=summary)

        points = saturation_points(result, saturation_limit=10)

        self.assertEqual([(10.0, 9.5)], points[0])
        self.assertEqual([(10.0, 9.2)], points[1])
        self.assertEqual([(10.0, 2.0)], points[2])
        self.assertEqual([(10.0, 125.0)], points[3])
        self.assertEqual(
            [], saturation_latency_timeout_points(result, saturation_limit=10)
        )

    def test_limit_ignores_pending_samples_for_excluded_rungs(self) -> None:
        summary = self._saturation_summary("NATS")
        first, second = summary["saturation"]["rungs"]
        first.update(
            {
                "started_elapsed_seconds": 0,
                "ended_elapsed_seconds": 10,
                "nats": {"available": False},
            }
        )
        second.update(
            {
                "started_elapsed_seconds": 11,
                "ended_elapsed_seconds": 20,
                "nats": {"available": False},
            }
        )
        summary["nats"] = {
            "consumer_pending": {
                "series": [
                    {"elapsed_seconds": 5, "waiting_events": 3},
                    {"elapsed_seconds": 15, "waiting_events": "invalid"},
                ]
            }
        }

        _, _, pending, _ = saturation_points(
            Result(path=Path("summary.json"), summary=summary),
            saturation_limit=10,
        )

        self.assertEqual([(10.0, 3.0)], pending)

    def test_main_passes_saturation_limit_to_result_processing(self) -> None:
        with mock.patch(
            "parse_results.process_folder", return_value=[]
        ) as process:
            exit_code = main(
                [
                    "results",
                    "--saturation-limit",
                    "15.5",
                    "--closed-infer-end",
                    "--closed-compare",
                    "comparison-results",
                    "--saturation-add-accepted",
                ]
            )

        self.assertEqual(0, exit_code)
        process.assert_called_once_with(
            Path("results"),
            15.5,
            True,
            True,
            Path("comparison-results"),
        )

    def test_process_folder_applies_saturation_limit_to_graphs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            (folder / "summary.json").write_text(
                json.dumps(self._saturation_summary("NATS")),
                encoding="utf-8",
            )

            with (
                mock.patch(
                    "parse_results.write_png_multi_series_chart"
                ) as write_multi_series,
                mock.patch(
                    "parse_results.write_png_chart"
                ) as write_chart,
            ):
                process_folder(
                    folder,
                    saturation_limit=10,
                    saturation_add_accepted=True,
                )

        self.assertEqual(
            [
                ("Zaključena znotraj 30s oddaje", [(10.0, 9.5)]),
                (
                    "Zaključena tekom obremenitvene stopnice",
                    [(10.0, 9.2)],
                ),
                ("Sprejeta naročila", [(10.0, 9.8)]),
            ],
            write_multi_series.call_args_list[0].args[1],
        )
        self.assertEqual(
            [(10.0, 2.0)], write_chart.call_args_list[0].args[1]
        )
        self.assertEqual(
            [(10.0, 2.0)], write_chart.call_args_list[1].args[1]
        )

    def test_averages_saturation_runs_with_population_standard_deviation(
        self,
    ) -> None:
        first = self._saturation_summary("GRPC")
        second = self._saturation_summary("GRPC")
        second_rungs = second["saturation"]["rungs"]
        second_rungs[0]["business"]["goodput_orders_per_second"] = 11.5
        second_rungs[1]["business"]["goodput_orders_per_second"] = 20.5
        second_rungs[0]["processing_goodput_orders_per_second"] = 11.2
        second_rungs[1]["processing_goodput_orders_per_second"] = 19.5
        second_rungs[0]["business"]["checkout_to_outcome"]["p95_ms"] = 175
        second_rungs[1]["business"]["checkout_to_outcome"]["p95_ms"] = 350

        measurements = average_saturation_points(
            [
                Result(path=Path("first.json"), summary=first),
                Result(path=Path("second.json"), summary=second),
            ]
        )

        self.assertEqual([(10.0, 10.5), (20.0, 19.5)], measurements[0])
        self.assertEqual([(10.0, 1.0), (20.0, 1.0)], measurements[1])
        self.assertEqual([(10.0, 10.2), (20.0, 18.5)], measurements[2])
        self.assertEqual([(10.0, 1.0), (20.0, 1.0)], measurements[3])
        self.assertEqual([(10.0, 150.0), (20.0, 300.0)], measurements[4])
        self.assertEqual([(10.0, 25.0), (20.0, 50.0)], measurements[5])

    def test_writes_average_saturation_graphs_for_matching_durations(
        self,
    ) -> None:
        results: list[Result] = []
        for name, duration in (("first", 60), ("second", 60), ("third", 90)):
            summary = self._saturation_summary("GRPC")
            summary["configured_steady_seconds"] = duration
            results.append(Result(path=Path(f"{name}.json"), summary=summary))

        with (
            mock.patch(
                "parse_results.write_png_multi_series_chart"
            ) as write_multi_series,
            mock.patch("parse_results.write_png_chart"),
        ):
            outputs = write_average_saturation_graphs(Path("results"), results)

        self.assertEqual(
            [
                Path("results/average_goodput_20.png"),
                Path("results/average_latency_20.png"),
            ],
            outputs,
        )
        goodput_call, latency_call = write_multi_series.call_args_list
        self.assertEqual(
            [[(10.0, 0.0), (20.0, 0.0)]] * 2,
            goodput_call.kwargs["deviation_bands"],
        )
        self.assertEqual(
            [[(10.0, 0.0), (20.0, 0.0)], None],
            latency_call.kwargs["deviation_bands"],
        )
        self.assertEqual(["-", "--"], latency_call.kwargs["line_styles"])
        for chart_call in write_multi_series.call_args_list:
            self.assertNotIn("error_bars", chart_call.kwargs)
        write_chart.assert_not_called()

    def test_adds_accepted_orders_to_average_saturation_graph(self) -> None:
        results: list[Result] = []
        for name, accepted in (
            ("first", (98, 195)),
            ("second", (100, 190)),
        ):
            summary = self._saturation_summary("GRPC")
            summary["configured_steady_seconds"] = 60
            for rung, accepted_orders in zip(
                summary["saturation"]["rungs"], accepted
            ):
                rung["business"]["accepted"] = accepted_orders
            results.append(Result(Path(f"{name}.json"), summary))

        with mock.patch(
            "parse_results.write_png_multi_series_chart"
        ) as write_chart:
            write_average_saturation_graphs(
                Path("results"), results, add_accepted=True
            )

        goodput_call = write_chart.call_args_list[0]
        self.assertEqual(
            (
                "Sprejeta naročila",
                [(10.0, 9.9), (20.0, 19.25)],
            ),
            goodput_call.args[1][-1],
        )
        accepted_deviations = goodput_call.kwargs["deviation_bands"][-1]
        self.assertEqual(
            [10.0, 20.0],
            [point[0] for point in accepted_deviations],
        )
        self.assertAlmostEqual(0.1, accepted_deviations[0][1])
        self.assertAlmostEqual(0.25, accepted_deviations[1][1])
        self.assertEqual("#ff7f0e", goodput_call.kwargs["colors"][-1])

    def test_average_latency_uses_timeout_when_any_run_has_no_data(self) -> None:
        first = self._saturation_summary("GRPC")
        second = self._saturation_summary("GRPC")
        for summary in (first, second):
            summary["configured_steady_seconds"] = 60
        second["saturation"]["rungs"][1]["business"][
            "checkout_to_outcome"
        ]["p95_ms"] = None

        with mock.patch(
            "parse_results.write_png_multi_series_chart"
        ) as write_multi_series:
            write_average_saturation_graphs(
                Path("results"),
                [
                    Result(path=Path("first.json"), summary=first),
                    Result(path=Path("second.json"), summary=second),
                ],
            )

        latency_call = write_multi_series.call_args_list[1]
        self.assertEqual(
            [
                ("Povprečna izmerjena P95 latenca", [(10.0, 125.0)]),
                (
                    "30 s časovna omejitev",
                    [(10.0, 125.0), (20.0, 30_000.0)],
                ),
            ],
            latency_call.args[1],
        )
        self.assertEqual(
            [[(10.0, 0.0)], None],
            latency_call.kwargs["deviation_bands"],
        )

    def test_averages_interpolated_nats_waiting_events(self) -> None:
        results: list[Result] = []
        for name, pending_values in (
            ("first", (10, 10, 30)),
            ("second", (20, 20, 40)),
        ):
            summary = self._saturation_summary("NATS")
            rungs = summary["saturation"]["rungs"]
            rungs.insert(
                1,
                {
                    **rungs[1],
                    "target_requests_per_second": 15,
                    "nats": {"consumer_pending": {"max": pending_values[1]}},
                },
            )
            for rung, pending in zip(rungs, pending_values):
                rung["nats"]["consumer_pending"]["max"] = pending
            results.append(Result(path=Path(f"{name}.json"), summary=summary))

        points, deviations = average_interpolated_saturation_pending_points(
            results
        )

        self.assertEqual(
            [(10.0, 15.0), (15.0, 25.0), (20.0, 35.0)], points
        )
        self.assertEqual(
            [(10.0, 5.0), (15.0, 5.0), (20.0, 5.0)], deviations
        )

    def test_writes_average_nats_waiting_events_chart(self) -> None:
        results: list[Result] = []
        for name in ("first", "second"):
            summary = self._saturation_summary("NATS")
            summary["configured_steady_seconds"] = 60
            results.append(Result(path=Path(f"{name}.json"), summary=summary))

        with (
            mock.patch(
                "parse_results.write_png_multi_series_chart"
            ) as write_multi_series,
            mock.patch("parse_results.write_png_chart") as write_chart,
        ):
            outputs = write_average_saturation_graphs(Path("results"), results)

        waiting_path = Path("results/average_waiting_events_20.png")
        self.assertIn(waiting_path, outputs)
        self.assertEqual(waiting_path, write_chart.call_args.args[0])
        self.assertEqual(
            [(10.0, 2.0), (20.0, 7.0)], write_chart.call_args.args[1]
        )
        self.assertEqual(
            [(10.0, 0.0), (20.0, 0.0)],
            write_chart.call_args.kwargs["deviation_band"],
        )
        latency_call = write_multi_series.call_args_list[1]
        self.assertEqual(
            (
                "P95 sprejema naročila",
                [(10.0, 25.0), (20.0, 50.0)],
            ),
            latency_call.args[1][1],
        )
        self.assertEqual(
            [(10.0, 0.0), (20.0, 0.0)],
            latency_call.kwargs["deviation_bands"][1],
        )

    def test_keeps_per_run_plots_when_writing_saturation_averages(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            for name in ("first", "second"):
                summary = self._saturation_summary("GRPC")
                summary["configured_steady_seconds"] = 60
                (folder / f"{name}.json").write_text(
                    json.dumps(summary), encoding="utf-8"
                )

            outputs = process_folder(folder)

            self.assertEqual(
                {
                    folder / "first_goodput.png",
                    folder / "first_p95_latency.png",
                    folder / "second_goodput.png",
                    folder / "second_p95_latency.png",
                    folder / "average_goodput_20.png",
                    folder / "average_latency_20.png",
                },
                set(outputs),
            )
            for output in outputs:
                self.assert_png(output)

    def test_uses_submitted_and_processed_goodput_for_saturation_graph(
        self,
    ) -> None:
        summary = self._saturation_summary("NATS")

        submitted, processed, _, _ = saturation_points(
            Result(path=Path("summary.json"), summary=summary)
        )

        self.assertEqual([(10.0, 9.5), (20.0, 18.5)], submitted)
        self.assertEqual([(10.0, 9.2), (20.0, 17.5)], processed)

    def test_does_not_replace_unavailable_processing_goodput_with_client_count(
        self,
    ) -> None:
        summary = self._saturation_summary("NATS")
        summary["saturation"]["rungs"][0][
            "processing_goodput_orders_per_second"
        ] = None

        _, processed, _, _ = saturation_points(
            Result(path=Path("summary.json"), summary=summary)
        )

        self.assertEqual([(20.0, 17.5)], processed)

    def test_parses_saturation_when_nats_metrics_are_unavailable(self) -> None:
        summary = self._saturation_summary("NATS")
        for rung in summary["saturation"]["rungs"]:
            rung["nats"] = {
                "available": False,
                "reason": "no steady-state samples",
            }

        submitted, processed, pending, latency = saturation_points(
            Result(path=Path("summary.json"), summary=summary)
        )

        self.assertEqual([(10.0, 9.5), (20.0, 18.5)], submitted)
        self.assertEqual([(10.0, 9.2), (20.0, 17.5)], processed)
        self.assertEqual([], pending)
        self.assertEqual([(10.0, 125.0), (20.0, 250.0)], latency)

    def test_derives_saturation_pending_maxima_from_top_level_series(self) -> None:
        summary = self._saturation_summary("NATS")
        first, second = summary["saturation"]["rungs"]
        first.update(
            {
                "started_elapsed_seconds": 30.2,
                "ended_elapsed_seconds": 60.2,
                "pending_start": 1,
                "pending_end": 3,
                "nats": {"available": False},
            }
        )
        second.update(
            {
                "started_elapsed_seconds": 60.3,
                "ended_elapsed_seconds": 90.3,
                "pending_start": 3,
                "pending_end": 8,
                "nats": {"available": False},
            }
        )
        summary["nats"] = {
            "consumer_pending": {
                "series": [
                    {"elapsed_seconds": 31, "waiting_events": 2},
                    {"elapsed_seconds": 45, "waiting_events": 7},
                    {"elapsed_seconds": 61, "waiting_events": 5},
                    {"elapsed_seconds": 75, "waiting_events": 11},
                ]
            }
        }

        _, _, pending, _ = saturation_points(
            Result(path=Path("summary.json"), summary=summary)
        )

        self.assertEqual([(10.0, 7.0), (20.0, 11.0)], pending)

    def test_interpolates_missing_saturation_pending_rungs(self) -> None:
        summary = self._saturation_summary("NATS")
        rungs = summary["saturation"]["rungs"]
        rungs[0]["nats"]["consumer_pending"]["max"] = 10
        rungs.insert(
            1,
            {
                **rungs[1],
                "target_requests_per_second": 15,
                "nats": {"consumer_pending": {"max": 10}},
            },
        )
        rungs[2]["nats"]["consumer_pending"]["max"] = 30

        points = saturation_interpolated_pending_points(
            Result(path=Path("summary.json"), summary=summary)
        )

        self.assertEqual([(10.0, 10.0), (15.0, 20.0), (20.0, 30.0)], points)

    def test_continues_null_saturation_latency_at_timeout(self) -> None:
        summary = self._saturation_summary("NATS")
        summary["saturation"]["rungs"][1]["business"][
            "checkout_to_outcome"
        ]["p95_ms"] = None

        points = saturation_latency_timeout_points(
            Result(path=Path("summary.json"), summary=summary)
        )

        self.assertEqual([(10.0, 125.0), (20.0, 30_000.0)], points)

    def test_writes_single_rung_nats_saturation_pngs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            summary = self._saturation_summary("NATS")
            summary["saturation"]["rungs"] = summary["saturation"]["rungs"][:1]
            (folder / "summary.json").write_text(
                json.dumps(summary), encoding="utf-8"
            )

            outputs = process_folder(folder)

            self.assertEqual(4, len(outputs))
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

    def test_multi_series_chart_uses_explicit_colors(self) -> None:
        with (
            mock.patch("parse_results.Figure") as figure_constructor,
            mock.patch("parse_results.FigureCanvasAgg"),
        ):
            axes = figure_constructor.return_value.subplots.return_value

            write_png_multi_series_chart(
                Path("chart.png"),
                [
                    ("First", [(1.0, 2.0)]),
                    ("Second", [(1.0, 3.0)]),
                ],
                title="Chart",
                y_label="Values",
                x_label="Time",
                colors=["blue", "green"],
            )

        self.assertEqual(
            ["blue", "green"],
            [call.kwargs["color"] for call in axes.plot.call_args_list],
        )

    def test_multi_series_chart_draws_standard_deviation_band(self) -> None:
        with (
            mock.patch("parse_results.Figure") as figure_constructor,
            mock.patch("parse_results.FigureCanvasAgg"),
        ):
            axes = figure_constructor.return_value.subplots.return_value

            write_png_multi_series_chart(
                Path("chart.png"),
                [("Mean", [(10.0, 100.0), (20.0, 200.0)])],
                title="Chart",
                y_label="Values",
                x_label="Rate",
                colors=["blue"],
                deviation_bands=[[(10.0, 5.0), (20.0, 20.0)]],
                x_limit=20.0,
            )

        band = axes.fill_between.call_args
        self.assertEqual([10.0, 20.0], band.args[0])
        self.assertEqual([95.0, 180.0], band.args[1])
        self.assertEqual([105.0, 220.0], band.args[2])
        self.assertEqual("blue", band.kwargs["color"])
        self.assertEqual(0.2, band.kwargs["alpha"])
        axes.set_xlim.assert_called_once_with(0, 20.0)
        axes.errorbar.assert_not_called()

    def test_multi_series_chart_allows_timeout_without_deviation_band(
        self,
    ) -> None:
        with (
            mock.patch("parse_results.Figure") as figure_constructor,
            mock.patch("parse_results.FigureCanvasAgg"),
        ):
            axes = figure_constructor.return_value.subplots.return_value

            write_png_multi_series_chart(
                Path("chart.png"),
                [
                    ("Measured", []),
                    ("Timeout", [(10.0, 30_000.0)]),
                ],
                title="Chart",
                y_label="Latency",
                x_label="Rate",
                deviation_bands=[[], None],
            )

        axes.fill_between.assert_not_called()
        self.assertEqual(1, axes.plot.call_count)

    def test_saturation_goodput_lines_use_requested_colors(self) -> None:
        result = Result(
            path=Path("summary.json"),
            summary=self._saturation_summary("NATS"),
        )

        with (
            mock.patch(
                "parse_results.write_png_multi_series_chart"
            ) as write_multi_series,
            mock.patch("parse_results.write_png_chart"),
        ):
            write_saturation_graphs(result)

        self.assertEqual(
            ["#1f77b4", "#2ca02c"],
            write_multi_series.call_args_list[0].kwargs["colors"],
        )

    def test_adds_nats_checkout_acceptance_p95_to_latency_graph(self) -> None:
        result = Result(
            path=Path("summary.json"),
            summary=self._saturation_summary("NATS"),
        )

        with (
            mock.patch(
                "parse_results.write_png_multi_series_chart"
            ) as write_multi_series,
            mock.patch("parse_results.write_png_chart"),
        ):
            write_saturation_graphs(result)

        latency_call = write_multi_series.call_args_list[1]
        self.assertEqual(
            (
                "P95 sprejema naročila",
                [(10.0, 25.0), (20.0, 50.0)],
            ),
            latency_call.args[1][1],
        )
        self.assertEqual(
            ["#1f77b4", "#ff7f0e", "#1f77b4"],
            latency_call.kwargs["colors"],
        )
        self.assertEqual(
            ["-", "-", "--"],
            latency_call.kwargs["line_styles"],
        )

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

    def test_fault_tolerance_graph_includes_accepted_orders(self) -> None:
        result = Result(
            Path("fault-summary.json"),
            self._fault_tolerance_summary("NATS"),
        )

        with (
            mock.patch(
                "parse_results.write_png_multi_series_chart"
            ) as write_multi_series,
            mock.patch("parse_results.write_png_chart") as write_chart,
        ):
            write_fault_tolerance_graphs(result)

        successful_call = write_multi_series.call_args
        self.assertEqual(
            [
                (
                    "Uspešno obdelana naročila",
                    [(0.0, 10.0), (1.0, 7.0)],
                ),
                ("Sprejeta naročila", [(0.0, 12.0), (1.0, 9.0)]),
            ],
            successful_call.args[1],
        )
        self.assertEqual(
            ["#1f77b4", "#2ca02c"], successful_call.kwargs["colors"]
        )
        self.assertEqual(
            fault_tolerance_markers(result),
            successful_call.kwargs["event_markers"],
        )
        self.assertEqual(2.0, successful_call.kwargs["x_limit"])
        self.assertEqual(
            [(1.0, 4.0)], write_chart.call_args.args[1]
        )
        self.assertEqual(2.0, write_chart.call_args.kwargs["x_limit"])

    def test_fault_tolerance_markers_use_service_colors(self) -> None:
        summary = self._fault_tolerance_summary("NATS")

        markers = fault_tolerance_markers(
            Result(Path("fault-summary.json"), summary)
        )

        self.assertEqual(
            [
                (0.2, "paymentservice onemogočen", "red", "v"),
                (0.4, "paymentservice omogočen", "red", "^"),
                (0.6, "shippingservice onemogočen", "orange", "v"),
                (0.8, "shippingservice omogočen", "orange", "^"),
            ],
            markers,
        )

    def test_chart_draws_directional_event_marker_columns(self) -> None:
        with (
            mock.patch("parse_results.Figure") as figure_constructor,
            mock.patch("parse_results.FigureCanvasAgg"),
        ):
            axes = figure_constructor.return_value.subplots.return_value

            write_png_chart(
                Path("chart.png"),
                [(0.0, 1.0), (3.0, 2.0)],
                title="Chart",
                y_label="Values",
                event_markers=[
                    (1.0, "disabled", "red", "v"),
                    (2.0, "enabled", "red", "^"),
                ],
            )

        disabled, enabled = axes.scatter.call_args_list
        expected_y_positions = [step / 16 for step in range(17)]
        self.assertEqual(
            ([1.0] * 17, expected_y_positions), disabled.args
        )
        self.assertEqual("v", disabled.kwargs["marker"])
        self.assertEqual(
            ([2.0] * 17, expected_y_positions), enabled.args
        )
        self.assertEqual("^", enabled.kwargs["marker"])
        axes.axvline.assert_not_called()

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
        steady_seconds: float | None = None,
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
                    **(
                        {"steady_seconds": steady_seconds}
                        if steady_seconds is not None
                        else {}
                    ),
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
        *,
        application_type: str = "GRPC",
    ) -> None:
        path.write_text(
            json.dumps(
                {
                    "application_type": application_type,
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
                        "duration_seconds": 10,
                        "observed_goodput_orders_per_second": 9.0,
                        "processing_goodput_orders_per_second": 9.2,
                        "business": {
                            "accepted": 98,
                            "goodput_orders_per_second": 9.5,
                            "checkout_acceptance": {"p95_ms": 25},
                            "checkout_to_outcome": {"p95_ms": 125},
                        },
                        "nats": {"consumer_pending": {"max": 2}},
                    },
                    {
                        "target_requests_per_second": 20,
                        "duration_seconds": 10,
                        "observed_goodput_orders_per_second": 17.0,
                        "processing_goodput_orders_per_second": 17.5,
                        "business": {
                            "accepted": 195,
                            "goodput_orders_per_second": 18.5,
                            "checkout_acceptance": {"p95_ms": 50},
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
            "warmup_seconds": 0,
            "steady_seconds": 2,
            "business": {"submitted": 20, "completed": 17},
            "fault_tolerance": {
                "faults": [
                    {
                        "service": "paymentservice",
                        "disabled_at_elapsed_seconds": 0.2,
                        "reenabled_at_elapsed_seconds": 0.4,
                    },
                    {
                        "service": "shippingservice",
                        "disabled_at_elapsed_seconds": 0.6,
                        "reenabled_at_elapsed_seconds": 0.8,
                    },
                ],
                "per_second": [
                    {
                        "elapsed_seconds": 0,
                        "accepted_requests": 12,
                        "successfully_processed": 10,
                        "nats_waiting_events": None,
                    },
                    {
                        "elapsed_seconds": 1,
                        "accepted_requests": 9,
                        "successfully_processed": 7,
                        "nats_waiting_events": 4,
                    },
                    {
                        "elapsed_seconds": 2,
                        "phase": "drain",
                        "accepted_requests": 0,
                        "successfully_processed": 3,
                        "nats_waiting_events": 2,
                    },
                ]
            },
        }


if __name__ == "__main__":
    unittest.main()
