#!/usr/bin/env python3
"""Create charts and a LaTeX table from benchmark summary JSON files."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape


class ResultError(ValueError):
    """Raised when benchmark results cannot be analyzed."""


@dataclass(frozen=True)
class Result:
    path: Path
    summary: dict[str, Any]


@dataclass
class ClosedMeasurements:
    orders: int = 0
    completed: int = 0
    p95_values: list[float] = field(default_factory=list)


def _number(value: Any, field_name: str, source: Path) -> float:
    if isinstance(value, bool):
        raise ResultError(f"{source}: {field_name} must be a number")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ResultError(f"{source}: {field_name} must be a number") from error
    if not math.isfinite(number):
        raise ResultError(f"{source}: {field_name} must be finite")
    return number


def _integer(value: Any, field_name: str, source: Path) -> int:
    number = _number(value, field_name, source)
    if not number.is_integer():
        raise ResultError(f"{source}: {field_name} must be an integer")
    return int(number)


def find_results(folder: Path) -> list[Result]:
    """Find benchmark summaries while ignoring config and status JSON files."""
    if not folder.is_dir():
        raise ResultError(f"results folder does not exist: {folder}")

    results: list[Result] = []
    for path in sorted(folder.rglob("*.json")):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except OSError as error:
            raise ResultError(f"cannot read {path}: {error}") from error
        except json.JSONDecodeError as error:
            raise ResultError(f"invalid JSON in {path}: {error}") from error

        # Standalone run directories also contain config.json and status.json.
        # A generated summary is distinguished by its business section.
        if (
            isinstance(value, dict)
            and "workload" in value
            and isinstance(value.get("business"), dict)
        ):
            results.append(Result(path=path, summary=value))

    if not results:
        raise ResultError(f"no benchmark result JSON files found in {folder}")
    return results


def _nested(document: dict[str, Any], *keys: str) -> Any:
    value: Any = document
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def saturation_points(
    result: Result,
) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    rungs = _nested(result.summary, "saturation", "rungs")
    if not isinstance(rungs, list) or not rungs:
        raise ResultError(f"{result.path}: saturation.rungs must be a non-empty list")

    goodput_points: list[tuple[float, float]] = []
    pending_points: list[tuple[float, float]] = []
    is_nats = str(result.summary.get("application_type", "")).upper() == "NATS"
    for index, rung in enumerate(rungs):
        if not isinstance(rung, dict):
            raise ResultError(
                f"{result.path}: saturation.rungs[{index}] must be an object"
            )
        rate = _number(
            rung.get("target_requests_per_second"),
            f"saturation.rungs[{index}].target_requests_per_second",
            result.path,
        )
        goodput_value = _nested(rung, "business", "goodput_orders_per_second")
        if goodput_value is None:
            goodput_value = rung.get("observed_goodput_orders_per_second")
        goodput_points.append(
            (
                rate,
                _number(
                    goodput_value,
                    f"saturation.rungs[{index}].business.goodput_orders_per_second",
                    result.path,
                ),
            )
        )

        if is_nats:
            pending_points.append(
                (
                    rate,
                    _number(
                        _nested(rung, "nats", "consumer_pending", "max"),
                        f"saturation.rungs[{index}].nats.consumer_pending.max",
                        result.path,
                    ),
                )
            )

    return sorted(goodput_points), sorted(pending_points)


def _display_number(value: float) -> str:
    if value == 0:
        return "0"
    if abs(value) >= 1_000 or abs(value) < 0.01:
        return f"{value:.3g}"
    return f"{value:.2f}".rstrip("0").rstrip(".")


def write_svg_chart(
    destination: Path,
    points: list[tuple[float, float]],
    *,
    title: str,
    y_label: str,
) -> None:
    if not points:
        raise ResultError(f"cannot create {destination}: graph has no points")

    width, height = 960, 600
    left, right, top, bottom = 95, 35, 60, 80
    plot_width = width - left - right
    plot_height = height - top - bottom
    maximum_x = max(point[0] for point in points)
    maximum_y = max(point[1] for point in points)
    x_limit = maximum_x if maximum_x > 0 else 1.0
    y_limit = maximum_y * 1.1 if maximum_y > 0 else 1.0

    def x_position(value: float) -> float:
        return left + value / x_limit * plot_width

    def y_position(value: float) -> float:
        return top + plot_height - value / y_limit * plot_height

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        (
            '<svg xmlns="http://www.w3.org/2000/svg" '
            f'viewBox="0 0 {width} {height}" role="img">'
        ),
        f"  <title>{escape(title)}</title>",
        (
            "  <style>text{font-family:sans-serif;fill:#202124}"
            ".grid{stroke:#dadce0;stroke-width:1}"
            ".axis{stroke:#3c4043;stroke-width:1.5}"
            ".series{fill:none;stroke:#1967d2;stroke-width:3}"
            ".point{fill:#1967d2}</style>"
        ),
        "  <rect width=" + f'"{width}" height="{height}" fill="#fff"/>',
        (
            f'  <text x="{width / 2:g}" y="32" font-size="22" '
            f'text-anchor="middle">{escape(title)}</text>'
        ),
    ]

    for tick in range(6):
        fraction = tick / 5
        x = left + fraction * plot_width
        y = top + plot_height - fraction * plot_height
        lines.extend(
            [
                (
                    f'  <line class="grid" x1="{left}" y1="{y:.2f}" '
                    f'x2="{left + plot_width}" y2="{y:.2f}"/>'
                ),
                (
                    f'  <text x="{left - 12}" y="{y + 5:.2f}" '
                    f'font-size="13" text-anchor="end">'
                    f'{escape(_display_number(fraction * y_limit))}</text>'
                ),
                (
                    f'  <line class="grid" x1="{x:.2f}" y1="{top}" '
                    f'x2="{x:.2f}" y2="{top + plot_height}"/>'
                ),
                (
                    f'  <text x="{x:.2f}" '
                    f'y="{top + plot_height + 24}" font-size="13" '
                    f'text-anchor="middle">'
                    f'{escape(_display_number(fraction * x_limit))}</text>'
                ),
            ]
        )

    lines.extend(
        [
            (
                f'  <line class="axis" x1="{left}" '
                f'y1="{top + plot_height}" x2="{left + plot_width}" '
                f'y2="{top + plot_height}"/>'
            ),
            (
                f'  <line class="axis" x1="{left}" y1="{top}" '
                f'x2="{left}" y2="{top + plot_height}"/>'
            ),
            (
                f'  <text x="{left + plot_width / 2:g}" '
                f'y="{height - 24}" font-size="16" '
                'text-anchor="middle">Requests per second</text>'
            ),
            (
                f'  <text x="24" y="{top + plot_height / 2:g}" '
                'font-size="16" text-anchor="middle" '
                f'transform="rotate(-90 24 {top + plot_height / 2:g})">'
                f"{escape(y_label)}</text>"
            ),
        ]
    )

    polyline = " ".join(
        f"{x_position(x):.2f},{y_position(y):.2f}" for x, y in points
    )
    lines.append(f'  <polyline class="series" points="{polyline}"/>')
    for x_value, y_value in points:
        x = x_position(x_value)
        y = y_position(y_value)
        lines.extend(
            [
                f'  <circle class="point" cx="{x:.2f}" cy="{y:.2f}" r="5"/>',
                (
                    f'  <text x="{x:.2f}" y="{y - 10:.2f}" '
                    f'font-size="12" text-anchor="middle">'
                    f'{escape(_display_number(y_value))}</text>'
                ),
            ]
        )
    lines.append("</svg>")
    destination.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_saturation_graphs(result: Result) -> list[Path]:
    goodput, pending = saturation_points(result)
    base = result.path.with_suffix("")
    goodput_path = base.with_name(f"{base.name}_goodput.svg")
    write_svg_chart(
        goodput_path,
        goodput,
        title=f"{result.path.stem}: goodput",
        y_label="Goodput (orders/s)",
    )
    outputs = [goodput_path]
    if pending:
        pending_path = base.with_name(f"{base.name}_max_pending_events.svg")
        write_svg_chart(
            pending_path,
            pending,
            title=f"{result.path.stem}: maximum pending events",
            y_label="Maximum pending events",
        )
        outputs.append(pending_path)
    return outputs


def collect_closed_measurements(results: list[Result]) -> dict[int, ClosedMeasurements]:
    groups: dict[int, ClosedMeasurements] = {}
    for result in results:
        worker_count = _integer(
            result.summary.get("worker_count"), "worker_count", result.path
        )
        if worker_count < 1:
            raise ResultError(f"{result.path}: worker_count must be positive")
        business = result.summary["business"]
        submitted = _integer(
            business.get("submitted"), "business.submitted", result.path
        )
        completed = _integer(
            business.get("completed"), "business.completed", result.path
        )
        if submitted < 0 or completed < 0 or completed > submitted:
            raise ResultError(
                f"{result.path}: invalid submitted/completed order counts"
            )

        group = groups.setdefault(worker_count, ClosedMeasurements())
        group.orders += submitted
        group.completed += completed
        p95_value = _nested(business, "checkout_to_outcome", "p95_ms")
        if p95_value is not None:
            group.p95_values.append(
                _number(p95_value, "business.checkout_to_outcome.p95_ms", result.path)
            )
    return groups


def _latex_number(value: float | None) -> str:
    return "--" if value is None else f"{value:.3f}"


def write_closed_table(
    destination: Path, groups: dict[int, ClosedMeasurements]
) -> None:
    lines = [
        r"\begin{tabular}{rrrrr}",
        r"\hline",
        r"Users & Orders & Success rate & P95 & std \\",
        r"\hline",
    ]
    for worker_count in sorted(groups):
        group = groups[worker_count]
        success_rate = (
            f"{group.completed / group.orders * 100:.2f}\\%"
            if group.orders
            else "--"
        )
        median_p95 = (
            statistics.median(group.p95_values) if group.p95_values else None
        )
        p95_stddev = (
            statistics.pstdev(group.p95_values) if group.p95_values else None
        )
        lines.append(
            f"{worker_count * 1000} & {group.orders} & {success_rate} & "
            f"{_latex_number(median_p95)} & {_latex_number(p95_stddev)} \\\\"
        )
    lines.extend([r"\hline", r"\end{tabular}"])
    destination.write_text("\n".join(lines) + "\n", encoding="utf-8")


def process_folder(folder: Path) -> list[Path]:
    results = find_results(folder)
    for result in results:
        workload = result.summary.get("workload")
        if workload not in {"closed", "saturation"}:
            raise ResultError(f"unsupported workload {workload}")

    outputs: list[Path] = []
    closed_results = [
        result for result in results if result.summary["workload"] == "closed"
    ]
    if closed_results:
        table_path = folder / "closed_results.tex"
        write_closed_table(
            table_path, collect_closed_measurements(closed_results)
        )
        outputs.append(table_path)

    for result in results:
        if result.summary["workload"] == "saturation":
            outputs.extend(write_saturation_graphs(result))
    return outputs


def parser() -> argparse.ArgumentParser:
    argument_parser = argparse.ArgumentParser(
        description="Create graphs and a LaTeX table from benchmark results."
    )
    argument_parser.add_argument(
        "results_folder",
        type=Path,
        help="folder containing benchmark result JSON files",
    )
    return argument_parser


def main(arguments: list[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    try:
        outputs = process_folder(options.results_folder)
    except ResultError as error:
        print(error, file=sys.stderr)
        return 1
    for output in outputs:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
