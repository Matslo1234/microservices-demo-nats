#!/usr/bin/env python3
"""Create PNG charts and LaTeX tables from benchmark summary JSON files."""

from __future__ import annotations

import argparse
import colorsys
import json
import math
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.ticker import StrMethodFormatter


class ResultError(ValueError):
    """Raised when benchmark results cannot be analyzed."""


@dataclass(frozen=True)
class Result:
    path: Path
    summary: dict[str, Any]


@dataclass
class ClosedMeasurements:
    submitted: int = 0
    completed: int = 0
    runs: int = 0
    p95_values: list[float] = field(default_factory=list)


@dataclass
class ServiceResourceMeasurements:
    cpu_seconds_per_order: list[float] = field(default_factory=list)
    average_memory_mb: list[float] = field(default_factory=list)


@dataclass
class ClosedResourceMeasurements:
    services: dict[str, ServiceResourceMeasurements] = field(
        default_factory=dict
    )
    total: ServiceResourceMeasurements = field(
        default_factory=ServiceResourceMeasurements
    )


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


def _json_line_values(path: Path) -> Iterator[tuple[int, Any]]:
    try:
        with path.open(encoding="utf-8") as source:
            for line_number, line in enumerate(source, 1):
                if not line.strip():
                    continue
                try:
                    yield line_number, json.loads(line)
                except json.JSONDecodeError as error:
                    raise ResultError(
                        f"invalid JSON in {path}:{line_number}: {error}"
                    ) from error
    except OSError as error:
        raise ResultError(f"cannot read {path}: {error}") from error


def _nested(document: dict[str, Any], *keys: str) -> Any:
    value: Any = document
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def saturation_points(
    result: Result,
) -> tuple[
    list[tuple[float, float]],
    list[tuple[float, float]],
    list[tuple[float, float]],
]:
    rungs = _nested(result.summary, "saturation", "rungs")
    if not isinstance(rungs, list) or not rungs:
        raise ResultError(f"{result.path}: saturation.rungs must be a non-empty list")

    goodput_points: list[tuple[float, float]] = []
    pending_points: list[tuple[float, float]] = []
    p95_latency_points: list[tuple[float, float]] = []
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
        goodput_points.append(
            (
                rate,
                _number(
                    rung.get("observed_goodput_orders_per_second"),
                    (
                        f"saturation.rungs[{index}]."
                        "observed_goodput_orders_per_second"
                    ),
                    result.path,
                ),
            )
        )

        p95_latency = _nested(
            rung, "business", "checkout_to_outcome", "p95_ms"
        )
        if p95_latency is not None:
            p95_latency_points.append(
                (
                    rate,
                    _number(
                        p95_latency,
                        (
                            f"saturation.rungs[{index}].business."
                            "checkout_to_outcome.p95_ms"
                        ),
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

    return (
        sorted(goodput_points),
        sorted(pending_points),
        sorted(p95_latency_points),
    )


def fault_tolerance_points(
    result: Result,
) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    samples = _nested(result.summary, "fault_tolerance", "per_second")
    if not isinstance(samples, list) or not samples:
        raise ResultError(
            f"{result.path}: fault_tolerance.per_second must be a non-empty list"
        )

    successful_points: list[tuple[float, float]] = []
    queued_points: list[tuple[float, float]] = []
    is_nats = str(result.summary.get("application_type", "")).upper() == "NATS"
    for index, sample in enumerate(samples):
        if not isinstance(sample, dict):
            raise ResultError(
                f"{result.path}: fault_tolerance.per_second[{index}] "
                "must be an object"
            )
        elapsed_seconds = _number(
            sample.get("elapsed_seconds"),
            f"fault_tolerance.per_second[{index}].elapsed_seconds",
            result.path,
        )
        successful_field = "successfully_processed"
        successful_value = sample.get(successful_field)
        if successful_field not in sample:
            # Fall back to outcomes attributed to their scheduled second when
            # the summary does not contain observed completion counts.
            successful_field = "successful_requests"
            successful_value = sample.get(successful_field)
        successful_points.append(
            (
                elapsed_seconds,
                _number(
                    successful_value,
                    f"fault_tolerance.per_second[{index}].{successful_field}",
                    result.path,
                ),
            )
        )

        if is_nats and sample.get("nats_waiting_events") is not None:
            queued_points.append(
                (
                    elapsed_seconds,
                    _number(
                        sample["nats_waiting_events"],
                        (
                            f"fault_tolerance.per_second[{index}]."
                            "nats_waiting_events"
                        ),
                        result.path,
                    ),
                )
            )

    if is_nats and not queued_points:
        raise ResultError(
            f"{result.path}: fault_tolerance.per_second must contain "
            "NATS queued-event samples"
        )
    return sorted(successful_points), sorted(queued_points)


def _axis_limit(maximum: float, *, headroom: float = 1.0) -> float:
    """Return an axis limit whose five evenly spaced ticks are integers."""
    target = maximum * headroom
    return float(max(5, math.ceil(target / 5) * 5))


def write_png_chart(
    destination: Path,
    points: list[tuple[float, float]],
    *,
    title: str,
    y_label: str,
    x_label: str = "Št. naročil na sekundo",
    error_bars: list[tuple[float, float]] | None = None,
) -> None:
    _write_png_series_chart(
        destination,
        [("", points)],
        title=title,
        y_label=y_label,
        x_label=x_label,
        error_bars=error_bars,
        show_legend=False,
    )


def write_png_multi_series_chart(
    destination: Path,
    series: list[tuple[str, list[tuple[float, float]]]],
    *,
    title: str,
    y_label: str,
    x_label: str,
) -> None:
    _write_png_series_chart(
        destination,
        series,
        title=title,
        y_label=y_label,
        x_label=x_label,
        show_legend=True,
    )


def _series_color(index: int) -> str:
    hue = (217 + index * 137) % 360
    red, green, blue = colorsys.hls_to_rgb(hue / 360, 0.42, 0.65)
    return "#" + "".join(
        f"{round(channel * 255):02x}" for channel in (red, green, blue)
    )


def _write_png_series_chart(
    destination: Path,
    series: list[tuple[str, list[tuple[float, float]]]],
    *,
    title: str,
    y_label: str,
    x_label: str,
    error_bars: list[tuple[float, float]] | None = None,
    show_legend: bool,
) -> None:
    populated_series = [item for item in series if item[1]]
    all_points = [point for _, points in populated_series for point in points]
    if not all_points:
        raise ResultError(f"cannot create {destination}: graph has no points")
    if any(
        not math.isfinite(coordinate)
        for point in all_points
        for coordinate in point
    ):
        raise ResultError(
            f"cannot create {destination}: graph points must be finite"
        )
    if error_bars is not None and len(error_bars) != len(all_points):
        raise ResultError(
            f"cannot create {destination}: each point must have an error bar"
        )
    if error_bars is not None and any(
        point[0] != error_x or error < 0 or not math.isfinite(error)
        for point, (error_x, error) in zip(all_points, error_bars)
    ):
        raise ResultError(
            f"cannot create {destination}: error bars must match points "
            "and be finite and non-negative"
        )

    maximum_x = max(point[0] for point in all_points)
    maximum_y = max(point[1] for point in all_points)
    if error_bars:
        maximum_y = max(
            maximum_y,
            max(
                point[1] + error
                for point, (_, error) in zip(all_points, error_bars)
            ),
        )
    x_limit = _axis_limit(maximum_x)
    y_limit = _axis_limit(maximum_y, headroom=1.1)

    figure = Figure(figsize=(9.6, 6), dpi=100, layout="constrained")
    FigureCanvasAgg(figure)
    axes = figure.subplots()
    axes.set_title(title, fontsize=16)
    axes.set_xlabel(x_label, fontsize=12)
    axes.set_ylabel(y_label, fontsize=12)
    axes.set_xlim(0, x_limit)
    axes.set_ylim(0, y_limit)
    axes.set_xticks([tick * x_limit / 5 for tick in range(6)])
    axes.set_yticks([tick * y_limit / 5 for tick in range(6)])
    axes.xaxis.set_major_formatter(StrMethodFormatter("{x:.0f}"))
    axes.yaxis.set_major_formatter(StrMethodFormatter("{x:.0f}"))
    axes.grid(color="#dadce0", linewidth=1)
    axes.set_axisbelow(True)
    axes.spines["top"].set_visible(False)
    axes.spines["right"].set_visible(False)

    if error_bars:
        axes.errorbar(
            [point[0] for point in all_points],
            [point[1] for point in all_points],
            yerr=[error for _, error in error_bars],
            fmt="none",
            ecolor=_series_color(0),
            elinewidth=2,
            capsize=5,
            zorder=2,
        )

    for index, (label, points) in enumerate(populated_series):
        axes.plot(
            [point[0] for point in points],
            [point[1] for point in points],
            color=_series_color(index),
            linewidth=3,
            marker="o" if len(points) == 1 else None,
            markersize=7,
            label=label or None,
            zorder=3,
        )
    if show_legend:
        axes.legend(loc="center left", bbox_to_anchor=(1.02, 0.5))

    figure.savefig(
        destination,
        format="png",
        dpi=100,
        facecolor="white",
        metadata={
            "Title": title,
            "Description": f"X axis: {x_label}; Y axis: {y_label}",
        },
    )


def write_saturation_graphs(result: Result) -> list[Path]:
    goodput, pending, p95_latency = saturation_points(result)
    base = result.path.with_suffix("")
    goodput_path = base.with_name(f"{base.name}_goodput.png")
    write_png_chart(
        goodput_path,
        goodput,
        title=f"",
        y_label="Koristna prepustnost",
    )
    outputs = [goodput_path]
    if p95_latency:
        p95_latency_path = base.with_name(f"{base.name}_p95_latency.png")
        write_png_chart(
            p95_latency_path,
            p95_latency,
            title=f"",
            y_label="P95 latenca naročila (ms)",
        )
        outputs.append(p95_latency_path)
    if pending:
        pending_path = base.with_name(f"{base.name}_max_pending_events.png")
        write_png_chart(
            pending_path,
            pending,
            title=f"",
            y_label="Št. čakajočih dogodkov",
        )
        outputs.append(pending_path)
    return outputs


def write_fault_tolerance_graphs(result: Result) -> list[Path]:
    successful, queued = fault_tolerance_points(result)
    base = result.path.with_suffix("")
    successful_path = base.with_name(f"{base.name}_successful_requests.png")
    write_png_chart(
        successful_path,
        successful,
        title=f"{result.path.stem}: successfully processed requests",
        x_label="Elapsed time (s)",
        y_label="Successful requests/s",
    )
    outputs = [successful_path]
    if queued:
        queued_path = base.with_name(f"{base.name}_queued_events.png")
        write_png_chart(
            queued_path,
            queued,
            title=f"{result.path.stem}: queued NATS events",
            x_label="Elapsed time (s)",
            y_label="Queued events",
        )
        outputs.append(queued_path)
    return outputs


def collect_closed_measurements(results: list[Result]) -> dict[int, ClosedMeasurements]:
    groups: dict[int, ClosedMeasurements] = {}
    for result in results:
        users = _integer(result.summary.get("users"), "users", result.path)
        if users < 1:
            raise ResultError(f"{result.path}: users must be positive")
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

        group = groups.setdefault(users, ClosedMeasurements())
        group.submitted += submitted
        group.completed += completed
        group.runs += 1
        p95_value = _nested(business, "checkout_to_outcome", "p95_ms")
        if p95_value is not None:
            group.p95_values.append(
                _number(p95_value, "business.checkout_to_outcome.p95_ms", result.path)
            )
    return groups


def _summary_nats_waiting_samples(
    result: Result,
) -> list[tuple[float, float]] | None:
    samples = _nested(result.summary, "nats", "consumer_pending", "series")
    series_field = "nats.consumer_pending.series"
    waiting_fields = ("waiting_events", "nats_waiting_events", "value")
    if samples is None or samples == []:
        samples = _nested(result.summary, "outstanding_orders", "series")
        series_field = "outstanding_orders.series"
        waiting_fields = ("max_outstanding",)
    if samples is None or samples == []:
        return None
    if not isinstance(samples, list):
        raise ResultError(
            f"{result.path}: {series_field} must be a list"
        )

    points: list[tuple[float, float]] = []
    for index, sample in enumerate(samples):
        if not isinstance(sample, dict):
            raise ResultError(
                f"{result.path}: {series_field}[{index}] must be an object"
            )
        elapsed = _number(
            sample.get("elapsed_seconds"),
            f"{series_field}[{index}].elapsed_seconds",
            result.path,
        )
        waiting_field = next(
            (field for field in waiting_fields if sample.get(field) is not None),
            waiting_fields[0],
        )
        waiting = _number(
            sample.get(waiting_field),
            f"{series_field}[{index}].{waiting_field}",
            result.path,
        )
        if elapsed < 0 or waiting < 0:
            raise ResultError(
                f"{result.path}: NATS waiting-event samples must not be negative"
            )
        points.append((elapsed, waiting))
    return points


def _resource_nats_waiting_samples(
    result: Result,
) -> list[tuple[float, float]] | None:
    if result.path.name != "summary.json":
        return None
    resources_path = result.path.parent / "resources.jsonl"
    if not resources_path.is_file():
        return None

    points: list[tuple[float, float]] = []
    for line_number, record in _json_line_values(resources_path):
        if not isinstance(record, dict) or record.get("phase") != "steady":
            continue

        consumers: dict[str, float] = {}
        metrics = record.get("nats_metrics", [])
        if not isinstance(metrics, list):
            raise ResultError(
                f"{resources_path}:{line_number}: nats_metrics must be a list"
            )
        for metric in metrics:
            if (
                not isinstance(metric, dict)
                or metric.get("name") != "jetstream_consumer_num_pending"
            ):
                continue
            labels = metric.get("labels", {})
            if not isinstance(labels, dict):
                continue
            stream = labels.get("stream_name") or labels.get("stream")
            consumer = labels.get("consumer_name") or labels.get("consumer")
            if (
                stream not in {"BOUTIQUE_COMMANDS", "BOUTIQUE_EVENTS"}
                or not consumer
            ):
                continue
            key = f"{stream}/{consumer}"
            value = _number(
                metric.get("value"),
                f"nats_metrics waiting-event value at line {line_number}",
                resources_path,
            )
            if value < 0:
                raise ResultError(
                    f"{resources_path}:{line_number}: NATS waiting-event "
                    "samples must not be negative"
                )
            consumers[key] = max(value, consumers.get(key, 0.0))
        if consumers:
            elapsed = _number(
                record.get("elapsed_seconds"),
                f"elapsed_seconds at line {line_number}",
                resources_path,
            )
            if elapsed < 0:
                raise ResultError(
                    f"{resources_path}:{line_number}: elapsed_seconds must "
                    "not be negative"
                )
            points.append(
                (
                    elapsed,
                    sum(consumers.values()),
                )
            )
    return points or None


def collect_closed_nats_waiting_series(
    results: list[Result],
) -> dict[int, list[tuple[float, float]]]:
    by_users: dict[int, dict[int, list[float]]] = {}
    for result in results:
        if str(result.summary.get("application_type", "")).upper() != "NATS":
            continue
        users = _integer(result.summary.get("users"), "users", result.path)
        if users < 1:
            raise ResultError(f"{result.path}: users must be positive")
        samples = _summary_nats_waiting_samples(result)
        if samples is None:
            samples = _resource_nats_waiting_samples(result)
        if not samples:
            raise ResultError(
                f"{result.path}: NATS closed-loop result must contain "
                "per-second waiting-event samples"
            )

        # Keep the last observation in a second, then average matching seconds
        # across repeated runs with the same closed-loop user count.
        observed_by_second: dict[int, float] = {}
        for elapsed, waiting in samples:
            observed_by_second[math.floor(elapsed)] = waiting
        run_by_second: dict[int, float] = {}
        latest: float | None = None
        for second in range(
            min(observed_by_second), max(observed_by_second) + 1
        ):
            latest = observed_by_second.get(second, latest)
            if latest is not None:
                run_by_second[second] = latest
        grouped_seconds = by_users.setdefault(users, {})
        for second, waiting in run_by_second.items():
            grouped_seconds.setdefault(second, []).append(waiting)

    return {
        users: [
            (float(second), statistics.mean(values))
            for second, values in sorted(seconds.items())
        ]
        for users, seconds in sorted(by_users.items())
    }


def collect_closed_resource_measurements(
    results: list[Result],
) -> dict[int, ClosedResourceMeasurements]:
    groups: dict[int, ClosedResourceMeasurements] = {}
    for result in results:
        users = _integer(result.summary.get("users"), "users", result.path)
        group = groups.setdefault(users, ClosedResourceMeasurements())
        resources = result.summary.get("resources")
        if not isinstance(resources, dict) or not resources.get("available"):
            continue
        by_service = resources.get("by_service")
        if not isinstance(by_service, dict):
            raise ResultError(
                f"{result.path}: resources.by_service must be an object"
            )

        completed = _integer(
            _nested(result.summary, "business", "completed"),
            "business.completed",
            result.path,
        )
        total_cpu_seconds_per_order = 0.0
        total_average_memory_mb = 0.0
        complete_cpu_total = completed > 0 and bool(by_service)
        complete_memory_total = bool(by_service)
        for service, values in sorted(by_service.items()):
            if not isinstance(service, str) or not isinstance(values, dict):
                raise ResultError(
                    f"{result.path}: resources.by_service entries must be objects"
                )
            measurements = group.services.setdefault(
                service, ServiceResourceMeasurements()
            )

            cpu_seconds_value = values.get("cpu_seconds")
            if cpu_seconds_value is None or completed <= 0:
                complete_cpu_total = False
            else:
                cpu_seconds = _number(
                    cpu_seconds_value,
                    f"resources.by_service.{service}.cpu_seconds",
                    result.path,
                )
                if cpu_seconds < 0:
                    raise ResultError(
                        f"{result.path}: resources.by_service.{service}."
                        "cpu_seconds must not be negative"
                    )
                cpu_seconds_per_order = cpu_seconds / completed
                measurements.cpu_seconds_per_order.append(
                    cpu_seconds_per_order
                )
                total_cpu_seconds_per_order += cpu_seconds_per_order

            average_memory_value = values.get("average_memory_bytes")
            if average_memory_value is None:
                complete_memory_total = False
            else:
                average_memory_bytes = _number(
                    average_memory_value,
                    f"resources.by_service.{service}.average_memory_bytes",
                    result.path,
                )
                if average_memory_bytes < 0:
                    raise ResultError(
                        f"{result.path}: resources.by_service.{service}."
                        "average_memory_bytes must not be negative"
                    )
                average_memory_mb = average_memory_bytes / 1_000_000
                measurements.average_memory_mb.append(average_memory_mb)
                total_average_memory_mb += average_memory_mb

        if complete_cpu_total:
            group.total.cpu_seconds_per_order.append(
                total_cpu_seconds_per_order
            )
        if complete_memory_total:
            group.total.average_memory_mb.append(total_average_memory_mb)
    return groups


def _latex_number(value: float | None) -> str:
    return "--" if value is None else f"{value:.3f}"


def _latex_text(value: str) -> str:
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(
        replacements.get(character, character) for character in value
    )


def _mean_and_stddev(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    return statistics.mean(values), statistics.pstdev(values)


def _resource_table_row(
    label: str, measurements: ServiceResourceMeasurements
) -> str:
    cpu_mean, cpu_stddev = _mean_and_stddev(
        measurements.cpu_seconds_per_order
    )
    memory_mean, memory_stddev = _mean_and_stddev(
        measurements.average_memory_mb
    )
    cpu = "--" if cpu_mean is None else f"{cpu_mean:.6f}"
    cpu_std = "--" if cpu_stddev is None else f"{cpu_stddev:.6f}"
    memory = "--" if memory_mean is None else f"{memory_mean:.3f}"
    memory_std = "--" if memory_stddev is None else f"{memory_stddev:.3f}"
    return (
        f"{_latex_text(label)} & {cpu} & {cpu_std} & {memory} & "
        f"{memory_std} \\\\"
    )


def write_closed_table(
    destination: Path, groups: dict[int, ClosedMeasurements]
) -> None:
    lines = [
        r"\begin{tabular}{rrrrr}",
        r"\hline",
        r"Users & Orders & Success rate & P95 & std \\",
        r"\hline",
    ]
    for users in sorted(groups):
        group = groups[users]
        success_rate = (
            f"{group.completed / group.submitted * 100:.2f}\\%"
            if group.submitted
            else "--"
        )
        average_orders = group.submitted / group.runs
        average_orders_text = f"{average_orders:.3f}".rstrip("0").rstrip(".")
        median_p95 = (
            statistics.median(group.p95_values) if group.p95_values else None
        )
        p95_stddev = (
            statistics.pstdev(group.p95_values) if group.p95_values else None
        )
        lines.append(
            f"{users} & {average_orders_text} & {success_rate} & "
            f"{_latex_number(median_p95)} & {_latex_number(p95_stddev)} \\\\"
        )
    lines.extend([r"\hline", r"\end{tabular}"])
    destination.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_closed_graphs(
    folder: Path,
    results: list[Result],
    groups: dict[int, ClosedMeasurements],
) -> list[Path]:
    outputs: list[Path] = []
    latency_points: list[tuple[float, float]] = []
    latency_errors: list[tuple[float, float]] = []
    for users, group in sorted(groups.items()):
        if not group.p95_values:
            continue
        latency_points.append(
            (float(users), statistics.median(group.p95_values))
        )
        latency_errors.append(
            (float(users), statistics.pstdev(group.p95_values))
        )
    if latency_points:
        latency_path = folder / "closed_p95_latency.png"
        write_png_chart(
            latency_path,
            latency_points,
            title="Closed-loop P95 outcome latency",
            x_label="Concurrent users",
            y_label="P95 outcome latency (ms)",
            error_bars=latency_errors,
        )
        outputs.append(latency_path)

    waiting_by_users = collect_closed_nats_waiting_series(results)
    if waiting_by_users:
        waiting_path = folder / "closed_nats_waiting_events.png"
        write_png_multi_series_chart(
            waiting_path,
            [
                (f"{users} users", points)
                for users, points in waiting_by_users.items()
            ],
            title="Closed-loop NATS waiting events",
            x_label="Elapsed time (s)",
            y_label="Waiting events",
        )
        outputs.append(waiting_path)
    return outputs


def write_resource_usage_tables(
    destination: Path, groups: dict[int, ClosedResourceMeasurements]
) -> None:
    lines: list[str] = []
    for users in sorted(groups):
        group = groups[users]
        lines.extend(
            [
                r"\begin{table}[htbp]",
                r"\centering",
                f"\\caption{{Resource usage for {users} users}}",
                r"\begin{tabular}{lrrrr}",
                r"\hline",
                (
                    r"Service & CPU seconds per order & CPU seconds std. & "
                    r"Avg memory MB & Avg memory MB std. \\"
                ),
                r"\hline",
            ]
        )
        for service in sorted(group.services):
            lines.append(
                _resource_table_row(service, group.services[service])
            )
        lines.extend(
            [
                r"\hline",
                _resource_table_row("Total", group.total),
                r"\hline",
                r"\end{tabular}",
                r"\end{table}",
                "",
            ]
        )
    destination.write_text("\n".join(lines), encoding="utf-8")


def process_folder(folder: Path) -> list[Path]:
    results = find_results(folder)
    for result in results:
        workload = result.summary.get("workload")
        if workload not in {"closed", "saturation", "fault_tolerance"}:
            raise ResultError(f"unsupported workload {workload}")

    outputs: list[Path] = []
    closed_results = [
        result for result in results if result.summary["workload"] == "closed"
    ]
    if closed_results:
        closed_measurements = collect_closed_measurements(closed_results)
        table_path = folder / "closed_results.tex"
        write_closed_table(table_path, closed_measurements)
        outputs.append(table_path)
        resource_table_path = folder / "resource-usage.tex"
        write_resource_usage_tables(
            resource_table_path,
            collect_closed_resource_measurements(closed_results),
        )
        outputs.append(resource_table_path)
        outputs.extend(
            write_closed_graphs(folder, closed_results, closed_measurements)
        )

    for result in results:
        if result.summary["workload"] == "saturation":
            outputs.extend(write_saturation_graphs(result))
        elif result.summary["workload"] == "fault_tolerance":
            outputs.extend(write_fault_tolerance_graphs(result))
    return outputs


def parser() -> argparse.ArgumentParser:
    argument_parser = argparse.ArgumentParser(
        description="Create PNG graphs and LaTeX tables from benchmark results."
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
