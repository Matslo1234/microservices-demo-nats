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
    saturation_limit: float | None = None,
) -> tuple[
    list[tuple[float, float]],
    list[tuple[float, float]],
    list[tuple[float, float]],
    list[tuple[float, float]],
]:
    rungs = _nested(result.summary, "saturation", "rungs")
    if not isinstance(rungs, list) or not rungs:
        raise ResultError(f"{result.path}: saturation.rungs must be a non-empty list")

    submitted_goodput_points: list[tuple[float, float]] = []
    processed_goodput_points: list[tuple[float, float]] = []
    pending_points: list[tuple[float, float]] = []
    p95_latency_points: list[tuple[float, float]] = []
    is_nats = str(result.summary.get("application_type", "")).upper() == "NATS"
    pending_series = _nested(
        result.summary, "nats", "consumer_pending", "series"
    )
    parsed_pending_series: list[tuple[float, float]] | None = None
    if (
        is_nats
        and isinstance(pending_series, list)
        and saturation_limit is None
    ):
        parsed_pending_series = []
        for sample_index, sample in enumerate(pending_series):
            if not isinstance(sample, dict):
                raise ResultError(
                    f"{result.path}: nats.consumer_pending."
                    f"series[{sample_index}] must be an object"
                )
            parsed_pending_series.append(
                (
                    _number(
                        sample.get("elapsed_seconds"),
                        (
                            "nats.consumer_pending."
                            f"series[{sample_index}].elapsed_seconds"
                        ),
                        result.path,
                    ),
                    _number(
                        sample.get("waiting_events"),
                        (
                            "nats.consumer_pending."
                            f"series[{sample_index}].waiting_events"
                        ),
                        result.path,
                    ),
                )
            )
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
        if saturation_limit is not None and rate > saturation_limit:
            continue
        submitted_goodput_points.append(
            (
                rate,
                _number(
                    _nested(rung, "business", "goodput_orders_per_second"),
                    (
                        f"saturation.rungs[{index}].business."
                        "goodput_orders_per_second"
                    ),
                    result.path,
                ),
            )
        )
        processing_field = "processing_goodput_orders_per_second"
        processing_goodput = rung.get(processing_field)
        if processing_field not in rung:
            # Compatibility with summaries written before NATS goodput was
            # counted independently from per-order outcome watchers.
            processing_goodput = rung.get(
                "observed_goodput_orders_per_second"
            )
        if processing_goodput is not None:
            processed_goodput_points.append(
                (
                    rate,
                    _number(
                        processing_goodput,
                        f"saturation.rungs[{index}].{processing_field}",
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
            pending_max = _nested(rung, "nats", "consumer_pending", "max")
            if pending_max is None:
                pending_candidates = [
                    value
                    for value in (
                        rung.get("pending_start"),
                        rung.get("pending_end"),
                    )
                    if value is not None
                ]
                started = rung.get("started_elapsed_seconds")
                ended = rung.get("ended_elapsed_seconds")
                if started is not None and ended is not None:
                    started_number = _number(
                        started,
                        f"saturation.rungs[{index}].started_elapsed_seconds",
                        result.path,
                    )
                    ended_number = _number(
                        ended,
                        f"saturation.rungs[{index}].ended_elapsed_seconds",
                        result.path,
                    )
                    if parsed_pending_series is not None:
                        pending_candidates.extend(
                            waiting_events
                            for elapsed_seconds, waiting_events in (
                                parsed_pending_series
                            )
                            if started_number <= elapsed_seconds <= ended_number
                        )
                    elif isinstance(pending_series, list):
                        for sample_index, sample in enumerate(pending_series):
                            if not isinstance(sample, dict):
                                raise ResultError(
                                    f"{result.path}: nats.consumer_pending."
                                    f"series[{sample_index}] must be an object"
                                )
                            elapsed_seconds = _number(
                                sample.get("elapsed_seconds"),
                                (
                                    "nats.consumer_pending."
                                    f"series[{sample_index}].elapsed_seconds"
                                ),
                                result.path,
                            )
                            if not (
                                started_number
                                <= elapsed_seconds
                                <= ended_number
                            ):
                                continue
                            pending_candidates.append(
                                _number(
                                    sample.get("waiting_events"),
                                    (
                                        "nats.consumer_pending."
                                        f"series[{sample_index}].waiting_events"
                                    ),
                                    result.path,
                                )
                            )
                if pending_candidates:
                    pending_max = max(
                        _number(
                            value,
                            f"saturation.rungs[{index}].pending events",
                            result.path,
                        )
                        for value in pending_candidates
                    )

            # NATS metrics can legitimately be entirely unavailable for a
            # rung. Such a rung still contains valid business measurements.
            if pending_max is not None:
                pending_points.append(
                    (
                        rate,
                        _number(
                            pending_max,
                            (
                                f"saturation.rungs[{index}].nats."
                                "consumer_pending.max"
                            ),
                            result.path,
                        ),
                    )
                )

    return (
        sorted(submitted_goodput_points),
        sorted(processed_goodput_points),
        sorted(pending_points),
        sorted(p95_latency_points),
    )


def saturation_interpolated_pending_points(
    result: Result,
    saturation_limit: float | None = None,
) -> list[tuple[float, float]]:
    """Replace repeated pending values between two changes by a linear ramp."""
    _, _, faithful, _ = saturation_points(result, saturation_limit)
    interpolated = list(faithful)
    index = 0
    while index < len(faithful):
        plateau_end = index + 1
        while (
            plateau_end < len(faithful)
            and faithful[plateau_end][1] == faithful[index][1]
        ):
            plateau_end += 1
        if plateau_end < len(faithful) and plateau_end > index + 1:
            start_rate, start_value = faithful[index]
            end_rate, end_value = faithful[plateau_end]
            for repeated_index in range(index + 1, plateau_end):
                rate = faithful[repeated_index][0]
                fraction = (rate - start_rate) / (end_rate - start_rate)
                interpolated[repeated_index] = (
                    rate,
                    start_value + fraction * (end_value - start_value),
                )
        index = plateau_end
    return interpolated


def saturation_latency_timeout_points(
    result: Result,
    saturation_limit: float | None = None,
) -> list[tuple[float, float]]:
    """Return a 30-second timeout line from the first null latency onward."""
    rungs = _nested(result.summary, "saturation", "rungs")
    if not isinstance(rungs, list) or not rungs:
        return []
    timed_out = False
    points: list[tuple[float, float]] = []
    previous_measured: tuple[float, float] | None = None
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
        if saturation_limit is not None and rate > saturation_limit:
            continue
        latency = _nested(rung, "business", "checkout_to_outcome", "p95_ms")
        if latency is None:
            if not timed_out and previous_measured is not None:
                points.append(previous_measured)
            timed_out = True
        elif not timed_out:
            previous_measured = (
                rate,
                _number(
                    latency,
                    (
                        f"saturation.rungs[{index}].business."
                        "checkout_to_outcome.p95_ms"
                    ),
                    result.path,
                ),
            )
        if timed_out:
            points.append((rate, 30_000.0))
    return points


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


def fault_tolerance_markers(
    result: Result,
) -> list[tuple[float, str, str]]:
    faults = _nested(result.summary, "fault_tolerance", "faults")
    if faults is None:
        return []
    if not isinstance(faults, list):
        raise ResultError(f"{result.path}: fault_tolerance.faults must be a list")

    service_colors = {
        "paymentservice": "red",
        "shippingservice": "orange",
    }
    markers: list[tuple[float, str, str]] = []
    for index, fault in enumerate(faults):
        if not isinstance(fault, dict):
            raise ResultError(
                f"{result.path}: fault_tolerance.faults[{index}] "
                "must be an object"
            )
        service = str(fault.get("service", "")).lower()
        color = service_colors.get(service)
        if color is None:
            continue
        for field_name, action in (
            ("disabled_at_elapsed_seconds", "disabled"),
            ("reenabled_at_elapsed_seconds", "enabled"),
        ):
            value = fault.get(field_name)
            if value is None:
                continue
            elapsed_seconds = _number(
                value,
                f"fault_tolerance.faults[{index}].{field_name}",
                result.path,
            )
            if elapsed_seconds < 0:
                raise ResultError(
                    f"{result.path}: fault_tolerance.faults[{index}]."
                    f"{field_name} must not be negative"
                )
            markers.append(
                (elapsed_seconds, f"{service} {action}", color)
            )
    return markers


def _axis_limit(maximum: float, *, headroom: float = 1.0) -> float:
    """Return five intervals whose size has only one significant digit."""
    target = max(5.0, maximum * headroom)
    raw_interval = target / 5
    magnitude = 10 ** math.floor(math.log10(raw_interval))
    interval = math.ceil(raw_interval / magnitude) * magnitude
    return float(interval * 5)


def write_png_chart(
    destination: Path,
    points: list[tuple[float, float]],
    *,
    title: str,
    y_label: str,
    x_label: str = "Št. naročil na sekundo",
    error_bars: list[tuple[float, float]] | None = None,
    deviation_band: list[tuple[float, float]] | None = None,
    vertical_lines: list[tuple[float, str, str]] | None = None,
    colors: list[str | None] | None = None,
) -> None:
    _write_png_series_chart(
        destination,
        [("", points)],
        title=title,
        y_label=y_label,
        x_label=x_label,
        error_bars=[error_bars] if error_bars is not None else None,
        deviation_bands=(
            [deviation_band] if deviation_band is not None else None
        ),
        vertical_lines=vertical_lines,
        show_legend=False,
        colors=colors,
    )


def write_png_multi_series_chart(
    destination: Path,
    series: list[tuple[str, list[tuple[float, float]]]],
    *,
    title: str,
    y_label: str,
    x_label: str,
    colors: list[str | None] | None = None,
    line_styles: list[str] | None = None,
    error_bars: list[list[tuple[float, float]]] | None = None,
    deviation_bands: list[list[tuple[float, float]] | None] | None = None,
) -> None:
    _write_png_series_chart(
        destination,
        series,
        title=title,
        y_label=y_label,
        x_label=x_label,
        colors=colors,
        line_styles=line_styles,
        error_bars=error_bars,
        deviation_bands=deviation_bands,
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
    error_bars: list[list[tuple[float, float]]] | None = None,
    deviation_bands: list[list[tuple[float, float]] | None] | None = None,
    vertical_lines: list[tuple[float, str, str]] | None = None,
    colors: list[str | None] | None = None,
    line_styles: list[str] | None = None,
    show_legend: bool,
) -> None:
    if colors is not None and len(colors) != len(series):
        raise ResultError(
            f"cannot create {destination}: each series must have a color"
        )
    if line_styles is not None and len(line_styles) != len(series):
        raise ResultError(
            f"cannot create {destination}: each series must have a line style"
        )
    populated_series = [
        (index, label, points)
        for index, (label, points) in enumerate(series)
        if points
    ]
    all_points = [
        point for _, _, points in populated_series for point in points
    ]
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
    for deviations, description in (
        (error_bars, "error bars"),
        (deviation_bands, "deviation bands"),
    ):
        if deviations is None:
            continue
        if len(deviations) != len(series):
            raise ResultError(
                f"cannot create {destination}: each series must have "
                f"{description}"
            )
        for (_, points), series_deviations in zip(series, deviations):
            if series_deviations is None:
                continue
            if len(series_deviations) != len(points):
                raise ResultError(
                    f"cannot create {destination}: each point must have "
                    f"a {description[:-1]}"
                )
            if any(
                point[0] != deviation_x
                or deviation < 0
                or not math.isfinite(deviation)
                for point, (deviation_x, deviation) in zip(
                    points, series_deviations
                )
            ):
                raise ResultError(
                    f"cannot create {destination}: {description} must match "
                    "points and be finite and non-negative"
                )
    if vertical_lines is not None and any(
        position < 0 or not math.isfinite(position)
        for position, _, _ in vertical_lines
    ):
        raise ResultError(
            f"cannot create {destination}: vertical lines must be finite "
            "and non-negative"
        )

    maximum_x = max(point[0] for point in all_points)
    if vertical_lines:
        maximum_x = max(
            maximum_x, max(position for position, _, _ in vertical_lines)
        )
    maximum_y = max(point[1] for point in all_points)
    for deviations in (error_bars, deviation_bands):
        if deviations:
            upper_bounds = [
                point[1] + deviation
                for (_, points), series_deviations in zip(series, deviations)
                if series_deviations is not None
                for point, (_, deviation) in zip(points, series_deviations)
            ]
            if upper_bounds:
                maximum_y = max(maximum_y, max(upper_bounds))
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

    for plotted_index, (series_index, label, points) in enumerate(
        populated_series
    ):
        color = (
            colors[series_index]
            if colors is not None and colors[series_index] is not None
            else _series_color(plotted_index)
        )
        if deviation_bands is not None:
            series_deviations = deviation_bands[series_index]
            if series_deviations is not None:
                axes.fill_between(
                    [point[0] for point in points],
                    [
                        point[1] - deviation
                        for point, (_, deviation) in zip(
                            points, series_deviations
                        )
                    ],
                    [
                        point[1] + deviation
                        for point, (_, deviation) in zip(
                            points, series_deviations
                        )
                    ],
                    color=color,
                    alpha=0.2,
                    linewidth=0,
                    zorder=1,
                )
        if error_bars is not None:
            series_errors = error_bars[series_index]
            axes.errorbar(
                [point[0] for point in points],
                [point[1] for point in points],
                yerr=[error for _, error in series_errors],
                fmt="none",
                ecolor=color,
                elinewidth=2,
                capsize=5,
                zorder=2,
            )
        axes.plot(
            [point[0] for point in points],
            [point[1] for point in points],
            color=color,
            linewidth=3,
            linestyle=(
                line_styles[series_index]
                if line_styles is not None
                else "-"
            ),
            marker="o" if len(points) == 1 else None,
            markersize=7,
            label=label or None,
            zorder=3,
        )
    if vertical_lines:
        for position, label, color in vertical_lines:
            axes.axvline(
                position,
                color=color,
                linestyle="--",
                linewidth=2,
                label=label,
                zorder=2,
            )
    if show_legend or vertical_lines:
        axes.legend(loc="best")

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


def write_saturation_graphs(
    result: Result, saturation_limit: float | None = None
) -> list[Path]:
    submitted_goodput, processed_goodput, pending, p95_latency = (
        saturation_points(result, saturation_limit)
    )
    base = result.path.with_suffix("")
    goodput_path = base.with_name(f"{base.name}_goodput.png")
    write_png_multi_series_chart(
        goodput_path,
        [
            ("Zaključena znotraj 30s oddaje", submitted_goodput),
            ("Zaključena tekom obremenitvene stopnice", processed_goodput),
        ],
        title=f"",
        y_label="Št. zaključenih naročil na sekundo",
        x_label="Št. oddanih naročil na sekundo",
        colors=["#1f77b4", "#2ca02c"],
    )
    outputs = [goodput_path]
    timeout_latency = saturation_latency_timeout_points(
        result, saturation_limit
    )
    if p95_latency or timeout_latency:
        p95_latency_path = base.with_name(f"{base.name}_p95_latency.png")
        write_png_multi_series_chart(
            p95_latency_path,
            [
                ("P95 tranjanje naročila", p95_latency),
                ("30s časovna omejitev", timeout_latency),
            ],
            title=f"",
            y_label="P95 trajanje naročila (ms)",
            x_label="Št. naročil na sekundo",
            colors=["#1f77b4", "#1f77b4"],
            line_styles=["-", "--"],
        )
        outputs.append(p95_latency_path)
    if pending:
        pending_path = base.with_name(f"{base.name}_max_pending_events.png")
        write_png_chart(
            pending_path,
            pending,
            title=f"",
            y_label="Št. čakajočih dogodkov",
            colors=["#1f77b4"],
        )
        outputs.append(pending_path)
        interpolated_pending = saturation_interpolated_pending_points(
            result, saturation_limit
        )
        if interpolated_pending:
            interpolated_path = base.with_name(
                f"{base.name}_max_pending_events_interpolated.png"
            )
            write_png_chart(
                interpolated_path,
                interpolated_pending,
                title=f"",
                y_label="Št. čakajočih dogodkov",
                colors=["#1f77b4"],
            )
            outputs.append(interpolated_path)
    return outputs


def _average_saturation_series(
    series_by_run: list[list[tuple[float, float]]],
    *,
    require_all_runs: bool = False,
) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    """Average one value per requested rate from each saturation run."""
    values_by_rate: dict[float, list[float]] = {}
    for points in series_by_run:
        run_values_by_rate: dict[float, list[float]] = {}
        for rate, value in points:
            run_values_by_rate.setdefault(rate, []).append(value)
        for rate, values in run_values_by_rate.items():
            # A saturation run can remain at its configured maximum for more
            # than one rung. Collapse those repeated rungs first so a longer
            # run does not receive more weight than another run.
            values_by_rate.setdefault(rate, []).append(statistics.mean(values))

    selected_values = [
        (rate, values)
        for rate, values in sorted(values_by_rate.items())
        if not require_all_runs or len(values) == len(series_by_run)
    ]
    averaged = [
        (rate, statistics.mean(values))
        for rate, values in selected_values
    ]
    deviations = [
        (rate, statistics.pstdev(values))
        for rate, values in selected_values
    ]
    return averaged, deviations


def average_saturation_points(
    results: list[Result],
    saturation_limit: float | None = None,
) -> tuple[
    list[tuple[float, float]],
    list[tuple[float, float]],
    list[tuple[float, float]],
    list[tuple[float, float]],
    list[tuple[float, float]],
    list[tuple[float, float]],
]:
    """Return means and population deviations for saturation measurements."""
    submitted_by_run: list[list[tuple[float, float]]] = []
    processed_by_run: list[list[tuple[float, float]]] = []
    latency_by_run: list[list[tuple[float, float]]] = []
    for result in results:
        submitted, processed, _, latency = saturation_points(
            result, saturation_limit
        )
        submitted_by_run.append(submitted)
        processed_by_run.append(processed)
        latency_by_run.append(latency)

    submitted, submitted_deviations = _average_saturation_series(
        submitted_by_run
    )
    processed, processed_deviations = _average_saturation_series(
        processed_by_run
    )
    latency, latency_deviations = _average_saturation_series(
        latency_by_run, require_all_runs=True
    )
    return (
        submitted,
        submitted_deviations,
        processed,
        processed_deviations,
        latency,
        latency_deviations,
    )


def _average_saturation_latency_plot_points(
    submitted: list[tuple[float, float]],
    latency: list[tuple[float, float]],
    latency_deviations: list[tuple[float, float]],
) -> tuple[
    list[tuple[float, float]],
    list[tuple[float, float]],
    list[tuple[float, float]],
]:
    """Split aggregate latency into measured and 30-second timeout lines."""
    latency_by_rate = dict(latency)
    deviations_by_rate = dict(latency_deviations)
    measured: list[tuple[float, float]] = []
    measured_deviations: list[tuple[float, float]] = []
    timeout: list[tuple[float, float]] = []
    timed_out = False
    for rate, _ in submitted:
        if not timed_out and rate in latency_by_rate:
            measured.append((rate, latency_by_rate[rate]))
            measured_deviations.append((rate, deviations_by_rate[rate]))
            continue
        if not timed_out and measured:
            timeout.append(measured[-1])
        timed_out = True
        timeout.append((rate, 30_000.0))
    return measured, measured_deviations, timeout


def average_interpolated_saturation_pending_points(
    results: list[Result],
    saturation_limit: float | None = None,
) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    """Average linearly interpolated NATS pending-event measurements."""
    return _average_saturation_series(
        [
            saturation_interpolated_pending_points(result, saturation_limit)
            for result in results
        ],
        require_all_runs=True,
    )


def _saturation_duration(result: Result) -> float | None:
    for field_name in (
        "configured_steady_seconds",
        "steady_seconds",
        "duration_seconds",
    ):
        value = result.summary.get(field_name)
        if value is None:
            continue
        duration = _number(value, field_name, result.path)
        if duration <= 0:
            raise ResultError(f"{result.path}: {field_name} must be positive")
        return duration
    return None


def _filename_number(value: float) -> str:
    return format(value, ".15g")


def write_average_saturation_graphs(
    folder: Path,
    results: list[Result],
    saturation_limit: float | None = None,
) -> list[Path]:
    by_duration: dict[float, list[Result]] = {}
    for result in results:
        duration = _saturation_duration(result)
        if duration is not None:
            by_duration.setdefault(duration, []).append(result)

    outputs: list[Path] = []
    for _, matching_results in sorted(by_duration.items()):
        if len(matching_results) < 2:
            continue
        (
            submitted,
            submitted_deviations,
            processed,
            processed_deviations,
            latency,
            latency_deviations,
        ) = average_saturation_points(matching_results, saturation_limit)
        latency, latency_deviations, timeout_latency = (
            _average_saturation_latency_plot_points(
                submitted, latency, latency_deviations
            )
        )
        maximum_orders = max(rate for rate, _ in submitted)
        suffix = _filename_number(maximum_orders)

        goodput_path = folder / f"average_goodput_{suffix}.png"
        write_png_multi_series_chart(
            goodput_path,
            [
                ("Zaključena znotraj 30s oddaje", submitted),
                (
                    "Zaključena tekom obremenitvene stopnice",
                    processed,
                ),
            ],
            title="",
            y_label="Št. zaključenih naročil na sekundo",
            x_label="Št. oddanih naročil na sekundo",
            colors=["#1f77b4", "#2ca02c"],
            deviation_bands=[submitted_deviations, processed_deviations],
        )
        outputs.append(goodput_path)

        if latency or timeout_latency:
            latency_path = folder / f"average_latency_{suffix}.png"
            write_png_multi_series_chart(
                latency_path,
                [
                    ("P95 trajanje naročila", latency),
                    ("30s časovna omejitev", timeout_latency),
                ],
                title="",
                y_label="P95 trajanje naročila (ms)",
                x_label="Št. naročil na sekundo",
                colors=["#1f77b4", "#1f77b4"],
                line_styles=["-", "--"],
                deviation_bands=[latency_deviations, None],
            )
            outputs.append(latency_path)

        nats_results = [
            result
            for result in matching_results
            if str(result.summary.get("application_type", "")).upper()
            == "NATS"
        ]
        if len(nats_results) >= 2:
            pending, pending_deviations = (
                average_interpolated_saturation_pending_points(
                    nats_results, saturation_limit
                )
            )
            if pending:
                nats_maximum_orders = max(
                    rate
                    for result in nats_results
                    for rate, _ in saturation_points(
                        result, saturation_limit
                    )[0]
                )
                nats_suffix = _filename_number(nats_maximum_orders)
                pending_path = (
                    folder / f"average_waiting_events_{nats_suffix}.png"
                )
                write_png_chart(
                    pending_path,
                    pending,
                    title="",
                    y_label="Št. čakajočih dogodkov",
                    x_label="Št. naročil na sekundo",
                    deviation_band=pending_deviations,
                    colors=["#1f77b4"],
                )
                outputs.append(pending_path)
    return outputs


def write_fault_tolerance_graphs(result: Result) -> list[Path]:
    successful, queued = fault_tolerance_points(result)
    markers = fault_tolerance_markers(result)
    base = result.path.with_suffix("")
    successful_path = base.with_name(f"{base.name}_successful_requests.png")
    write_png_chart(
        successful_path,
        successful,
        title=f"{result.path.stem}: successfully processed requests",
        x_label="Elapsed time (s)",
        y_label="Successful requests/s",
        vertical_lines=markers,
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
            vertical_lines=markers,
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

        # Closed-loop summaries exclude warm-up samples, but their timestamps
        # are measured from the beginning of the complete run. Plot steady
        # state relative to the end of warm-up so the graph starts at zero.
        warmup_seconds = _number(
            result.summary.get("warmup_seconds", 0),
            "warmup_seconds",
            result.path,
        )
        if warmup_seconds < 0:
            raise ResultError(
                f"{result.path}: warmup_seconds must not be negative"
            )

        # Keep the last observation in a second, then average matching seconds
        # across repeated runs with the same closed-loop user count.
        observed_by_second: dict[int, float] = {}
        for elapsed, waiting in samples:
            steady_elapsed = max(0.0, elapsed - warmup_seconds)
            observed_by_second[math.floor(steady_elapsed)] = waiting
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
            title="",
            x_label="Št. sočasnih uporabnikov",
            y_label="P95 trajanje naročila (ms)",
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
            title="",
            x_label="Čas (s)",
            y_label="Št. čakajočih dogodkov",
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


def process_folder(
    folder: Path, saturation_limit: float | None = None
) -> list[Path]:
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

    saturation_results = [
        result
        for result in results
        if result.summary["workload"] == "saturation"
    ]
    for result in results:
        if result.summary["workload"] == "saturation":
            outputs.extend(
                write_saturation_graphs(result, saturation_limit)
            )
        elif result.summary["workload"] == "fault_tolerance":
            outputs.extend(write_fault_tolerance_graphs(result))
    outputs.extend(
        write_average_saturation_graphs(
            folder, saturation_results, saturation_limit
        )
    )
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
    argument_parser.add_argument(
        "--saturation-limit",
        type=float,
        metavar="REQUESTS_PER_SECOND",
        help=(
            "only include saturation data at or below this requested "
            "requests-per-second rate"
        ),
    )
    return argument_parser


def main(arguments: list[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    try:
        outputs = process_folder(
            options.results_folder, options.saturation_limit
        )
    except ResultError as error:
        print(error, file=sys.stderr)
        return 1
    for output in outputs:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
