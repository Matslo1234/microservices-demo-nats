# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from config import (
    BenchmarkConfig,
    SATURATION_START_RATE,
    SATURATION_STEP_RATE,
    SATURATION_STEP_SECONDS,
)
from saturation import RAPID_PENDING_GROWTH_PER_SECOND


APPLICATION_STREAMS = {"BOUTIQUE_COMMANDS", "BOUTIQUE_EVENTS"}


def read_json_lines(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                # A forcibly stopped process can leave one partial final line.
                if line_number > 1:
                    continue
                raise
            if isinstance(value, dict):
                records.append(value)
    return records


def read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    return value if isinstance(value, dict) else {}


def percentile(values: Iterable[float], quantile: float) -> float | None:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return round(ordered[0], 3)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        result = ordered[lower]
    else:
        result = ordered[lower] + (ordered[upper] - ordered[lower]) * (
            position - lower
        )
    return round(result, 3)


def latency_summary(samples: list[dict[str, Any]]) -> dict[str, Any]:
    values = [
        float(sample["response_time_ms"])
        for sample in samples
        if sample.get("success") and sample.get("response_time_ms") is not None
    ]
    return {
        "count": len(values),
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "max_ms": round(max(values), 3) if values else None,
    }


def recorded_latency_summary(
    samples: list[dict[str, Any]], *, successful: bool
) -> dict[str, Any]:
    values = [
        float(sample["response_time_ms"])
        for sample in samples
        if bool(sample.get("success")) == successful
        and sample.get("response_time_ms") is not None
    ]
    return {
        "count": len(values),
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "max_ms": round(max(values), 3) if values else None,
    }


def rate(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return round(numerator / denominator, 6)


def business_summary(
    records: list[dict[str, Any]], duration_seconds: float
) -> dict[str, Any]:
    steady = [record for record in records if record.get("phase") == "steady"]
    outcomes = [
        record
        for record in steady
        if record.get("name") == "checkout_to_outcome"
    ]
    acceptances = [
        record
        for record in steady
        if record.get("name") == "checkout_acceptance"
    ]
    settlements = [
        record
        for record in steady
        if record.get("name") == "checkout_to_settled"
    ]

    outcome_counts = Counter(
        str(record.get("context", {}).get("outcome", "UNKNOWN"))
        for record in outcomes
    )
    for required_outcome in (
        "COMPLETED",
        "REJECTED",
        "CANCELLED",
        "MANUAL_REVIEW",
        "TIMEOUT",
        "INCOMPLETE",
        "GENERATOR_SATURATED",
    ):
        outcome_counts.setdefault(required_outcome, 0)
    submitted = len(outcomes)
    accepted = sum(
        1 for record in outcomes if record.get("context", {}).get("accepted")
    )
    completed = outcome_counts["COMPLETED"]
    scheduled = sum(
        1
        for record in outcomes
        if record.get("context", {}).get("scheduled_at") is not None
    )

    settlement_counts = Counter(
        str(record.get("context", {}).get("settlement", "UNKNOWN"))
        for record in settlements
    )
    notification_counts = Counter(
        str(record.get("context", {}).get("notification_status", "UNKNOWN"))
        for record in settlements
        if record.get("context", {}).get("notification_status")
    )
    cart_clear_counts = Counter(
        str(record.get("context", {}).get("cart_clear_status", "UNKNOWN"))
        for record in settlements
        if record.get("context", {}).get("cart_clear_status")
    )

    return {
        "submitted": submitted,
        "scheduled_open_loop": scheduled,
        "accepted": accepted,
        "completed": completed,
        "acceptance_rate": rate(accepted, submitted),
        "completion_rate": rate(completed, submitted),
        "completion_rate_of_accepted": rate(completed, accepted),
        "goodput_orders_per_second": round(
            completed / duration_seconds, 6
        ),
        "outcomes": dict(sorted(outcome_counts.items())),
        "outcome_rates": {
            outcome: rate(count, submitted)
            for outcome, count in sorted(outcome_counts.items())
        },
        "checkout_to_outcome": latency_summary(
            [
                record
                for record in outcomes
                if record.get("context", {}).get("outcome") == "COMPLETED"
            ]
        ),
        "checkout_acceptance": latency_summary(
            [
                record
                for record in acceptances
                if record.get("context", {}).get("accepted")
            ]
        ),
        "checkout_to_settled": latency_summary(
            [
                record
                for record in settlements
                if record.get("context", {}).get("settlement") == "SETTLED"
            ]
        ),
        "settlements": dict(sorted(settlement_counts.items())),
        "notification_outcomes": dict(sorted(notification_counts.items())),
        "cart_clear_outcomes": dict(sorted(cart_clear_counts.items())),
    }


def _pod_totals(
    pods: dict[str, dict[str, Any]]
) -> tuple[int, int, int, int]:
    cpu = memory = received = transmitted = 0
    for pod in pods.values():
        cpu += int(pod.get("cpu_usage_core_nanoseconds", 0))
        memory += int(pod.get("memory_working_set_bytes", 0))
        received += int(pod.get("network_rx_bytes", 0))
        transmitted += int(pod.get("network_tx_bytes", 0))
    return cpu, memory, received, transmitted


def resource_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    steady = [record for record in records if record.get("phase") == "steady"]
    if len(steady) < 2:
        return {"available": False, "reason": "fewer than two steady-state samples"}

    cpu_ns = memory_byte_seconds = rx_bytes = tx_bytes = 0.0
    max_memory = 0
    elapsed = 0.0
    by_service: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "cpu_nanoseconds": 0.0,
            "memory_byte_seconds": 0.0,
            "network_rx_bytes": 0.0,
            "network_tx_bytes": 0.0,
        }
    )

    for previous, current in zip(steady, steady[1:]):
        delta_seconds = float(current["timestamp"]) - float(previous["timestamp"])
        if delta_seconds <= 0:
            continue
        elapsed += delta_seconds
        previous_pods = previous.get("pods", {})
        current_pods = current.get("pods", {})
        _, previous_memory, _, _ = _pod_totals(previous_pods)
        _, current_memory, _, _ = _pod_totals(current_pods)
        memory_byte_seconds += (
            previous_memory + current_memory
        ) / 2.0 * delta_seconds
        max_memory = max(max_memory, previous_memory, current_memory)

        for pod_name, current_pod in current_pods.items():
            previous_pod = previous_pods.get(pod_name)
            if previous_pod is None:
                continue
            service = str(current_pod.get("service", "unknown"))
            cpu_delta = max(
                0,
                int(current_pod.get("cpu_usage_core_nanoseconds", 0))
                - int(previous_pod.get("cpu_usage_core_nanoseconds", 0)),
            )
            rx_delta = max(
                0,
                int(current_pod.get("network_rx_bytes", 0))
                - int(previous_pod.get("network_rx_bytes", 0)),
            )
            tx_delta = max(
                0,
                int(current_pod.get("network_tx_bytes", 0))
                - int(previous_pod.get("network_tx_bytes", 0)),
            )
            average_memory = (
                int(previous_pod.get("memory_working_set_bytes", 0))
                + int(current_pod.get("memory_working_set_bytes", 0))
            ) / 2.0
            cpu_ns += cpu_delta
            rx_bytes += rx_delta
            tx_bytes += tx_delta
            by_service[service]["cpu_nanoseconds"] += cpu_delta
            by_service[service]["memory_byte_seconds"] += (
                average_memory * delta_seconds
            )
            by_service[service]["network_rx_bytes"] += rx_delta
            by_service[service]["network_tx_bytes"] += tx_delta

    normalized_services: dict[str, dict[str, Any]] = {}
    for service, values in sorted(by_service.items()):
        normalized_services[service] = {
            "cpu_seconds": round(values["cpu_nanoseconds"] / 1_000_000_000, 6),
            "memory_byte_seconds": round(values["memory_byte_seconds"], 3),
            "average_memory_bytes": round(
                values["memory_byte_seconds"] / elapsed, 3
            )
            if elapsed
            else None,
            "network_rx_bytes": int(values["network_rx_bytes"]),
            "network_tx_bytes": int(values["network_tx_bytes"]),
        }

    return {
        "available": True,
        "sampled_seconds": round(elapsed, 3),
        "cpu_seconds": round(cpu_ns / 1_000_000_000, 6),
        "memory_byte_seconds": round(memory_byte_seconds, 3),
        "average_memory_bytes": round(memory_byte_seconds / elapsed, 3)
        if elapsed
        else None,
        "max_sampled_memory_bytes": max_memory,
        "network_rx_bytes": int(rx_bytes),
        "network_tx_bytes": int(tx_bytes),
        "by_service": normalized_services,
    }


def nats_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    steady = [record for record in records if record.get("phase") == "steady"]
    if not steady:
        return {"available": False, "reason": "no steady-state samples"}

    names = {
        "jetstream_consumer_num_pending": "consumer_pending",
        "jetstream_consumer_num_ack_pending": "consumer_ack_pending",
        "jetstream_consumer_num_redelivered": "consumer_redelivered",
        "gnatsd_varz_jetstream_stats_storage": "storage_bytes",
    }
    series: dict[str, list[float]] = defaultdict(list)
    consumers: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for record in steady:
        totals: dict[str, float] = defaultdict(float)
        record_consumers: dict[str, dict[str, float]] = defaultdict(dict)
        for metric in record.get("nats_metrics", []):
            metric_name = str(metric.get("name", ""))
            alias = names.get(metric_name)
            if alias is None:
                continue
            value = float(metric.get("value", 0))
            labels = metric.get("labels", {})
            consumer = labels.get("consumer_name") or labels.get("consumer")
            stream = labels.get("stream_name") or labels.get("stream")
            if consumer:
                if stream not in APPLICATION_STREAMS:
                    continue
                key = f"{stream or 'unknown'}/{consumer}"
                record_consumers[key][alias] = max(
                    value, record_consumers[key].get(alias, 0.0)
                )
            else:
                # Non-consumer values such as storage are per server.
                totals[alias] += value
        for consumer, metrics in record_consumers.items():
            for alias, value in metrics.items():
                consumers[consumer][alias].append(value)
                totals[alias] += value
        for alias, value in totals.items():
            series[alias].append(value)

    result: dict[str, Any] = {"available": bool(series)}
    for alias, values in sorted(series.items()):
        result[alias] = {
            "first": values[0],
            "last": values[-1],
            "max": max(values),
            "change": values[-1] - values[0],
        }
    result["by_consumer"] = {
        consumer: {
            alias: {
                "last": values[-1],
                "max": max(values),
                "change": values[-1] - values[0],
            }
            for alias, values in sorted(metrics.items())
        }
        for consumer, metrics in sorted(consumers.items())
    }
    return result


def outstanding_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    if not records:
        return {
            "available": False,
            "max": 0,
            "final": 0,
            "series": [],
        }
    buckets: dict[int, dict[str, Any]] = {}
    maximum = 0
    for record in records:
        elapsed = float(record.get("elapsed_seconds", 0))
        value = int(record.get("outstanding", 0))
        maximum = max(maximum, value)
        second = max(0, math.floor(elapsed))
        point = buckets.setdefault(
            second,
            {
                "elapsed_seconds": second,
                "phase": record.get("phase"),
                "outstanding": value,
                "max_outstanding": value,
            },
        )
        point["phase"] = record.get("phase")
        point["outstanding"] = value
        point["max_outstanding"] = max(point["max_outstanding"], value)
    series = [buckets[second] for second in sorted(buckets)]
    return {
        "available": True,
        "max": maximum,
        "final": int(records[-1].get("outstanding", 0)),
        "series": series,
    }


def _fault_expected_requests(config: BenchmarkConfig, second: int) -> int:
    start = min(float(second), float(config.submission_seconds))
    end = min(float(second + 1), float(config.submission_seconds))
    if end <= start:
        return 0
    return sum(
        max(
            0,
            math.ceil(worker.arrival_rate * end - 1e-9)
            - math.ceil(worker.arrival_rate * start - 1e-9),
        )
        for worker in (
            config.for_worker(index) for index in range(config.worker_count)
        )
    )


def _fault_nats_totals(record: dict[str, Any]) -> dict[str, float] | None:
    aliases = {
        "jetstream_consumer_num_pending": "waiting_events",
        "jetstream_consumer_num_ack_pending": "ack_pending_events",
        "jetstream_consumer_num_redelivered": "redelivered_events",
    }
    consumers: dict[str, dict[str, float]] = defaultdict(dict)
    for metric in record.get("nats_metrics", []):
        alias = aliases.get(str(metric.get("name", "")))
        if alias is None:
            continue
        labels = metric.get("labels", {})
        stream = labels.get("stream_name") or labels.get("stream")
        consumer = labels.get("consumer_name") or labels.get("consumer")
        if not consumer or stream not in APPLICATION_STREAMS:
            continue
        key = f"{stream}/{consumer}"
        value = float(metric.get("value", 0))
        consumers[key][alias] = max(
            value, consumers[key].get(alias, 0.0)
        )
    if not consumers:
        return None
    return {
        alias: sum(values.get(alias, 0.0) for values in consumers.values())
        for alias in aliases.values()
    }


def _fault_phase(
    elapsed: float, spans: list[dict[str, Any]], duration_seconds: int
) -> str:
    if elapsed >= duration_seconds:
        return "drain"
    phase = "baseline"
    for span in spans:
        disabled = float(span["scale_down_started_at_elapsed_seconds"])
        reenabled = span.get("scale_up_started_at_elapsed_seconds")
        if elapsed < disabled:
            break
        if reenabled is None or elapsed < float(reenabled):
            return f"{span['service']}_disabled"
        phase = f"{span['service']}_recovery"
    return phase


def _fault_spans(
    fault_records: list[dict[str, Any]], plan: dict[str, Any]
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    plan_faults = plan.get("faults", [])
    if not isinstance(plan_faults, (list, tuple)):
        return result
    for planned in plan_faults:
        if not isinstance(planned, dict):
            continue
        service = str(planned.get("service", "unknown"))
        service_events = [
            record
            for record in fault_records
            if record.get("service") == service
        ]
        down = next(
            (
                record
                for record in service_events
                if record.get("action") == "scale_down_started"
            ),
            None,
        )
        up = next(
            (
                record
                for record in service_events
                if record.get("action") == "scale_up_started"
            ),
            None,
        )
        disabled = next(
            (
                record
                for record in service_events
                if record.get("action") == "disabled"
            ),
            None,
        )
        reenabled = next(
            (
                record
                for record in service_events
                if record.get("action") == "reenabled"
            ),
            None,
        )
        if down is None:
            continue
        result.append(
            {
                "service": service,
                "deployment": planned.get("deployment"),
                "original_replicas": planned.get("original_replicas"),
                "scale_down_started_at": down.get("at"),
                "scale_down_started_at_elapsed_seconds": float(
                    down.get("elapsed_seconds", 0)
                ),
                "disabled_at": disabled.get("at") if disabled else None,
                "disabled_at_elapsed_seconds": (
                    float(disabled.get("elapsed_seconds", 0))
                    if disabled
                    else None
                ),
                "scale_up_started_at": up.get("at") if up else None,
                "scale_up_started_at_elapsed_seconds": (
                    float(up.get("elapsed_seconds", 0)) if up else None
                ),
                "reenabled_at": (
                    reenabled.get("at") if reenabled else None
                ),
                "reenabled_at_elapsed_seconds": (
                    float(reenabled.get("elapsed_seconds", 0))
                    if reenabled
                    else None
                ),
                "disabled_duration_seconds": (
                    round(
                        float(up.get("elapsed_seconds", 0))
                        - float(down.get("elapsed_seconds", 0)),
                        6,
                    )
                    if up
                    else None
                ),
            }
        )
    return sorted(
        result,
        key=lambda span: float(
            span["scale_down_started_at_elapsed_seconds"]
        ),
    )


def _fault_convergence(
    points: list[dict[str, Any]],
    spans: list[dict[str, Any]],
    plan: dict[str, Any],
    application_type: str,
) -> list[dict[str, Any]]:
    stable_seconds = int(plan.get("convergence_stable_seconds", 3))
    success_fraction = float(
        plan.get("convergence_success_fraction", 0.9)
    )
    pending_tolerance = float(
        plan.get("convergence_pending_tolerance", 0)
    )
    results: list[dict[str, Any]] = []
    for index, span in enumerate(spans):
        down = float(span["scale_down_started_at_elapsed_seconds"])
        up_value = span.get("scale_up_started_at_elapsed_seconds")
        result = dict(span)
        if up_value is None or span.get("reenabled_at_elapsed_seconds") is None:
            result.update(
                {
                    "converged": False,
                    "convergence_seconds": None,
                    "reason": "the service was not re-enabled",
                }
            )
            results.append(result)
            continue
        up = float(up_value)
        search_end = (
            float(
                spans[index + 1][
                    "scale_down_started_at_elapsed_seconds"
                ]
            )
            if index + 1 < len(spans)
            else float(plan.get("duration_seconds", len(points)))
        )
        baseline = [
            point
            for point in points
            if max(0.0, down - 10.0)
            <= float(point["elapsed_seconds"])
            < down
        ]
        baseline_expected = sum(
            int(point["expected_requests"]) for point in baseline
        )
        baseline_successful = sum(
            int(point["successfully_processed"]) for point in baseline
        )
        if baseline_expected <= 0 or baseline_successful <= 0:
            result.update(
                {
                    "converged": False,
                    "convergence_seconds": None,
                    "reason": "no successful pre-fault baseline traffic",
                }
            )
            results.append(result)
            continue
        baseline_success_rate = min(
            1.0, baseline_successful / baseline_expected
        )
        success_threshold = baseline_success_rate * success_fraction
        baseline_pending_values = [
            float(point["nats_waiting_events"])
            for point in baseline
            if point.get("nats_waiting_events") is not None
            and point.get("nats_metrics_sample_age_seconds") is not None
            and float(point["nats_metrics_sample_age_seconds"]) <= 2
        ]
        baseline_pending = (
            baseline_pending_values[-1]
            if baseline_pending_values
            else None
        )
        if application_type == "NATS" and baseline_pending is None:
            result.update(
                {
                    "converged": False,
                    "convergence_seconds": None,
                    "reason": "pre-fault NATS pending metrics were unavailable",
                }
            )
            results.append(result)
            continue

        converged_at: int | None = None
        first_candidate = max(0, math.ceil(up))
        last_candidate = math.floor(search_end) - stable_seconds
        for candidate in range(first_candidate, last_candidate + 1):
            window = points[candidate : candidate + stable_seconds]
            if len(window) != stable_seconds:
                continue
            expected = sum(
                int(point["expected_requests"]) for point in window
            )
            successful = sum(
                int(point["successfully_processed"]) for point in window
            )
            lost = sum(int(point["lost_requests"]) for point in window)
            if expected <= 0 or successful / expected < success_threshold:
                continue
            if lost:
                continue
            if application_type == "NATS":
                pending = [
                    point.get("nats_waiting_events") for point in window
                ]
                sample_ages = [
                    point.get("nats_metrics_sample_age_seconds")
                    for point in window
                ]
                if any(value is None for value in pending) or any(
                    age is None or float(age) > 2 for age in sample_ages
                ):
                    continue
                assert baseline_pending is not None
                if any(
                    float(value) > baseline_pending + pending_tolerance
                    for value in pending
                ):
                    continue
            converged_at = candidate
            break

        result.update(
            {
                "baseline_successful_requests_per_second": round(
                    baseline_successful / len(baseline), 6
                ),
                "required_success_fraction_of_baseline": success_fraction,
                "baseline_nats_waiting_events": baseline_pending,
                "nats_pending_tolerance": pending_tolerance,
                "stable_seconds": stable_seconds,
                "converged": converged_at is not None,
                "convergence_at_elapsed_seconds": converged_at,
                "convergence_seconds": (
                    round(max(0.0, converged_at - up), 6)
                    if converged_at is not None
                    else None
                ),
                "confirmed_after_seconds": (
                    round(
                        max(0.0, converged_at + stable_seconds - up), 6
                    )
                    if converged_at is not None
                    else None
                ),
                "reason": (
                    None
                    if converged_at is not None
                    else "recovery criteria were not stable before the next phase"
                ),
            }
        )
        results.append(result)
    return results


def fault_tolerance_summary(
    config: BenchmarkConfig,
    plan: dict[str, Any],
    fault_records: list[dict[str, Any]],
    business_records: list[dict[str, Any]],
    resource_records: list[dict[str, Any]],
    outstanding_records: list[dict[str, Any]],
) -> dict[str, Any]:
    if not plan:
        return {
            "available": False,
            "reason": "fault-plan.json was not recorded",
            "per_second": [],
            "faults": [],
        }
    start_epoch = float(plan.get("start_epoch", 0))
    spans = _fault_spans(fault_records, plan)
    point_count = max(1, math.ceil(config.run_seconds))
    points: list[dict[str, Any]] = []
    for second in range(point_count):
        points.append(
            {
                "elapsed_seconds": second,
                "phase": _fault_phase(
                    second, spans, config.duration_seconds
                ),
                "expected_requests": _fault_expected_requests(
                    config, second
                ),
                "submitted_requests": 0,
                "accepted_requests": 0,
                "successful_requests": 0,
                "failed_requests": 0,
                "lost_requests": 0,
                "successfully_processed": 0,
                "failures_observed": 0,
                "completion_latency_ms": {
                    "count": 0,
                    "p50_ms": None,
                    "p95_ms": None,
                    "p99_ms": None,
                    "max_ms": None,
                },
                "failure_latency_ms": {
                    "count": 0,
                    "p50_ms": None,
                    "p95_ms": None,
                    "p99_ms": None,
                    "max_ms": None,
                },
                "nats_waiting_events": None,
                "nats_ack_pending_events": None,
                "nats_redelivered_events": None,
                "nats_metrics_sample_age_seconds": None,
                "outstanding_orders": 0,
            }
        )

    outcomes = [
        record
        for record in business_records
        if record.get("name") == "checkout_to_outcome"
        and record.get("phase") == "steady"
    ]
    completion_latencies: dict[int, list[dict[str, Any]]] = defaultdict(list)
    failure_latencies: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for record in outcomes:
        context = record.get("context", {})
        scheduled_at = context.get("scheduled_at")
        if not isinstance(scheduled_at, (int, float)):
            scheduled_at = record.get("timestamp", start_epoch)
        scheduled_second = math.floor(float(scheduled_at) - start_epoch)
        outcome = str(context.get("outcome", "UNKNOWN"))
        if 0 <= scheduled_second < len(points):
            point = points[scheduled_second]
            point["submitted_requests"] += 1
            point["accepted_requests"] += int(bool(context.get("accepted")))
            if outcome == "COMPLETED":
                point["successful_requests"] += 1
            else:
                point["failed_requests"] += 1

        recorded_at = record.get("recorded_at")
        if not isinstance(recorded_at, (int, float)):
            recorded_at = float(record.get("timestamp", start_epoch)) + (
                float(record.get("response_time_ms", 0)) / 1000.0
            )
        observed_second = math.floor(float(recorded_at) - start_epoch)
        if 0 <= observed_second < len(points):
            if outcome == "COMPLETED":
                points[observed_second]["successfully_processed"] += 1
                completion_latencies[observed_second].append(record)
            else:
                points[observed_second]["failures_observed"] += 1
                failure_latencies[observed_second].append(record)

    for second, point in enumerate(points):
        point["lost_requests"] = max(
            0,
            int(point["expected_requests"])
            - int(point["submitted_requests"]),
        )
        point["completion_latency_ms"] = latency_summary(
            completion_latencies.get(second, [])
        )
        point["failure_latency_ms"] = recorded_latency_summary(
            failure_latencies.get(second, []), successful=False
        )

    nats_by_second: dict[int, dict[str, float]] = {}
    for record in resource_records:
        second = math.floor(float(record.get("elapsed_seconds", -1)))
        totals = _fault_nats_totals(record)
        if 0 <= second < len(points) and totals is not None:
            nats_by_second[second] = totals
    latest_nats: dict[str, float] | None = None
    latest_nats_second: int | None = None
    for second, point in enumerate(points):
        if second in nats_by_second:
            latest_nats = nats_by_second[second]
            latest_nats_second = second
        if latest_nats is not None:
            point["nats_waiting_events"] = latest_nats["waiting_events"]
            point["nats_ack_pending_events"] = latest_nats[
                "ack_pending_events"
            ]
            point["nats_redelivered_events"] = latest_nats[
                "redelivered_events"
            ]
            assert latest_nats_second is not None
            point["nats_metrics_sample_age_seconds"] = (
                second - latest_nats_second
            )

    outstanding_by_second: dict[int, int] = {}
    for record in outstanding_records:
        second = math.floor(float(record.get("elapsed_seconds", -1)))
        if 0 <= second < len(points):
            outstanding_by_second[second] = int(
                record.get("outstanding", 0)
            )
    latest_outstanding = 0
    for second, point in enumerate(points):
        latest_outstanding = outstanding_by_second.get(
            second, latest_outstanding
        )
        point["outstanding_orders"] = latest_outstanding

    submitted = sum(int(point["submitted_requests"]) for point in points)
    successful = sum(int(point["successful_requests"]) for point in points)
    failed = sum(int(point["failed_requests"]) for point in points)
    lost = sum(int(point["lost_requests"]) for point in points)
    faults = _fault_convergence(
        points, spans, plan, config.application_type
    )
    return {
        "available": bool(spans),
        "reason": None if spans else "no scale-down event was recorded",
        "plan": plan,
        "totals": {
            "expected_requests": sum(
                int(point["expected_requests"]) for point in points
            ),
            "submitted_requests": submitted,
            "successful_requests": successful,
            "failed_requests": failed,
            "lost_requests": lost,
            "failed_or_lost_requests": failed + lost,
        },
        "faults": faults,
        "per_second": points,
    }


def capacity_assessment(
    config: BenchmarkConfig,
    business: dict[str, Any],
    outstanding: dict[str, Any],
    nats: dict[str, Any],
) -> dict[str, Any]:
    if config.workload == "fault_tolerance":
        return {
            "sustainable": None,
            "reasons": [
                "capacity is not assessed while faults are intentional"
            ],
        }
    if config.workload not in {"open", "saturation"}:
        return {
            "sustainable": None,
            "reasons": ["capacity is assessed only for open-loop runs"],
        }
    if business["submitted"] == 0:
        return {
            "sustainable": None,
            "reasons": ["no steady-state transactions were recorded"],
        }

    reasons: list[str] = []
    saturated = int(
        business.get("outcomes", {}).get("GENERATOR_SATURATED", 0)
    )
    if saturated:
        reasons.append(
            f"load generator concurrency limit was reached {saturated} times"
        )
    if business["completed"] != business["submitted"]:
        reasons.append("not every submitted transaction completed")
    if outstanding.get("final", 0) > 0:
        reasons.append("accepted orders remained outstanding after drain")

    if config.application_type == "NATS":
        pending = nats.get("consumer_pending") if nats.get("available") else None
        if pending is None:
            reasons.append("consumer backlog metrics were unavailable")
            return {"sustainable": None, "reasons": reasons}
        if float(pending.get("change", 0)) > 0:
            reasons.append("JetStream consumer pending messages increased")

    return {"sustainable": not reasons, "reasons": reasons}


def add_per_order_resource_metrics(
    resources: dict[str, Any], completed: int
) -> None:
    if not resources.get("available") or completed <= 0:
        return
    resources["per_completed_order"] = {
        "cpu_seconds": round(resources["cpu_seconds"] / completed, 9),
        "memory_byte_seconds": round(
            resources["memory_byte_seconds"] / completed, 3
        ),
        "network_rx_bytes": round(resources["network_rx_bytes"] / completed, 3),
        "network_tx_bytes": round(resources["network_tx_bytes"] / completed, 3),
    }


def saturation_summary(
    config: BenchmarkConfig,
    decisions: list[dict[str, Any]],
    business_records: list[dict[str, Any]],
    resource_records: list[dict[str, Any]],
    outstanding_records: list[dict[str, Any]],
) -> dict[str, Any]:
    if not decisions:
        return {
            "available": False,
            "reason": "no saturation rung decisions were recorded",
            "rungs": [],
        }

    rungs: list[dict[str, Any]] = []
    for decision in decisions:
        rung_number = int(decision["rung"])
        started = float(decision["started_elapsed_seconds"])
        ended = float(decision["ended_elapsed_seconds"])
        duration = float(decision["duration_seconds"])
        rung_business_records = [
            record
            for record in business_records
            if record.get("context", {}).get("saturation_rung")
            == rung_number
        ]
        rung_resource_records = [
            record
            for record in resource_records
            if (
                record.get("saturation_rung") == rung_number
                or (
                    record.get("saturation_rung") is None
                    and started
                    <= float(record.get("elapsed_seconds", -1))
                    <= ended
                )
            )
        ]
        rung_outstanding_records = [
            record
            for record in outstanding_records
            if started <= float(record.get("elapsed_seconds", -1)) <= ended
        ]
        business = business_summary(rung_business_records, duration)
        resources = resource_summary(rung_resource_records)
        add_per_order_resource_metrics(resources, business["completed"])
        nats = (
            nats_summary(rung_resource_records)
            if config.application_type == "NATS"
            else {
                "available": False,
                "reason": "not applicable to GRPC application",
            }
        )
        rungs.append(
            {
                **decision,
                "business": business,
                "outstanding_orders": outstanding_summary(
                    rung_outstanding_records
                ),
                "resources": resources,
                "nats": nats,
            }
        )

    final = rungs[-1]
    first_saturated_index = next(
        (
            index
            for index, rung in enumerate(rungs)
            if rung.get("saturated")
        ),
        None,
    )
    first_saturated = (
        rungs[first_saturated_index]
        if first_saturated_index is not None
        else None
    )
    sustainable_rungs = (
        rungs[:first_saturated_index]
        if first_saturated_index is not None
        else rungs
    )
    saturation_reason = None
    if first_saturated is not None:
        saturation_reason = first_saturated.get("saturation_reason")
        if saturation_reason is None:
            # Compatibility with artifacts written before saturation signals
            # were separated from the reason the ladder stopped.
            saturation_reason = first_saturated.get("stop_reason")
    return {
        "available": True,
        "start_requests_per_second": SATURATION_START_RATE,
        "step_requests_per_second": SATURATION_STEP_RATE,
        "step_seconds": SATURATION_STEP_SECONDS,
        "maximum_requests_per_second": config.saturation_max_rate,
        "rapid_pending_growth_threshold_per_second": (
            RAPID_PENDING_GROWTH_PER_SECOND
        ),
        "saturated": first_saturated is not None,
        "stop_reason": final.get("stop_reason"),
        "saturation_reason": saturation_reason,
        "saturation_requests_per_second": (
            first_saturated.get("target_requests_per_second")
            if first_saturated is not None
            else None
        ),
        "highest_sustainable_requests_per_second": (
            sustainable_rungs[-1].get("target_requests_per_second")
            if sustainable_rungs
            else None
        ),
        "rungs": rungs,
    }


def build_report(run_directory: Path) -> dict[str, Any]:
    with (run_directory / "config.json").open(encoding="utf-8") as source:
        config = BenchmarkConfig.from_dict(json.load(source))
    business_records = read_json_lines(run_directory / "business.jsonl")
    resource_records = read_json_lines(run_directory / "resources.jsonl")
    outstanding_records = read_json_lines(run_directory / "outstanding.jsonl")
    fault_records = read_json_lines(run_directory / "faults.jsonl")
    fault_plan = read_json_object(run_directory / "fault-plan.json")
    saturation_decisions = read_json_lines(
        run_directory / "saturation.jsonl"
    )
    measured_duration = (
        sum(
            float(decision.get("duration_seconds", 0))
            for decision in saturation_decisions
        )
        if config.workload == "saturation" and saturation_decisions
        else float(config.duration_seconds)
    )
    business = business_summary(business_records, measured_duration)
    resources = resource_summary(resource_records)
    nats = (
        nats_summary(resource_records)
        if config.application_type == "NATS"
        else {"available": False, "reason": "not applicable to GRPC application"}
    )
    outstanding = outstanding_summary(outstanding_records)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "application_type": config.application_type,
        "workload": config.workload,
        "worker_count": config.worker_count,
        "warmup_seconds": config.warmup_seconds,
        "steady_seconds": measured_duration,
        "drain_seconds": config.drain_seconds,
        "business": business,
        "outstanding_orders": outstanding,
        "resources": resources,
        "nats": nats,
        "capacity": capacity_assessment(
            config, business, outstanding, nats
        ),
    }
    if config.workload == "closed":
        summary["users"] = config.users
    if config.workload == "saturation":
        summary["configured_steady_seconds"] = config.duration_seconds
        summary["saturation"] = saturation_summary(
            config,
            saturation_decisions,
            business_records,
            resource_records,
            outstanding_records,
        )
    if config.workload == "fault_tolerance":
        summary["arrival_rate"] = config.arrival_rate
        summary["fault_tolerance"] = fault_tolerance_summary(
            config,
            fault_plan,
            fault_records,
            business_records,
            resource_records,
            outstanding_records,
        )
    add_per_order_resource_metrics(
        summary["resources"], summary["business"]["completed"]
    )
    with (run_directory / "summary.json").open("w", encoding="utf-8") as target:
        json.dump(summary, target, indent=2, sort_keys=True)
        target.write("\n")
    write_business_csv(business_records, run_directory / "business.csv")
    if config.workload == "fault_tolerance":
        write_fault_tolerance_csv(
            summary["fault_tolerance"],
            run_directory / "fault-tolerance.csv",
        )
    return summary


def write_business_csv(
    records: list[dict[str, Any]], destination: Path
) -> None:
    context_fields = sorted(
        {
            key
            for record in records
            for key in record.get("context", {}).keys()
            if not key.startswith("_")
        }
    )
    fields = [
        "timestamp",
        "recorded_at",
        "recorded_elapsed_seconds",
        "phase",
        "request_type",
        "name",
        "response_time_ms",
        "success",
        "error",
        *context_fields,
    ]
    with destination.open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        for record in records:
            row = {key: record.get(key) for key in fields}
            row.update(
                {
                    key: value
                    for key, value in record.get("context", {}).items()
                    if key in context_fields
                }
            )
            writer.writerow(row)


def write_fault_tolerance_csv(
    summary: dict[str, Any], destination: Path
) -> None:
    fields = [
        "elapsed_seconds",
        "phase",
        "expected_requests",
        "submitted_requests",
        "accepted_requests",
        "successful_requests",
        "failed_requests",
        "lost_requests",
        "successfully_processed",
        "failures_observed",
        "outstanding_orders",
        "nats_waiting_events",
        "nats_ack_pending_events",
        "nats_redelivered_events",
        "nats_metrics_sample_age_seconds",
        "latency_count",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "latency_max_ms",
        "failure_latency_count",
        "failure_latency_p50_ms",
        "failure_latency_p95_ms",
        "failure_latency_p99_ms",
        "failure_latency_max_ms",
    ]
    with destination.open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=fields)
        writer.writeheader()
        for point in summary.get("per_second", []):
            latency = point.get("completion_latency_ms", {})
            failure_latency = point.get("failure_latency_ms", {})
            writer.writerow(
                {
                    **{
                        field: point.get(field)
                        for field in fields
                        if not field.startswith("latency_")
                        and not field.startswith("failure_latency_")
                    },
                    "latency_count": latency.get("count"),
                    "latency_p50_ms": latency.get("p50_ms"),
                    "latency_p95_ms": latency.get("p95_ms"),
                    "latency_p99_ms": latency.get("p99_ms"),
                    "latency_max_ms": latency.get("max_ms"),
                    "failure_latency_count": failure_latency.get("count"),
                    "failure_latency_p50_ms": failure_latency.get("p50_ms"),
                    "failure_latency_p95_ms": failure_latency.get("p95_ms"),
                    "failure_latency_p99_ms": failure_latency.get("p99_ms"),
                    "failure_latency_max_ms": failure_latency.get("max_ms"),
                }
            )
