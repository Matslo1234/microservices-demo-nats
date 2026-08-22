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


def capacity_assessment(
    config: BenchmarkConfig,
    business: dict[str, Any],
    outstanding: dict[str, Any],
    nats: dict[str, Any],
) -> dict[str, Any]:
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
    saturated = bool(final.get("saturated"))
    sustainable_rungs = rungs[:-1] if saturated else rungs
    return {
        "available": True,
        "start_requests_per_second": SATURATION_START_RATE,
        "step_requests_per_second": SATURATION_STEP_RATE,
        "step_seconds": SATURATION_STEP_SECONDS,
        "maximum_requests_per_second": config.saturation_max_rate,
        "rapid_pending_growth_threshold_per_second": (
            RAPID_PENDING_GROWTH_PER_SECOND
        ),
        "saturated": saturated,
        "stop_reason": final.get("stop_reason"),
        "saturation_requests_per_second": (
            final.get("target_requests_per_second") if saturated else None
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
    if config.workload == "saturation":
        summary["configured_steady_seconds"] = config.duration_seconds
        summary["saturation"] = saturation_summary(
            config,
            saturation_decisions,
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
