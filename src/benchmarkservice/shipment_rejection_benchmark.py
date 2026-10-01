#!/usr/bin/env python3
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

"""Run the fixed-rate shipment rejection and compensation benchmark."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from config import BenchmarkConfig, ConfigError
from reporting import read_json_object
from standalone import run


ARRIVAL_RATE = 50.0
DURATION_SECONDS = 30


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Send 50 NATS checkout transactions per second for 30 seconds "
            "and report shipment rejection compensation outcomes."
        )
    )
    result.add_argument("--url", dest="target_url", required=True)
    result.add_argument("--metrics-url", required=True)
    result.add_argument("--drain-seconds", type=int, default=60)
    result.add_argument("--outcome-timeout-seconds", type=float, default=30.0)
    result.add_argument(
        "--settlement-timeout-seconds", type=float, default=60.0
    )
    result.add_argument(
        "--output", type=Path, default=Path("benchmark-results")
    )
    return result


def config_from_args(arguments: argparse.Namespace) -> BenchmarkConfig:
    return BenchmarkConfig.from_request(
        {
            "target_url": arguments.target_url,
            "metrics_url": arguments.metrics_url,
            "workload": "open",
            "warmup_seconds": 0,
            "duration_seconds": DURATION_SECONDS,
            "drain_seconds": arguments.drain_seconds,
            "arrival_rate": ARRIVAL_RATE,
            "outcome_timeout_seconds": arguments.outcome_timeout_seconds,
            "settlement_timeout_seconds": (
                arguments.settlement_timeout_seconds
            ),
            "collect_resources": True,
            "collect_nats_metrics": True,
        },
        "NATS",
    )


def monitored_outcomes(summary: dict[str, Any]) -> dict[str, Any]:
    events = summary.get("workflow_events", {})
    return {
        "successful_orders": events.get("successful_orders"),
        "shipments_rejected": events.get("shipments_rejected"),
        "payment_authorizations_released": events.get(
            "payment_authorizations_released"
        ),
        "orders_in_manual_review": events.get("orders_in_manual_review"),
        "monitoring_available": events.get("available", False),
        "monitoring_unavailable_reason": events.get("reason"),
    }


def main() -> int:
    arguments = parser().parse_args()
    try:
        config = config_from_args(arguments)
    except ConfigError as error:
        parser().error(str(error))
    return_code, run_directory = run(config, arguments.output)
    summary = read_json_object(run_directory / "summary.json")
    result = {
        "run_directory": str(run_directory),
        "arrival_rate": ARRIVAL_RATE,
        "duration_seconds": DURATION_SECONDS,
        **monitored_outcomes(summary),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    if not result["monitoring_available"]:
        return 1
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
