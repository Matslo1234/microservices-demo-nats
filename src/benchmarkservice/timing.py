# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def outcome_latency_ms(
    request_started_epoch: float, outcome_at: Any
) -> float:
    if not isinstance(outcome_at, str) or not outcome_at.strip():
        raise ValueError("terminal order response omitted outcome_at")
    value = outcome_at.strip()
    if value.endswith(("Z", "z")):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError("terminal order response has invalid outcome_at") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("terminal order response has timezone-less outcome_at")
    request_started = datetime.fromtimestamp(
        request_started_epoch, timezone.utc
    )
    return (
        parsed.astimezone(timezone.utc) - request_started
    ).total_seconds() * 1000
