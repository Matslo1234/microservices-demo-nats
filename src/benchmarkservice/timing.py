# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

def received_latency_ms(
    request_started_monotonic: float, received_monotonic: float
) -> float:
    """Measure local elapsed time through receipt of a streamed event."""
    if received_monotonic < request_started_monotonic:
        raise ValueError("event receipt preceded request start")
    return (received_monotonic - request_started_monotonic) * 1000
