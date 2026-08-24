# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, replace
from typing import Any, Mapping
from urllib.parse import urlparse


APPLICATION_TYPES = {"GRPC", "NATS"}
WORKLOADS = {"closed", "open", "saturation", "fault_tolerance"}
LOCAL_CLUSTER = "local"
LOCAL_TARGET_URL = "http://frontend:80"
MAX_OPEN_ARRIVAL_RATE_PER_WORKER = 100.0
MAX_CLOSED_USERS_PER_WORKER = 1_000
SATURATION_START_RATE = 10.0
SATURATION_STEP_RATE = 10.0
SATURATION_STEP_SECONDS = 30
LEGACY_SATURATION_STEP_SECONDS = 10


class ConfigError(ValueError):
    """Raised when a benchmark request is not safe or internally consistent."""


def _integer(
    values: Mapping[str, Any],
    name: str,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = values.get(name, default)
    if isinstance(raw, bool):
        raise ConfigError(f"{name} must be an integer")
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if value < minimum or value > maximum:
        raise ConfigError(f"{name} must be between {minimum} and {maximum}")
    return value


def _number(
    values: Mapping[str, Any],
    name: str,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    raw = values.get(name, default)
    if isinstance(raw, bool):
        raise ConfigError(f"{name} must be a number")
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be a number") from exc
    if value < minimum or value > maximum:
        raise ConfigError(f"{name} must be between {minimum} and {maximum}")
    return value


def _boolean(values: Mapping[str, Any], name: str, default: bool) -> bool:
    raw = values.get(name, default)
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        normalized = raw.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ConfigError(f"{name} must be a boolean")


def normalize_http_url(value: str, name: str) -> str:
    candidate = value.strip()
    if not candidate:
        raise ConfigError(f"{name} is required")
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigError(f"{name} must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ConfigError(
            f"{name} must not contain credentials, a query, or a fragment"
        )
    return candidate.rstrip("/")


def normalize_target_url(value: str) -> str:
    if value.strip() == LOCAL_CLUSTER:
        return LOCAL_TARGET_URL
    return normalize_http_url(value, "target_url")


def normalize_metrics_url(value: str) -> str:
    if value.strip() == LOCAL_CLUSTER:
        return LOCAL_CLUSTER
    return normalize_http_url(value, "metrics_url")


@dataclass(frozen=True)
class BenchmarkConfig:
    application_type: str
    target_url: str
    metrics_url: str | None
    workload: str
    warmup_seconds: int
    duration_seconds: int
    drain_seconds: int
    users: int
    spawn_rate: float
    arrival_rate: float
    saturation_max_rate: float
    saturation_step_seconds: int
    outcome_timeout_seconds: float
    settlement_timeout_seconds: float
    resource_sample_interval_seconds: float
    seed: int
    collect_resources: bool
    collect_nats_metrics: bool

    @classmethod
    def from_request(
        cls,
        values: Mapping[str, Any],
        application_type: str,
    ) -> "BenchmarkConfig":
        return cls._from_values(
            values,
            application_type,
            minimum_spawn_rate=0.01,
        )

    @classmethod
    def _from_values(
        cls,
        values: Mapping[str, Any],
        application_type: str,
        minimum_spawn_rate: float,
    ) -> "BenchmarkConfig":
        app_type = application_type.strip().upper()
        if app_type not in APPLICATION_TYPES:
            raise ConfigError("APPLICATION_TYPE must be GRPC or NATS")

        workload = str(values.get("workload", "closed")).strip().lower()
        if workload not in WORKLOADS:
            raise ConfigError(
                "workload must be closed, open, saturation, or "
                "fault_tolerance"
            )
        target_url = normalize_target_url(
            str(values.get("target_url", ""))
        )

        collect_resources = _boolean(values, "collect_resources", True)
        collect_nats_metrics = app_type == "NATS" and _boolean(
            values, "collect_nats_metrics", True
        )
        raw_metrics_url = str(values.get("metrics_url") or "")
        metrics_url = (
            normalize_metrics_url(raw_metrics_url)
            if raw_metrics_url.strip()
            else None
        )
        if (collect_resources or collect_nats_metrics) and metrics_url is None:
            raise ConfigError(
                "metrics_url is required when metrics collection is enabled"
            )
        resource_sample_interval_seconds = _number(
            values, "resource_sample_interval_seconds", 5.0, 1.0, 60.0
        )
        if (
            workload == "saturation"
            and collect_nats_metrics
            and resource_sample_interval_seconds > 5.0
        ):
            raise ConfigError(
                "resource_sample_interval_seconds must be no more than 5 "
                "for NATS saturation workloads"
            )

        return cls(
            application_type=app_type,
            target_url=target_url,
            metrics_url=metrics_url,
            workload=workload,
            warmup_seconds=_integer(values, "warmup_seconds", 30, 0, 3600),
            duration_seconds=_integer(
                values,
                "duration_seconds",
                600 if workload == "saturation" else 120,
                1,
                3600,
            ),
            drain_seconds=_integer(values, "drain_seconds", 60, 1, 1800),
            users=_integer(values, "users", 10, 1, 10_000),
            spawn_rate=_number(
                values,
                "spawn_rate",
                1.0,
                minimum_spawn_rate,
                10_000.0,
            ),
            arrival_rate=_number(values, "arrival_rate", 1.0, 0.01, 10_000.0),
            saturation_max_rate=_number(
                values,
                "saturation_max_rate",
                1_000.0,
                SATURATION_START_RATE,
                10_000.0,
            ),
            saturation_step_seconds=_integer(
                values,
                "saturation_step_seconds",
                SATURATION_STEP_SECONDS,
                10,
                300,
            ),
            outcome_timeout_seconds=_number(
                values, "outcome_timeout_seconds", 30.0, 1.0, 1800.0
            ),
            settlement_timeout_seconds=_number(
                values, "settlement_timeout_seconds", 60.0, 1.0, 3600.0
            ),
            resource_sample_interval_seconds=(
                resource_sample_interval_seconds
            ),
            seed=_integer(values, "seed", 1, 0, 2_147_483_647),
            collect_resources=collect_resources,
            collect_nats_metrics=collect_nats_metrics,
        )

    @classmethod
    def from_dict(cls, values: Mapping[str, Any]) -> "BenchmarkConfig":
        values = dict(values)
        values.setdefault(
            "saturation_step_seconds", LEGACY_SATURATION_STEP_SECONDS
        )
        return cls.from_request(
            values,
            str(values.get("application_type", "")),
        )

    @classmethod
    def from_worker_dict(
        cls, values: Mapping[str, Any]
    ) -> "BenchmarkConfig":
        values = dict(values)
        values.setdefault(
            "saturation_step_seconds", LEGACY_SATURATION_STEP_SECONDS
        )
        return cls._from_values(
            values,
            str(values.get("application_type", "")),
            minimum_spawn_rate=0.001,
        )

    @property
    def submission_seconds(self) -> int:
        return self.warmup_seconds + self.duration_seconds

    @property
    def run_seconds(self) -> int:
        return self.submission_seconds + self.drain_seconds

    @property
    def worker_count(self) -> int:
        if self.workload in {"open", "fault_tolerance"}:
            return max(
                1,
                math.ceil(
                    self.arrival_rate / MAX_OPEN_ARRIVAL_RATE_PER_WORKER
                ),
            )
        if self.workload == "saturation":
            return max(
                1,
                math.ceil(
                    self.saturation_effective_max_rate
                    / MAX_OPEN_ARRIVAL_RATE_PER_WORKER
                ),
            )
        return max(1, math.ceil(self.users / MAX_CLOSED_USERS_PER_WORKER))

    @property
    def saturation_step_count(self) -> int:
        return math.ceil(
            self.duration_seconds / self.saturation_step_seconds
        )

    @property
    def saturation_effective_max_rate(self) -> float:
        duration_limited_rate = (
            SATURATION_START_RATE
            + (self.saturation_step_count - 1) * SATURATION_STEP_RATE
        )
        return min(self.saturation_max_rate, duration_limited_rate)

    def for_worker(self, index: int) -> "BenchmarkConfig":
        workers = self.worker_count
        if index < 0 or index >= workers:
            raise IndexError(
                f"worker index {index} is outside [0, {workers})"
            )

        seed = (self.seed + index) % 2_147_483_648
        common = {
            "seed": seed,
            # Resource and NATS metrics describe the shared target, so sampling
            # them in every worker would duplicate and corrupt the aggregate.
            "collect_resources": self.collect_resources and index == 0,
            "collect_nats_metrics": self.collect_nats_metrics and index == 0,
        }
        if self.workload in {"open", "fault_tolerance"}:
            return replace(
                self,
                arrival_rate=min(
                    MAX_OPEN_ARRIVAL_RATE_PER_WORKER,
                    self.arrival_rate
                    - index * MAX_OPEN_ARRIVAL_RATE_PER_WORKER,
                ),
                **common,
            )
        if self.workload == "saturation":
            # Every synchronized worker needs the global ladder. The driver
            # deterministically assigns each global request ordinal to one
            # worker, so retaining the global rate does not duplicate load.
            return replace(self, **common)

        users_per_worker, remainder = divmod(self.users, workers)
        return replace(
            self,
            users=users_per_worker + (1 if index < remainder else 0),
            spawn_rate=self.spawn_rate / workers,
            **common,
        )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
