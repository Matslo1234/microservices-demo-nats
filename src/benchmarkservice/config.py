# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping
from urllib.parse import urlparse


APPLICATION_TYPES = {"GRPC", "NATS"}
WORKLOADS = {"closed", "open"}


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


def normalize_target_url(value: str) -> str:
    candidate = value.strip()
    if "://" not in candidate:
        candidate = "http://" + candidate
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigError("FRONTEND_ADDR must be an HTTP(S) address")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ConfigError("FRONTEND_ADDR must not contain credentials, a query, or a fragment")
    return candidate.rstrip("/")


@dataclass(frozen=True)
class BenchmarkConfig:
    application_type: str
    target_url: str
    workload: str
    warmup_seconds: int
    duration_seconds: int
    drain_seconds: int
    users: int
    spawn_rate: float
    arrival_rate: float
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
        target_url: str,
    ) -> "BenchmarkConfig":
        app_type = application_type.strip().upper()
        if app_type not in APPLICATION_TYPES:
            raise ConfigError("APPLICATION_TYPE must be GRPC or NATS")

        workload = str(values.get("workload", "closed")).strip().lower()
        if workload not in WORKLOADS:
            raise ConfigError("workload must be closed or open")

        return cls(
            application_type=app_type,
            target_url=normalize_target_url(target_url),
            workload=workload,
            warmup_seconds=_integer(values, "warmup_seconds", 30, 0, 3600),
            duration_seconds=_integer(values, "duration_seconds", 120, 1, 3600),
            drain_seconds=_integer(values, "drain_seconds", 60, 1, 1800),
            users=_integer(values, "users", 10, 1, 10_000),
            spawn_rate=_number(values, "spawn_rate", 1.0, 0.01, 10_000.0),
            arrival_rate=_number(values, "arrival_rate", 1.0, 0.01, 10_000.0),
            outcome_timeout_seconds=_number(
                values, "outcome_timeout_seconds", 30.0, 1.0, 1800.0
            ),
            settlement_timeout_seconds=_number(
                values, "settlement_timeout_seconds", 60.0, 1.0, 3600.0
            ),
            resource_sample_interval_seconds=_number(
                values, "resource_sample_interval_seconds", 5.0, 1.0, 60.0
            ),
            seed=_integer(values, "seed", 1, 0, 2_147_483_647),
            collect_resources=_boolean(values, "collect_resources", True),
            collect_nats_metrics=app_type == "NATS"
            and _boolean(values, "collect_nats_metrics", True),
        )

    @classmethod
    def from_dict(cls, values: Mapping[str, Any]) -> "BenchmarkConfig":
        return cls.from_request(
            values,
            str(values.get("application_type", "")),
            str(values.get("target_url", "")),
        )

    @property
    def submission_seconds(self) -> int:
        return self.warmup_seconds + self.duration_seconds

    @property
    def run_seconds(self) -> int:
        return self.submission_seconds + self.drain_seconds

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
