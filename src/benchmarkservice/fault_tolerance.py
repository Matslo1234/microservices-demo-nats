#!/usr/bin/env python3
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

"""Run an open-loop benchmark while scaling target services to zero."""

from __future__ import annotations

import argparse
import json
import subprocess
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import BenchmarkConfig, ConfigError
from standalone import run


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Fault:
    service: str
    deployment: str
    disable_at_seconds: int
    enable_at_seconds: int
    original_replicas: int


@dataclass(frozen=True)
class FaultPlan:
    namespace: str
    context: str | None
    startup_time: int
    disable_paymentservice_time: int
    between_disable_time: int
    disable_shippingservice_time: int
    recovery_time: int
    convergence_stable_seconds: int
    convergence_success_fraction: float
    convergence_pending_tolerance: int
    faults: tuple[Fault, ...]

    @property
    def duration_seconds(self) -> int:
        return (
            self.startup_time
            + self.disable_paymentservice_time
            + self.between_disable_time
            + self.disable_shippingservice_time
            + self.recovery_time
        )

    def as_dict(self, start_epoch: float) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "start_epoch": start_epoch,
            "duration_seconds": self.duration_seconds,
            **asdict(self),
        }


class Kubectl:
    def __init__(
        self,
        namespace: str,
        context: str | None,
        executable: str,
        timeout_seconds: int,
    ) -> None:
        self.namespace = namespace
        self.context = context
        self.executable = executable
        self.timeout_seconds = timeout_seconds

    def _command(self, *arguments: str) -> list[str]:
        command = [self.executable]
        if self.context:
            command.extend(["--context", self.context])
        command.extend(["--namespace", self.namespace, *arguments])
        return command

    def _run(self, *arguments: str) -> str:
        command = self._command(*arguments)
        try:
            completed = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
        except FileNotFoundError as error:
            raise RuntimeError(
                f"kubectl executable {self.executable!r} was not found"
            ) from error
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(
                f"kubectl timed out after {self.timeout_seconds}s: "
                + " ".join(command)
            ) from error
        except subprocess.CalledProcessError as error:
            detail = (error.stderr or error.stdout or "").strip()
            raise RuntimeError(
                f"kubectl failed ({' '.join(command)}): {detail}"
            ) from error
        return completed.stdout.strip()

    def replicas(self, deployment: str) -> int:
        raw = self._run(
            "get",
            "deployment",
            deployment,
            "--output",
            "jsonpath={.spec.replicas}",
        )
        try:
            replicas = int(raw or "1")
        except ValueError as error:
            raise RuntimeError(
                f"deployment/{deployment} returned invalid replicas {raw!r}"
            ) from error
        if replicas < 1:
            raise RuntimeError(
                f"deployment/{deployment} must have at least one replica "
                "before the benchmark"
            )
        return replicas

    def ensure_scalable(self, deployment: str) -> None:
        allowed = self._run(
            "auth",
            "can-i",
            "update",
            f"deployment/{deployment}",
            "--subresource=scale",
        )
        if allowed.strip().lower() != "yes":
            raise RuntimeError(
                f"kubectl user cannot scale deployment/{deployment} in "
                f"namespace {self.namespace}"
            )

    def scale(self, deployment: str, replicas: int) -> None:
        self._run(
            "scale",
            "deployment",
            deployment,
            f"--replicas={replicas}",
        )


class FaultController:
    def __init__(self, kubectl: Kubectl, plan: FaultPlan) -> None:
        self.kubectl = kubectl
        self.plan = plan

    @staticmethod
    def _write_json(path: Path, value: Any) -> None:
        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("w", encoding="utf-8") as target:
            json.dump(value, target, indent=2, sort_keys=True)
            target.write("\n")
        temporary.replace(path)

    @staticmethod
    def _wait_until(
        deadline_epoch: float, stop_requested: threading.Event
    ) -> None:
        while True:
            remaining = deadline_epoch - time.time()
            if remaining <= 0:
                return
            if stop_requested.wait(min(remaining, 0.5)):
                raise InterruptedError("fault-tolerance run was interrupted")

    def __call__(
        self,
        start_epoch: float,
        run_directory: Path,
        stop_requested: threading.Event,
    ) -> None:
        self._write_json(
            run_directory / "fault-plan.json",
            self.plan.as_dict(start_epoch),
        )
        events_path = run_directory / "faults.jsonl"
        scaled_down: dict[str, Fault] = {}

        with events_path.open("a", encoding="utf-8") as events:

            def record(fault: Fault, action: str, **details: Any) -> None:
                timestamp = time.time()
                value = {
                    "timestamp": timestamp,
                    "at": utc_now(),
                    "elapsed_seconds": round(
                        max(0.0, timestamp - start_epoch), 6
                    ),
                    "service": fault.service,
                    "deployment": fault.deployment,
                    "action": action,
                    "original_replicas": fault.original_replicas,
                    **details,
                }
                events.write(json.dumps(value, sort_keys=True) + "\n")
                events.flush()
                print(
                    "[fault-tolerance] "
                    f"t={value['elapsed_seconds']:.3f}s "
                    f"deployment/{fault.deployment} {action}",
                    flush=True,
                )

            try:
                for fault in self.plan.faults:
                    self._wait_until(
                        start_epoch + fault.disable_at_seconds,
                        stop_requested,
                    )
                    record(
                        fault,
                        "scale_down_started",
                        planned_elapsed_seconds=fault.disable_at_seconds,
                    )
                    scaled_down[fault.deployment] = fault
                    self.kubectl.scale(fault.deployment, 0)
                    record(fault, "disabled", replicas=0)

                    self._wait_until(
                        start_epoch + fault.enable_at_seconds,
                        stop_requested,
                    )
                    record(
                        fault,
                        "scale_up_started",
                        planned_elapsed_seconds=fault.enable_at_seconds,
                    )
                    self.kubectl.scale(
                        fault.deployment, fault.original_replicas
                    )
                    scaled_down.pop(fault.deployment, None)
                    record(
                        fault,
                        "reenabled",
                        replicas=fault.original_replicas,
                    )

                self._wait_until(
                    start_epoch + self.plan.duration_seconds,
                    stop_requested,
                )
            finally:
                restoration_errors: list[str] = []
                for fault in reversed(tuple(scaled_down.values())):
                    try:
                        self.kubectl.scale(
                            fault.deployment, fault.original_replicas
                        )
                        record(
                            fault,
                            "restored",
                            replicas=fault.original_replicas,
                        )
                    except Exception as error:
                        restoration_errors.append(
                            f"deployment/{fault.deployment}: {error}"
                        )
                        record(fault, "restore_failed", error=str(error))
                if restoration_errors:
                    raise RuntimeError(
                        "could not restore scaled deployments: "
                        + "; ".join(restoration_errors)
                    )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Run open-loop load while kubectl temporarily scales "
            "paymentservice and shippingservice to zero."
        )
    )
    result.add_argument("--url", dest="target_url", required=True)
    result.add_argument("--metrics-url", required=True)
    result.add_argument(
        "--application-type", choices=("GRPC", "NATS"), required=True
    )
    result.add_argument("--arrival-rate", type=float, default=1.0)
    result.add_argument("--drain-seconds", type=int, default=60)
    result.add_argument("--outcome-timeout-seconds", type=float, default=30.0)
    result.add_argument(
        "--settlement-timeout-seconds", type=float, default=60.0
    )
    result.add_argument("--seed", type=int, default=1)
    result.add_argument(
        "--startup-time", "--startup_time", type=int, default=30
    )
    result.add_argument(
        "--disable-paymentservice-time",
        "--disable_paymentservice_time",
        type=int,
        default=30,
    )
    result.add_argument(
        "--between-disable-time",
        "--between_disable_time",
        type=int,
        default=30,
    )
    result.add_argument(
        "--disable-shippingservice-time",
        "--disable_shippingservice_time",
        type=int,
        default=30,
    )
    result.add_argument(
        "--recovery-time", "--recovery_time", type=int, default=60
    )
    result.add_argument("--namespace", default="default")
    result.add_argument("--context")
    result.add_argument(
        "--payment-deployment", default="paymentservice"
    )
    result.add_argument(
        "--shipping-deployment", default="shippingservice"
    )
    result.add_argument("--kubectl", default="kubectl")
    result.add_argument("--kubectl-timeout-seconds", type=int, default=30)
    result.add_argument("--convergence-stable-seconds", type=int, default=3)
    result.add_argument(
        "--convergence-success-fraction", type=float, default=0.9
    )
    result.add_argument("--convergence-pending-tolerance", type=int, default=0)
    result.add_argument(
        "--output", type=Path, default=Path("benchmark-results")
    )
    return result


def plan_from_args(arguments: argparse.Namespace, kubectl: Kubectl) -> FaultPlan:
    timing_fields = (
        "startup_time",
        "disable_paymentservice_time",
        "between_disable_time",
        "disable_shippingservice_time",
        "recovery_time",
    )
    for field in timing_fields:
        if getattr(arguments, field) < 1:
            raise ConfigError(f"{field} must be at least 1 second")
    if arguments.convergence_stable_seconds < 1:
        raise ConfigError("convergence_stable_seconds must be at least 1")
    if arguments.recovery_time < arguments.convergence_stable_seconds:
        raise ConfigError(
            "recovery_time must be at least convergence_stable_seconds"
        )
    if not 0 < arguments.convergence_success_fraction <= 1:
        raise ConfigError(
            "convergence_success_fraction must be greater than 0 and no "
            "more than 1"
        )
    if arguments.convergence_pending_tolerance < 0:
        raise ConfigError(
            "convergence_pending_tolerance must not be negative"
        )
    if arguments.payment_deployment == arguments.shipping_deployment:
        raise ConfigError(
            "payment_deployment and shipping_deployment must be different"
        )

    kubectl.ensure_scalable(arguments.payment_deployment)
    kubectl.ensure_scalable(arguments.shipping_deployment)
    payment_replicas = kubectl.replicas(arguments.payment_deployment)
    shipping_replicas = kubectl.replicas(arguments.shipping_deployment)
    payment_down = arguments.startup_time
    payment_up = payment_down + arguments.disable_paymentservice_time
    shipping_down = payment_up + arguments.between_disable_time
    shipping_up = shipping_down + arguments.disable_shippingservice_time
    return FaultPlan(
        namespace=arguments.namespace,
        context=arguments.context,
        startup_time=arguments.startup_time,
        disable_paymentservice_time=(
            arguments.disable_paymentservice_time
        ),
        between_disable_time=arguments.between_disable_time,
        disable_shippingservice_time=(
            arguments.disable_shippingservice_time
        ),
        recovery_time=arguments.recovery_time,
        convergence_stable_seconds=arguments.convergence_stable_seconds,
        convergence_success_fraction=(
            arguments.convergence_success_fraction
        ),
        convergence_pending_tolerance=(
            arguments.convergence_pending_tolerance
        ),
        faults=(
            Fault(
                service="paymentservice",
                deployment=arguments.payment_deployment,
                disable_at_seconds=payment_down,
                enable_at_seconds=payment_up,
                original_replicas=payment_replicas,
            ),
            Fault(
                service="shippingservice",
                deployment=arguments.shipping_deployment,
                disable_at_seconds=shipping_down,
                enable_at_seconds=shipping_up,
                original_replicas=shipping_replicas,
            ),
        ),
    )


def config_from_args(
    arguments: argparse.Namespace, plan: FaultPlan
) -> BenchmarkConfig:
    return BenchmarkConfig.from_request(
        {
            "target_url": arguments.target_url,
            "metrics_url": arguments.metrics_url,
            "workload": "fault_tolerance",
            "warmup_seconds": 0,
            "duration_seconds": plan.duration_seconds,
            "drain_seconds": arguments.drain_seconds,
            "arrival_rate": arguments.arrival_rate,
            "outcome_timeout_seconds": arguments.outcome_timeout_seconds,
            "settlement_timeout_seconds": (
                arguments.settlement_timeout_seconds
            ),
            "resource_sample_interval_seconds": 1,
            "seed": arguments.seed,
            "collect_resources": True,
            "collect_nats_metrics": arguments.application_type == "NATS",
        },
        arguments.application_type,
    )


def main() -> int:
    argument_parser = parser()
    arguments = argument_parser.parse_args()
    if arguments.kubectl_timeout_seconds < 1:
        argument_parser.error("kubectl_timeout_seconds must be at least 1")
    kubectl = Kubectl(
        arguments.namespace,
        arguments.context,
        arguments.kubectl,
        arguments.kubectl_timeout_seconds,
    )
    try:
        plan = plan_from_args(arguments, kubectl)
        config = config_from_args(arguments, plan)
    except (ConfigError, RuntimeError) as error:
        argument_parser.error(str(error))
    return_code, run_directory = run(
        config,
        arguments.output,
        FaultController(kubectl, plan),
    )
    print(run_directory)
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
