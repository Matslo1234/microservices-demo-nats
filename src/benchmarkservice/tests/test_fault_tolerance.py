# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import json
import subprocess
import tempfile
import threading
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import Mock, patch

from fault_tolerance import (
    Fault,
    FaultController,
    FaultPlan,
    Kubectl,
    config_from_args,
    plan_from_args,
)


class FakeKubectl:
    def __init__(self) -> None:
        self.scales = []
        self.shipping_failure = True

    def scale(self, deployment, replicas):
        self.scales.append((deployment, replicas))
        if (
            deployment == "shippingservice"
            and replicas == 0
            and self.shipping_failure
        ):
            self.shipping_failure = False
            raise RuntimeError("injected kubectl failure")


class FaultToleranceTest(unittest.TestCase):
    @staticmethod
    def arguments(**overrides):
        values = {
            "target_url": "https://shop.example",
            "metrics_url": "https://metrics.example/snapshot",
            "application_type": "NATS",
            "arrival_rate": 250,
            "drain_seconds": 60,
            "outcome_timeout_seconds": 30.0,
            "settlement_timeout_seconds": 60.0,
            "seed": 1,
            "startup_time": 10,
            "disable_paymentservice_time": 20,
            "between_disable_time": 30,
            "disable_shippingservice_time": 40,
            "recovery_time": 50,
            "namespace": "shop",
            "context": "test",
            "payment_deployment": "paymentservice",
            "shipping_deployment": "shippingservice",
            "convergence_stable_seconds": 3,
            "convergence_success_fraction": 0.9,
            "convergence_pending_tolerance": 0,
        }
        values.update(overrides)
        return Namespace(**values)

    def test_plan_preserves_replicas_and_sets_absolute_timeline(self):
        kubectl = Mock()
        kubectl.replicas.side_effect = [2, 3]

        plan = plan_from_args(self.arguments(), kubectl)
        config = config_from_args(self.arguments(), plan)

        self.assertEqual(150, plan.duration_seconds)
        self.assertEqual(
            (10, 30),
            (
                plan.faults[0].disable_at_seconds,
                plan.faults[0].enable_at_seconds,
            ),
        )
        self.assertEqual(
            (60, 100),
            (
                plan.faults[1].disable_at_seconds,
                plan.faults[1].enable_at_seconds,
            ),
        )
        self.assertEqual(
            [2, 3], [fault.original_replicas for fault in plan.faults]
        )
        self.assertEqual("fault_tolerance", config.workload)
        self.assertEqual(150, config.duration_seconds)
        self.assertEqual(3, config.worker_count)
        self.assertEqual(1, config.resource_sample_interval_seconds)

    @patch("fault_tolerance.subprocess.run")
    def test_kubectl_uses_explicit_context_and_namespace(self, run_mock):
        run_mock.side_effect = [
            subprocess.CompletedProcess([], 0, stdout="yes", stderr=""),
            subprocess.CompletedProcess([], 0, stdout="2", stderr=""),
        ]
        kubectl = Kubectl("shop", "cluster-a", "kubectl", 15)

        kubectl.ensure_scalable("paymentservice")
        self.assertEqual(2, kubectl.replicas("paymentservice"))

        command = run_mock.call_args.args[0]
        self.assertEqual(
            ["kubectl", "--context", "cluster-a", "--namespace", "shop"],
            command[:5],
        )
        self.assertIn("paymentservice", command)
        self.assertEqual(15, run_mock.call_args.kwargs["timeout"])
        permission_command = run_mock.call_args_list[0].args[0]
        self.assertIn("--subresource=scale", permission_command)

    def test_controller_restores_a_deployment_after_scale_failure(self):
        kubectl = FakeKubectl()
        plan = FaultPlan(
            namespace="default",
            context=None,
            startup_time=1,
            disable_paymentservice_time=1,
            between_disable_time=1,
            disable_shippingservice_time=1,
            recovery_time=1,
            convergence_stable_seconds=1,
            convergence_success_fraction=0.9,
            convergence_pending_tolerance=0,
            faults=(
                Fault("paymentservice", "paymentservice", 0, 0, 2),
                Fault("shippingservice", "shippingservice", 0, 0, 3),
            ),
        )
        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(FaultController, "_wait_until"), patch(
                "builtins.print"
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "injected kubectl failure"
                ):
                    FaultController(kubectl, plan)(
                        1.0, Path(temporary), threading.Event()
                    )

            records = [
                json.loads(line)
                for line in (Path(temporary) / "faults.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]

        self.assertEqual(
            [
                ("paymentservice", 0),
                ("paymentservice", 2),
                ("shippingservice", 0),
                ("shippingservice", 3),
            ],
            kubectl.scales,
        )
        self.assertEqual("restored", records[-1]["action"])


if __name__ == "__main__":
    unittest.main()
