# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from saturation import (
    SaturationCoordinator,
    _NatsBackend,
    consumer_pending_total,
    evaluate_rung,
)
from saturation_nats_bridge import dispatch
from shared_store import RecordNotFound


class _FakeInput:
    def __init__(self, process: "_FakeProcess") -> None:
        self.process = process

    def write(self, value: str) -> int:
        self.process.request = json.loads(value)
        return len(value)

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class _FakeOutput:
    def __init__(self, process: "_FakeProcess") -> None:
        self.process = process

    def readline(self) -> str:
        request = self.process.request
        operation = request["operation"]
        response: dict = {"id": request["id"], "ok": True}
        if operation == "put":
            self.process.values[request["name"]] = request["value"]
        elif operation == "get":
            name = request["name"]
            if name in self.process.values:
                response["value"] = self.process.values[name]
            else:
                response.update(
                    {
                        "ok": False,
                        "error_type": "RecordNotFound",
                        "error": name,
                    }
                )
        elif operation == "close":
            self.process.closing = True
        return json.dumps(response) + "\n"

    def close(self) -> None:
        pass


class _FakeProcess:
    def __init__(self) -> None:
        self.values: dict[str, dict] = {}
        self.request: dict = {}
        self.closing = False
        self.returncode: int | None = None
        self.stdin = _FakeInput(self)
        self.stdout = _FakeOutput(self)

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float) -> int:
        self.returncode = 0
        return self.returncode

    def terminate(self) -> None:
        self.returncode = -15

    def kill(self) -> None:
        self.returncode = -9


class _FakeStore:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}

    def put_object(self, name: str, value: bytes) -> None:
        self.values[name] = value

    def get_object(self, name: str) -> bytes:
        try:
            return self.values[name]
        except KeyError as error:
            raise RecordNotFound(name) from error


class SaturationTest(unittest.TestCase):
    def test_coordinator_differences_global_completed_event_counter(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            coordinator = SaturationCoordinator(
                application_type="NATS",
                output_directory=Path(temporary),
                worker_index=0,
                worker_count=1,
            )

            decision = coordinator.finish_rung(
                rung=0,
                target_rate=20,
                started_elapsed_seconds=0,
                ended_elapsed_seconds=10,
                completed_before=5,
                completed_after=5,
                processed_before={
                    "observer_id": "observer-a",
                    "total": 1_000,
                },
                processed_after={
                    "observer_id": "observer-a",
                    "total": 1_180,
                },
                pending_start=0,
                pending_end=0,
                final_rung=False,
                maximum_rate_reached=False,
            )

        self.assertEqual(0, decision["completed_during_rung"])
        self.assertEqual(180, decision["processed_during_rung"])
        self.assertEqual(18, decision["processing_goodput_orders_per_second"])

    def test_coordinator_rejects_discontinuous_completed_event_count(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            coordinator = SaturationCoordinator(
                application_type="NATS",
                output_directory=Path(temporary),
                worker_index=0,
                worker_count=1,
            )

            decision = coordinator.finish_rung(
                rung=0,
                target_rate=20,
                started_elapsed_seconds=0,
                ended_elapsed_seconds=10,
                completed_before=5,
                completed_after=10,
                processed_before={"observer_id": "before", "total": 1_000},
                processed_after={"observer_id": "after", "total": 10},
                pending_start=0,
                pending_end=0,
                final_rung=False,
                maximum_rate_reached=False,
            )

        self.assertIsNone(decision["processed_during_rung"])
        self.assertIsNone(decision["processing_goodput_orders_per_second"])
        self.assertIn(
            "lost continuity",
            decision["processing_goodput_unavailable_reason"],
        )

    def test_nats_backend_uses_the_bridge_protocol(self) -> None:
        process = _FakeProcess()
        with mock.patch(
            "saturation.subprocess.Popen", return_value=process
        ) as popen:
            backend = _NatsBackend()
            backend.put("run/rung.json", {"completed": 5})

            self.assertEqual(
                {"completed": 5}, backend.get("run/rung.json")
            )
            with self.assertRaises(RecordNotFound):
                backend.get("run/missing.json")
            backend.close()

        command = popen.call_args.args[0]
        self.assertEqual("-u", command[1])
        self.assertTrue(command[2].endswith("saturation_nats_bridge.py"))
        self.assertTrue(process.closing)

    def test_nats_bridge_dispatches_without_gevent_dependencies(self) -> None:
        store = _FakeStore()

        put_response, should_close = dispatch(
            store,
            {
                "id": 1,
                "operation": "put",
                "name": "run/rung.json",
                "value": {"completed": 5},
            },
        )
        get_response, _ = dispatch(
            store,
            {
                "id": 2,
                "operation": "get",
                "name": "run/rung.json",
            },
        )
        missing_response, _ = dispatch(
            store,
            {
                "id": 3,
                "operation": "get",
                "name": "run/missing.json",
            },
        )

        self.assertEqual({"id": 1, "ok": True}, put_response)
        self.assertFalse(should_close)
        self.assertEqual(
            {"id": 2, "ok": True, "value": {"completed": 5}},
            get_response,
        )
        self.assertEqual("RecordNotFound", missing_response["error_type"])

    def test_goodput_plateau_does_not_stop_the_ladder(self) -> None:
        increasing = evaluate_rung(
            application_type="GRPC",
            target_rate=20,
            duration_seconds=10,
            completed=100,
            previous_goodput=9,
            pending_start=None,
            pending_end=None,
            final_rung=False,
            maximum_rate_reached=False,
        )
        plateau = evaluate_rung(
            application_type="GRPC",
            target_rate=30,
            duration_seconds=10,
            completed=100,
            previous_goodput=10,
            pending_start=None,
            pending_end=None,
            final_rung=False,
            maximum_rate_reached=False,
        )

        self.assertFalse(increasing["stop"])
        self.assertFalse(plateau["stop"])
        self.assertTrue(plateau["saturated"])
        self.assertEqual(
            "goodput_stopped_increasing", plateau["saturation_reason"]
        )
        self.assertIsNone(plateau["stop_reason"])

    def test_nats_rapid_pending_growth_does_not_stop_the_ladder(self) -> None:
        decision = evaluate_rung(
            application_type="NATS",
            target_rate=20,
            duration_seconds=10,
            completed=150,
            previous_goodput=10,
            pending_start=5,
            pending_end=105,
            final_rung=False,
            maximum_rate_reached=False,
        )

        self.assertFalse(decision["stop"])
        self.assertEqual(
            "nats_pending_increasing_rapidly",
            decision["saturation_reason"],
        )
        self.assertEqual(10, decision["pending_growth_per_second"])

    def test_nats_goodput_uses_stream_events_after_client_timeouts(self) -> None:
        decision = evaluate_rung(
            application_type="NATS",
            target_rate=150,
            duration_seconds=30,
            completed=0,
            processed=2_400,
            previous_goodput=70,
            pending_start=5_000,
            pending_end=5_000,
            final_rung=False,
            maximum_rate_reached=False,
        )

        self.assertEqual(0, decision["client_observed_completed_during_rung"])
        self.assertEqual(0, decision["observed_goodput_orders_per_second"])
        self.assertEqual(2_400, decision["processed_during_rung"])
        self.assertEqual(80, decision["processing_goodput_orders_per_second"])
        self.assertEqual(
            "nats_order_completed_events",
            decision["processing_goodput_source"],
        )
        self.assertFalse(decision["saturated"])

    def test_only_final_rung_stops_the_ladder(self) -> None:
        maximum_rate = evaluate_rung(
            application_type="GRPC",
            target_rate=20,
            duration_seconds=10,
            completed=150,
            previous_goodput=10,
            pending_start=None,
            pending_end=None,
            final_rung=False,
            maximum_rate_reached=True,
        )
        final = evaluate_rung(
            application_type="GRPC",
            target_rate=20,
            duration_seconds=10,
            completed=90,
            previous_goodput=15,
            pending_start=None,
            pending_end=None,
            final_rung=True,
            maximum_rate_reached=True,
        )

        self.assertFalse(maximum_rate["stop"])
        self.assertTrue(maximum_rate["maximum_rate_reached"])
        self.assertTrue(final["stop"])
        self.assertEqual("maximum_duration_reached", final["stop_reason"])
        self.assertEqual(
            "goodput_stopped_increasing", final["saturation_reason"]
        )

    def test_pending_total_deduplicates_exporter_replicas(self) -> None:
        metrics = [
            self._pending("BOUTIQUE_EVENTS", "checkout", 3),
            self._pending("BOUTIQUE_EVENTS", "checkout", 3),
            self._pending("BOUTIQUE_COMMANDS", "cart", 2),
            self._pending("KV_BENCHMARK_RUNS", "controller", 100),
        ]

        self.assertEqual(5, consumer_pending_total(metrics))

    @staticmethod
    def _pending(stream: str, consumer: str, value: float) -> dict:
        return {
            "name": "jetstream_consumer_num_pending",
            "labels": {
                "stream_name": stream,
                "consumer_name": consumer,
            },
            "value": value,
        }


if __name__ == "__main__":
    unittest.main()
