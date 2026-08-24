# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import json
import unittest
from unittest import mock

from nats_order_observer import NatsOrderCompletedObserver


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
        response: dict = {"id": request["id"], "ok": True}
        if request["operation"] == "sample":
            response["value"] = {
                "observer_id": "observer-a",
                "total": 12_345,
            }
        elif request["operation"] == "close":
            self.process.closing = True
        return json.dumps(response) + "\n"

    def close(self) -> None:
        pass


class _FakeProcess:
    def __init__(self) -> None:
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


class NatsOrderCompletedObserverTest(unittest.TestCase):
    def test_uses_unpatched_bridge_protocol(self) -> None:
        process = _FakeProcess()
        with mock.patch(
            "nats_order_observer.subprocess.Popen", return_value=process
        ) as popen:
            observer = NatsOrderCompletedObserver()

            self.assertEqual(
                {"observer_id": "observer-a", "total": 12_345},
                observer.sample(),
            )
            observer.close()

        command = popen.call_args.args[0]
        self.assertEqual("-u", command[1])
        self.assertTrue(command[2].endswith("nats_order_observer_bridge.py"))
        self.assertTrue(process.closing)


if __name__ == "__main__":
    unittest.main()
