# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

"""Synchronous facade for the unpatched NATS order observer process."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any


class NatsOrderCompletedObserver:
    """Run the asyncio NATS observer outside Locust's patched runtime."""

    def __init__(self) -> None:
        bridge = Path(__file__).with_name(
            "nats_order_observer_bridge.py"
        ).resolve()
        self.process = subprocess.Popen(
            [sys.executable, "-u", str(bridge)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        self.request_id = 0

    def _request(self, operation: str) -> dict[str, Any]:
        if self.process.poll() is not None:
            raise RuntimeError(
                "NATS completed-order observer exited with code "
                f"{self.process.returncode}"
            )
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError(
                "NATS completed-order observer pipes are unavailable"
            )
        self.request_id += 1
        request = {"id": self.request_id, "operation": operation}
        try:
            self.process.stdin.write(
                json.dumps(request, separators=(",", ":")) + "\n"
            )
            self.process.stdin.flush()
            line = self.process.stdout.readline()
        except (BrokenPipeError, OSError) as error:
            raise RuntimeError(
                "lost NATS completed-order observer process"
            ) from error
        if not line:
            raise RuntimeError(
                "NATS completed-order observer closed its output"
            )
        try:
            response = json.loads(line)
        except json.JSONDecodeError as error:
            raise RuntimeError(
                "NATS completed-order observer returned invalid JSON"
            ) from error
        if (
            not isinstance(response, dict)
            or response.get("id") != self.request_id
        ):
            raise RuntimeError(
                "NATS completed-order observer returned an unexpected response"
            )
        if response.get("ok") is not True:
            detail = str(response.get("error", "unknown error"))
            raise RuntimeError(
                f"NATS completed-order observer failed: {detail}"
            )
        return response

    def sample(self) -> dict[str, Any]:
        value = self._request("sample").get("value")
        if not isinstance(value, dict):
            raise RuntimeError(
                "NATS completed-order observer returned an invalid sample"
            )
        observer_id = value.get("observer_id")
        total = value.get("total")
        if (
            not isinstance(observer_id, str)
            or not observer_id
            or not isinstance(total, int)
            or isinstance(total, bool)
        ):
            raise RuntimeError(
                "NATS completed-order observer returned an invalid sample"
            )
        return value

    def close(self) -> None:
        if self.process.poll() is None:
            try:
                self._request("close")
            except Exception:
                self.process.terminate()
        if self.process.stdin is not None:
            self.process.stdin.close()
        if self.process.stdout is not None:
            self.process.stdout.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
