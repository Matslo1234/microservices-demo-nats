# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

"""Observe completed-order events outside Locust's gevent-patched process."""

from __future__ import annotations

import json
import sys
from typing import Any

from cluster_metrics import NatsMicroStatsClient


def dispatch(
    observer: NatsMicroStatsClient, request: dict[str, Any]
) -> tuple[dict[str, Any], bool]:
    request_id = request.get("id")
    operation = request.get("operation")
    try:
        if operation == "sample":
            return {
                "id": request_id,
                "ok": True,
                "value": observer.order_completed_sample(),
            }, False
        if operation == "close":
            return {"id": request_id, "ok": True}, True
        raise ValueError(f"unknown operation {operation!r}")
    except Exception as error:
        return {
            "id": request_id,
            "ok": False,
            "error_type": type(error).__name__,
            "error": str(error),
        }, False


def main() -> int:
    observer = NatsMicroStatsClient()
    try:
        for line in sys.stdin:
            try:
                request = json.loads(line)
                if not isinstance(request, dict):
                    raise ValueError("request is not an object")
                response, should_close = dispatch(observer, request)
            except Exception as error:
                response = {
                    "id": None,
                    "ok": False,
                    "error_type": type(error).__name__,
                    "error": str(error),
                }
                should_close = False
            sys.stdout.write(
                json.dumps(response, separators=(",", ":")) + "\n"
            )
            sys.stdout.flush()
            if should_close:
                break
    finally:
        observer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
