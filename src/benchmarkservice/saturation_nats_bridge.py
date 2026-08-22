# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

"""Run saturation coordination's asyncio NATS client outside gevent."""

from __future__ import annotations

import json
import sys
from typing import Any

from shared_store import NatsSharedStore, RecordNotFound


def dispatch(
    store: NatsSharedStore, request: dict[str, Any]
) -> tuple[dict[str, Any], bool]:
    request_id = request.get("id")
    operation = request.get("operation")
    try:
        if operation == "put":
            name = request.get("name")
            value = request.get("value")
            if not isinstance(name, str) or not isinstance(value, dict):
                raise ValueError("put requires a string name and object value")
            store.put_object(
                name,
                json.dumps(value, sort_keys=True).encode("utf-8"),
            )
            return {"id": request_id, "ok": True}, False
        if operation == "get":
            name = request.get("name")
            if not isinstance(name, str):
                raise ValueError("get requires a string name")
            value = json.loads(store.get_object(name))
            if not isinstance(value, dict):
                raise ValueError(f"coordination value {name} is not an object")
            return {
                "id": request_id,
                "ok": True,
                "value": value,
            }, False
        if operation == "close":
            return {"id": request_id, "ok": True}, True
        raise ValueError(f"unknown operation {operation!r}")
    except RecordNotFound as error:
        return {
            "id": request_id,
            "ok": False,
            "error_type": "RecordNotFound",
            "error": str(error),
        }, False
    except Exception as error:
        return {
            "id": request_id,
            "ok": False,
            "error_type": type(error).__name__,
            "error": str(error),
        }, False


def main() -> int:
    store: NatsSharedStore | None = None
    try:
        for line in sys.stdin:
            try:
                request = json.loads(line)
                if not isinstance(request, dict):
                    raise ValueError("request is not an object")
                if request.get("operation") == "close":
                    response = {"id": request.get("id"), "ok": True}
                    should_close = True
                else:
                    if store is None:
                        store = NatsSharedStore(operation_timeout=10)
                    response, should_close = dispatch(store, request)
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
        if store is not None:
            store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
