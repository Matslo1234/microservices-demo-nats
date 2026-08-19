# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Iterator
from urllib.parse import urlsplit, urlunsplit


class SSEProtocolError(ValueError):
    """Raised when an order event stream violates its JSON/SSE contract."""


@dataclass(frozen=True)
class ServerSentEvent:
    event: str
    data: str
    event_id: str | None


@dataclass(frozen=True)
class ReceivedOrderEvent:
    order: dict[str, Any]
    event_id: str | None
    received_monotonic: float
    received_epoch: float

    @property
    def received_at(self) -> str:
        return utc_timestamp(self.received_epoch)


def order_events_url(location: str) -> str:
    """Return the SSE URL corresponding to an order resource Location."""
    parsed = urlsplit(location)
    path = parsed.path.rstrip("/")
    if not path:
        raise ValueError("order Location has no path")
    return urlunsplit(
        (parsed.scheme, parsed.netloc, path + "/events", parsed.query, "")
    )


def is_event_stream_content_type(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return value.split(";", 1)[0].strip().lower() == "text/event-stream"


def utc_timestamp(epoch: float) -> str:
    return (
        datetime.fromtimestamp(epoch, timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def iter_sse_events(
    lines: Iterable[bytes | bytearray | memoryview | str],
) -> Iterator[ServerSentEvent]:
    event_type = "message"
    data: list[str] = []
    last_event_id: str | None = None
    first_line = True

    for raw_line in lines:
        if isinstance(raw_line, (bytes, bytearray, memoryview)):
            try:
                line = bytes(raw_line).decode("utf-8")
            except UnicodeDecodeError as error:
                raise SSEProtocolError(
                    "order event stream is not valid UTF-8"
                ) from error
        else:
            line = raw_line
        line = line.rstrip("\r\n")
        if first_line:
            line = line.removeprefix("\ufeff")
            first_line = False

        if line == "":
            if data:
                yield ServerSentEvent(
                    event=event_type,
                    data="\n".join(data),
                    event_id=last_event_id,
                )
            event_type = "message"
            data = []
            continue
        if line.startswith(":"):
            continue

        field, separator, value = line.partition(":")
        if separator and value.startswith(" "):
            value = value[1:]
        if field == "event":
            event_type = value
        elif field == "data":
            data.append(value)
        elif field == "id" and "\x00" not in value:
            last_event_id = value


def iter_received_orders(
    response: Any,
    *,
    monotonic_clock: Callable[[], float] = time.monotonic,
    epoch_clock: Callable[[], float] = time.time,
) -> Iterator[ReceivedOrderEvent]:
    """Decode order SSE frames and close their streaming response on exit."""
    raw_response = getattr(response, "_response", None)
    if raw_response is None or not hasattr(raw_response, "readline"):
        raise SSEProtocolError("order event response is not streamable")

    def lines() -> Iterator[bytes | bytearray | memoryview | str]:
        while True:
            line = raw_response.readline(b"\n")
            if not line:
                return
            yield line

    try:
        for event in iter_sse_events(lines()):
            if event.event != "order":
                continue
            received_monotonic = monotonic_clock()
            received_epoch = epoch_clock()
            try:
                order = json.loads(event.data)
            except (TypeError, json.JSONDecodeError) as error:
                raise SSEProtocolError(
                    "order event data is not valid JSON"
                ) from error
            if not isinstance(order, dict):
                raise SSEProtocolError("order event data is not a JSON object")
            if not str(order.get("order_id", "")).strip():
                raise SSEProtocolError("order event omitted order_id")
            if not str(order.get("status", "")).strip():
                raise SSEProtocolError("order event omitted status")
            yield ReceivedOrderEvent(
                order=order,
                event_id=event.event_id,
                received_monotonic=received_monotonic,
                received_epoch=received_epoch,
            )
    finally:
        close_stream_response(response)


def close_stream_response(response: Any) -> None:
    raw_response = getattr(response, "_response", None)
    release = getattr(raw_response, "release", None)
    if callable(release):
        release()
