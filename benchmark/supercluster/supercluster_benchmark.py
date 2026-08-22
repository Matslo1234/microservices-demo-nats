#!/usr/bin/env python3
"""External, dependency-free NATS supercluster benchmark.

The runner connects to both private NATS client Services through kubectl
port-forwards.  It deliberately uses one process and one monotonic clock so
that cross-cluster delivery latencies do not depend on clock synchronization
between regions.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import csv
import dataclasses
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import ssl
import statistics
import sys
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any


NANOSECONDS = 1_000_000_000
DEFAULT_INVENTORY = Path(
    "kubernetes-manifests/regions/aws-supercluster-inventory.yaml"
)


class BenchmarkError(RuntimeError):
    """An actionable benchmark or preflight failure."""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds")


def log(message: str) -> None:
    print(f"[{utc_now()}] {message}", file=sys.stderr, flush=True)


def milliseconds(nanoseconds: int | float) -> float:
    return round(float(nanoseconds) / 1_000_000, 6)


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return ordered[index]


def metric_summary(
    values: list[float], *, expected: int | None = None
) -> dict[str, Any]:
    """Summarize a complete set using population standard deviation."""
    result: dict[str, Any] = {
        "count": len(values),
        "expected": len(values) if expected is None else expected,
        "missing": 0 if expected is None else expected - len(values),
        "median_ms": None,
        "population_stddev_ms": None,
        "min_ms": None,
        "p95_ms": None,
        "max_ms": None,
    }
    if not values:
        return result
    result.update(
        {
            "median_ms": round(statistics.median(values), 6),
            "population_stddev_ms": round(statistics.pstdev(values), 6),
            "min_ms": round(min(values), 6),
            "p95_ms": round(percentile(values, 0.95) or 0.0, 6),
            "max_ms": round(max(values), 6),
        }
    )
    return result


@dataclasses.dataclass(frozen=True)
class Region:
    region_id: str
    context: str
    nats_cluster_name: str


def load_regions(path: Path) -> list[Region]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BenchmarkError(f"cannot read inventory {path}: {error}") from error
    entries = document.get("regions")
    if not isinstance(entries, list) or len(entries) != 2:
        raise BenchmarkError(
            f"inventory {path} must contain exactly two regions"
        )
    regions: list[Region] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise BenchmarkError(f"inventory region {index} is not an object")
        missing = [
            key
            for key in ("region_id", "k8s_context", "nats_cluster_name")
            if not entry.get(key)
        ]
        if missing:
            raise BenchmarkError(
                f"inventory region {index} is missing {', '.join(missing)}"
            )
        regions.append(
            Region(
                region_id=str(entry["region_id"]),
                context=str(entry["k8s_context"]),
                nats_cluster_name=str(entry["nats_cluster_name"]),
            )
        )
    if regions[0].context == regions[1].context:
        raise BenchmarkError("the two inventory regions use the same context")
    return regions


async def command_output(*arguments: str, timeout: float = 30.0) -> str:
    process = await asyncio.create_subprocess_exec(
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=timeout
        )
    except TimeoutError:
        process.kill()
        await process.wait()
        raise BenchmarkError(
            f"command timed out after {timeout:g}s: {arguments[0]}"
        ) from None
    if process.returncode != 0:
        detail = stderr.decode("utf-8", errors="replace").strip()
        raise BenchmarkError(
            f"{arguments[0]} exited {process.returncode}: {detail}"
        )
    return stdout.decode("utf-8")


async def kubectl_json(
    region: Region,
    namespace: str,
    *arguments: str,
    timeout: float = 30.0,
) -> dict[str, Any]:
    output = await command_output(
        "kubectl",
        "--context",
        region.context,
        "--namespace",
        namespace,
        *arguments,
        timeout=timeout,
    )
    try:
        value = json.loads(output)
    except json.JSONDecodeError as error:
        raise BenchmarkError(
            f"kubectl returned invalid JSON for {region.region_id}: {error}"
        ) from error
    if not isinstance(value, dict):
        raise BenchmarkError(
            f"kubectl returned non-object JSON for {region.region_id}"
        )
    return value


async def read_credentials(
    region: Region, namespace: str
) -> tuple[str, str, str]:
    """Read the local CA and admin credential without logging secret values."""
    ca_document, secret_document = await asyncio.gather(
        kubectl_json(region, namespace, "get", "configmap/nats-ca", "-o", "json"),
        kubectl_json(
            region,
            namespace,
            "get",
            "secret/nats-admin-credentials",
            "-o",
            "json",
        ),
    )
    try:
        ca = str(ca_document["data"]["ca.crt"])
        encoded = secret_document["data"]
        user = base64.b64decode(encoded["NATS_USER"], validate=True).decode()
        password = base64.b64decode(
            encoded["NATS_PASSWORD"], validate=True
        ).decode()
    except (KeyError, ValueError, UnicodeDecodeError) as error:
        raise BenchmarkError(
            f"invalid NATS CA or admin credential in {region.region_id}"
        ) from error
    return ca, user, password


def free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


class PortForward:
    def __init__(self, region: Region, namespace: str, local_port: int) -> None:
        self.region = region
        self.namespace = namespace
        self.local_port = local_port
        self.process: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._ready = asyncio.Event()
        self._tail: deque[str] = deque(maxlen=20)

    async def _read_output(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        while line := await self.process.stdout.readline():
            text = line.decode("utf-8", errors="replace").rstrip()
            self._tail.append(text)
            if "Forwarding from 127.0.0.1:" in text:
                self._ready.set()

    async def start(self, timeout: float = 20.0) -> None:
        if self.process is not None:
            raise BenchmarkError(
                f"port-forward for {self.region.region_id} is already running"
            )
        self._ready = asyncio.Event()
        self._tail.clear()
        self.process = await asyncio.create_subprocess_exec(
            "kubectl",
            "--context",
            self.region.context,
            "--namespace",
            self.namespace,
            "port-forward",
            "service/nats",
            f"{self.local_port}:4222",
            "--address=127.0.0.1",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        self._reader_task = asyncio.create_task(self._read_output())
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=timeout)
        except TimeoutError:
            tail = " | ".join(self._tail) or "no kubectl output"
            await self.stop()
            raise BenchmarkError(
                f"port-forward for {self.region.region_id} did not become "
                f"ready: {tail}"
            ) from None
        if self.process.returncode is not None:
            tail = " | ".join(self._tail) or "no kubectl output"
            await self.stop()
            raise BenchmarkError(
                f"port-forward for {self.region.region_id} exited: {tail}"
            )

    async def stop(self) -> None:
        process, self.process = self.process, None
        if process is not None and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except TimeoutError:
                process.kill()
                await process.wait()
        if self._reader_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
        self._reader_task = None


@dataclasses.dataclass
class Message:
    connection: "NatsConnection"
    subject: str
    reply: str
    data: bytes
    headers: dict[str, str]
    status: int | None = None
    description: str = ""

    async def ack(self) -> None:
        if self.reply:
            await self.connection.publish(self.reply, b"+ACK")


MessageCallback = Callable[[Message], Awaitable[None]]


@dataclasses.dataclass
class Subscription:
    sid: int
    subject: str
    callback: MessageCallback
    max_messages: int | None = None
    received: int = 0


class NatsConnection:
    """Small NATS protocol client implementing only benchmark operations."""

    def __init__(
        self,
        *,
        name: str,
        host: str,
        port: int,
        user: str,
        password: str,
        ca: str,
        tls_server_name: str,
        reconnect_wait: float = 0.25,
    ) -> None:
        self.name = name
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.tls_server_name = tls_server_name
        self.reconnect_wait = reconnect_wait
        self.ssl_context = ssl.create_default_context(cadata=ca)
        if hasattr(ssl, "VERIFY_X509_STRICT"):
            self.ssl_context.verify_flags &= ~ssl.VERIFY_X509_STRICT

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()
        self._connect_lock = asyncio.Lock()
        self._subscriptions: dict[int, Subscription] = {}
        self._next_sid = 1
        self._next_inbox = 1
        self._generation = 0
        self._closing = False
        self.connected = asyncio.Event()
        self.disconnected = asyncio.Event()
        self.reconnected = asyncio.Event()
        self.last_disconnected_ns: int | None = None
        self.last_reconnected_ns: int | None = None
        self.protocol_errors: list[str] = []
        self._pongs: deque[asyncio.Future[None]] = deque()
        self._callback_tasks: set[asyncio.Task[None]] = set()

    async def connect(self, timeout: float = 10.0) -> None:
        await asyncio.wait_for(self._connect_once(reconnect=False), timeout)

    async def _connect_once(self, *, reconnect: bool) -> None:
        async with self._connect_lock:
            if self._closing:
                return
            # NATS sends its initial INFO line in plaintext and then upgrades
            # the connection to TLS unless tls.handshake_first is enabled.
            # The repository's broker configuration uses that standard flow.
            reader, writer = await asyncio.open_connection(
                self.host,
                self.port,
            )
            try:
                info_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
                if not info_line.startswith(b"INFO "):
                    raise BenchmarkError(
                        f"{self.name} did not send a NATS INFO line"
                    )
                info = json.loads(info_line[5:])
                if not info.get("tls_required"):
                    raise BenchmarkError(
                        f"{self.name} did not require TLS as expected"
                    )
                await writer.start_tls(
                    self.ssl_context,
                    server_hostname=self.tls_server_name,
                    ssl_handshake_timeout=5.0,
                )
                connect_payload = json.dumps(
                    {
                        "verbose": False,
                        "pedantic": False,
                        "tls_required": True,
                        "name": self.name,
                        "lang": "python-stdlib",
                        "version": "1",
                        "protocol": 1,
                        "echo": True,
                        "headers": True,
                        "no_responders": True,
                        "user": self.user,
                        "pass": self.password,
                    },
                    separators=(",", ":"),
                ).encode()
                frames = [b"CONNECT " + connect_payload + b"\r\n"]
                for subscription in self._subscriptions.values():
                    frames.append(
                        f"SUB {subscription.subject} {subscription.sid}\r\n".encode()
                    )
                    if subscription.max_messages is not None:
                        remaining = (
                            subscription.max_messages - subscription.received
                        )
                        if remaining > 0:
                            frames.append(
                                f"UNSUB {subscription.sid} {remaining}\r\n".encode()
                            )
                pong = asyncio.get_running_loop().create_future()
                self._pongs.append(pong)
                frames.append(b"PING\r\n")
                writer.write(b"".join(frames))
                await writer.drain()
            except Exception:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
                raise

            self._reader = reader
            self._writer = writer
            self._generation += 1
            generation = self._generation
            self._reader_task = asyncio.create_task(
                self._read_loop(reader, writer, generation)
            )
            try:
                await asyncio.wait_for(pong, timeout=5.0)
            except Exception:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
                raise
            self.connected.set()
            if reconnect:
                self.last_reconnected_ns = time.perf_counter_ns()
                self.reconnected.set()

    async def _send(
        self,
        frame: bytes,
        *,
        before_send: Callable[[], None] | None = None,
    ) -> None:
        await asyncio.wait_for(self.connected.wait(), timeout=10.0)
        async with self._write_lock:
            writer = self._writer
            if writer is None or writer.is_closing():
                raise BenchmarkError(f"{self.name} is disconnected")
            if before_send is not None:
                before_send()
            writer.write(frame)
            await writer.drain()

    async def publish(
        self,
        subject: str,
        data: bytes = b"",
        reply: str = "",
        *,
        before_send: Callable[[], None] | None = None,
    ) -> None:
        if reply:
            header = f"PUB {subject} {reply} {len(data)}\r\n".encode()
        else:
            header = f"PUB {subject} {len(data)}\r\n".encode()
        await self._send(
            header + data + b"\r\n", before_send=before_send
        )

    async def subscribe(
        self,
        subject: str,
        callback: MessageCallback,
        *,
        max_messages: int | None = None,
    ) -> int:
        sid = self._next_sid
        self._next_sid += 1
        subscription = Subscription(sid, subject, callback, max_messages)
        self._subscriptions[sid] = subscription
        frame = f"SUB {subject} {sid}\r\n".encode()
        if max_messages is not None:
            frame += f"UNSUB {sid} {max_messages}\r\n".encode()
        try:
            await self._send(frame)
        except Exception:
            self._subscriptions.pop(sid, None)
            raise
        return sid

    async def unsubscribe(self, sid: int) -> None:
        self._subscriptions.pop(sid, None)
        if self.connected.is_set():
            await self._send(f"UNSUB {sid}\r\n".encode())

    async def flush(self, timeout: float = 5.0) -> None:
        pong = asyncio.get_running_loop().create_future()
        self._pongs.append(pong)
        try:
            await self._send(b"PING\r\n")
            await asyncio.wait_for(pong, timeout=timeout)
        except Exception:
            with contextlib.suppress(ValueError):
                self._pongs.remove(pong)
            raise

    async def request(
        self,
        subject: str,
        data: bytes = b"",
        *,
        timeout: float = 10.0,
        before_publish: Callable[[], None] | None = None,
    ) -> Message:
        inbox = f"_INBOX.SCB.{secrets.token_hex(8)}.{self._next_inbox}"
        self._next_inbox += 1
        response = asyncio.get_running_loop().create_future()

        async def receive(message: Message) -> None:
            if not response.done():
                response.set_result(message)

        sid = await self.subscribe(inbox, receive, max_messages=1)
        try:
            await self.publish(
                subject,
                data,
                reply=inbox,
                before_send=before_publish,
            )
            message = await asyncio.wait_for(response, timeout=timeout)
        except TimeoutError:
            raise BenchmarkError(f"NATS request to {subject} timed out") from None
        finally:
            self._subscriptions.pop(sid, None)
        if message.status is not None and message.status >= 300:
            raise BenchmarkError(
                f"NATS request to {subject} returned "
                f"{message.status} {message.description}".rstrip()
            )
        return message

    async def _read_loop(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        generation: int,
    ) -> None:
        error: BaseException | None = None
        try:
            while True:
                line = await reader.readline()
                if not line:
                    raise ConnectionError("NATS connection closed")
                command = line.rstrip(b"\r\n")
                if command == b"PING":
                    writer.write(b"PONG\r\n")
                    await writer.drain()
                elif command == b"PONG":
                    if self._pongs:
                        pong = self._pongs.popleft()
                        if not pong.done():
                            pong.set_result(None)
                elif command.startswith(b"MSG "):
                    await self._read_message(reader, command, headers=False)
                elif command.startswith(b"HMSG "):
                    await self._read_message(reader, command, headers=True)
                elif command.startswith(b"-ERR"):
                    detail = command.decode("utf-8", errors="replace")
                    self.protocol_errors.append(detail)
                    log(f"{self.name}: {detail}")
                elif command.startswith(b"INFO ") or command == b"+OK":
                    continue
                else:
                    raise BenchmarkError(
                        f"{self.name} received unknown protocol line {command!r}"
                    )
        except asyncio.CancelledError:
            raise
        except BaseException as caught:
            error = caught
        finally:
            if generation == self._generation and not self._closing:
                await self._on_disconnect(error)

    async def _read_message(
        self,
        reader: asyncio.StreamReader,
        command: bytes,
        *,
        headers: bool,
    ) -> None:
        tokens = command.decode("ascii").split()
        if headers:
            if len(tokens) == 5:
                _, subject, sid_text, header_text, total_text = tokens
                reply = ""
            elif len(tokens) == 6:
                _, subject, sid_text, reply, header_text, total_text = tokens
            else:
                raise BenchmarkError(f"invalid HMSG line {command!r}")
            header_size, total_size = int(header_text), int(total_text)
            wire = await reader.readexactly(total_size + 2)
            if wire[-2:] != b"\r\n":
                raise BenchmarkError("HMSG payload is missing CRLF")
            raw_headers = wire[:header_size]
            data = wire[header_size:total_size]
            parsed_headers, status, description = self._parse_headers(raw_headers)
        else:
            if len(tokens) == 4:
                _, subject, sid_text, size_text = tokens
                reply = ""
            elif len(tokens) == 5:
                _, subject, sid_text, reply, size_text = tokens
            else:
                raise BenchmarkError(f"invalid MSG line {command!r}")
            size = int(size_text)
            wire = await reader.readexactly(size + 2)
            if wire[-2:] != b"\r\n":
                raise BenchmarkError("MSG payload is missing CRLF")
            data = wire[:size]
            parsed_headers, status, description = {}, None, ""
        sid = int(sid_text)
        subscription = self._subscriptions.get(sid)
        if subscription is None:
            return
        subscription.received += 1
        if (
            subscription.max_messages is not None
            and subscription.received >= subscription.max_messages
        ):
            self._subscriptions.pop(sid, None)
        message = Message(
            connection=self,
            subject=subject,
            reply=reply,
            data=data,
            headers=parsed_headers,
            status=status,
            description=description,
        )
        task = asyncio.create_task(self._invoke_callback(subscription, message))
        self._callback_tasks.add(task)
        task.add_done_callback(self._callback_tasks.discard)

    async def _invoke_callback(
        self, subscription: Subscription, message: Message
    ) -> None:
        try:
            await subscription.callback(message)
        except Exception as callback_error:
            detail = f"subscription callback failed: {callback_error}"
            self.protocol_errors.append(detail)
            log(f"{self.name}: {detail}")

    @staticmethod
    def _parse_headers(
        raw: bytes,
    ) -> tuple[dict[str, str], int | None, str]:
        text = raw.decode("utf-8", errors="replace")
        lines = text.split("\r\n")
        status: int | None = None
        description = ""
        if lines and lines[0].startswith("NATS/1.0"):
            parts = lines[0].split(" ", 2)
            if len(parts) >= 2 and parts[1].isdigit():
                status = int(parts[1])
                description = parts[2] if len(parts) == 3 else ""
        headers: dict[str, str] = {}
        for line in lines[1:]:
            if ":" in line:
                key, value = line.split(":", 1)
                headers[key.strip()] = value.strip()
        return headers, status, description

    async def _on_disconnect(self, error: BaseException | None) -> None:
        self.connected.clear()
        self.last_disconnected_ns = time.perf_counter_ns()
        self.disconnected.set()
        while self._pongs:
            pong = self._pongs.popleft()
            if not pong.done():
                pong.set_exception(
                    ConnectionError(f"{self.name} disconnected: {error}")
                )
        if self._writer is not None:
            self._writer.close()
            with contextlib.suppress(Exception):
                await self._writer.wait_closed()
        self._writer = None
        self._reader = None
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        while not self._closing and not self.connected.is_set():
            try:
                await self._connect_once(reconnect=True)
                return
            except (OSError, asyncio.TimeoutError, ssl.SSLError, BenchmarkError):
                await asyncio.sleep(self.reconnect_wait)

    async def close(self) -> None:
        self._closing = True
        self.connected.clear()
        if self._reconnect_task is not None:
            self._reconnect_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reconnect_task
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
        for task in self._callback_tasks:
            task.cancel()
        if self._callback_tasks:
            await asyncio.gather(*self._callback_tasks, return_exceptions=True)
        self._callback_tasks.clear()
        if self._writer is not None:
            self._writer.close()
            with contextlib.suppress(Exception):
                await self._writer.wait_closed()
        self._writer = None


async def js_request(
    connection: NatsConnection,
    subject: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = 10.0,
    before_publish: Callable[[], None] | None = None,
) -> dict[str, Any]:
    body = (
        json.dumps(payload, separators=(",", ":")).encode()
        if payload is not None
        else b""
    )
    message = await connection.request(
        subject,
        body,
        timeout=timeout,
        before_publish=before_publish,
    )
    try:
        response = json.loads(message.data)
    except json.JSONDecodeError as error:
        raise BenchmarkError(f"invalid JetStream response from {subject}") from error
    if not isinstance(response, dict):
        raise BenchmarkError(f"non-object JetStream response from {subject}")
    if response.get("error"):
        api_error = response["error"]
        raise BenchmarkError(
            f"JetStream {subject} failed: "
            f"{api_error.get('code', '?')} {api_error.get('description', api_error)}"
        )
    return response


async def create_stream(
    connection: NatsConnection,
    name: str,
    subject: str,
    cluster: str,
    event_count: int,
    replicas: int,
) -> dict[str, Any]:
    config = {
        "name": name,
        "description": (
            "Temporary external supercluster benchmark "
            f"R{replicas} stream"
        ),
        "subjects": [subject],
        "retention": "limits",
        "max_consumers": 4,
        "max_msgs": max(1000, event_count * 10),
        "max_bytes": 67_108_864,
        "max_age": 3_600 * NANOSECONDS,
        "max_msgs_per_subject": -1,
        "max_msg_size": 65_536,
        "storage": "file",
        "discard": "old",
        "num_replicas": replicas,
        "duplicate_window": 120 * NANOSECONDS,
        "allow_direct": True,
        "placement": {"cluster": cluster},
    }
    return await js_request(
        connection,
        f"$JS.API.STREAM.CREATE.{name}",
        config,
        timeout=30.0,
    )


async def create_push_consumer(
    connection: NatsConnection,
    stream: str,
    consumer: str,
    filter_subject: str,
    delivery_subject: str,
    event_count: int,
) -> dict[str, Any]:
    payload = {
        "stream_name": stream,
        "config": {
            "durable_name": consumer,
            "name": consumer,
            "deliver_subject": delivery_subject,
            "deliver_policy": "all",
            "ack_policy": "explicit",
            "ack_wait": 30 * NANOSECONDS,
            "max_deliver": 10,
            "filter_subject": filter_subject,
            "replay_policy": "instant",
            "max_ack_pending": max(1024, event_count * 4),
        },
    }
    return await js_request(
        connection,
        (
            f"$JS.API.CONSUMER.CREATE.{stream}.{consumer}."
            f"{filter_subject}"
        ),
        payload,
        timeout=30.0,
    )


async def delete_stream(connection: NatsConnection, name: str) -> None:
    await js_request(
        connection, f"$JS.API.STREAM.DELETE.{name}", timeout=30.0
    )


async def stream_info(
    connection: NatsConnection, name: str
) -> dict[str, Any]:
    return await js_request(
        connection, f"$JS.API.STREAM.INFO.{name}", timeout=30.0
    )


@dataclasses.dataclass
class EventRecord:
    run_id: str
    direction: str
    source_region: str
    destination_region: str
    phase: str
    transport: str
    event_id: str
    sent_at_utc: str
    sent_ns: int = dataclasses.field(repr=False)
    delivered_at_utc: str | None = None
    delivered_ns: int | None = dataclasses.field(default=None, repr=False)
    delivery_latency_ms: float | None = None
    publish_ack_latency_ms: float | None = None
    stream_sequence: int | None = None
    publish_duplicate: bool = False
    duplicate_deliveries: int = 0

    @property
    def delivered(self) -> bool:
        return self.delivery_latency_ms is not None

    def public_dict(self) -> dict[str, Any]:
        value = dataclasses.asdict(self)
        value.pop("sent_ns", None)
        value.pop("delivered_ns", None)
        value["delivered"] = self.delivered
        return value


class DirectionRun:
    def __init__(
        self,
        *,
        run_id: str,
        source: Region,
        destination: Region,
        source_connection: NatsConnection,
        destination_connection: NatsConnection,
        destination_forward: PortForward,
        events: int,
        interval: float,
        delivery_timeout: float,
        drop_seconds: float,
        keep_streams: bool,
    ) -> None:
        self.run_id = run_id
        self.source = source
        self.destination = destination
        self.source_connection = source_connection
        self.destination_connection = destination_connection
        self.destination_forward = destination_forward
        self.events = events
        self.interval = interval
        self.delivery_timeout = delivery_timeout
        self.drop_seconds = drop_seconds
        self.keep_streams = keep_streams
        self.direction = f"{source.region_id}-to-{destination.region_id}"
        token = re.sub(r"[^A-Za-z0-9]", "", run_id).upper()
        suffix = secrets.token_hex(3).upper()
        self.stream_r1 = f"SCB_{token}_{suffix}_R1"
        self.stream_r3 = f"SCB_{token}_{suffix}_R3"
        self.consumer = f"SCB_{token}_{suffix}_C"
        subject_token = f"{token.lower()}.{suffix.lower()}"
        self.core_subject = f"scb.{subject_token}.{self.direction}.core"
        self.js_r1_subject = f"scb.{subject_token}.{self.direction}.js.r1"
        self.js_r3_subject = f"scb.{subject_token}.{self.direction}.js.r3"
        self.delivery_subject = (
            f"_SCB.DELIVER.{subject_token}.{self.direction}"
        )
        self.records: list[EventRecord] = []
        self._records_by_id: dict[str, EventRecord] = {}
        self._changed = asyncio.Event()
        self._warm_core = asyncio.Event()
        self._warm_js_r1 = asyncio.Event()
        self._warm_js_r3 = asyncio.Event()
        self._core_sid: int | None = None
        self._r1_sid: int | None = None
        self._delivery_sid: int | None = None
        self.created_streams: list[str] = []
        self.failures: list[str] = []

    def _new_record(self, phase: str, transport: str, index: int) -> tuple[EventRecord, bytes]:
        event_id = f"{self.direction}/{phase}/{transport}/{index:06d}"
        record = EventRecord(
            run_id=self.run_id,
            direction=self.direction,
            source_region=self.source.region_id,
            destination_region=self.destination.region_id,
            phase=phase,
            transport=transport,
            event_id=event_id,
            sent_at_utc="",
            sent_ns=0,
        )
        self.records.append(record)
        self._records_by_id[event_id] = record
        payload = json.dumps(
            {
                "run_id": self.run_id,
                "direction": self.direction,
                "phase": phase,
                "transport": transport,
                "event_id": event_id,
            },
            separators=(",", ":"),
        ).encode()
        return record, payload

    async def _record_delivery(self, message: Message, transport: str) -> None:
        try:
            event = json.loads(message.data)
        except json.JSONDecodeError:
            if transport == "jetstream_r3":
                await message.ack()
            return
        event_id = event.get("event_id") if isinstance(event, dict) else None
        if event_id == f"warm/{transport}":
            warm_events = {
                "core": self._warm_core,
                "jetstream_r1": self._warm_js_r1,
                "jetstream_r3": self._warm_js_r3,
            }
            warm_events[transport].set()
        elif isinstance(event_id, str) and event_id in self._records_by_id:
            record = self._records_by_id[event_id]
            if record.delivered:
                record.duplicate_deliveries += 1
            else:
                delivered_ns = time.perf_counter_ns()
                record.delivered_at_utc = utc_now()
                record.delivered_ns = delivered_ns
                record.delivery_latency_ms = milliseconds(
                    delivered_ns - record.sent_ns
                )
            self._changed.set()
        if transport == "jetstream_r3":
            await message.ack()

    async def _core_callback(self, message: Message) -> None:
        await self._record_delivery(message, "core")

    async def _r1_callback(self, message: Message) -> None:
        await self._record_delivery(message, "jetstream_r1")

    async def _r3_callback(self, message: Message) -> None:
        if message.status is None:
            await self._record_delivery(message, "jetstream_r3")

    async def _publish_core(self, phase: str) -> list[EventRecord]:
        batch: list[EventRecord] = []
        for index in range(self.events):
            record, payload = self._new_record(phase, "core", index)
            batch.append(record)

            def mark_sent(record: EventRecord = record) -> None:
                record.sent_at_utc = utc_now()
                record.sent_ns = time.perf_counter_ns()

            await self.source_connection.publish(
                self.core_subject, payload, before_send=mark_sent
            )
            if self.interval:
                await asyncio.sleep(self.interval)
        await self.source_connection.flush()
        return batch

    async def _publish_js_pipelined(
        self, phase: str, transport: str, subject: str
    ) -> list[EventRecord]:
        """Launch paced requests without making the next send wait for an ACK."""
        started = time.monotonic()

        async def publish_at(index: int) -> EventRecord:
            due = started + index * self.interval
            delay = due - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            return await self._publish_one_js(
                phase, transport, subject, index
            )

        tasks = [
            asyncio.create_task(publish_at(index))
            for index in range(self.events)
        ]
        try:
            return list(await asyncio.gather(*tasks))
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _publish_one_js(
        self,
        phase: str,
        transport: str,
        subject: str,
        index: int,
    ) -> EventRecord:
        record, payload = self._new_record(phase, transport, index)
        document = json.loads(payload)

        def mark_sent() -> None:
            record.sent_at_utc = utc_now()
            record.sent_ns = time.perf_counter_ns()

        response = await js_request(
            self.source_connection,
            subject,
            document,
            timeout=self.delivery_timeout,
            before_publish=mark_sent,
        )
        record.publish_ack_latency_ms = milliseconds(
            time.perf_counter_ns() - record.sent_ns
        )
        record.stream_sequence = int(response["seq"])
        record.publish_duplicate = bool(response.get("duplicate", False))
        return record

    async def _publish_replication_pair(
        self, phase: str
    ) -> tuple[list[EventRecord], list[EventRecord]]:
        """Alternate R1/R3 order to reduce time-order bias in the comparison."""
        batches: dict[str, list[EventRecord]] = {
            "jetstream_r1": [],
            "jetstream_r3": [],
        }
        subjects = {
            "jetstream_r1": self.js_r1_subject,
            "jetstream_r3": self.js_r3_subject,
        }
        for index in range(self.events):
            order = (
                ("jetstream_r1", "jetstream_r3")
                if index % 2 == 0
                else ("jetstream_r3", "jetstream_r1")
            )
            for transport in order:
                batches[transport].append(
                    await self._publish_one_js(
                        phase, transport, subjects[transport], index
                    )
                )
            if self.interval:
                await asyncio.sleep(self.interval)
        return batches["jetstream_r1"], batches["jetstream_r3"]

    async def _wait_delivered(
        self, batch: list[EventRecord], label: str
    ) -> None:
        deadline = time.monotonic() + self.delivery_timeout
        while not all(record.delivered for record in batch):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                missing = sum(not record.delivered for record in batch)
                self.failures.append(f"{label}: {missing}/{len(batch)} missing")
                return
            self._changed.clear()
            try:
                await asyncio.wait_for(self._changed.wait(), timeout=remaining)
            except TimeoutError:
                missing = sum(not record.delivered for record in batch)
                self.failures.append(f"{label}: {missing}/{len(batch)} missing")
                return

    async def _restart_destination_after_drop(
        self, disconnected_ns: int
    ) -> tuple[int, int]:
        restart_deadline_ns = disconnected_ns + int(
            self.drop_seconds * NANOSECONDS
        )
        remaining = (restart_deadline_ns - time.perf_counter_ns()) / NANOSECONDS
        if remaining > 0:
            await asyncio.sleep(remaining)
        restart_started = time.perf_counter_ns()
        await self.destination_forward.start()
        try:
            await asyncio.wait_for(
                self.destination_connection.reconnected.wait(), timeout=20.0
            )
        except TimeoutError:
            raise BenchmarkError(
                f"{self.destination.region_id} client did not reconnect"
            ) from None
        reconnected_ns = self.destination_connection.last_reconnected_ns
        assert reconnected_ns is not None
        return restart_started, reconnected_ns

    async def _warm_up(self) -> None:
        warm_core = json.dumps(
            {"run_id": self.run_id, "event_id": "warm/core"}
        ).encode()
        for _ in range(20):
            await self.source_connection.publish(self.core_subject, warm_core)
            try:
                await asyncio.wait_for(self._warm_core.wait(), timeout=0.25)
                break
            except TimeoutError:
                continue
        if not self._warm_core.is_set():
            raise BenchmarkError(
                f"core traffic did not cross {self.direction}; check gateway health"
            )

        warm_js_r1 = {
            "run_id": self.run_id,
            "event_id": "warm/jetstream_r1",
        }
        await js_request(
            self.source_connection,
            self.js_r1_subject,
            warm_js_r1,
            timeout=self.delivery_timeout,
        )
        try:
            await asyncio.wait_for(
                self._warm_js_r1.wait(), timeout=self.delivery_timeout
            )
        except TimeoutError:
            raise BenchmarkError(
                f"JetStream R1 traffic did not cross {self.direction}"
            ) from None

        warm_js_r3 = {
            "run_id": self.run_id,
            "event_id": "warm/jetstream_r3",
        }
        await js_request(
            self.source_connection,
            self.js_r3_subject,
            warm_js_r3,
            timeout=self.delivery_timeout,
        )
        try:
            await asyncio.wait_for(
                self._warm_js_r3.wait(), timeout=self.delivery_timeout
            )
        except TimeoutError:
            raise BenchmarkError(
                f"JetStream R3 traffic did not cross {self.direction}"
            ) from None

    @staticmethod
    def _replication_health(
        info: dict[str, Any], expected_cluster: str, expected_replicas: int
    ) -> dict[str, Any]:
        config = info.get("config") or {}
        cluster = info.get("cluster") or {}
        replicas = cluster.get("replicas") or []
        replica_states = [
            {
                "name": replica.get("name"),
                "current": replica.get("current"),
                "offline": bool(replica.get("offline", False)),
                "active_ns": replica.get("active"),
                "lag": replica.get("lag", 0),
            }
            for replica in replicas
        ]
        healthy = (
            config.get("num_replicas") == expected_replicas
            and cluster.get("name") == expected_cluster
            and bool(cluster.get("leader"))
            and len(replica_states) == expected_replicas - 1
            and all(
                replica["current"] is True
                and not replica["offline"]
                and replica["lag"] == 0
                for replica in replica_states
            )
        )
        return {
            "healthy": healthy,
            "configured_replicas": config.get("num_replicas"),
            "cluster": cluster.get("name"),
            "leader": cluster.get("leader"),
            "followers": replica_states,
            "messages": (info.get("state") or {}).get("messages"),
            "bytes": (info.get("state") or {}).get("bytes"),
        }

    def _phase_metric(
        self, phase: str, transport: str, attribute: str
    ) -> dict[str, Any]:
        matching = [
            record
            for record in self.records
            if record.phase == phase and record.transport == transport
        ]
        values = [
            float(value)
            for record in matching
            if (value := getattr(record, attribute)) is not None
        ]
        return metric_summary(values, expected=len(matching))

    async def _wait_replication_ready(
        self, stream: str, replicas: int
    ) -> dict[str, Any]:
        deadline = time.monotonic() + self.delivery_timeout
        last_health: dict[str, Any] = {}
        while time.monotonic() < deadline:
            info = await stream_info(self.destination_connection, stream)
            last_health = self._replication_health(
                info, self.destination.nats_cluster_name, replicas
            )
            if last_health["healthy"]:
                return last_health
            await asyncio.sleep(0.25)
        raise BenchmarkError(
            f"stream {stream} did not reach healthy R{replicas} placement: "
            f"{json.dumps(last_health, sort_keys=True)}"
        )

    async def run(self) -> dict[str, Any]:
        log(f"starting direction {self.direction}")
        self._core_sid = await self.destination_connection.subscribe(
            self.core_subject, self._core_callback
        )
        self._r1_sid = await self.destination_connection.subscribe(
            self.js_r1_subject, self._r1_callback
        )
        await self.destination_connection.flush()
        await create_stream(
            self.source_connection,
            self.stream_r1,
            self.js_r1_subject,
            self.destination.nats_cluster_name,
            self.events,
            1,
        )
        self.created_streams.append(self.stream_r1)
        initial_r1_health = await self._wait_replication_ready(
            self.stream_r1, 1
        )
        await create_stream(
            self.source_connection,
            self.stream_r3,
            self.js_r3_subject,
            self.destination.nats_cluster_name,
            self.events,
            3,
        )
        self.created_streams.append(self.stream_r3)
        initial_r3_health = await self._wait_replication_ready(
            self.stream_r3, 3
        )
        self._delivery_sid = await self.destination_connection.subscribe(
            self.delivery_subject, self._r3_callback
        )
        await self.destination_connection.flush()
        await create_push_consumer(
            self.destination_connection,
            self.stream_r3,
            self.consumer,
            self.js_r3_subject,
            self.delivery_subject,
            self.events,
        )
        await self._warm_up()

        baseline_core = await self._publish_core("baseline")
        await self._wait_delivered(baseline_core, "baseline core")
        baseline_r1, baseline_r3 = await self._publish_replication_pair(
            "baseline"
        )
        await self._wait_delivered(baseline_r1, "baseline JetStream R1")
        await self._wait_delivered(baseline_r3, "baseline JetStream R3")

        self.destination_connection.disconnected.clear()
        self.destination_connection.reconnected.clear()
        drop_started = time.perf_counter_ns()
        log(f"dropping destination tunnel for {self.direction}")
        await self.destination_forward.stop()
        try:
            await asyncio.wait_for(
                self.destination_connection.disconnected.wait(), timeout=10.0
            )
        except TimeoutError:
            raise BenchmarkError(
                f"{self.destination.region_id} client did not observe tunnel loss"
            ) from None
        disconnected_ns = self.destination_connection.last_disconnected_ns
        assert disconnected_ns is not None

        restart_task = asyncio.create_task(
            self._restart_destination_after_drop(disconnected_ns)
        )
        publishers = [
            asyncio.create_task(self._publish_core("connection_drop")),
            asyncio.create_task(
                self._publish_js_pipelined(
                    "connection_drop", "jetstream_r3", self.js_r3_subject
                )
            ),
        ]
        try:
            outage_core, outage_js = await asyncio.gather(*publishers)
            restart_started, reconnected_ns = await restart_task
        except BaseException:
            for publisher in publishers:
                publisher.cancel()
            await asyncio.gather(*publishers, return_exceptions=True)
            with contextlib.suppress(Exception):
                await asyncio.shield(restart_task)
            raise

        core_after_restart = sum(
            record.sent_ns >= restart_started for record in outage_core
        )
        js_after_restart = sum(
            record.sent_ns >= restart_started for record in outage_js
        )
        if core_after_restart or js_after_restart:
            self.failures.append(
                "connection-drop publication window overran tunnel restart: "
                f"core={core_after_restart}, jetstream={js_after_restart}"
            )
        await self._wait_delivered(outage_js, "connection-drop JetStream")
        await asyncio.sleep(0.25)

        recovered_delivery_ns = [
            record.delivered_ns
            for record in outage_js
            if record.delivered_ns is not None
        ]
        if recovered_delivery_ns:
            first_recovered_ns = min(recovered_delivery_ns)
            last_recovered_ns = max(recovered_delivery_ns)
            first_recovered_after_restart_ms = milliseconds(
                first_recovered_ns - restart_started
            )
            last_recovered_after_restart_ms = milliseconds(
                last_recovered_ns - restart_started
            )
            backlog_drain_ms = milliseconds(
                last_recovered_ns - first_recovered_ns
            )
        else:
            first_recovered_after_restart_ms = None
            last_recovered_after_restart_ms = None
            backlog_drain_ms = None

        recovery_core = await self._publish_core("recovery")
        await self._wait_delivered(recovery_core, "recovery core")
        recovery_r1, recovery_r3 = await self._publish_replication_pair(
            "recovery"
        )
        await self._wait_delivered(recovery_r1, "recovery JetStream R1")
        await self._wait_delivered(recovery_r3, "recovery JetStream R3")

        convergence_started = time.perf_counter_ns()
        await self._wait_replication_ready(self.stream_r3, 3)
        r3_convergence_wait_ms = milliseconds(
            time.perf_counter_ns() - convergence_started
        )
        (
            r1_source_info,
            r1_destination_info,
            r3_source_info,
            r3_destination_info,
        ) = await asyncio.gather(
            stream_info(self.source_connection, self.stream_r1),
            stream_info(self.destination_connection, self.stream_r1),
            stream_info(self.source_connection, self.stream_r3),
            stream_info(self.destination_connection, self.stream_r3),
        )
        r1_source_health = self._replication_health(
            r1_source_info, self.destination.nats_cluster_name, 1
        )
        r1_destination_health = self._replication_health(
            r1_destination_info, self.destination.nats_cluster_name, 1
        )
        r3_source_health = self._replication_health(
            r3_source_info, self.destination.nats_cluster_name, 3
        )
        r3_destination_health = self._replication_health(
            r3_destination_info, self.destination.nats_cluster_name, 3
        )
        if not r1_source_health["healthy"]:
            self.failures.append("source view reports unhealthy R1 control")
        if not r1_destination_health["healthy"]:
            self.failures.append(
                "destination view reports unhealthy R1 control"
            )
        if not r3_source_health["healthy"]:
            self.failures.append("source view reports unhealthy R3 replication")
        if not r3_destination_health["healthy"]:
            self.failures.append(
                "destination view reports unhealthy R3 replication"
            )

        for phase in ("baseline", "recovery"):
            for transport in ("core", "jetstream_r1", "jetstream_r3"):
                missing = sum(
                    not record.delivered
                    for record in self.records
                    if record.phase == phase
                    and record.transport == transport
                )
                if missing:
                    self.failures.append(
                        f"{phase} {transport}: {missing}/{self.events} missing"
                    )
        missing_outage_js = sum(not record.delivered for record in outage_js)
        if missing_outage_js:
            self.failures.append(
                f"connection-drop JetStream: {missing_outage_js}/"
                f"{self.events} not recovered"
            )

        latency: dict[str, dict[str, dict[str, Any]]] = {}
        for phase in ("baseline", "connection_drop", "recovery"):
            phase_latency = {
                "core_delivery": self._phase_metric(
                    phase, "core", "delivery_latency_ms"
                ),
                "jetstream_r3_publish_ack": self._phase_metric(
                    phase, "jetstream_r3", "publish_ack_latency_ms"
                ),
            }
            if phase == "connection_drop":
                phase_latency["jetstream_r3_queued_age"] = self._phase_metric(
                    phase, "jetstream_r3", "delivery_latency_ms"
                )
            else:
                phase_latency.update(
                    {
                        "jetstream_r3_delivery": self._phase_metric(
                            phase, "jetstream_r3", "delivery_latency_ms"
                        ),
                        "jetstream_r1_delivery": self._phase_metric(
                            phase, "jetstream_r1", "delivery_latency_ms"
                        ),
                        "jetstream_r1_publish_ack": self._phase_metric(
                            phase, "jetstream_r1", "publish_ack_latency_ms"
                        ),
                    }
                )
            latency[phase] = phase_latency

        replication_factor_impact: dict[str, Any] = {}
        for phase in ("baseline", "recovery"):
            r1_ack = latency[phase]["jetstream_r1_publish_ack"]["median_ms"]
            r3_ack = latency[phase]["jetstream_r3_publish_ack"]["median_ms"]
            r1_delivery = latency[phase]["jetstream_r1_delivery"]["median_ms"]
            r3_delivery = latency[phase]["jetstream_r3_delivery"]["median_ms"]
            replication_factor_impact[phase] = {
                "r3_minus_r1_publish_ack_median_ms": (
                    round(r3_ack - r1_ack, 6)
                    if r1_ack is not None and r3_ack is not None
                    else None
                ),
                "r3_minus_r1_delivery_median_ms": (
                    round(r3_delivery - r1_delivery, 6)
                    if r1_delivery is not None and r3_delivery is not None
                    else None
                ),
            }

        result = {
            "direction": self.direction,
            "source_region": self.source.region_id,
            "destination_region": self.destination.region_id,
            "streams": {"r1_control": self.stream_r1, "r3": self.stream_r3},
            "stream_placement": self.destination.nats_cluster_name,
            "latency": latency,
            "connection_drop": {
                "scheduled_drop_ms": round(
                    self.drop_seconds * 1000, 6
                ),
                "disconnect_detection_ms": milliseconds(
                    disconnected_ns - drop_started
                ),
                "tunnel_restart_started_ms": milliseconds(
                    restart_started - disconnected_ns
                ),
                "unavailable_ms": milliseconds(reconnected_ns - disconnected_ns),
                "reconnect_after_tunnel_restart_ms": milliseconds(
                    reconnected_ns - restart_started
                ),
                "jetstream_first_recovered_after_tunnel_restart_ms": (
                    first_recovered_after_restart_ms
                ),
                "jetstream_last_recovered_after_tunnel_restart_ms": (
                    last_recovered_after_restart_ms
                ),
                "jetstream_backlog_drain_ms": backlog_drain_ms,
                "jetstream_queued_age": latency["connection_drop"][
                    "jetstream_r3_queued_age"
                ],
                "core_published": len(outage_core),
                "core_published_before_tunnel_restart": (
                    len(outage_core) - core_after_restart
                ),
                "core_delivered_after_reconnect": sum(
                    record.delivered for record in outage_core
                ),
                "core_lost": sum(not record.delivered for record in outage_core),
                "jetstream_published": len(outage_js),
                "jetstream_published_before_tunnel_restart": (
                    len(outage_js) - js_after_restart
                ),
                "jetstream_recovered": sum(
                    record.delivered for record in outage_js
                ),
                "jetstream_missing": sum(
                    not record.delivered for record in outage_js
                ),
                "jetstream_duplicate_deliveries": sum(
                    record.duplicate_deliveries for record in outage_js
                ),
            },
            "replication": {
                "r1_control": {
                    "initial": initial_r1_health,
                    "source_view_after": r1_source_health,
                    "destination_view_after": r1_destination_health,
                },
                "r3": {
                    "initial": initial_r3_health,
                    "source_view_after": r3_source_health,
                    "destination_view_after": r3_destination_health,
                    "post_publish_convergence_wait_ms": r3_convergence_wait_ms,
                },
                "factor_impact": replication_factor_impact,
            },
            "failures": sorted(set(self.failures)),
        }
        log(
            f"finished direction {self.direction}: "
            f"{'PASS' if not result['failures'] else 'FAIL'}"
        )
        return result

    async def cleanup(self) -> list[str]:
        failures: list[str] = []
        if not self.keep_streams:
            for stream in reversed(self.created_streams):
                try:
                    await delete_stream(self.source_connection, stream)
                except Exception as error:
                    failure = f"could not delete stream {stream}: {error}"
                    failures.append(failure)
                    log(f"warning: {failure}")
            self.created_streams.clear()
        if self._core_sid is not None:
            with contextlib.suppress(Exception):
                await self.destination_connection.unsubscribe(self._core_sid)
        if self._r1_sid is not None:
            with contextlib.suppress(Exception):
                await self.destination_connection.unsubscribe(self._r1_sid)
        if self._delivery_sid is not None:
            with contextlib.suppress(Exception):
                await self.destination_connection.unsubscribe(self._delivery_sid)
        return failures


async def nats_rtt(connection: NatsConnection, samples: int) -> dict[str, Any]:
    values: list[float] = []
    for _ in range(samples):
        started = time.perf_counter_ns()
        await connection.flush()
        values.append(milliseconds(time.perf_counter_ns() - started))
    return metric_summary(values)


async def gateway_snapshot(
    region: Region, namespace: str
) -> dict[str, Any]:
    try:
        return await kubectl_json(
            region,
            namespace,
            "exec",
            "nats-0",
            "-c",
            "nats",
            "--",
            "wget",
            "-qO-",
            "http://127.0.0.1:8222/gatewayz",
            timeout=20.0,
        )
    except BenchmarkError as error:
        return {"snapshot_error": str(error)}


def format_number(value: Any) -> str:
    return "-" if value is None else str(value)


def write_outputs(
    output: Path,
    metadata: dict[str, Any],
    summaries: list[dict[str, Any]],
    records: list[EventRecord],
    snapshots: dict[str, Any],
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    result = {
        **metadata,
        "directions": summaries,
        "passed": (
            bool(summaries)
            and not metadata.get("fatal_error")
            and all(not summary["failures"] for summary in summaries)
        ),
    }
    (output / "summary.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output / "gateway-snapshots.json").write_text(
        json.dumps(snapshots, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    fieldnames = [
        "run_id",
        "direction",
        "source_region",
        "destination_region",
        "phase",
        "transport",
        "event_id",
        "sent_at_utc",
        "delivered_at_utc",
        "delivery_latency_ms",
        "publish_ack_latency_ms",
        "stream_sequence",
        "publish_duplicate",
        "duplicate_deliveries",
        "delivered",
    ]
    with (output / "events.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record.public_dict())

    lines = [
        f"# NATS supercluster benchmark {metadata['run_id']}",
        "",
        f"Result: **{'PASS' if result['passed'] else 'FAIL'}**",
        "",
        (
            "All standard deviations below are population standard deviations. "
            "Delivery time is measured by one external process from immediately "
            "before publish to the destination callback."
        ),
        "",
        "## Latency",
        "",
        "| Direction | Phase | Metric | Count | Missing | Median ms | "
        "Stddev ms | P95 ms | Max ms |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for summary in summaries:
        for phase, metrics in summary["latency"].items():
            for metric, values in metrics.items():
                lines.append(
                    "| {direction} | {phase} | {metric} | {count} | {missing} | "
                    "{median} | {stddev} | {p95} | {maximum} |".format(
                        direction=summary["direction"],
                        phase=phase,
                        metric=metric,
                        count=values["count"],
                        missing=values["missing"],
                        median=format_number(values["median_ms"]),
                        stddev=format_number(values["population_stddev_ms"]),
                        p95=format_number(values["p95_ms"]),
                        maximum=format_number(values["max_ms"]),
                    )
                )
    lines.extend(
        [
            "",
            "## Connection drop",
            "",
            "The destination tunnel restart is initiated on the configured "
            "wall-clock deadline, independently of publish acknowledgments.",
            "",
            "| Direction | Scheduled drop ms | Restart initiated ms | "
            "Client unavailable ms | Client reconnect ms | Core lost |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for summary in summaries:
        drop = summary["connection_drop"]
        lines.append(
            "| {direction} | {scheduled} | {restart_started} | {unavailable} | "
            "{reconnect} | {core_lost}/{core_total} |".format(
                direction=summary["direction"],
                scheduled=drop["scheduled_drop_ms"],
                restart_started=drop["tunnel_restart_started_ms"],
                unavailable=drop["unavailable_ms"],
                reconnect=drop["reconnect_after_tunnel_restart_ms"],
                core_lost=drop["core_lost"],
                core_total=drop["core_published"],
            )
        )
    lines.extend(
        [
            "",
            "### JetStream backlog recovery",
            "",
            "Queued age is measured from publish start until durable delivery. "
            "Recovery and drain timings start when tunnel restart begins.",
            "",
            "| Direction | Sent before restart | Recovered | Missing | "
            "Duplicates | Queued age median ms | Queued age p95 ms | Queued "
            "age max ms | First recovered ms | Backlog drain ms |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | "
            "---: |",
        ]
    )
    for summary in summaries:
        drop = summary["connection_drop"]
        queued_age = drop["jetstream_queued_age"]
        lines.append(
            "| {direction} | {before}/{published} | {recovered}/{published} | "
            "{missing} | {duplicates} | {median} | {p95} | {maximum} | "
            "{first} | {drain} |".format(
                direction=summary["direction"],
                before=drop["jetstream_published_before_tunnel_restart"],
                recovered=drop["jetstream_recovered"],
                published=drop["jetstream_published"],
                missing=drop["jetstream_missing"],
                duplicates=drop["jetstream_duplicate_deliveries"],
                median=format_number(queued_age["median_ms"]),
                p95=format_number(queued_age["p95_ms"]),
                maximum=format_number(queued_age["max_ms"]),
                first=format_number(
                    drop[
                        "jetstream_first_recovered_after_tunnel_restart_ms"
                    ]
                ),
                drain=format_number(drop["jetstream_backlog_drain_ms"]),
            )
        )
    lines.extend(
        [
            "",
            "## JetStream replication",
            "",
            "| Direction | Factor | Placement | Healthy from source | "
            "Healthy from destination | Leader | Followers | Convergence wait ms |",
            "| --- | --- | --- | --- | --- | --- | ---: | ---: |",
        ]
    )
    for summary in summaries:
        replication = summary["replication"]
        for factor, key in (("R1 control", "r1_control"), ("R3", "r3")):
            source_view = replication[key]["source_view_after"]
            destination_view = replication[key]["destination_view_after"]
            lines.append(
                "| {direction} | {factor} | {placement} | {source} | "
                "{destination} | {leader} | {followers} | {convergence} |".format(
                    direction=summary["direction"],
                    factor=factor,
                    placement=summary["stream_placement"],
                    source=source_view["healthy"],
                    destination=destination_view["healthy"],
                    leader=destination_view["leader"],
                    followers=len(destination_view["followers"]),
                    convergence=format_number(
                        replication[key].get(
                            "post_publish_convergence_wait_ms"
                        )
                    ),
                )
            )
    lines.extend(
        [
            "",
            "### R3 minus R1 median",
            "",
            "| Direction | Phase | Publish ack delta ms | Delivery delta ms |",
            "| --- | --- | ---: | ---: |",
        ]
    )
    for summary in summaries:
        for phase, impact in summary["replication"]["factor_impact"].items():
            lines.append(
                "| {direction} | {phase} | {ack} | {delivery} |".format(
                    direction=summary["direction"],
                    phase=phase,
                    ack=format_number(
                        impact["r3_minus_r1_publish_ack_median_ms"]
                    ),
                    delivery=format_number(
                        impact["r3_minus_r1_delivery_median_ms"]
                    ),
                )
            )
    failures = [
        f"{summary['direction']}: {failure}"
        for summary in summaries
        for failure in summary["failures"]
    ]
    if failures:
        lines.extend(["", "## Failures", ""])
        lines.extend(f"- {failure}" for failure in failures)
    if metadata.get("fatal_error"):
        lines.extend(
            ["", "## Fatal error", "", str(metadata["fatal_error"])]
        )
    lines.extend(
        [
            "",
            "Per-event measurements are in `events.csv`; machine-readable "
            "aggregates are in `summary.json`; raw broker gateway snapshots "
            "are in `gateway-snapshots.json`.",
            "",
        ]
    )
    (output / "report.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args(arguments: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark both directions of a two-region NATS supercluster"
    )
    parser.add_argument(
        "--inventory", type=Path, default=DEFAULT_INVENTORY
    )
    parser.add_argument("--namespace", default="nats")
    parser.add_argument("--events", type=int, default=100)
    parser.add_argument(
        "--interval",
        type=float,
        default=0.01,
        help="seconds between events (default: 0.01)",
    )
    parser.add_argument("--delivery-timeout", type=float, default=30.0)
    parser.add_argument(
        "--drop-seconds",
        type=float,
        default=5.0,
        help=(
            "seconds before destination tunnel restart; the paced outage "
            "publish window must fit inside it (default: 5)"
        ),
    )
    parser.add_argument("--rtt-samples", type=int, default=20)
    parser.add_argument(
        "--directions",
        choices=("both", "first-to-second", "second-to-first"),
        default="both",
    )
    parser.add_argument(
        "--tls-server-name", default="nats.nats.svc.cluster.local"
    )
    parser.add_argument("--keep-streams", action="store_true")
    parser.add_argument(
        "--output",
        type=Path,
        help="result directory (default: benchmark/supercluster/results/RUN_ID)",
    )
    args = parser.parse_args(arguments)
    if args.events < 2:
        parser.error("--events must be at least 2")
    if args.interval < 0:
        parser.error("--interval cannot be negative")
    if args.delivery_timeout <= 0:
        parser.error("--delivery-timeout must be positive")
    if args.drop_seconds <= 0:
        parser.error("--drop-seconds must be positive")
    outage_publish_span = (args.events - 1) * args.interval
    if outage_publish_span >= args.drop_seconds:
        parser.error(
            "outage publish span must be shorter than --drop-seconds "
            f"({outage_publish_span:g}s for {args.events} events at "
            f"--interval {args.interval:g}); reduce --interval or increase "
            "--drop-seconds"
        )
    if args.rtt_samples < 2:
        parser.error("--rtt-samples must be at least 2")
    return args


async def async_main(args: argparse.Namespace) -> int:
    if shutil.which("kubectl") is None:
        raise BenchmarkError("kubectl is required")
    regions = load_regions(args.inventory)
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id += f"-{secrets.token_hex(3)}"
    output = args.output or Path("benchmark/supercluster/results") / run_id

    log("reading regional CA and admin credentials through kubectl")
    credentials = await asyncio.gather(
        *(read_credentials(region, args.namespace) for region in regions)
    )
    forwards = {
        region.region_id: PortForward(region, args.namespace, free_local_port())
        for region in regions
    }
    connections: dict[str, NatsConnection] = {}
    direction_runs: list[DirectionRun] = []
    summaries: list[dict[str, Any]] = []
    snapshots: dict[str, Any] = {"before": {}, "after": {}}
    metadata: dict[str, Any] = {
        "run_id": run_id,
        "started_at_utc": utc_now(),
        "finished_at_utc": None,
        "events_per_phase": args.events,
        "interval_seconds": args.interval,
        "drop_seconds": args.drop_seconds,
        "connection_drop_schedule": "fixed_wall_clock",
        "connection_drop_publish_mode": (
            "concurrent_core_and_pipelined_jetstream"
        ),
        "inventory": str(args.inventory),
        "clock": "time.perf_counter_ns from one external process",
        "standard_deviation": "population",
        "local_nats_rtt": {},
        "fatal_error": None,
    }
    try:
        await asyncio.gather(*(forward.start() for forward in forwards.values()))
        for region, (ca, user, password) in zip(regions, credentials, strict=True):
            forward = forwards[region.region_id]
            connection = NatsConnection(
                name=f"supercluster-benchmark/{run_id}/{region.region_id}",
                host="127.0.0.1",
                port=forward.local_port,
                user=user,
                password=password,
                ca=ca,
                tls_server_name=args.tls_server_name,
            )
            await connection.connect()
            connections[region.region_id] = connection
        log("connected to both clusters")

        before_snapshots = await asyncio.gather(
            *(gateway_snapshot(region, args.namespace) for region in regions)
        )
        snapshots["before"] = {
            region.region_id: snapshot
            for region, snapshot in zip(regions, before_snapshots, strict=True)
        }
        rtts = await asyncio.gather(
            *(
                nats_rtt(connections[region.region_id], args.rtt_samples)
                for region in regions
            )
        )
        metadata["local_nats_rtt"] = {
            region.region_id: rtt
            for region, rtt in zip(regions, rtts, strict=True)
        }

        pairs = [(regions[0], regions[1]), (regions[1], regions[0])]
        if args.directions == "first-to-second":
            pairs = pairs[:1]
        elif args.directions == "second-to-first":
            pairs = pairs[1:]
        for source, destination in pairs:
            direction_run = DirectionRun(
                run_id=run_id,
                source=source,
                destination=destination,
                source_connection=connections[source.region_id],
                destination_connection=connections[destination.region_id],
                destination_forward=forwards[destination.region_id],
                events=args.events,
                interval=args.interval,
                delivery_timeout=args.delivery_timeout,
                drop_seconds=args.drop_seconds,
                keep_streams=args.keep_streams,
            )
            direction_runs.append(direction_run)
            direction_summary: dict[str, Any] | None = None
            try:
                direction_summary = await direction_run.run()
                summaries.append(direction_summary)
            finally:
                cleanup_failures = await direction_run.cleanup()
                if direction_summary is not None and cleanup_failures:
                    direction_summary["failures"].extend(cleanup_failures)
                    direction_summary["failures"] = sorted(
                        set(direction_summary["failures"])
                    )

        after_snapshots = await asyncio.gather(
            *(gateway_snapshot(region, args.namespace) for region in regions)
        )
        snapshots["after"] = {
            region.region_id: snapshot
            for region, snapshot in zip(regions, after_snapshots, strict=True)
        }
    except BaseException as error:
        metadata["fatal_error"] = str(error)
        raise
    finally:
        metadata["finished_at_utc"] = utc_now()
        if direction_runs or summaries:
            write_outputs(
                output,
                metadata,
                summaries,
                [record for run in direction_runs for record in run.records],
                snapshots,
            )
            log(f"wrote results to {output}")
        await asyncio.gather(
            *(connection.close() for connection in connections.values()),
            return_exceptions=True,
        )
        await asyncio.gather(
            *(forward.stop() for forward in forwards.values()),
            return_exceptions=True,
        )
    passed = bool(summaries) and all(
        not item["failures"] for item in summaries
    )
    log(f"benchmark {'PASS' if passed else 'FAIL'}; report: {output / 'report.md'}")
    return 0 if passed else 1


def main(arguments: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if arguments is None else arguments)
    try:
        return asyncio.run(async_main(args))
    except KeyboardInterrupt:
        print("benchmark interrupted", file=sys.stderr)
        return 130
    except BenchmarkError as error:
        print(f"benchmark failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
