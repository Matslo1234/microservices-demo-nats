# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import asyncio
import json
import os
import ssl
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Coroutine


RUN_BUCKET = "BENCHMARK_RUNS"
ARTIFACT_BUCKET = "BENCHMARK_ARTIFACTS"


class RecordNotFound(KeyError):
    pass


class RevisionConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class StoredRecord:
    value: dict[str, Any]
    revision: int


def encode_json(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def decode_json(data: bytes | None) -> dict[str, Any]:
    if data is None:
        raise ValueError("shared record has no value")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("shared record is not a JSON object")
    return value


class NatsSharedStore:
    """Synchronous facade over one reconnecting asyncio NATS client.

    HTTP request threads and the benchmark Job runner use this facade without
    owning correctness state. Every record is persisted in JetStream KV and
    every artifact is persisted in JetStream Object Store.
    """

    def __init__(self, operation_timeout: float = 10.0) -> None:
        self.operation_timeout = operation_timeout
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="benchmark-nats-store",
            daemon=True,
        )
        self._thread.start()
        self._connection: Any = None
        self._kv: Any = None
        self._objects: Any = None
        self._connect_lock: asyncio.Lock | None = None
        self._errors: dict[str, type[BaseException]] = {}
        self._closed = False

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _call(self, operation: Coroutine[Any, Any, Any]) -> Any:
        if self._closed:
            operation.close()
            raise RuntimeError("shared store is closed")
        result: Future[Any] = asyncio.run_coroutine_threadsafe(
            operation, self._loop
        )
        try:
            return result.result(timeout=self.operation_timeout)
        except TimeoutError:
            result.cancel()
            raise

    async def _ensure_connected(self) -> None:
        if (
            self._connection is not None
            and not self._connection.is_closed
            and self._kv is not None
            and self._objects is not None
        ):
            return
        if self._connect_lock is None:
            self._connect_lock = asyncio.Lock()
        async with self._connect_lock:
            if (
                self._connection is not None
                and not self._connection.is_closed
                and self._kv is not None
                and self._objects is not None
            ):
                return

            import nats
            from nats.js import errors as js_errors

            ca_file = os.environ.get("NATS_CA_FILE")
            tls_context = (
                ssl.create_default_context(cafile=ca_file)
                if ca_file
                else None
            )
            if tls_context is not None and hasattr(ssl, "VERIFY_X509_STRICT"):
                tls_context.verify_flags &= ~ssl.VERIFY_X509_STRICT

            connection = await nats.connect(
                servers=[os.environ["NATS_URL"]],
                user=os.environ.get("NATS_USER") or None,
                password=os.environ.get("NATS_PASSWORD") or None,
                name=f"benchmarkservice/{os.environ.get('HOSTNAME', 'local')}",
                tls=tls_context,
                connect_timeout=float(
                    os.environ.get("NATS_CONNECT_TIMEOUT", "2s").rstrip("s")
                ),
                reconnect_time_wait=float(
                    os.environ.get("NATS_RECONNECT_WAIT", "2s").rstrip("s")
                ),
                max_reconnect_attempts=int(
                    os.environ.get("NATS_MAX_RECONNECTS", "-1")
                ),
                ping_interval=float(
                    os.environ.get("NATS_PING_INTERVAL", "20s").rstrip("s")
                ),
                max_outstanding_pings=int(
                    os.environ.get("NATS_MAX_PINGS_OUT", "2")
                ),
                allow_reconnect=True,
            )
            try:
                jetstream = connection.jetstream(
                    timeout=float(
                        os.environ.get(
                            "NATS_PUBLISH_TIMEOUT", "5s"
                        ).rstrip("s")
                    )
                )
                kv = await jetstream.key_value(
                    os.environ.get("BENCHMARK_RUN_BUCKET", RUN_BUCKET)
                )
                objects = await jetstream.object_store(
                    os.environ.get(
                        "BENCHMARK_ARTIFACT_BUCKET", ARTIFACT_BUCKET
                    )
                )
            except Exception:
                await connection.close()
                raise
            self._connection = connection
            self._kv = kv
            self._objects = objects
            self._errors = {
                "not_found": js_errors.KeyNotFoundError,
                "no_keys": js_errors.NoKeysError,
                "conflict": js_errors.KeyWrongLastSequenceError,
                "object_not_found": js_errors.ObjectNotFoundError,
            }

    async def _get(self, key: str) -> StoredRecord:
        await self._ensure_connected()
        try:
            entry = await self._kv.get(key)
        except self._errors["not_found"] as error:
            raise RecordNotFound(key) from error
        return StoredRecord(
            decode_json(entry.value),
            int(entry.revision),
        )

    def get(self, key: str) -> StoredRecord:
        return self._call(self._get(key))

    async def _create(self, key: str, value: dict[str, Any]) -> int:
        await self._ensure_connected()
        try:
            return int(await self._kv.create(key, encode_json(value)))
        except self._errors["conflict"] as error:
            raise RevisionConflict(key) from error

    def create(self, key: str, value: dict[str, Any]) -> int:
        return self._call(self._create(key, value))

    async def _update(
        self, key: str, value: dict[str, Any], revision: int
    ) -> int:
        await self._ensure_connected()
        try:
            return int(
                await self._kv.update(
                    key, encode_json(value), last=revision
                )
            )
        except self._errors["conflict"] as error:
            raise RevisionConflict(key) from error

    def update(
        self, key: str, value: dict[str, Any], revision: int
    ) -> int:
        return self._call(self._update(key, value, revision))

    async def _keys(self, prefix: str) -> list[str]:
        await self._ensure_connected()
        try:
            keys = await self._kv.keys(filters=[prefix])
        except self._errors["no_keys"]:
            return []
        return sorted(key for key in keys if key.startswith(prefix))

    def keys(self, prefix: str) -> list[str]:
        return self._call(self._keys(prefix))

    async def _put_object(self, name: str, data: bytes) -> None:
        await self._ensure_connected()
        await self._objects.put(name, data)

    def put_object(self, name: str, data: bytes) -> None:
        self._call(self._put_object(name, data))

    async def _get_object(self, name: str) -> bytes:
        await self._ensure_connected()
        try:
            result = await self._objects.get(name)
        except self._errors["object_not_found"] as error:
            raise RecordNotFound(name) from error
        return bytes(result.data or b"")

    def get_object(self, name: str) -> bytes:
        return self._call(self._get_object(name))

    async def _ready(self) -> None:
        await self._ensure_connected()
        await self._kv.status()
        await self._objects.status()

    def ready(self) -> bool:
        try:
            self._call(self._ready())
            return True
        except Exception:
            return False

    async def _close(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.drain()

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._call(self._close())
        finally:
            self._closed = True
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)
