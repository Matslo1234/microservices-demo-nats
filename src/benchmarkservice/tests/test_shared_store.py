# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

from nats.errors import TimeoutError as NatsTimeoutError
from nats.js import errors as js_errors

from shared_store import NatsSharedStore


class NatsSharedStoreRecoveryTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = NatsSharedStore.__new__(NatsSharedStore)
        self.store.operation_timeout = 2
        self.store._connection = None
        self.store._kv = None
        self.store._objects = None
        self.store._errors = {}

    async def test_transient_timeout_rebuilds_connection_and_retries(self):
        first = AsyncMock()
        first.get.side_effect = NatsTimeoutError
        second = AsyncMock()
        second.get.return_value.value = b'{"state":"running"}'
        second.get.return_value.revision = 7

        async def connect():
            self.store._kv = first if ensure.await_count == 1 else second
            self.store._errors = {
                "not_found": js_errors.KeyNotFoundError,
            }

        ensure = AsyncMock(side_effect=connect)
        with (
            patch.object(self.store, "_ensure_connected", ensure),
            patch.object(
                self.store, "_invalidate_connection", AsyncMock()
            ) as invalidate,
            patch("shared_store.asyncio.sleep", AsyncMock()),
        ):
            record = await self.store._get("run.test")

        self.assertEqual({"state": "running"}, record.value)
        self.assertEqual(7, record.revision)
        self.assertEqual(2, ensure.await_count)
        invalidate.assert_awaited_once()

    async def test_object_file_transfer_uses_file_streams(self):
        self.store._objects = AsyncMock()

        async def call(operation):
            return await operation()

        async def download(name, writeinto):
            self.assertEqual("result", name)
            writeinto.write(b"downloaded")

        self.store._with_recovery = call
        self.store._objects.get.side_effect = download

        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source.zip"
            destination = Path(temporary) / "destination.zip"
            source.write_bytes(b"uploaded")

            await self.store._put_object_file("result", source)
            upload = self.store._objects.put.await_args.args[1]
            self.assertTrue(hasattr(upload, "readinto"))

            await self.store._get_object_file("result", destination)
            self.assertEqual(b"downloaded", destination.read_bytes())


if __name__ == "__main__":
    unittest.main()
