# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest
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


if __name__ == "__main__":
    unittest.main()
