#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from nats.js.errors import NoKeysError, ServiceUnavailableError

from nats_events import _NATSCatalogStore, _consume, _ready, _run, _stop


class Message:

  def __init__(self):
    self.data = b""
    self.subject = "boutique.evt.test"
    self.acks = 0
    self.naks = 0

  async def ack(self):
    self.acks += 1

  async def nak(self, delay):
    del delay
    self.naks += 1


class Subscription:

  def __init__(self, message):
    self.message = message
    self.requests = []

  async def fetch(self, **request):
    self.requests.append(request)
    return [self.message]


class EmptyBucket:

  async def keys(self):
    raise NoKeysError


class StreamingConsumerTests(unittest.IsolatedAsyncioTestCase):

  async def asyncSetUp(self):
    _stop.clear()
    _ready.clear()

  async def asyncTearDown(self):
    _stop.clear()
    _ready.clear()

  async def test_processes_each_message_as_soon_as_it_arrives(self):
    message = Message()
    subscription = Subscription(message)
    processed = []

    async def handler(received):
      processed.append(received)
      _stop.set()

    with patch("nats_events._message_context") as message_context:
      await _consume(subscription, handler)

    self.assertEqual([message], processed)
    self.assertEqual([{"batch": 32, "timeout": 1}], subscription.requests)
    self.assertEqual(1, message.acks)
    self.assertEqual(0, message.naks)
    message_context.assert_not_called()

  async def test_processes_a_batch_with_bounded_concurrency(self):
    messages = [Message(), Message(), Message()]
    subscription = Subscription(messages[0])
    subscription.message = messages
    active = 0
    maximum_active = 0
    processed = []

    async def fetch(**request):
      subscription.requests.append(request)
      return messages

    async def handler(received):
      nonlocal active, maximum_active
      active += 1
      maximum_active = max(maximum_active, active)
      await asyncio.sleep(0)
      processed.append(received)
      active -= 1
      if len(processed) == len(messages):
        _stop.set()

    subscription.fetch = fetch
    await _consume(
        subscription, handler, batch_size=3, concurrency=2)

    self.assertEqual(3, len(processed))
    self.assertEqual(2, maximum_active)
    self.assertEqual([{"batch": 3, "timeout": 1}], subscription.requests)
    self.assertTrue(all(message.acks == 1 for message in messages))

  async def test_empty_catalog_bucket_has_no_keys(self):
    store = _NATSCatalogStore(EmptyBucket())

    self.assertEqual([], await store.keys())

  async def test_failed_subscription_is_returned_to_outer_supervisor(self):
    class FailedSubscription:
      async def fetch(self, **_request):
        raise ServiceUnavailableError

    _ready.set()
    with self.assertRaisesRegex(RuntimeError, "subscription fetch failed"):
      await _consume(FailedSubscription(), lambda _message: None)

    self.assertFalse(_ready.is_set())

  async def test_worker_uses_bounded_reconnects_for_supervised_recovery(self):
    connection = MagicMock()
    connection.is_closed = False
    connection.close = AsyncMock()
    jetstream = connection.jetstream.return_value
    bucket = AsyncMock()
    jetstream.key_value = AsyncMock(return_value=bucket)

    with (
        patch.dict(
            "os.environ",
            {
                "NATS_REQUIRED": "true",
                "NATS_URL": "tls://nats:4222",
                "NATS_USER": "user",
                "NATS_PASSWORD": "password",
                "NATS_CA_FILE": "/ca.crt",
                "REGION_ID": "local",
                "K8S_CLUSTER_NAME": "test",
            },
            clear=True,
        ),
        patch("nats_events.ssl.create_default_context"),
        patch(
            "nats_events.nats.connect",
            AsyncMock(return_value=connection),
        ) as connect,
        patch("nats_events.ensure_catalog_index", AsyncMock()),
        patch("nats_events._durable", AsyncMock(return_value=MagicMock())),
        patch(
            "nats_events._consume",
            AsyncMock(side_effect=RuntimeError("interrupted")),
        ),
    ):
      with self.assertRaisesRegex(RuntimeError, "interrupted"):
        await _run()

    self.assertEqual(3, connect.await_args.kwargs["max_reconnect_attempts"])
    connection.close.assert_awaited_once()


if __name__ == "__main__":
  unittest.main()
