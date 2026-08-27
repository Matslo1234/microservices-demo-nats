#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from google.protobuf.any_pb2 import Any
from google.protobuf.timestamp_pb2 import Timestamp
from nats.js.api import DeliverPolicy
from nats.js.errors import NoKeysError, ServiceUnavailableError

from nats_events import (
    _CatalogCache,
    _NATSCatalogStore,
    _consume,
    _fresh_page_views,
    _latest_cart_triggers,
    _ready,
    _run,
    _stop,
)
from protos.common.v1 import message_pb2
from protos.events.v1 import events_pb2


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


def page_view(session_id, occurred_at, version):
  payload = events_pb2.StorefrontPageViewedEvent(session_id=session_id)
  wrapped = Any()
  wrapped.Pack(payload)
  timestamp = Timestamp()
  timestamp.FromDatetime(occurred_at)
  envelope = message_pb2.MessageEnvelope(
      message_id=f"{session_id}-{version}",
      aggregate_type="storefront-session",
      aggregate_version=version,
      occurred_at=timestamp,
      data=wrapped,
  )
  message = Message()
  message.subject = "boutique.evt.storefront.page-viewed.v1"
  message.data = envelope.SerializeToString()
  return message


def cart_event(user_id, occurred_at, version, cleared=False):
  cart = message_pb2.CartSnapshot(user_id=user_id)
  payload = (
      events_pb2.CartClearedEvent(cart=cart)
      if cleared
      else events_pb2.CartItemAddedEvent(cart=cart)
  )
  wrapped = Any()
  wrapped.Pack(payload)
  timestamp = Timestamp()
  timestamp.FromDatetime(occurred_at)
  envelope = message_pb2.MessageEnvelope(
      message_id=f"{user_id}-{version}",
      aggregate_version=version,
      occurred_at=timestamp,
      data=wrapped,
  )
  message = Message()
  message.subject = (
      "boutique.evt.cart.cleared.v1"
      if cleared
      else "boutique.evt.cart.item-added.v1"
  )
  message.data = envelope.SerializeToString()
  return message


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
      if len(subscription.requests) == 1:
        return messages
      await asyncio.sleep(0)
      raise asyncio.TimeoutError

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
    self.assertEqual(
        {"batch": 3, "timeout": 1}, subscription.requests[0])
    self.assertTrue(all(message.acks == 1 for message in messages))

  async def test_page_views_discard_stale_and_superseded_messages(self):
    now = datetime.now(timezone.utc)
    old = page_view("old", now - timedelta(seconds=30), 1)
    superseded = page_view("active", now, 1)
    newest = page_view("active", now, 2)
    other = page_view("other", now, 1)

    retained = await _fresh_page_views(
        [old, superseded, newest, other], max_age=5)

    self.assertEqual({newest, other}, set(retained))
    self.assertEqual(1, old.acks)
    self.assertEqual(1, superseded.acks)
    self.assertEqual(0, newest.acks)
    self.assertEqual(0, other.acks)

  async def test_page_view_filter_keeps_malformed_message_for_redelivery(self):
    malformed = Message()

    self.assertEqual(
        [malformed], await _fresh_page_views([malformed], max_age=5))
    self.assertEqual(0, malformed.acks)

  async def test_cart_triggers_keep_newest_complete_snapshot_per_user(self):
    now = datetime.now(timezone.utc)
    item_added = cart_event("active", now, 1)
    cleared = cart_event("active", now + timedelta(milliseconds=1), 2, True)
    other = cart_event("other", now, 1)

    retained = await _latest_cart_triggers([item_added, cleared, other])

    self.assertEqual({cleared, other}, set(retained))
    self.assertEqual(1, item_added.acks)
    self.assertEqual(0, cleared.acks)
    self.assertEqual(0, other.acks)

  async def test_cart_filter_keeps_unhandled_event_for_normal_ack(self):
    rejected = Message()
    rejected.subject = "boutique.evt.cart.command-rejected.v1"

    self.assertEqual([rejected], await _latest_cart_triggers([rejected]))
    self.assertEqual(0, rejected.acks)

  async def test_empty_catalog_bucket_has_no_keys(self):
    store = _NATSCatalogStore(EmptyBucket())

    self.assertEqual([], await store.keys())

  async def test_catalog_cache_reuses_immutable_snapshot(self):
    store = MagicMock()
    cache = _CatalogCache(store, refresh_seconds=60)

    with patch(
        "nats_events.catalog_snapshot",
        AsyncMock(return_value=(("one", "two"), 7)),
    ) as snapshot:
      first = await cache.candidates(set(), "seed", "model")
      second = await cache.candidates(set(), "seed", "model")

    self.assertEqual(first, second)
    snapshot.assert_awaited_once_with(store)

  async def test_catalog_cache_refreshes_after_invalidation(self):
    store = MagicMock()
    cache = _CatalogCache(store, refresh_seconds=60)

    with patch(
        "nats_events.catalog_snapshot",
        AsyncMock(side_effect=[(("one",), 1), (("two",), 2)]),
    ) as snapshot:
      await cache.candidates(set(), "seed", "model")
      cache.invalidate()
      selected, revision = await cache.candidates(
          set(), "seed", "model")

    self.assertEqual(2, revision)
    self.assertEqual(["two"], selected)
    self.assertEqual(2, snapshot.await_count)

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
        patch(
            "nats_events.catalog_snapshot",
            AsyncMock(return_value=(("one",), 1)),
        ),
        patch(
            "nats_events._durable", AsyncMock(return_value=MagicMock())
        ) as durable,
        patch(
            "nats_events._consume",
            AsyncMock(side_effect=RuntimeError("interrupted")),
        ),
    ):
      with self.assertRaisesRegex(RuntimeError, "interrupted"):
        await _run()

    self.assertEqual(3, connect.await_args.kwargs["max_reconnect_attempts"])
    self.assertEqual(DeliverPolicy.ALL, durable.await_args_list[0].kwargs.get(
        "deliver_policy", DeliverPolicy.ALL))
    self.assertEqual(DeliverPolicy.ALL, durable.await_args_list[1].kwargs.get(
        "deliver_policy", DeliverPolicy.ALL))
    self.assertEqual(
        DeliverPolicy.NEW,
        durable.await_args_list[2].kwargs["deliver_policy"],
    )
    connection.close.assert_awaited_once()


if __name__ == "__main__":
  unittest.main()
