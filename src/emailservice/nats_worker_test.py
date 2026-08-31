#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import asyncio
import logging
import os
import unittest
from datetime import datetime, timezone
from unittest import mock

from google.protobuf.any_pb2 import Any
from google.protobuf.timestamp_pb2 import Timestamp
from nats.js.errors import ServiceUnavailableError

from logger import getJSONLogger
from nats_worker import (
    FAILED_SUBJECT,
    NOTIFICATION_TYPE,
    RESULT_SLOT,
    SENT_SUBJECT,
    _build_outcome,
    _fetch_message,
    _process_messages,
    _process_message,
    _provider_idempotency_key,
    _ready,
    _result_message_id,
    _stop,
)
from protos.common.v1 import message_pb2
from protos.events.v1 import events_pb2


def completed_order_envelope(
    message_id="event-order-completed-42",
    order_id="order-42",
    occurred_at=None,
):
  completed = events_pb2.OrderCompletedEvent(
      order=message_pb2.SanitizedOrderSnapshot(
          order_id=order_id,
          user_id="user-1",
          email="buyer@example.com",
      ))
  wrapped = Any()
  wrapped.Pack(completed, deterministic=True)
  timestamp = Timestamp()
  timestamp.FromDatetime(
      occurred_at
      or datetime(2026, 7, 27, 10, 30, 0, 123000, tzinfo=timezone.utc))
  return message_pb2.MessageEnvelope(
      message_id=message_id,
      message_type="boutique.order.Completed.v1",
      schema_version=1,
      occurred_at=timestamp,
      producer="checkoutservice",
      aggregate_type="order",
      aggregate_id=order_id,
      aggregate_version=7,
      correlation_id=order_id,
      data=wrapped,
  )


class DeterministicProviderTest(unittest.TestCase):
  def test_result_id_matches_phase0_contract_vector(self):
    self.assertEqual(
        "br1_BipmFE_ifI2JqRb67NFrgisjZYeejPTlkKhojRP1Mz8",
        _result_message_id("event-order-completed-42", RESULT_SLOT))

  def test_replicas_and_retries_build_identical_success(self):
    envelope = completed_order_envelope()
    replica_a = _build_outcome(envelope, "")
    replica_b = _build_outcome(envelope, "")
    retry = _build_outcome(envelope, "")
    self.assertEqual(replica_a, replica_b)
    self.assertEqual(replica_a, retry)
    self.assertEqual(SENT_SUBJECT, replica_a["subject"])
    self.assertEqual(
        f"order-42:{NOTIFICATION_TYPE}",
        replica_a["provider_idempotency_key"])

    result = message_pb2.MessageEnvelope.FromString(replica_a["data"])
    self.assertEqual(envelope.occurred_at, result.occurred_at)
    self.assertEqual(envelope.message_id, result.causation_id)
    sent = events_pb2.NotificationOrderConfirmationSentEvent()
    self.assertTrue(result.data.Unpack(sent))
    self.assertEqual("order-42", sent.order_id)
    self.assertEqual("b***@example.com", sent.masked_recipient)
    self.assertTrue(sent.provider_message_id)

  def test_failure_retries_reuse_provider_key_and_exact_result(self):
    envelope = completed_order_envelope()
    first = _build_outcome(envelope, "failed")
    second = _build_outcome(envelope, "failed")
    self.assertEqual(first, second)
    self.assertEqual(FAILED_SUBJECT, first["subject"])
    self.assertEqual(
        _provider_idempotency_key("order-42"),
        first["provider_idempotency_key"])
    result = message_pb2.MessageEnvelope.FromString(first["data"])
    failed = events_pb2.NotificationOrderConfirmationFailedEvent()
    self.assertTrue(result.data.Unpack(failed))
    self.assertEqual(1, failed.attempt_count)

  def test_business_key_is_order_plus_notification_type(self):
    self.assertEqual(
        "order-1:order-confirmation",
        _provider_idempotency_key("order-1"))
    self.assertEqual(
        "order-1:shipping-update",
        _provider_idempotency_key("order-1", "shipping-update"))

  def test_invalid_envelope_is_rejected(self):
    envelope = completed_order_envelope()
    envelope.correlation_id = ""
    with self.assertRaisesRegex(ValueError, "incomplete"):
      _build_outcome(envelope, "")


class PublishBeforeAckTest(unittest.IsolatedAsyncioTestCase):
  async def asyncSetUp(self):
    _stop.clear()
    _ready.set()

  async def asyncTearDown(self):
    _stop.clear()
    _ready.clear()

  async def test_ambiguous_publish_retries_identical_result_on_another_replica(self):
    envelope = completed_order_envelope()

    class Message:
      subject = "boutique.evt.order.completed.v1"
      data = envelope.SerializeToString(deterministic=True)

      def __init__(self):
        self.acks = 0
        self.naks = 0

      async def ack(self):
        self.acks += 1

      async def nak(self, **_):
        self.naks += 1

    class Publisher:
      def __init__(self, fail=False):
        self.fail = fail
        self.published = []

      async def publish(self, subject, data, headers):
        self.published.append((subject, data, headers))
        if self.fail:
          raise TimeoutError("ambiguous provider response")

    message = Message()
    first_replica = Publisher(fail=True)
    await _process_message(message, first_replica, "")
    self.assertEqual(0, message.acks)
    self.assertEqual(1, message.naks)

    second_replica = Publisher()
    await _process_message(message, second_replica, "")
    self.assertEqual(1, message.acks)
    self.assertEqual(1, message.naks)
    self.assertEqual(
        first_replica.published[0],
        second_replica.published[0],
        "another replica changed the result after an ambiguous response")

  async def test_message_envelope_is_decoded_once(self):
    envelope = completed_order_envelope()

    class Message:
      subject = "boutique.evt.order.completed.v1"
      data = envelope.SerializeToString(deterministic=True)

      def __init__(self):
        self.naks = 0

      async def ack(self):
        return

      async def nak(self, **_):
        self.naks += 1

    class Publisher:
      async def publish(self, *_args, **_kwargs):
        return

    decode = message_pb2.MessageEnvelope.FromString
    with (mock.patch.object(
        message_pb2.MessageEnvelope,
        "FromString",
        wraps=decode,
    ) as mocked_decode,
          mock.patch("nats_worker.TRACING_ENABLED", False),
          mock.patch("nats_worker.consumer_span") as consumer_span,
          mock.patch("nats_worker.inject_envelope") as inject_envelope):
      message = Message()
      await _process_message(message, Publisher(), "")

    self.assertEqual(1, mocked_decode.call_count)
    consumer_span.assert_not_called()
    inject_envelope.assert_not_called()
    self.assertEqual(0, message.naks)


class FetchRecoveryTest(unittest.IsolatedAsyncioTestCase):
  async def asyncSetUp(self):
    _stop.clear()
    _ready.set()

  async def asyncTearDown(self):
    _stop.clear()
    _ready.clear()

  async def test_service_unavailable_rebuilds_the_subscription(self):
    class Subscription:
      def __init__(self):
        self.calls = 0
        self.requests = []

      async def fetch(self, **request):
        self.calls += 1
        self.requests.append(request)
        raise ServiceUnavailableError

    subscription = Subscription()
    with self.assertRaisesRegex(RuntimeError, "subscription fetch failed"):
      await _fetch_message(subscription, retry_delay=0)
    self.assertEqual(1, subscription.calls)
    self.assertEqual(
        [{"batch": 1, "timeout": 30}],
        subscription.requests)
    self.assertFalse(_ready.is_set())

  async def test_batch_processing_is_concurrency_bounded(self):
    active = 0
    maximum_active = 0

    async def process(*unused):
      nonlocal active, maximum_active
      active += 1
      maximum_active = max(maximum_active, active)
      await asyncio.sleep(0)
      active -= 1

    with mock.patch("nats_worker._process_message", side_effect=process):
      await _process_messages([object(), object(), object()], None, "", 2)

    self.assertEqual(2, maximum_active)

  async def test_batch_processing_creates_only_concurrency_workers(self):
    started = 0
    all_started = asyncio.Event()
    release = asyncio.Event()

    async def process(*unused):
      nonlocal started
      started += 1
      if started == 8:
        all_started.set()
      await release.wait()

    with mock.patch("nats_worker._process_message", side_effect=process):
      task = asyncio.create_task(
          _process_messages([object()] * 32, None, "", 8))
      await asyncio.wait_for(all_started.wait(), timeout=1)
      self.assertEqual(8, started)
      release.set()
      await task


class LoggerConfigurationTest(unittest.TestCase):
  def tearDown(self):
    for name in ("emailservice-test-default", "emailservice-test-debug"):
      logging.getLogger(name).handlers.clear()

  def test_logging_defaults_to_info_without_duplicate_handlers(self):
    with mock.patch.dict(os.environ, {}, clear=True):
      logger = getJSONLogger("emailservice-test-default")
      same_logger = getJSONLogger("emailservice-test-default")

    self.assertIs(logger, same_logger)
    self.assertEqual(logging.INFO, logger.level)
    self.assertEqual(1, len(logger.handlers))

  def test_log_level_can_enable_debug(self):
    with mock.patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"}, clear=True):
      logger = getJSONLogger("emailservice-test-debug")

    self.assertEqual(logging.DEBUG, logger.level)


if __name__ == "__main__":
  unittest.main()
