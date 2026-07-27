#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import asyncio
import base64
import hashlib
import os
import ssl
import struct
import threading
import uuid

import nats
from google.protobuf.any_pb2 import Any
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.errors import ServiceUnavailableError

from logger import getJSONLogger
from protos.common.v1 import message_pb2
from protos.events.v1 import events_pb2

logger = getJSONLogger("emailservice-nats")
ORDER_SUBJECT = "boutique.evt.order.completed.v1"
DURABLE = "email-order-completed-v1"
SENT_SUBJECT = "boutique.evt.notification.order-confirmation-sent.v1"
FAILED_SUBJECT = "boutique.evt.notification.order-confirmation-failed.v1"
NOTIFICATION_TYPE = "order-confirmation"
RESULT_SLOT = "notification.order-confirmation"
_ready = threading.Event()
_stop = threading.Event()
_thread = None


def messaging_ready():
  return _ready.is_set()


def _message_context(message):
  try:
    envelope = message_pb2.MessageEnvelope.FromString(message.data)
    return (envelope.correlation_id or "unknown",
            envelope.message_id or "unknown")
  except Exception:
    return "unknown", "unknown"


def _stable_id(*parts):
  digest = bytearray(hashlib.sha256("\0".join(parts).encode()).digest()[:16])
  digest[6] = (digest[6] & 0x0F) | 0x50
  digest[8] = (digest[8] & 0x3F) | 0x80
  return str(uuid.UUID(bytes=bytes(digest)))


def _result_message_id(input_message_id, result_slot):
  if not input_message_id:
    raise ValueError("input message ID is required")
  input_bytes = input_message_id.encode()
  slot_bytes = result_slot.encode()
  digest = hashlib.sha256(
      b"boutique.result.v1\0"
      + struct.pack(">I", len(input_bytes))
      + input_bytes
      + struct.pack(">I", len(slot_bytes))
      + slot_bytes).digest()
  return "br1_" + base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _mask_recipient(address):
  local, separator, domain = address.partition("@")
  if not separator:
    return "***"
  return (local[:1] if local else "*") + "***@" + domain


def _provider_idempotency_key(order_id, notification_type=NOTIFICATION_TYPE):
  if not order_id or not notification_type:
    raise ValueError("order ID and notification type are required")
  return f"{order_id}:{notification_type}"


def _validate_input(envelope):
  if (not envelope.message_id or not envelope.correlation_id
      or not envelope.aggregate_id or not envelope.aggregate_version
      or not envelope.HasField("occurred_at") or not envelope.HasField("data")):
    raise ValueError("completed order envelope is incomplete")
  # ToDatetime validates the protobuf timestamp range and nanos.
  envelope.occurred_at.ToDatetime()


def _build_outcome(envelope, failure_mode=None):
  _validate_input(envelope)
  completed = events_pb2.OrderCompletedEvent()
  if not envelope.data.Unpack(completed) or not completed.order.order_id:
    raise ValueError("completed order payload is invalid")
  order = completed.order
  if order.order_id != envelope.aggregate_id:
    raise ValueError("completed order does not match the envelope aggregate")

  provider_key = _provider_idempotency_key(order.order_id)
  selected_failure_mode = (
      os.getenv("EMAIL_FAILURE_MODE", "")
      if failure_mode is None else failure_mode
  )
  if selected_failure_mode == "failed":
    subject = FAILED_SUBJECT
    payload = events_pb2.NotificationOrderConfirmationFailedEvent(
      order_id=order.order_id,
      failure=message_pb2.Failure(
        code="EMAIL_PROVIDER_UNAVAILABLE",
        retryable=True,
        safe_message="Order confirmation could not be sent.",
      ),
      attempt_count=1,
    )
    message_type = "boutique.notification.OrderConfirmationFailed.v1"
  else:
    subject = SENT_SUBJECT
    payload = events_pb2.NotificationOrderConfirmationSentEvent(
      order_id=order.order_id,
      masked_recipient=_mask_recipient(order.email),
      provider_message_id=_stable_id("email-provider-v1", provider_key),
    )
    message_type = "boutique.notification.OrderConfirmationSent.v1"

  wrapped = Any()
  wrapped.Pack(payload, deterministic=True)
  message_id = _result_message_id(envelope.message_id, RESULT_SLOT)
  result = message_pb2.MessageEnvelope(
    message_id=message_id,
    message_type=message_type,
    schema_version=1,
    producer="emailservice/phase3",
    aggregate_type="order",
    aggregate_id=order.order_id,
    aggregate_version=envelope.aggregate_version,
    correlation_id=envelope.correlation_id,
    causation_id=envelope.message_id,
    traceparent=envelope.traceparent,
    tracestate=envelope.tracestate,
    data=wrapped,
  )
  result.occurred_at.CopyFrom(envelope.occurred_at)
  return {
      "subject": subject,
      "message_id": message_id,
      "provider_idempotency_key": provider_key,
      "data": result.SerializeToString(deterministic=True),
  }


async def _fetch_messages(subscription, retry_delay=0.1):
  while not _stop.is_set():
    try:
      messages = await subscription.fetch(batch=16, timeout=1)
    except (NatsTimeoutError, asyncio.TimeoutError):
      _ready.set()
      continue
    except (nats.errors.Error, ServiceUnavailableError) as error:
      _ready.clear()
      logger.warning(
          "Email consumer fetch interrupted; retrying",
          extra={"error": str(error), "retry_delay_seconds": retry_delay})
      await asyncio.sleep(retry_delay)
      continue
    _ready.set()
    return messages
  return []


async def _process_message(message, js, failure_mode):
  correlation_id, source_event_id = _message_context(message)
  logger.debug(
      "NATS event received",
      extra={
          "topic": message.subject,
          "message_kind": "event",
          "message_id": source_event_id,
          "correlation_id": correlation_id,
      })
  try:
    envelope = message_pb2.MessageEnvelope.FromString(message.data)
    outcome = _build_outcome(envelope, failure_mode)
    await js.publish(
        outcome["subject"],
        outcome["data"],
        headers={"Nats-Msg-Id": outcome["message_id"]})
    logger.debug(
        "NATS event sent",
        extra={
            "topic": outcome["subject"],
            "message_kind": "event",
            "message_id": outcome["message_id"],
            "correlation_id": correlation_id,
            "provider_idempotency_key": outcome["provider_idempotency_key"],
        })
    await message.ack()
  except Exception:
    logger.exception(
        "Order confirmation event processing failed",
        extra={
            "topic": message.subject,
            "source_event_id": source_event_id,
            "message_id": source_event_id,
            "correlation_id": correlation_id,
        })
    await message.nak(delay=1)


async def _consume_connection():
  tls_context = ssl.create_default_context(cafile=os.environ["NATS_CA_FILE"])
  if hasattr(ssl, "VERIFY_X509_STRICT"):
    tls_context.verify_flags &= ~ssl.VERIFY_X509_STRICT
  connection = await nats.connect(
    servers=[os.environ["NATS_URL"]],
    user=os.environ["NATS_USER"],
    password=os.environ["NATS_PASSWORD"],
    name="emailservice/phase3",
    tls=tls_context,
    allow_reconnect=True,
    max_reconnect_attempts=-1)
  try:
    js = connection.jetstream(timeout=5)
    config = ConsumerConfig(
      durable_name=DURABLE,
      deliver_policy=DeliverPolicy.ALL,
      ack_policy=AckPolicy.EXPLICIT,
      ack_wait=30,
      max_deliver=10,
      filter_subject=ORDER_SUBJECT)
    subscription = await js.pull_subscribe(
      ORDER_SUBJECT,
      durable=DURABLE,
      stream="BOUTIQUE_EVENTS",
      config=config)
    failure_mode = os.getenv("EMAIL_FAILURE_MODE", "")
    _ready.set()
    logger.info("Email order-completed consumer is ready")
    while not _stop.is_set():
      messages = await _fetch_messages(subscription)
      await asyncio.gather(*(
          _process_message(message, js, failure_mode)
          for message in messages
      ))
  finally:
    _ready.clear()
    await connection.drain()


async def _run(retry_delay=1):
  for name in ("NATS_URL", "NATS_USER", "NATS_PASSWORD", "NATS_CA_FILE"):
    if not os.getenv(name):
      raise RuntimeError(f"{name} is required")
  while not _stop.is_set():
    try:
      await _consume_connection()
    except Exception as error:
      _ready.clear()
      if _stop.is_set():
        return
      logger.warning(
          "Email NATS initialization interrupted; retrying",
          extra={"error": str(error), "retry_delay_seconds": retry_delay})
      await asyncio.sleep(retry_delay)


def start_nats_worker():
  global _thread
  if _thread is not None:
    return

  def target():
    try:
      asyncio.run(_run())
    except Exception:
      logger.exception("Email NATS worker stopped")
      _ready.clear()

  _thread = threading.Thread(target=target, name="email-nats", daemon=True)
  _thread.start()


def stop_nats_worker():
  _stop.set()
