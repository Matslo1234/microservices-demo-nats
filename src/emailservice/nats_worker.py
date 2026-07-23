#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import asyncio
import base64
import hashlib
import json
import os
import ssl
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

import nats
from google.protobuf.any_pb2 import Any
from google.protobuf.timestamp_pb2 import Timestamp
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

from logger import getJSONLogger
from protos.common.v1 import message_pb2
from protos.events.v1 import events_pb2

logger = getJSONLogger("emailservice-nats")
ORDER_SUBJECT = "boutique.evt.order.completed.v1"
DURABLE = "email-order-completed-v1"
SENT_SUBJECT = "boutique.evt.notification.order-confirmation-sent.v1"
FAILED_SUBJECT = "boutique.evt.notification.order-confirmation-failed.v1"

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


def _timestamp_now():
  value = Timestamp()
  value.FromDatetime(datetime.now(timezone.utc))
  return value


def _stable_id(*parts):
  digest = bytearray(hashlib.sha256("\0".join(parts).encode()).digest()[:16])
  digest[6] = (digest[6] & 0x0F) | 0x50
  digest[8] = (digest[8] & 0x3F) | 0x80
  return str(uuid.UUID(bytes=bytes(digest)))


def _mask_recipient(address):
  local, separator, domain = address.partition("@")
  if not separator:
    return "***"
  return (local[:1] if local else "*") + "***@" + domain


class _State:
  def __init__(self, filename):
    self.path = Path(filename)
    self.path.parent.mkdir(parents=True, exist_ok=True)
    self.outcomes = {}
    if self.path.exists():
      self.outcomes = json.loads(self.path.read_text()).get("outcomes", {})

  def record(self, message_id, outcome):
    if message_id in self.outcomes:
      return self.outcomes[message_id]
    next_outcomes = dict(self.outcomes)
    next_outcomes[message_id] = outcome
    temporary = self.path.with_suffix(self.path.suffix + ".tmp")
    with temporary.open("w") as output:
      json.dump({"outcomes": next_outcomes}, output, separators=(",", ":"))
      output.flush()
      os.fsync(output.fileno())
    temporary.replace(self.path)
    self.outcomes = next_outcomes
    return outcome


def _build_outcome(envelope):
  completed = events_pb2.OrderCompletedEvent()
  if not envelope.data.Unpack(completed) or not completed.order.order_id:
    raise ValueError("completed order payload is invalid")
  order = completed.order
  if os.getenv("EMAIL_FAILURE_MODE", "") == "failed":
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
      provider_message_id=_stable_id("email", order.order_id),
    )
    message_type = "boutique.notification.OrderConfirmationSent.v1"

  wrapped = Any()
  wrapped.Pack(payload)
  message_id = _stable_id(subject, envelope.message_id)
  result = message_pb2.MessageEnvelope(
    message_id=message_id,
    message_type=message_type,
    schema_version=1,
    occurred_at=_timestamp_now(),
    producer="emailservice/phase5",
    aggregate_type="order",
    aggregate_id=order.order_id,
    aggregate_version=envelope.aggregate_version,
    correlation_id=envelope.correlation_id,
    causation_id=envelope.message_id,
    traceparent=envelope.traceparent,
    tracestate=envelope.tracestate,
    data=wrapped,
  )
  return {"subject": subject, "message_id": message_id,
          "data": base64.b64encode(result.SerializeToString()).decode()}


async def _run():
  for name in ("NATS_URL", "NATS_USER", "NATS_PASSWORD", "NATS_CA_FILE"):
    if not os.getenv(name):
      raise RuntimeError(f"{name} is required")
  state = _State(os.getenv("EMAIL_STORE_PATH", "/tmp/email/inbox.json"))
  tls_context = ssl.create_default_context(cafile=os.environ["NATS_CA_FILE"])
  if hasattr(ssl, "VERIFY_X509_STRICT"):
    tls_context.verify_flags &= ~ssl.VERIFY_X509_STRICT
  connection = await nats.connect(
    servers=[os.environ["NATS_URL"]], user=os.environ["NATS_USER"],
    password=os.environ["NATS_PASSWORD"], name="emailservice/phase5",
    tls=tls_context, allow_reconnect=True, max_reconnect_attempts=-1)
  js = connection.jetstream(timeout=5)
  config = ConsumerConfig(
    durable_name=DURABLE, deliver_policy=DeliverPolicy.ALL,
    ack_policy=AckPolicy.EXPLICIT, ack_wait=30, max_deliver=10,
    filter_subject=ORDER_SUBJECT)
  subscription = await js.pull_subscribe(
    ORDER_SUBJECT, durable=DURABLE, stream="BOUTIQUE_EVENTS", config=config)
  _ready.set()
  logger.info("Email order-completed consumer is ready")
  try:
    while not _stop.is_set():
      try:
        messages = await subscription.fetch(batch=16, timeout=1)
      except (NatsTimeoutError, asyncio.TimeoutError):
        continue
      for message in messages:
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
          outcome = state.outcomes.get(envelope.message_id)
          if outcome is None:
            outcome = state.record(envelope.message_id, _build_outcome(envelope))
          await js.publish(outcome["subject"], base64.b64decode(outcome["data"]),
                           headers={"Nats-Msg-Id": outcome["message_id"]})
          logger.debug(
              "NATS event sent",
              extra={
                  "topic": outcome["subject"],
                  "message_kind": "event",
                  "message_id": outcome["message_id"],
                  "correlation_id": correlation_id,
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
  finally:
    _ready.clear()
    await connection.drain()


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
