#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import asyncio
import hashlib
import json
import os
import ssl
import threading
import uuid
from datetime import datetime, timedelta, timezone

import nats
from google.protobuf.any_pb2 import Any
from google.protobuf.timestamp_pb2 import Timestamp
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.errors import ServiceUnavailableError

from catalog_kv import (
    CatalogConflict,
    CatalogNotFound,
    apply_product,
    apply_snapshot,
    catalog_candidates,
)
from logger import getJSONLogger
from protos.common.v1 import message_pb2
from protos.events.v1 import events_pb2


logger = getJSONLogger("recommendationservice-nats")
CATALOG_SUBJECT = "boutique.evt.catalog.>"
CART_SUBJECT = "boutique.evt.cart.>"
PAGE_VIEW_SUBJECT = "boutique.evt.storefront.page-viewed.v1"
RESULT_SUBJECT = "boutique.evt.recommendation.generated.v1"
CATALOG_BUCKET = "RECOMMENDATION_CATALOG"
MODEL_REVISION = "deterministic-sample-v1"

_ready = threading.Event()
_stop = threading.Event()
_loop = None
_connection = None
_thread = None


def _duration(name, fallback):
    value = os.getenv(name)
    if not value:
        return fallback
    units = {"ms": 0.001, "s": 1, "m": 60}
    for suffix, multiplier in units.items():
        if value.endswith(suffix):
            return float(value[: -len(suffix)]) * multiplier
    raise ValueError(f"invalid duration in {name}")


def _integer(name, fallback):
    value = os.getenv(name)
    return int(value) if value else fallback


def _context_version(envelope):
    if envelope.aggregate_type == "storefront-session" and envelope.aggregate_version:
        return envelope.aggregate_version
    if envelope.occurred_at.seconds:
        return envelope.occurred_at.seconds * 1_000_000_000 + envelope.occurred_at.nanos
    digest = hashlib.sha256(envelope.message_id.encode()).digest()
    version = int.from_bytes(digest[:8], "big") & 0x7fffffffffffffff
    return version or 1


def _message_id(*parts):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "\0".join(parts)))


def messaging_ready():
    return _ready.is_set()


def _message_context(message):
    try:
        envelope = message_pb2.MessageEnvelope.FromString(message.data)
        return (envelope.correlation_id or "unknown",
                envelope.message_id or "unknown")
    except Exception:
        return "unknown", "unknown"


def _trigger_time(envelope):
    if envelope.occurred_at.seconds or envelope.occurred_at.nanos:
        return envelope.occurred_at.ToDatetime(tzinfo=timezone.utc)
    return datetime.fromtimestamp(0, timezone.utc)


async def _publish_result(js, catalog_store, envelope, session_id, excluded):
    if not envelope.message_id or not session_id:
        raise ValueError("recommendation trigger identity is incomplete")
    context_version = _context_version(envelope)
    product_ids, catalog_revision = await catalog_candidates(
        catalog_store,
        set(excluded),
        envelope.message_id,
        MODEL_REVISION,
    )
    occurred_at = _trigger_time(envelope)
    expires = Timestamp()
    expires.FromDatetime(occurred_at + timedelta(minutes=10))
    payload = events_pb2.RecommendationGeneratedEvent(
        session_id=session_id,
        triggering_context_version=context_version,
        product_ids=product_ids,
        expires_at=expires,
    )
    wrapped = Any()
    wrapped.Pack(payload)
    message_id = _message_id(
        RESULT_SUBJECT,
        envelope.message_id,
        MODEL_REVISION,
        str(catalog_revision),
    )
    occurred_timestamp = Timestamp()
    occurred_timestamp.FromDatetime(occurred_at)
    result = message_pb2.MessageEnvelope(
        message_id=message_id,
        message_type="boutique.recommendation.Generated.v1",
        schema_version=1,
        occurred_at=occurred_timestamp,
        producer="recommendationservice/phase2",
        aggregate_type="recommendation-context",
        aggregate_id=session_id,
        aggregate_version=context_version,
        correlation_id=envelope.correlation_id,
        causation_id=envelope.message_id,
        traceparent=envelope.traceparent,
        tracestate=envelope.tracestate,
        data=wrapped,
    )
    await js.publish(RESULT_SUBJECT, result.SerializeToString(), headers={"Nats-Msg-Id": message_id})
    logger.debug(
        "NATS event sent",
        extra={
            "topic": RESULT_SUBJECT,
            "message_kind": "event",
            "message_id": message_id,
            "correlation_id": envelope.correlation_id or "unknown",
        })


class _NATSCatalogStore:

    def __init__(self, bucket):
        self._bucket = bucket

    async def get(self, key):
        try:
            entry = await self._bucket.get(key)
        except Exception as error:
            if _kv_not_found(error):
                raise CatalogNotFound(key) from error
            raise
        return json.loads(entry.value), entry.revision

    async def create(self, key, value):
        try:
            return await self._bucket.create(key, value)
        except Exception as error:
            if _kv_conflict(error):
                raise CatalogConflict(key) from error
            raise

    async def update(self, key, value, revision):
        try:
            return await self._bucket.update(key, value, last=revision)
        except Exception as error:
            if _kv_conflict(error):
                raise CatalogConflict(key) from error
            raise

    async def keys(self):
        try:
            return await self._bucket.keys()
        except Exception as error:
            if _kv_not_found(error) or type(error).__name__ == "NoKeysError":
                return []
            raise


def _kv_not_found(error):
    return type(error).__name__ in {
        "KeyNotFoundError",
        "KeyDeletedError",
    } or getattr(error, "code", None) == 404


def _kv_conflict(error):
    message = str(error).lower()
    return type(error).__name__ in {
        "KeyWrongLastSequenceError",
        "KeyExistsError",
    } or "wrong last sequence" in message


async def _apply_catalog(catalog_store, message):
    envelope = message_pb2.MessageEnvelope.FromString(message.data)
    if not envelope.message_id:
        raise ValueError("catalog event message ID is required")
    if message.subject == "boutique.evt.catalog.product-upserted.v1":
        payload = events_pb2.CatalogProductUpsertedEvent()
        if not envelope.data.Unpack(payload) or not payload.product.product_id:
            raise ValueError("catalog product payload is invalid")
        return await apply_product(catalog_store, {
            "product_id": payload.product.product_id,
            "product_version": payload.product.product_version,
            "catalog_revision": payload.catalog_revision,
            "removed": False,
            "source_event_id": envelope.message_id,
            "source_version": envelope.aggregate_version,
        })
    if message.subject == "boutique.evt.catalog.product-removed.v1":
        payload = events_pb2.CatalogProductRemovedEvent()
        if not envelope.data.Unpack(payload) or not payload.product_id:
            raise ValueError("catalog removal payload is invalid")
        return await apply_product(catalog_store, {
            "product_id": payload.product_id,
            "product_version": payload.product_version,
            "catalog_revision": payload.catalog_revision,
            "removed": True,
            "source_event_id": envelope.message_id,
            "source_version": envelope.aggregate_version,
        })
    if message.subject == "boutique.evt.catalog.snapshot-completed.v1":
        payload = events_pb2.CatalogSnapshotCompletedEvent()
        if not envelope.data.Unpack(payload):
            raise ValueError("catalog snapshot payload is invalid")
        return await apply_snapshot(catalog_store, {
            "catalog_revision": payload.catalog_revision,
            "product_count": payload.product_count,
            "checksum": payload.checksum,
            "source_event_id": envelope.message_id,
            "source_version": envelope.aggregate_version,
        })
    return "ignored"


async def _handle_trigger(js, catalog_store, message):
    envelope = message_pb2.MessageEnvelope.FromString(message.data)
    if message.subject == PAGE_VIEW_SUBJECT:
        payload = events_pb2.StorefrontPageViewedEvent()
        if not envelope.data.Unpack(payload):
            raise ValueError("page-view payload is invalid")
        excluded = [payload.product_id] if payload.product_id else []
        await _publish_result(js, catalog_store, envelope, payload.session_id, excluded)
        return
    if message.subject == "boutique.evt.cart.item-added.v1":
        payload = events_pb2.CartItemAddedEvent()
        if not envelope.data.Unpack(payload) or not payload.cart.user_id:
            raise ValueError("cart item payload is invalid")
        await _publish_result(js, catalog_store, envelope, payload.cart.user_id, [line.product_id for line in payload.cart.items])
    elif message.subject == "boutique.evt.cart.cleared.v1":
        payload = events_pb2.CartClearedEvent()
        if not envelope.data.Unpack(payload) or not payload.cart.user_id:
            raise ValueError("cart clear payload is invalid")
        await _publish_result(js, catalog_store, envelope, payload.cart.user_id, [])


async def _process_message(message, handler):
    correlation_id, message_id = _message_context(message)
    try:
        await handler(message)
        await message.ack()
    except Exception:
        logger.exception(
            "Event processing failed",
            extra={
                "topic": message.subject,
                "message_id": message_id,
                "correlation_id": correlation_id,
            })
        await message.nak(delay=1)


async def _consume(subscription, handler):
    while not _stop.is_set():
        try:
            messages = await subscription.fetch(batch=64, timeout=1)
        except (NatsTimeoutError, asyncio.TimeoutError):
            continue
        except (nats.errors.Error, ServiceUnavailableError):
            # A JetStream leader transition must not permanently stop the
            # background worker and leave a healthy-looking process idle.
            _ready.clear()
            await asyncio.sleep(0.1)
            if _connection and _connection.is_connected:
                _ready.set()
            continue
        if messages:
            logger.debug(
                "NATS event batch received",
                extra={"message_kind": "event", "batch_size": len(messages)},
            )
            await asyncio.gather(
                *(_process_message(message, handler) for message in messages)
            )


async def _durable(js, subject, durable):
    config = ConsumerConfig(
        durable_name=durable,
        deliver_policy=DeliverPolicy.ALL,
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=30,
        max_deliver=10,
        filter_subject=subject,
    )
    return await js.pull_subscribe(subject, durable=durable, stream="BOUTIQUE_EVENTS", config=config)


async def _run():
    global _connection
    required = os.getenv("NATS_REQUIRED", "false").lower() == "true"
    if not required:
        _ready.set()
        return
    for name in ("NATS_URL", "NATS_USER", "NATS_PASSWORD", "NATS_CA_FILE"):
        if not os.getenv(name):
            raise RuntimeError(f"{name} is required when NATS_REQUIRED=true")
    tls_context = ssl.create_default_context(cafile=os.environ["NATS_CA_FILE"])
    # Python 3.14 enables OpenSSL's strict extension checks by default. The
    # existing Phase 1 CA predates the CA key-usage extension, so retain normal
    # chain and hostname validation while accepting that already-deployed CA.
    if hasattr(ssl, "VERIFY_X509_STRICT"):
        tls_context.verify_flags &= ~ssl.VERIFY_X509_STRICT

    async def disconnected():
        _ready.clear()

    async def reconnected():
        _ready.set()

    async def closed():
        _ready.clear()

    _connection = await nats.connect(
        servers=[os.environ["NATS_URL"]],
        user=os.environ["NATS_USER"],
        password=os.environ["NATS_PASSWORD"],
        name="recommendationservice/phase2",
        tls=tls_context,
        connect_timeout=_duration("NATS_CONNECT_TIMEOUT", 2),
        reconnect_time_wait=_duration("NATS_RECONNECT_WAIT", 2),
        max_reconnect_attempts=_integer("NATS_MAX_RECONNECTS", -1),
        ping_interval=_duration("NATS_PING_INTERVAL", 20),
        max_outstanding_pings=_integer("NATS_MAX_PINGS_OUT", 2),
        allow_reconnect=True,
        disconnected_cb=disconnected,
        reconnected_cb=reconnected,
        closed_cb=closed,
    )
    js = _connection.jetstream(timeout=_duration("NATS_PUBLISH_TIMEOUT", 5))
    catalog_store = _NATSCatalogStore(
        await js.key_value(CATALOG_BUCKET)
    )
    catalog = await _durable(js, CATALOG_SUBJECT, "recommendation-catalog-v1")
    cart = await _durable(js, CART_SUBJECT, "recommendation-cart-v1")
    page = await _durable(js, PAGE_VIEW_SUBJECT, "recommendation-page-views-v1")
    _ready.set()
    logger.info(
        "NATS event consumers and shared catalog KV are ready",
        extra={"catalog_bucket": CATALOG_BUCKET},
    )
    try:
        await asyncio.gather(
            _consume(catalog, lambda message: _apply_catalog(catalog_store, message)),
            _consume(cart, lambda message: _handle_trigger(js, catalog_store, message)),
            _consume(page, lambda message: _handle_trigger(js, catalog_store, message)),
        )
    finally:
        if not _connection.is_closed:
            await _connection.drain()


def _thread_main():
    global _connection, _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    try:
        while not _stop.is_set():
            try:
                _loop.run_until_complete(_run())
                if os.getenv("NATS_REQUIRED", "false").lower() != "true":
                    break
            except Exception:
                if not _stop.is_set():
                    logger.exception(
                        "NATS event worker is unavailable; retrying startup")
                _ready.clear()
            if _connection and not _connection.is_closed:
                _loop.run_until_complete(_connection.close())
            _connection = None
            if not _stop.is_set():
                _loop.run_until_complete(asyncio.sleep(1))
    finally:
        _loop.close()


def start_event_worker():
    global _thread
    _thread = threading.Thread(target=_thread_main, name="recommendation-nats", daemon=True)
    _thread.start()


def stop_event_worker():
    _ready.clear()
    _stop.set()
    if _thread:
        _thread.join(timeout=10)
