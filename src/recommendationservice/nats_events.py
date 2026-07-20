#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import asyncio
import os
import random
import ssl
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import nats
from google.protobuf.any_pb2 import Any
from google.protobuf.timestamp_pb2 import Timestamp
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

from logger import getJSONLogger
from protos.common.v1 import message_pb2
from protos.events.v1 import events_pb2


logger = getJSONLogger("recommendationservice-nats")
CATALOG_SUBJECT = "boutique.evt.catalog.>"
CART_SUBJECT = "boutique.evt.cart.>"
PAGE_VIEW_SUBJECT = "boutique.evt.storefront.page-viewed.v1"
RESULT_SUBJECT = "boutique.evt.recommendation.generated.v1"

_products = set()
_products_lock = threading.Lock()
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


def _timestamp_now():
    value = Timestamp()
    value.FromDatetime(datetime.now(timezone.utc))
    return value


def _context_version(envelope):
    if envelope.aggregate_type == "storefront-session" and envelope.aggregate_version:
        return envelope.aggregate_version
    if envelope.occurred_at.seconds:
        return envelope.occurred_at.seconds * 1_000_000_000 + envelope.occurred_at.nanos
    return time.time_ns()


def _message_id(subject, causation_id):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, subject + "\0" + causation_id))


def recommend_products(excluded, seed):
    with _products_lock:
        available = sorted(_products.difference(excluded))
    count = min(5, len(available))
    return random.Random(seed).sample(available, count)


def messaging_ready():
    return _ready.is_set()


async def _publish_result(js, envelope, session_id, excluded):
    context_version = _context_version(envelope)
    product_ids = recommend_products(set(excluded), envelope.message_id)
    expires = Timestamp()
    expires.FromDatetime(datetime.now(timezone.utc) + timedelta(minutes=10))
    payload = events_pb2.RecommendationGeneratedEvent(
        session_id=session_id,
        triggering_context_version=context_version,
        product_ids=product_ids,
        expires_at=expires,
    )
    wrapped = Any()
    wrapped.Pack(payload)
    message_id = _message_id(RESULT_SUBJECT, envelope.message_id)
    result = message_pb2.MessageEnvelope(
        message_id=message_id,
        message_type="boutique.recommendation.Generated.v1",
        schema_version=1,
        occurred_at=_timestamp_now(),
        producer="recommendationservice/phase3",
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


def _apply_catalog(message):
    envelope = message_pb2.MessageEnvelope.FromString(message.data)
    if message.subject == "boutique.evt.catalog.product-upserted.v1":
        payload = events_pb2.CatalogProductUpsertedEvent()
        if not envelope.data.Unpack(payload) or not payload.product.product_id:
            raise ValueError("catalog product payload is invalid")
        with _products_lock:
            _products.add(payload.product.product_id)
    elif message.subject == "boutique.evt.catalog.product-removed.v1":
        payload = events_pb2.CatalogProductRemovedEvent()
        if not envelope.data.Unpack(payload):
            raise ValueError("catalog removal payload is invalid")
        with _products_lock:
            _products.discard(payload.product_id)


async def _handle_trigger(js, message):
    envelope = message_pb2.MessageEnvelope.FromString(message.data)
    if message.subject == PAGE_VIEW_SUBJECT:
        payload = events_pb2.StorefrontPageViewedEvent()
        if not envelope.data.Unpack(payload):
            raise ValueError("page-view payload is invalid")
        excluded = [payload.product_id] if payload.product_id else []
        await _publish_result(js, envelope, payload.session_id, excluded)
        return
    if message.subject == "boutique.evt.cart.item-added.v1":
        payload = events_pb2.CartItemAddedEvent()
        if not envelope.data.Unpack(payload) or not payload.cart.user_id:
            raise ValueError("cart item payload is invalid")
        await _publish_result(js, envelope, payload.cart.user_id, [line.product_id for line in payload.cart.items])
    elif message.subject == "boutique.evt.cart.cleared.v1":
        payload = events_pb2.CartClearedEvent()
        if not envelope.data.Unpack(payload) or not payload.cart.user_id:
            raise ValueError("cart clear payload is invalid")
        await _publish_result(js, envelope, payload.cart.user_id, [])


async def _consume(subscription, handler):
    while not _stop.is_set():
        try:
            messages = await subscription.fetch(batch=32, timeout=1)
        except (NatsTimeoutError, asyncio.TimeoutError):
            continue
        for message in messages:
            try:
                await handler(message)
                await message.ack()
            except Exception:
                logger.exception("event processing failed", extra={"subject": message.subject})
                await message.nak(delay=1)


async def _bootstrap_catalog(js):
    config = ConsumerConfig(
        deliver_policy=DeliverPolicy.ALL,
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=30,
        max_deliver=10,
        filter_subject=CATALOG_SUBJECT,
    )
    subscription = await js.pull_subscribe(CATALOG_SUBJECT, stream="BOUTIQUE_EVENTS", config=config)
    while True:
        info = await subscription.consumer_info()
        if info.num_pending == 0:
            break
        try:
            messages = await subscription.fetch(batch=min(64, info.num_pending), timeout=1)
        except (NatsTimeoutError, asyncio.TimeoutError):
            continue
        for message in messages:
            _apply_catalog(message)
            await message.ack()
    await subscription.unsubscribe()


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
        name="recommendationservice/phase3",
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
    await _bootstrap_catalog(js)
    catalog = await _durable(js, CATALOG_SUBJECT, "recommendation-catalog-v1")
    cart = await _durable(js, CART_SUBJECT, "recommendation-cart-v1")
    page = await _durable(js, PAGE_VIEW_SUBJECT, "recommendation-page-views-v1")
    _ready.set()
    logger.info("NATS event consumers are ready", extra={"catalog_products": len(_products)})
    try:
        await asyncio.gather(
            _consume(catalog, lambda message: asyncio.to_thread(_apply_catalog, message)),
            _consume(cart, lambda message: _handle_trigger(js, message)),
            _consume(page, lambda message: _handle_trigger(js, message)),
        )
    finally:
        if not _connection.is_closed:
            await _connection.drain()


def _thread_main():
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    try:
        _loop.run_until_complete(_run())
    except Exception:
        if not _stop.is_set():
            logger.exception("NATS event worker stopped")
        _ready.clear()
    finally:
        _loop.close()


def start_event_worker(timeout=30):
    global _thread
    _thread = threading.Thread(target=_thread_main, name="recommendation-nats", daemon=True)
    _thread.start()
    if not _ready.wait(timeout):
        raise RuntimeError("recommendation NATS consumers did not become ready")


def stop_event_worker():
    _ready.clear()
    _stop.set()
    if _thread:
        _thread.join(timeout=10)
