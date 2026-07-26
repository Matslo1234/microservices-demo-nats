#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

"""Shared, versioned recommendation catalog operations.

The runtime adapter is intentionally tiny so the CAS state machine can be
tested without a NATS server. A store implements async get/create/update/keys;
get returns ``(record, revision)`` and raises CatalogNotFound when absent.
"""

import asyncio
import json
import random


class CatalogNotFound(Exception):
  pass


class CatalogConflict(Exception):
  pass


class CatalogNotReady(Exception):
  pass


def product_key(product_id):
  if not product_id or any(character in product_id for character in ".*> /\\"):
    raise ValueError("product ID cannot be represented as a KV key")
  return f"product.{product_id}"


async def apply_product(store, record, max_attempts=20):
  key = product_key(record["product_id"])
  incoming_version = int(record["product_version"])
  if incoming_version <= 0 or not record.get("source_event_id"):
    raise ValueError("catalog record is missing version or source event identity")
  encoded = json.dumps(record, sort_keys=True, separators=(",", ":")).encode()
  for attempt in range(max_attempts):
    try:
      current, revision = await store.get(key)
    except CatalogNotFound:
      try:
        await store.create(key, encoded)
        return "created"
      except CatalogConflict:
        await _backoff(attempt)
        continue
    if current.get("source_event_id") == record["source_event_id"]:
      return "duplicate"
    if incoming_version <= int(current.get("product_version", 0)):
      return "stale"
    try:
      await store.update(key, encoded, revision)
      return "updated"
    except CatalogConflict:
      await _backoff(attempt)
  raise CatalogConflict(f"catalog CAS retry limit reached for {key}")


async def apply_snapshot(store, record, max_attempts=20):
  incoming_revision = int(record["catalog_revision"])
  if incoming_revision <= 0 or not record.get("source_event_id"):
    raise ValueError("catalog snapshot is missing revision or source event identity")
  encoded = json.dumps(record, sort_keys=True, separators=(",", ":")).encode()
  for attempt in range(max_attempts):
    try:
      current, revision = await store.get("catalog")
    except CatalogNotFound:
      try:
        await store.create("catalog", encoded)
        return "created"
      except CatalogConflict:
        await _backoff(attempt)
        continue
    if current.get("source_event_id") == record["source_event_id"]:
      return "duplicate"
    if incoming_revision <= int(current.get("catalog_revision", 0)):
      return "stale"
    try:
      await store.update("catalog", encoded, revision)
      return "updated"
    except CatalogConflict:
      await _backoff(attempt)
  raise CatalogConflict("catalog snapshot CAS retry limit reached")


async def catalog_candidates(store, excluded, seed, model_revision, limit=5):
  try:
    snapshot, _ = await store.get("catalog")
  except CatalogNotFound as error:
    raise CatalogNotReady("catalog snapshot has not completed") from error
  catalog_revision = int(snapshot.get("catalog_revision", 0))
  if catalog_revision <= 0:
    raise CatalogNotReady("catalog snapshot revision is invalid")

  available = []
  for key in await store.keys():
    if not key.startswith("product."):
      continue
    try:
      record, _ = await store.get(key)
    except CatalogNotFound:
      continue
    product_id = record.get("product_id", "")
    if product_id and product_id not in excluded and not record.get("removed", False):
      available.append(product_id)
  available.sort()
  count = min(limit, len(available))
  deterministic_seed = "\0".join((seed, model_revision, str(catalog_revision)))
  selected = random.Random(deterministic_seed).sample(available, count)
  return selected, catalog_revision


async def _backoff(attempt):
  exponent = min(attempt, 6)
  await asyncio.sleep((0.00025 * (2 ** exponent)) + (0.001 * random.random()))
