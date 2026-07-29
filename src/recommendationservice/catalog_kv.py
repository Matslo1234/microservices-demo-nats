#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

"""Shared, versioned recommendation catalog operations.

The runtime adapter is intentionally tiny so the CAS state machine can be
tested without a NATS server. A store implements async get/create/update; get
returns ``(record, revision)`` and raises CatalogNotFound when absent. Existing
deployments also use keys once at startup to seed the product index.
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


CATALOG_INDEX_KEY = "catalog-products"


def product_key(product_id):
  if not product_id or any(character in product_id for character in ".*> /\\"):
    raise ValueError("product ID cannot be represented as a KV key")
  return f"product.{product_id}"


async def ensure_catalog_index(store):
  """Seed the shared index once when upgrading an existing catalog bucket."""
  try:
    await store.get(CATALOG_INDEX_KEY)
    return "existing"
  except CatalogNotFound:
    pass

  product_ids = []
  for key in await store.keys():
    if not key.startswith("product."):
      continue
    try:
      record, _ = await store.get(key)
    except CatalogNotFound:
      continue
    product_id = str(record.get("product_id", ""))
    if product_id and not record.get("removed", False):
      product_ids.append(product_id)

  encoded = json.dumps(
      {"product_ids": sorted(set(product_ids))},
      sort_keys=True,
      separators=(",", ":"),
  ).encode()
  try:
    await store.create(CATALOG_INDEX_KEY, encoded)
    return "created"
  except CatalogConflict:
    # Another replica completed the same startup migration.
    return "existing"


async def apply_product(store, record, max_attempts=20):
  key = product_key(record["product_id"])
  incoming_version = int(record["product_version"])
  if incoming_version <= 0 or not record.get("source_event_id"):
    raise ValueError("catalog record is missing version or source event identity")
  encoded = json.dumps(record, sort_keys=True, separators=(",", ":")).encode()
  outcome = None
  for attempt in range(max_attempts):
    try:
      current, revision = await store.get(key)
    except CatalogNotFound:
      try:
        await store.create(key, encoded)
        outcome = "created"
        break
      except CatalogConflict:
        await _backoff(attempt)
        continue
    if current.get("source_event_id") == record["source_event_id"]:
      outcome = "duplicate"
      break
    if incoming_version <= int(current.get("product_version", 0)):
      outcome = "stale"
      break
    try:
      await store.update(key, encoded, revision)
      outcome = "updated"
      break
    except CatalogConflict:
      await _backoff(attempt)
  if outcome is None:
    raise CatalogConflict(f"catalog CAS retry limit reached for {key}")
  await _reconcile_catalog_index(store, key, max_attempts)
  return outcome


async def _reconcile_catalog_index(store, key, max_attempts):
  """Make the shared active-product index agree with the winning product."""
  try:
    product_record, _ = await store.get(key)
  except CatalogNotFound:
    return
  product_id = str(product_record.get("product_id", ""))
  if not product_id:
    raise ValueError("stored catalog product has no identity")
  removed = bool(product_record.get("removed", False))

  for attempt in range(max_attempts):
    try:
      current, revision = await store.get(CATALOG_INDEX_KEY)
    except CatalogNotFound:
      product_ids = [] if removed else [product_id]
      encoded = json.dumps(
          {"product_ids": product_ids},
          sort_keys=True,
          separators=(",", ":"),
      ).encode()
      try:
        await store.create(CATALOG_INDEX_KEY, encoded)
        return
      except CatalogConflict:
        await _backoff(attempt)
        continue

    product_ids = {
        str(candidate)
        for candidate in current.get("product_ids", [])
        if str(candidate)
    }
    before = set(product_ids)
    if removed:
      product_ids.discard(product_id)
    else:
      product_ids.add(product_id)
    if product_ids == before:
      return
    encoded = json.dumps(
        {"product_ids": sorted(product_ids)},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    try:
      await store.update(CATALOG_INDEX_KEY, encoded, revision)
      return
    except CatalogConflict:
      await _backoff(attempt)
  raise CatalogConflict("catalog product-index CAS retry limit reached")


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

  try:
    index, _ = await store.get(CATALOG_INDEX_KEY)
  except CatalogNotFound as error:
    raise CatalogNotReady("catalog product index is unavailable") from error
  product_ids = sorted({
      str(product_id)
      for product_id in index.get("product_ids", [])
      if str(product_id)
  })
  if len(product_ids) < int(snapshot.get("product_count", 0)):
    raise CatalogNotReady("catalog product index is incomplete")

  # The CAS-maintained index is the authoritative active-product set. Reading
  # every product record again made the request cost grow linearly with the
  # catalog even though recommendation selection only needs product IDs.
  available = [
      product_id
      for product_id in product_ids
      if product_id not in excluded
  ]
  count = min(limit, len(available))
  deterministic_seed = "\0".join((seed, model_revision, str(catalog_revision)))
  selected = random.Random(deterministic_seed).sample(available, count)
  return selected, catalog_revision


async def _backoff(attempt):
  exponent = min(attempt, 6)
  await asyncio.sleep((0.00025 * (2 ** exponent)) + (0.001 * random.random()))
