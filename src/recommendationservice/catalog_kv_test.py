#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import asyncio
import json
import unittest

from catalog_kv import (
    CatalogConflict,
    CatalogNotFound,
    apply_product,
    apply_snapshot,
    catalog_candidates,
    ensure_catalog_index,
)


class MemoryCatalog:

  def __init__(self):
    self._values = {}
    self._revision = 0
    self._lock = asyncio.Lock()
    self.keys_calls = 0

  async def get(self, key):
    async with self._lock:
      if key not in self._values:
        raise CatalogNotFound(key)
      value, revision = self._values[key]
      return json.loads(value), revision

  async def create(self, key, value):
    async with self._lock:
      if key in self._values:
        raise CatalogConflict(key)
      return self._store(key, value)

  async def update(self, key, value, revision):
    async with self._lock:
      if key not in self._values or self._values[key][1] != revision:
        raise CatalogConflict(key)
      return self._store(key, value)

  async def keys(self):
    async with self._lock:
      self.keys_calls += 1
      return sorted(self._values)

  def _store(self, key, value):
    self._revision += 1
    self._values[key] = (bytes(value), self._revision)
    return self._revision


def product(product_id, version, event_id, removed=False):
  return {
      "product_id": product_id,
      "product_version": version,
      "catalog_revision": 42,
      "removed": removed,
      "source_event_id": event_id,
      "source_version": version,
  }


class SharedCatalogTests(unittest.IsolatedAsyncioTestCase):

  async def test_startup_migration_seeds_index_with_one_key_scan(self):
    store = MemoryCatalog()
    await store.create(
        "product.a",
        json.dumps(product("a", 1, "event-a")).encode(),
    )
    await store.create(
        "product.b",
        json.dumps(product("b", 2, "event-b", removed=True)).encode(),
    )

    self.assertEqual("created", await ensure_catalog_index(store))
    index, _ = await store.get("catalog-products")
    self.assertEqual(["a"], index["product_ids"])
    self.assertEqual(1, store.keys_calls)

    self.assertEqual("existing", await ensure_catalog_index(store))
    self.assertEqual(1, store.keys_calls)

  async def test_concurrent_workers_converge_and_stale_event_is_noop(self):
    store = MemoryCatalog()
    await asyncio.gather(*(
        apply_product(store, product("sku", version, f"event-{version}"))
        for version in range(1, 31)
    ))
    current, revision = await store.get("product.sku")
    self.assertEqual(30, current["product_version"])

    outcome = await apply_product(store, product("sku", 5, "late-event"))
    after, after_revision = await store.get("product.sku")
    self.assertEqual("stale", outcome)
    self.assertEqual(revision, after_revision)
    self.assertEqual(current, after)

  async def test_three_replicas_read_equivalent_candidates(self):
    store = MemoryCatalog()
    await asyncio.gather(*(
        apply_product(store, product(product_id, index, f"event-{index}"))
        for index, product_id in enumerate(("a", "b", "c", "d", "e", "f"), 1)
    ))
    await apply_snapshot(store, {
        "catalog_revision": 42,
        "product_count": 6,
        "checksum": "checksum",
        "source_event_id": "snapshot-42",
        "source_version": 42,
    })

    results = await asyncio.gather(*(
        catalog_candidates(store, {"a"}, "page-view-1", "model-v1")
        for _ in range(3)
    ))

    self.assertEqual(results[0], results[1])
    self.assertEqual(results[1], results[2])
    self.assertEqual(42, results[0][1])
    self.assertNotIn("a", results[0][0])
    self.assertEqual(0, store.keys_calls)

  async def test_removed_product_is_removed_from_shared_index(self):
    store = MemoryCatalog()
    await apply_product(store, product("sku", 1, "event-1"))
    await apply_product(store, product("sku", 2, "event-2", removed=True))
    await apply_snapshot(store, {
        "catalog_revision": 42,
        "product_count": 0,
        "checksum": "checksum",
        "source_event_id": "snapshot-42",
        "source_version": 42,
    })

    candidates, _ = await catalog_candidates(
        store, set(), "page-view-1", "model-v1"
    )

    self.assertEqual([], candidates)
    self.assertEqual(0, store.keys_calls)


if __name__ == "__main__":
  unittest.main()
