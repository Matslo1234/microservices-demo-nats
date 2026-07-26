#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import json
import tempfile
import unittest
from pathlib import Path

from nats.js.errors import ServiceUnavailableError

from nats_worker import (
    _State,
    _fetch_messages,
    _ready,
    _stop,
)


class StateTest(unittest.TestCase):
  def test_outcomes_are_journaled_and_restored(self):
    with tempfile.TemporaryDirectory() as directory:
      path = Path(directory) / "inbox.json"
      state = _State(path)
      outcome = {
          "subject": "boutique.evt.notification.order-confirmation-sent.v1",
          "message_id": "event-1",
          "data": "cGF5bG9hZA==",
      }
      self.assertEqual(outcome, state.record("source-1", outcome))
      self.assertEqual(outcome, state.record("source-1", {"unexpected": True}))
      second = {
          "subject": "subject",
          "message_id": "event-2",
          "data": "c2Vjb25k",
      }
      self.assertEqual(
          {"source-1": outcome, "source-2": second},
          state.record_many([
              ("source-1", {"unexpected": True}),
              ("source-2", second),
          ]))
      state.close()

      restored = _State(path)
      self.assertEqual(outcome, restored.get("source-1"))
      self.assertEqual(second, restored.get("source-2"))
      restored.close()

  def test_legacy_json_is_preserved_and_uses_sqlite_sidecar(self):
    with tempfile.TemporaryDirectory() as directory:
      path = Path(directory) / "inbox.json"
      outcome = {
          "subject": "subject",
          "message_id": "event-1",
          "data": "cGF5bG9hZA==",
      }
      legacy = json.dumps({"outcomes": {"source-1": outcome}}).encode()
      path.write_bytes(legacy)

      state = _State(path)
      self.assertEqual(Path(str(path) + ".sqlite3"), state.path)
      self.assertEqual(legacy, path.read_bytes())
      self.assertIsNone(state.get("source-1"))
      self.assertEqual(outcome, state.record("source-1", outcome))
      state.close()

      restored = _State(path)
      self.assertEqual(outcome, restored.get("source-1"))
      self.assertEqual(legacy, path.read_bytes())
      restored.close()

  def test_large_history_is_queried_without_an_in_memory_index(self):
    with tempfile.TemporaryDirectory() as directory:
      path = Path(directory) / "outcomes.sqlite3"
      state = _State(path)
      for index in range(10_000):
        state.record(
            f"source-{index}",
            {
              "subject": "subject",
              "message_id": f"event-{index}",
              "data": "cGF5bG9hZA==",
            })
      state.close()

      restored = _State(path)
      self.assertEqual(
          "event-9999", restored.get("source-9999")["message_id"])
      self.assertFalse(hasattr(restored, "outcomes"))
      restored.close()


class FetchRecoveryTest(unittest.IsolatedAsyncioTestCase):
  async def asyncSetUp(self):
    _stop.clear()
    _ready.set()

  async def asyncTearDown(self):
    _stop.clear()
    _ready.clear()

  async def test_transient_service_unavailable_does_not_stop_consumer(self):
    expected = [object()]

    class Subscription:
      def __init__(self):
        self.calls = 0

      async def fetch(self, **_):
        self.calls += 1
        if self.calls == 1:
          raise ServiceUnavailableError
        return expected

    subscription = Subscription()
    self.assertEqual(
        expected, await _fetch_messages(subscription, retry_delay=0))
    self.assertEqual(2, subscription.calls)
    self.assertTrue(_ready.is_set())


if __name__ == "__main__":
  unittest.main()
