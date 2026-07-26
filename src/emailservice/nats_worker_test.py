#!/usr/bin/python
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import json
import tempfile
import unittest
from pathlib import Path

from nats_worker import STATE_VERSION, _State


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
      self.assertEqual(outcome, _State(path).outcomes["source-1"])

  def test_legacy_state_is_migrated_and_torn_append_is_removed(self):
    with tempfile.TemporaryDirectory() as directory:
      path = Path(directory) / "inbox.json"
      outcome = {
          "subject": "subject",
          "message_id": "event-1",
          "data": "cGF5bG9hZA==",
      }
      path.write_text(json.dumps({"outcomes": {"source-1": outcome}}))

      migrated = _State(path)
      self.assertEqual(outcome, migrated.outcomes["source-1"])
      migrated_bytes = path.read_bytes()
      self.assertEqual(
          STATE_VERSION, json.loads(migrated_bytes)["version"])

      path.write_bytes(
          migrated_bytes + b'{"version":1,"message_id":')
      recovered = _State(path)
      self.assertEqual(outcome, recovered.outcomes["source-1"])
      self.assertEqual(migrated_bytes, path.read_bytes())

  def test_record_size_does_not_scale_with_history(self):
    with tempfile.TemporaryDirectory() as directory:
      path = Path(directory) / "inbox.json"
      outcomes = {
          f"source-{index}": {
              "subject": "subject",
              "message_id": f"event-{index}",
              "data": "cGF5bG9hZA==",
          }
          for index in range(1_000)
      }
      path.write_text(json.dumps({"outcomes": outcomes}))
      state = _State(path)
      before = path.stat().st_size
      state.record(
          "new-source",
          {
              "subject": "subject",
              "message_id": "new-event",
              "data": "cGF5bG9hZA==",
          },
      )
      self.assertLess(path.stat().st_size - before, 512)


if __name__ == "__main__":
  unittest.main()
