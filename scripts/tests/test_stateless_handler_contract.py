#!/usr/bin/env python3
"""Contract tests for the stateless handler/result-journal protocol."""

from __future__ import annotations

import base64
import hashlib
import struct
import unittest

from scripts.testing.stateless_handler_contract import (
    InMemoryResultJournal,
    PublishTimeout,
    RecordingPublisher,
    ResultJournalHandler,
    StoredResult,
    concurrently,
)


def result_message_id(input_message_id: str, slot: str) -> str:
    encoded_input = input_message_id.encode("utf-8")
    encoded_slot = slot.encode("utf-8")
    digest = hashlib.sha256(
        b"boutique.result.v1\0"
        + struct.pack(">I", len(encoded_input))
        + encoded_input
        + struct.pack(">I", len(encoded_slot))
        + encoded_slot
    ).digest()
    return "br1_" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def result_factory(input_message_id: str, aggregate_version: int) -> tuple[StoredResult, ...]:
    slot = "test.transition"
    return (
        StoredResult(
            message_id=result_message_id(input_message_id, slot),
            slot=slot,
            payload=f"{input_message_id}:{aggregate_version}".encode(),
        ),
    )


class StatelessHandlerContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.journal = InMemoryResultJournal()
        self.publisher = RecordingPublisher()
        self.handler = ResultJournalHandler(
            self.journal, self.publisher, result_factory
        )

    def test_duplicate_input_commits_once_and_republishes_stored_result(self) -> None:
        failures = concurrently(4, lambda: self.handler.handle("input-duplicate"))

        self.assertEqual([], failures)
        self.assertEqual(1, self.journal.transition_count)
        self.assertEqual(4, len(self.publisher.attempts))
        self.assertEqual(1, len(self.publisher.accepted))
        self.assertEqual(
            {self.publisher.attempts[0]}, set(self.publisher.attempts)
        )

    def test_publish_timeout_reuses_the_stored_result(self) -> None:
        self.publisher.timeouts_remaining = 1

        with self.assertRaises(PublishTimeout):
            self.handler.handle("input-timeout")
        stored = self.journal.load("input-timeout")
        self.assertIsNotNone(stored)

        retried = self.handler.handle("input-timeout")

        self.assertEqual(stored, retried)
        self.assertEqual(1, self.journal.transition_count)
        self.assertEqual(2, len(self.publisher.attempts))
        self.assertEqual(
            self.publisher.attempts[0], self.publisher.attempts[1]
        )
        self.assertEqual(1, len(self.publisher.accepted))

    def test_state_conflict_is_bounded_and_rebuilds_for_new_version(self) -> None:
        self.journal.conflicts_remaining = 2

        commit = self.handler.handle("input-conflict")

        self.assertEqual(3, commit.aggregate_version)
        self.assertEqual(b"input-conflict:3", commit.results[0].payload)
        self.assertEqual(1, self.journal.transition_count)

    def test_restart_after_commit_loads_and_publishes_exact_bytes(self) -> None:
        committed = self.handler.commit_input("input-restart")
        replacement = ResultJournalHandler(
            self.journal, self.publisher, result_factory
        )

        recovered = replacement.handle("input-restart")

        self.assertEqual(committed, recovered)
        self.assertEqual(1, self.journal.transition_count)
        self.assertEqual(list(committed.results), self.publisher.attempts)


if __name__ == "__main__":
    unittest.main()
