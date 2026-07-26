#!/usr/bin/env python3
"""Reusable fault-injection model for result-journal handler contract tests."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Barrier, Lock, Thread
from typing import Callable


class StateConflict(RuntimeError):
    """The aggregate changed before the attempted atomic commit."""


class PublishTimeout(RuntimeError):
    """The result publish acknowledgement was ambiguous or timed out."""


@dataclass(frozen=True)
class StoredResult:
    message_id: str
    slot: str
    payload: bytes


@dataclass(frozen=True)
class Commit:
    input_message_id: str
    aggregate_version: int
    results: tuple[StoredResult, ...]


class InMemoryResultJournal:
    """Reference backend implementing atomic state/inbox/result persistence."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._version = 0
        self._commits: dict[str, Commit] = {}
        self.conflicts_remaining = 0

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    @property
    def transition_count(self) -> int:
        with self._lock:
            return len(self._commits)

    def load(self, input_message_id: str) -> Commit | None:
        with self._lock:
            return self._commits.get(input_message_id)

    def commit(
        self,
        input_message_id: str,
        expected_version: int,
        build_results: Callable[[int], tuple[StoredResult, ...]],
    ) -> Commit:
        with self._lock:
            duplicate = self._commits.get(input_message_id)
            if duplicate is not None:
                return duplicate
            if self.conflicts_remaining:
                self.conflicts_remaining -= 1
                self._version += 1
                raise StateConflict("injected optimistic-write conflict")
            if self._version != expected_version:
                raise StateConflict(
                    f"expected aggregate version {expected_version}, got {self._version}"
                )
            next_version = self._version + 1
            commit = Commit(
                input_message_id=input_message_id,
                aggregate_version=next_version,
                results=build_results(next_version),
            )
            self._version = next_version
            self._commits[input_message_id] = commit
            return commit


class RecordingPublisher:
    """Publisher whose accepted identities model JetStream de-duplication."""

    def __init__(self) -> None:
        self._lock = Lock()
        self.attempts: list[StoredResult] = []
        self.accepted: dict[str, StoredResult] = {}
        self.timeouts_remaining = 0

    def publish(self, result: StoredResult) -> None:
        with self._lock:
            self.attempts.append(result)
            if self.timeouts_remaining:
                self.timeouts_remaining -= 1
                # The server may have accepted this result even though the
                # acknowledgement was lost.
                self.accepted.setdefault(result.message_id, result)
                raise PublishTimeout("injected ambiguous publish acknowledgement")
            self.accepted.setdefault(result.message_id, result)


class ResultJournalHandler:
    """Reference orchestration shared by the four required failure scenarios."""

    def __init__(
        self,
        journal: InMemoryResultJournal,
        publisher: RecordingPublisher,
        result_factory: Callable[[str, int], tuple[StoredResult, ...]],
        *,
        max_conflict_retries: int = 4,
    ) -> None:
        self.journal = journal
        self.publisher = publisher
        self.result_factory = result_factory
        self.max_conflict_retries = max_conflict_retries

    def commit_input(self, input_message_id: str) -> Commit:
        duplicate = self.journal.load(input_message_id)
        if duplicate is not None:
            return duplicate
        for attempt in range(self.max_conflict_retries + 1):
            expected_version = self.journal.version
            try:
                return self.journal.commit(
                    input_message_id,
                    expected_version,
                    lambda version: self.result_factory(input_message_id, version),
                )
            except StateConflict:
                if attempt == self.max_conflict_retries:
                    raise
        raise AssertionError("unreachable conflict retry path")

    def publish_commit(self, commit: Commit) -> None:
        for result in commit.results:
            self.publisher.publish(result)

    def handle(self, input_message_id: str) -> Commit:
        commit = self.commit_input(input_message_id)
        self.publish_commit(commit)
        return commit


def concurrently(count: int, action: Callable[[], None]) -> list[BaseException]:
    """Run the same handler action concurrently and return raised exceptions."""

    barrier = Barrier(count)
    failures: list[BaseException] = []
    failures_lock = Lock()

    def run() -> None:
        barrier.wait()
        try:
            action()
        except BaseException as error:  # Tests need to preserve assertion failures.
            with failures_lock:
                failures.append(error)

    threads = [Thread(target=run) for _ in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return failures
