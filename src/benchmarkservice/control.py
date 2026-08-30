# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import copy
import io
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Callable

from config import BenchmarkConfig, ConfigError
from kubernetes_jobs import JobNotFound
from shared_store import (
    RecordNotFound,
    RevisionConflict,
    StoredRecord,
)


ACTIVE_STATES = {"submitted", "starting", "running", "stopping"}
TERMINAL_STATES = {"completed", "failed", "stopped", "interrupted"}


SUMMARY_OVERVIEW_FIELDS = (
    "application_type",
    "workload",
    "worker_count",
    "users",
    "arrival_rate",
    "warmup_seconds",
    "steady_seconds",
    "configured_steady_seconds",
    "drain_seconds",
    "business",
    "capacity",
)


def summary_overview(summary: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return the bounded run summary kept in the JetStream KV record.

    Detailed resource, NATS, fault-tolerance, saturation, and per-second data
    remains available in the summary.json Object Store artifact. Keeping those
    growing sections out of KV prevents long runs from exceeding max_payload.
    """
    if summary is None:
        return None
    return {
        field: copy.deepcopy(summary[field])
        for field in SUMMARY_OVERVIEW_FIELDS
        if field in summary
    }


RUN_PREFIX = "run."
LEASE_KEY = "lease.active"
SCHEMA_VERSION = 1


class RunConflict(RuntimeError):
    pass


class RunNotFound(KeyError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_run_id() -> str:
    return (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )


def run_key(run_id: str) -> str:
    return RUN_PREFIX + run_id


class BenchmarkManager:
    def __init__(
        self,
        store: Any,
        jobs: Any,
        application_type: str,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.store = store
        self.jobs = jobs
        self.application_type = application_type.strip().upper()
        if self.application_type not in {"GRPC", "NATS"}:
            raise ConfigError("APPLICATION_TYPE must be GRPC or NATS")
        self.clock = clock

    def _acquire_lease(self, run_id: str, duration: int) -> None:
        for _ in range(12):
            now = self.clock()
            lease = {
                "schema_version": SCHEMA_VERSION,
                "run_id": run_id,
                "owner": f"controller:{uuid.uuid4()}",
                "lease_until": now + duration,
                "updated_at": utc_now(),
            }
            try:
                current = self.store.get(LEASE_KEY)
            except RecordNotFound:
                try:
                    self.store.create(LEASE_KEY, lease)
                    return
                except RevisionConflict:
                    continue
            existing = current.value
            if float(existing.get("lease_until", 0)) > now:
                raise RunConflict(
                    f"run {existing.get('run_id', 'unknown')} is still active"
                )
            try:
                self.store.update(LEASE_KEY, lease, current.revision)
                return
            except RevisionConflict:
                continue
        raise RunConflict("active benchmark lease is contended")

    def _release_lease(self, run_id: str) -> None:
        for _ in range(12):
            try:
                current = self.store.get(LEASE_KEY)
            except RecordNotFound:
                return
            if current.value.get("run_id") != run_id:
                return
            released = copy.deepcopy(current.value)
            released["lease_until"] = 0
            released["updated_at"] = utc_now()
            released["released"] = True
            try:
                self.store.update(LEASE_KEY, released, current.revision)
                return
            except RevisionConflict:
                continue

    def _get(self, run_id: str) -> StoredRecord:
        if not run_id or len(run_id) > 64:
            raise RunNotFound(run_id)
        try:
            record = self.store.get(run_key(run_id))
        except RecordNotFound as error:
            raise RunNotFound(run_id) from error
        if record.value.get("run_id") != run_id:
            raise RunNotFound(run_id)
        return record

    def mutate(
        self,
        run_id: str,
        change: Callable[[dict[str, Any]], None],
    ) -> dict[str, Any]:
        for _ in range(12):
            current = self._get(run_id)
            updated = copy.deepcopy(current.value)
            change(updated)
            updated["updated_at"] = utc_now()
            try:
                self.store.update(
                    run_key(run_id), updated, current.revision
                )
                return updated
            except RevisionConflict:
                continue
        raise RunConflict(f"run {run_id} is being updated concurrently")

    def start(self, values: dict[str, Any]) -> dict[str, Any]:
        if str(values.get("workload", "")).strip().lower() == (
            "fault_tolerance"
        ):
            raise ConfigError(
                "fault_tolerance must be run with fault_tolerance.py so "
                "faults are injected into the target cluster"
            )
        config = BenchmarkConfig.from_request(
            values, self.application_type
        )
        run_id = new_run_id()
        maximum_seconds = config.run_seconds + 300
        self._acquire_lease(run_id, maximum_seconds)
        record = {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "config": config.as_dict(),
            "status": {
                "run_id": run_id,
                "state": "submitted",
                "created_at": utc_now(),
                "application_type": config.application_type,
                "target_url": config.target_url,
                "workload": config.workload,
                "lease_until": self.clock() + maximum_seconds,
            },
            "summary": None,
            "artifacts": {},
            "updated_at": utc_now(),
        }
        try:
            self.store.create(run_key(run_id), record)
            name = self.jobs.create(
                run_id, maximum_seconds, config.worker_count
            )
            record = self.mutate(
                run_id,
                lambda value: value["status"].update(
                    {
                        "job_name": name,
                        "worker_count": config.worker_count,
                    }
                ),
            )
        except Exception as error:
            try:
                self.mutate(
                    run_id,
                    lambda value: value["status"].update(
                        {
                            "state": "failed",
                            "ended_at": utc_now(),
                            "message": f"could not submit benchmark Job: {error}",
                        }
                    ),
                )
            except (RunNotFound, RunConflict):
                pass
            self._release_lease(run_id)
            raise
        return self._public(record)

    def _reconcile(self, record: dict[str, Any]) -> dict[str, Any]:
        status = record["status"]
        state = str(status.get("state", "unknown"))
        name = status.get("job_name")
        if state not in ACTIVE_STATES or not name:
            return record
        try:
            job_state = self.jobs.state(str(name))
        except JobNotFound:
            if float(status.get("lease_until", 0)) > self.clock():
                return record
            terminal = "stopped" if status.get("stop_requested") else "interrupted"
            message = "benchmark Job disappeared before finalization"
        except Exception:
            return record
        else:
            if job_state != "failed":
                return record
            terminal = "failed"
            message = "benchmark Job failed before publishing a terminal status"

        run_id = str(record["run_id"])
        record = self.mutate(
            run_id,
            lambda value: value["status"].update(
                {
                    "state": terminal,
                    "ended_at": utc_now(),
                    "message": message,
                }
            ),
        )
        self._release_lease(run_id)
        return record

    @staticmethod
    def _public(record: dict[str, Any]) -> dict[str, Any]:
        result = {
            "status": copy.deepcopy(record["status"]),
            "config": copy.deepcopy(record["config"]),
        }
        if record.get("summary") is not None:
            result["summary"] = copy.deepcopy(record["summary"])
        return result

    def details(self, run_id: str) -> dict[str, Any]:
        record = self._get(run_id).value
        record = self._reconcile(record)
        return self._public(record)

    def list_runs(self) -> list[dict[str, Any]]:
        runs: list[dict[str, Any]] = []
        for key in reversed(self.store.keys(RUN_PREFIX)):
            try:
                record = self.store.get(key).value
                record = self._reconcile(record)
                status = copy.deepcopy(record["status"])
            except (RecordNotFound, RunNotFound):
                continue
            summary = record.get("summary") or {}
            business = summary.get("business", {})
            status["completed_orders"] = business.get("completed")
            status["p95_ms"] = business.get(
                "checkout_to_outcome", {}
            ).get("p95_ms")
            summary_artifact = record.get("artifacts", {}).get(
                "summary.json"
            )
            status["summary_available"] = bool(
                isinstance(summary_artifact, dict)
                and summary_artifact.get("object")
            )
            archive = record.get("artifacts", {}).get("artifacts.zip")
            status["artifacts_available"] = bool(
                isinstance(archive, dict) and archive.get("object")
            )
            runs.append(status)
        return runs

    def stop(self, run_id: str) -> dict[str, Any]:
        current = self._get(run_id).value
        state = current["status"].get("state")
        if state not in ACTIVE_STATES:
            raise RunConflict("the requested run is not active")
        name = current["status"].get("job_name")

        self.mutate(
            run_id,
            lambda value: value["status"].update(
                {
                    "state": "stopping",
                    "stop_requested": True,
                    "stop_requested_at": utc_now(),
                }
            ),
        )
        if name:
            self.jobs.delete(str(name))
        record = self.mutate(
            run_id,
            lambda value: value["status"].update(
                {"state": "stopped", "ended_at": utc_now()}
            ),
        )
        self._release_lease(run_id)
        return self._public(record)

    def artifact(self, run_id: str, artifact: str) -> bytes:
        record = self._get(run_id).value
        metadata = record.get("artifacts", {}).get(artifact)
        if not isinstance(metadata, dict) or not metadata.get("object"):
            raise RunNotFound(f"{run_id}/{artifact}")
        try:
            return self.store.get_object(str(metadata["object"]))
        except RecordNotFound as error:
            raise RunNotFound(f"{run_id}/{artifact}") from error

    def combined_artifacts(self) -> bytes:
        output = io.BytesIO()
        archive_count = 0
        with zipfile.ZipFile(
            output, "w", compression=zipfile.ZIP_DEFLATED
        ) as combined:
            for key in self.store.keys(RUN_PREFIX):
                try:
                    record = self.store.get(key).value
                except RecordNotFound:
                    continue
                run_id = str(record.get("run_id", ""))
                if (
                    not run_id
                    or run_id in {".", ".."}
                    or "/" in run_id
                    or "\\" in run_id
                ):
                    continue
                metadata = record.get("artifacts", {}).get("artifacts.zip")
                if not isinstance(metadata, dict) or not metadata.get("object"):
                    continue
                try:
                    archive_data = self.store.get_object(
                        str(metadata["object"])
                    )
                except RecordNotFound as error:
                    raise RunNotFound(f"{run_id}/artifacts.zip") from error
                try:
                    with zipfile.ZipFile(io.BytesIO(archive_data)) as source:
                        for member in source.infolist():
                            if member.is_dir():
                                continue
                            path = PurePosixPath(
                                member.filename.replace("\\", "/")
                            )
                            if (
                                path.is_absolute()
                                or not path.parts
                                or ".." in path.parts
                            ):
                                raise ValueError(
                                    f"unsafe artifact path in run {run_id}"
                                )
                            artifact_name = "-".join(path.parts)
                            combined.writestr(
                                f"{run_id}-{artifact_name}",
                                source.read(member),
                            )
                except zipfile.BadZipFile as error:
                    raise ValueError(
                        f"invalid artifact archive for run {run_id}"
                    ) from error
                archive_count += 1
        if archive_count == 0:
            raise RunNotFound("artifacts.zip")
        return output.getvalue()

    def combined_summaries(self, run_ids: list[str]) -> bytes:
        output = io.BytesIO()
        included: set[str] = set()
        with zipfile.ZipFile(
            output, "w", compression=zipfile.ZIP_DEFLATED
        ) as combined:
            for run_id in run_ids:
                if (
                    not isinstance(run_id, str)
                    or not run_id
                    or len(run_id) > 64
                    or run_id in {".", ".."}
                    or "/" in run_id
                    or "\\" in run_id
                ):
                    raise RunNotFound(str(run_id))
                if run_id in included:
                    continue
                included.add(run_id)
                combined.writestr(
                    f"{run_id}-summary.json",
                    self.artifact(run_id, "summary.json"),
                )
        if not included:
            raise RunNotFound("summaries.zip")
        return output.getvalue()

    def ready(self) -> bool:
        return bool(self.store.ready())

    def shutdown(self) -> None:
        self.store.close()
