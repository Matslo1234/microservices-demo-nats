# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import copy
import io
import threading
import unittest
import zipfile

from config import ConfigError
from control import BenchmarkManager, RunConflict
from kubernetes_jobs import JobNotFound
from shared_store import RecordNotFound, RevisionConflict, StoredRecord


class MemoryStore:
    def __init__(self) -> None:
        self.records = {}
        self.objects = {}
        self.revision = 0
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            if key not in self.records:
                raise RecordNotFound(key)
            value, revision = self.records[key]
            return StoredRecord(copy.deepcopy(value), revision)

    def create(self, key, value):
        with self.lock:
            if key in self.records:
                raise RevisionConflict(key)
            self.revision += 1
            self.records[key] = (copy.deepcopy(value), self.revision)
            return self.revision

    def update(self, key, value, revision):
        with self.lock:
            if key not in self.records or self.records[key][1] != revision:
                raise RevisionConflict(key)
            self.revision += 1
            self.records[key] = (copy.deepcopy(value), self.revision)
            return self.revision

    def keys(self, prefix):
        with self.lock:
            return sorted(key for key in self.records if key.startswith(prefix))

    def get_object(self, name):
        with self.lock:
            if name not in self.objects:
                raise RecordNotFound(name)
            return self.objects[name]

    def put_object(self, name, data):
        with self.lock:
            self.objects[name] = bytes(data)

    def delete_object(self, name):
        with self.lock:
            self.objects.pop(name, None)

    def ready(self):
        return True

    def close(self):
        pass


class FakeJobs:
    def __init__(self) -> None:
        self.states = {}
        self.deleted = []
        self.worker_counts = []

    def create(self, run_id, maximum_seconds, worker_count=1):
        name = f"job-{run_id}"
        self.states[name] = "pending"
        self.worker_counts.append(worker_count)
        return name

    def state(self, name):
        if name not in self.states:
            raise JobNotFound(name)
        return self.states[name]

    def delete(self, name):
        self.deleted.append(name)
        self.states.pop(name, None)


class BenchmarkControlTest(unittest.TestCase):
    def manager(self, store, jobs, clock=lambda: 1000.0):
        return BenchmarkManager(store, jobs, "NATS", clock)

    @staticmethod
    def request(**values):
        return {
            "target_url": "https://shop.example",
            "metrics_url": "https://metrics.example/snapshot",
            **values,
        }

    def test_concurrent_api_replicas_submit_only_one_job(self):
        store, jobs = MemoryStore(), FakeJobs()
        managers = [self.manager(store, jobs), self.manager(store, jobs)]
        barrier = threading.Barrier(2)
        results, errors = [], []

        def submit(manager):
            barrier.wait()
            try:
                results.append(manager.start(self.request()))
            except RunConflict as error:
                errors.append(error)

        threads = [
            threading.Thread(target=submit, args=(manager,))
            for manager in managers
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(1, len(results))
        self.assertEqual(1, len(errors))
        self.assertEqual(1, len(jobs.states))

    def test_another_replica_observes_and_stops_run(self):
        store, jobs = MemoryStore(), FakeJobs()
        first, second = self.manager(store, jobs), self.manager(store, jobs)
        started = first.start(self.request())
        run_id = started["status"]["run_id"]

        self.assertEqual("submitted", second.details(run_id)["status"]["state"])
        stopped = second.stop(run_id)

        self.assertEqual("stopped", stopped["status"]["state"])
        self.assertEqual(1, len(jobs.deleted))
        third = second.start(self.request())
        self.assertNotEqual(run_id, third["status"]["run_id"])

    def test_parallel_worker_count_is_given_to_kubernetes_job(self):
        store, jobs = MemoryStore(), FakeJobs()
        manager = self.manager(store, jobs)

        result = manager.start(
            self.request(workload="open", arrival_rate=250)
        )

        self.assertEqual([3], jobs.worker_counts)
        self.assertEqual(3, result["status"]["worker_count"])

    def test_fault_tolerance_requires_the_external_controller(self):
        store, jobs = MemoryStore(), FakeJobs()

        with self.assertRaisesRegex(ConfigError, "fault_tolerance.py"):
            self.manager(store, jobs).start(
                self.request(workload="fault_tolerance")
            )

        self.assertEqual([], jobs.worker_counts)

    def test_expired_lease_is_recovered_by_another_replica(self):
        now = [1000.0]
        store, jobs = MemoryStore(), FakeJobs()
        first = self.manager(store, jobs, lambda: now[0])
        original = first.start(self.request())
        now[0] += 100_000

        replacement = self.manager(store, jobs, lambda: now[0]).start(
            self.request()
        )

        self.assertNotEqual(
            original["status"]["run_id"],
            replacement["status"]["run_id"],
        )

    def test_artifact_is_read_from_shared_object_store(self):
        store, jobs = MemoryStore(), FakeJobs()
        manager = self.manager(store, jobs)
        run_id = manager.start(self.request())["status"]["run_id"]
        record = store.get("run." + run_id)
        value = record.value
        value["artifacts"] = {
            "summary.json": {
                "object": f"{run_id}/summary.json",
                "content_type": "application/json",
            }
        }
        store.update("run." + run_id, value, record.revision)
        store.put_object(f"{run_id}/summary.json", b'{"ok":true}')

        self.assertEqual(b'{"ok":true}', manager.artifact(run_id, "summary.json"))
        self.assertTrue(manager.list_runs()[0]["summary_available"])

    def test_combined_artifacts_are_flat_and_prefixed_by_run(self):
        store, jobs = MemoryStore(), FakeJobs()
        manager = self.manager(store, jobs)
        run_ids = []
        for number in range(2):
            run_id = manager.start(self.request())["status"]["run_id"]
            run_ids.append(run_id)
            record = store.get("run." + run_id)
            value = record.value
            archive = io.BytesIO()
            with zipfile.ZipFile(archive, "w") as target:
                target.writestr("summary.json", f'{{"run":{number}}}')
                target.writestr("business.csv", f"run\n{number}\n")
            object_name = f"{run_id}/artifacts.zip"
            value["artifacts"] = {
                "artifacts.zip": {"object": object_name}
            }
            store.update("run." + run_id, value, record.revision)
            store.put_object(object_name, archive.getvalue())
            manager._release_lease(run_id)

        self.assertTrue(
            all(
                run["artifacts_available"]
                for run in manager.list_runs()
            )
        )
        with zipfile.ZipFile(
            io.BytesIO(manager.combined_artifacts())
        ) as combined:
            self.assertEqual(
                {
                    f"{run_ids[0]}-summary.json",
                    f"{run_ids[0]}-business.csv",
                    f"{run_ids[1]}-summary.json",
                    f"{run_ids[1]}-business.csv",
                },
                set(combined.namelist()),
            )
            self.assertEqual(
                b'{"run":1}',
                combined.read(f"{run_ids[1]}-summary.json"),
            )
            self.assertFalse(
                any("/" in name for name in combined.namelist())
            )

    def test_combined_summaries_contains_only_selected_json_files(self):
        store, jobs = MemoryStore(), FakeJobs()
        manager = self.manager(store, jobs)
        run_ids = []
        for number in range(2):
            run_id = manager.start(self.request())["status"]["run_id"]
            run_ids.append(run_id)
            record = store.get("run." + run_id)
            value = record.value
            object_name = f"{run_id}/summary.json"
            value["artifacts"] = {
                "summary.json": {"object": object_name}
            }
            store.update("run." + run_id, value, record.revision)
            store.put_object(object_name, f'{{"run":{number}}}'.encode())
            manager._release_lease(run_id)

        archive = manager.combined_summaries(
            [run_ids[1], run_ids[0], run_ids[1]]
        )

        with zipfile.ZipFile(io.BytesIO(archive)) as combined:
            self.assertEqual(
                {
                    f"{run_ids[0]}-summary.json",
                    f"{run_ids[1]}-summary.json",
                },
                set(combined.namelist()),
            )
            self.assertEqual(
                b'{"run":1}',
                combined.read(f"{run_ids[1]}-summary.json"),
            )


if __name__ == "__main__":
    unittest.main()
