# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from kubernetes_jobs import JobNotFound, KubernetesJobClient, job_name


class RecordingJobClient(KubernetesJobClient):
    def __init__(self):
        self.namespace = "default"
        self.image = "registry.example/benchmark@sha256:abc"
        self.requests = []

    def _request(self, method, path, body=None):
        self.requests.append((method, path, body))
        return {}


class AmbiguousCreateClient(RecordingJobClient):
    def __init__(self, job_exists):
        super().__init__()
        self.job_exists = job_exists

    def _request(self, method, path, body=None):
        self.requests.append((method, path, body))
        if method == "POST":
            raise TimeoutError("response was lost")
        if self.job_exists:
            return {"status": {"active": 1}}
        raise JobNotFound(path)


class FixedStatusClient(RecordingJobClient):
    def __init__(self, status):
        super().__init__()
        self.status = status

    def _request(self, method, path, body=None):
        self.requests.append((method, path, body))
        return {"status": self.status}


class KubernetesJobClientTest(unittest.TestCase):
    def test_job_uses_controller_image_and_disposable_staging(self):
        client = RecordingJobClient()

        name = client.create("20260727T120000Z-a1b2c3d4", 600)

        self.assertEqual(
            "benchmark-20260727t120000za1b2c3d4", name
        )
        method, path, job = client.requests[0]
        self.assertEqual("POST", method)
        self.assertEqual(
            "/apis/batch/v1/namespaces/default/jobs", path
        )
        self.assertEqual(600, job["spec"]["activeDeadlineSeconds"])
        pod = job["spec"]["template"]["spec"]
        self.assertEqual("benchmark-runner", pod["serviceAccountName"])
        self.assertEqual(
            "registry.example/benchmark@sha256:abc",
            pod["containers"][0]["image"],
        )
        self.assertEqual(["python", "job.py"], pod["containers"][0]["command"])
        self.assertNotIn(
            "BENCHMARK_WORKER_INDEX",
            [
                item["name"]
                for item in pod["containers"][0]["env"]
            ],
        )
        self.assertEqual("Never", pod["restartPolicy"])
        self.assertEqual(0, job["spec"]["backoffLimit"])
        self.assertEqual(
            ["nats-ca", "work", "tmp"],
            [volume["name"] for volume in pod["volumes"]],
        )

    def test_job_name_is_dns_safe_and_bounded(self):
        name = job_name("20260727T120000Z-ABCDEF12")
        self.assertLessEqual(len(name), 63)
        self.assertRegex(name, r"^[a-z0-9-]+$")

    def test_parallel_run_uses_indexed_workers(self):
        client = RecordingJobClient()

        client.create("20260727T120000Z-a1b2c3d4", 600, 3)

        job = client.requests[0][2]
        self.assertEqual("Indexed", job["spec"]["completionMode"])
        self.assertEqual(3, job["spec"]["completions"])
        self.assertEqual(3, job["spec"]["parallelism"])
        environment = {
            item["name"]: item
            for item in job["spec"]["template"]["spec"]["containers"][0][
                "env"
            ]
        }
        self.assertEqual("3", environment["BENCHMARK_WORKER_COUNT"]["value"])
        self.assertEqual(
            "metadata.annotations['batch.kubernetes.io/job-completion-index']",
            environment["BENCHMARK_WORKER_INDEX"]["valueFrom"]["fieldRef"][
                "fieldPath"
            ],
        )

    def test_ambiguous_create_reuses_job_committed_by_api_server(self):
        client = AmbiguousCreateClient(job_exists=True)

        name = client.create("20260727T120000Z-a1b2c3d4", 600)

        self.assertEqual("benchmark-20260727t120000za1b2c3d4", name)
        self.assertEqual(["POST", "GET"], [
            request[0] for request in client.requests
        ])

    def test_active_indexed_job_is_running_despite_failed_worker(self):
        client = FixedStatusClient({"active": 2, "failed": 1})

        self.assertEqual("running", client.state("benchmark-run"))

    def test_create_failure_is_preserved_when_job_does_not_exist(self):
        client = AmbiguousCreateClient(job_exists=False)

        with self.assertRaisesRegex(TimeoutError, "response was lost"):
            client.create("20260727T120000Z-a1b2c3d4", 600)


if __name__ == "__main__":
    unittest.main()
