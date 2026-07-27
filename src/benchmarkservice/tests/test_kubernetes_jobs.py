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

    def test_ambiguous_create_reuses_job_committed_by_api_server(self):
        client = AmbiguousCreateClient(job_exists=True)

        name = client.create("20260727T120000Z-a1b2c3d4", 600)

        self.assertEqual("benchmark-20260727t120000za1b2c3d4", name)
        self.assertEqual(["POST", "GET"], [
            request[0] for request in client.requests
        ])

    def test_create_failure_is_preserved_when_job_does_not_exist(self):
        client = AmbiguousCreateClient(job_exists=False)

        with self.assertRaisesRegex(TimeoutError, "response was lost"):
            client.create("20260727T120000Z-a1b2c3d4", 600)


if __name__ == "__main__":
    unittest.main()
