# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import json
import os
import ssl
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


class JobNotFound(KeyError):
    pass


def job_name(run_id: str) -> str:
    compact = "".join(character.lower() for character in run_id if character.isalnum())
    return f"benchmark-{compact[-40:]}"


class KubernetesJobClient:
    def __init__(self) -> None:
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not host:
            raise RuntimeError("Kubernetes service environment is unavailable")
        self.namespace = os.environ.get("POD_NAMESPACE", "default")
        self.base_url = f"https://{host}:{port}"
        token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        with open(token_path, encoding="utf-8") as source:
            self.token = source.read().strip()
        self.context = ssl.create_default_context(cafile=ca_path)
        # Resolve the controller image only when a run is submitted. A
        # transient Kubernetes API outage must not terminate the HTTP API or
        # make liveness depend on the control plane.
        self.image = os.environ.get("BENCHMARK_JOB_IMAGE")

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        data = None if body is None else json.dumps(body).encode()
        request = Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urlopen(
                request, timeout=10, context=self.context
            ) as response:
                if response.status == 204:
                    return None
                value = json.load(response)
        except HTTPError as error:
            if error.code == 404:
                raise JobNotFound(path) from error
            detail = error.read().decode(errors="replace")
            raise RuntimeError(
                f"Kubernetes API {method} {path} returned "
                f"{error.code}: {detail}"
            ) from error
        if not isinstance(value, dict):
            raise RuntimeError(
                f"Kubernetes API {method} {path} returned non-object JSON"
            )
        return value

    @property
    def collection_path(self) -> str:
        return (
            f"/apis/batch/v1/namespaces/{quote(self.namespace)}/jobs"
        )

    def _own_image(self) -> str:
        pod_name = os.environ.get("POD_NAME")
        if not pod_name:
            raise RuntimeError(
                "POD_NAME or BENCHMARK_JOB_IMAGE is required"
            )
        value = self._request(
            "GET",
            f"/api/v1/namespaces/{quote(self.namespace)}/pods/"
            f"{quote(pod_name)}",
        )
        assert value is not None
        for container in value.get("spec", {}).get("containers", []):
            if container.get("name") == "server" and container.get("image"):
                return str(container["image"])
        raise RuntimeError("benchmark API pod has no server image")

    def create(
        self, run_id: str, maximum_seconds: int, worker_count: int = 1
    ) -> str:
        if worker_count < 1:
            raise ValueError("worker_count must be at least one")
        name = job_name(run_id)
        if not self.image:
            self.image = self._own_image()
        labels = {
            "app": "benchmark-runner",
            "app.kubernetes.io/component": "load-generator",
            "app.kubernetes.io/part-of": "online-boutique",
            "benchmark.run-id": run_id,
        }
        job = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": name, "labels": labels},
            "spec": {
                "activeDeadlineSeconds": maximum_seconds,
                "backoffLimit": 0,
                "ttlSecondsAfterFinished": 86400,
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "serviceAccountName": "benchmark-runner",
                        "restartPolicy": "Never",
                        "terminationGracePeriodSeconds": 90,
                        "securityContext": {
                            "fsGroup": 1000,
                            "runAsGroup": 1000,
                            "runAsNonRoot": True,
                            "runAsUser": 1000,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "containers": [
                            {
                                "name": "runner",
                                "image": self.image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": ["python", "job.py"],
                                "env": [
                                    {"name": "BENCHMARK_RUN_ID", "value": run_id},
                                    {
                                        "name": "BENCHMARK_WORKER_COUNT",
                                        "value": str(worker_count),
                                    },
                                    {
                                        "name": "POD_NAMESPACE",
                                        "valueFrom": {
                                            "fieldRef": {
                                                "fieldPath": "metadata.namespace"
                                            }
                                        },
                                    },
                                    {
                                        "name": "POD_NAME",
                                        "valueFrom": {
                                            "fieldRef": {
                                                "fieldPath": "metadata.name"
                                            }
                                        },
                                    },
                                ],
                                "envFrom": [
                                    {
                                        "configMapRef": {
                                            "name": "nats-client-config"
                                        }
                                    },
                                    {
                                        "secretRef": {
                                            "name": (
                                                "nats-credentials-benchmarkservice"
                                            )
                                        }
                                    },
                                ],
                                "resources": {
                                    "requests": {
                                        "cpu": "500m",
                                        "memory": "256Mi",
                                    },
                                    "limits": {
                                        "cpu": "2",
                                        "memory": "2Gi",
                                    },
                                },
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "capabilities": {"drop": ["ALL"]},
                                    "readOnlyRootFilesystem": True,
                                },
                                "volumeMounts": [
                                    {
                                        "name": "nats-ca",
                                        "mountPath": "/etc/nats-ca",
                                        "readOnly": True,
                                    },
                                    {
                                        "name": "work",
                                        "mountPath": "/work",
                                    },
                                    {
                                        "name": "tmp",
                                        "mountPath": "/tmp",
                                    },
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "nats-ca",
                                "configMap": {"name": "nats-ca"},
                            },
                            {
                                "name": "work",
                                "emptyDir": {"sizeLimit": "5Gi"},
                            },
                            {
                                "name": "tmp",
                                "emptyDir": {"sizeLimit": "256Mi"},
                            },
                        ],
                    },
                },
            },
        }
        if worker_count > 1:
            job["spec"].update(
                {
                    "completionMode": "Indexed",
                    "completions": worker_count,
                    "parallelism": worker_count,
                }
            )
            job["spec"]["template"]["spec"]["containers"][0]["env"].append(
                {
                    "name": "BENCHMARK_WORKER_INDEX",
                    "valueFrom": {
                        "fieldRef": {
                            "fieldPath": (
                                "metadata.annotations["
                                "'batch.kubernetes.io/"
                                "job-completion-index']"
                            )
                        }
                    },
                }
            )
        try:
            self._request("POST", self.collection_path, job)
        except Exception as create_error:
            # A timed-out POST can still have committed at the API server.
            # The deterministic name makes submission idempotent: if the Job
            # is now observable, use it instead of marking the run failed and
            # potentially leaving an unowned workload behind.
            try:
                self.state(name)
            except Exception:
                raise create_error
        return name

    def state(self, name: str) -> str:
        path = f"{self.collection_path}/{quote(name)}"
        value = self._request("GET", path)
        assert value is not None
        status = value.get("status", {})
        if int(status.get("active", 0)):
            return "running"
        if int(status.get("succeeded", 0)):
            return "succeeded"
        if int(status.get("failed", 0)):
            return "failed"
        return "pending"

    def delete(self, name: str) -> None:
        path = (
            f"{self.collection_path}/{quote(name)}"
            "?propagationPolicy=Background"
        )
        try:
            self._request(
                "DELETE",
                path,
                {
                    "apiVersion": "v1",
                    "kind": "DeleteOptions",
                    "gracePeriodSeconds": 75,
                    "propagationPolicy": "Background",
                },
            )
        except JobNotFound:
            return
