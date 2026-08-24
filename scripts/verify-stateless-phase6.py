#!/usr/bin/env python3
"""Verify Phase 6 deployment, benchmark, and generated-release contracts."""

from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BENCHMARK = ROOT / "src" / "benchmarkservice"
PROGRESS = (
    ROOT / "docs" / "development" / "stateless-progress" / "Phase6.md"
)
APPLICATIONS = (
    "adservice",
    "benchmarkservice",
    "cartservice",
    "checkoutservice",
    "currencyservice",
    "emailservice",
    "frontend",
    "paymentservice",
    "productcatalogservice",
    "recommendationservice",
    "shippingservice",
    "storefrontprojectionservice",
)
COMPARABLE_CPU = {
    "adservice": ("125m", "300m"),
    "cartservice": ("300m", '"2"'),
    "checkoutservice": ("275m", '"2"'),
    "currencyservice": ("100m", "200m"),
    "emailservice": ("175m", "200m"),
    "frontend": ("300m", '"1"'),
    "paymentservice": ("225m", '"2"'),
    "productcatalogservice": ("5m", "200m"),
    "recommendationservice": ("300m", '"2"'),
    "shippingservice": ("175m", '"2"'),
}


class VerificationError(RuntimeError):
    pass


def run(command: list[str], cwd: Path = ROOT) -> str:
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if result.returncode:
        raise VerificationError(
            f"{' '.join(command)} failed:\n{result.stdout.rstrip()}"
        )
    return result.stdout


def require(content: str, *values: str) -> None:
    missing = [value for value in values if value not in content]
    if missing:
        raise VerificationError(f"missing Phase 6 contracts: {missing}")


def forbid(content: str, *values: str) -> None:
    retained = [value for value in values if value in content]
    if retained:
        raise VerificationError(f"retained obsolete Phase 6 paths: {retained}")


def documents(rendered: str) -> list[str]:
    return re.split(r"(?m)^---[ \t]*\r?\n", rendered)


def document(rendered: str, kind: str, name: str) -> str:
    for candidate in documents(rendered):
        if not re.search(
            rf"(?m)^kind:\s*{re.escape(kind)}\s*$", candidate
        ):
            continue
        metadata = re.search(
            r"(?ms)^metadata:\s*\n(?P<body>(?:^[ \t]+.*\n?)*)",
            candidate,
        )
        if metadata and re.search(
            rf"(?m)^[ \t]+name:\s*{re.escape(name)}\s*$",
            metadata.group("body"),
        ):
            return candidate
    raise VerificationError(f"rendered manifest lacks {kind}/{name}")


def forbid_document(rendered: str, kind: str, name: str) -> None:
    try:
        document(rendered, kind, name)
    except VerificationError:
        return
    raise VerificationError(
        f"rendered manifest unexpectedly contains {kind}/{name}"
    )


def verify_benchmark() -> None:
    app = (BENCHMARK / "app.py").read_text()
    control = (BENCHMARK / "control.py").read_text()
    job = (BENCHMARK / "job.py").read_text()
    store = (BENCHMARK / "shared_store.py").read_text()
    jobs = (BENCHMARK / "kubernetes_jobs.py").read_text()
    runtime = (BENCHMARK / "runtime.py").read_text()
    standalone = (BENCHMARK / "standalone.py").read_text()
    metrics_server = (BENCHMARK / "metrics_server.py").read_text()
    all_sources = "\n".join(
        (app, control, job, store, jobs, runtime, standalone, metrics_server)
    )
    require(
        all_sources,
        'RUN_BUCKET = os.environ.get("BENCHMARK_RUNS_BUCKET", "BENCHMARK_RUNS")',
        'ARTIFACT_BUCKET = os.environ.get('
        '"BENCHMARK_ARTIFACTS_BUCKET", "BENCHMARK_ARTIFACTS")',
        "self._kv.create",
        "self._kv.update",
        "lease_until",
        "KubernetesJobClient",
        '"kind": "Job"',
        '"serviceAccountName": "benchmark-runner"',
        "store.put_object",
        "store.get_object",
        "ttlSecondsAfterFinished",
        "build_report(run_directory)",
        "target_url",
        "metrics_url",
        "RemoteMetricsClient",
        "ClusterMetricsCollector",
    )
    forbid(
        app + control,
        "subprocess.Popen",
        "threading.Lock()",
        "result_directory",
        "RESULTS_DIR",
        "/var/lib/benchmarkservice",
    )
    forbid(
        all_sources,
        "sqlite",
        "bbolt",
    )
    requirements = (BENCHMARK / "requirements.txt").read_text()
    require(requirements, "locust==2.43.1", "nats-py==2.15.0")
    tests = (BENCHMARK / "tests" / "test_control.py").read_text()
    require(
        tests,
        "test_concurrent_api_replicas_submit_only_one_job",
        "test_another_replica_observes_and_stops_run",
        "test_expired_lease_is_recovered_by_another_replica",
        "test_artifact_is_read_from_shared_object_store",
    )
    run(
        [
            sys.executable,
            "-m",
            "unittest",
            "discover",
            "-s",
            "tests",
            "-p",
            "test_*.py",
        ],
        BENCHMARK,
    )


def verify_manifests() -> str:
    kubectl = shutil.which("kubectl")
    if not kubectl:
        raise VerificationError("kubectl with built-in kustomize is required")
    rendered = run([kubectl, "kustomize", "kubernetes-manifests"])
    for application in APPLICATIONS:
        deployment = document(rendered, "Deployment", application)
        require(
            deployment,
            "replicas: 2",
            "type: RollingUpdate",
            "maxUnavailable: 0",
            "maxSurge: 1",
            "topologySpreadConstraints:",
            "matchLabelKeys:",
            "podAntiAffinity:",
        )
        forbid(deployment, "matchExpressions:")
        forbid_document(rendered, "HorizontalPodAutoscaler", application)
        budget = document(rendered, "PodDisruptionBudget", application)
        require(budget, "maxUnavailable: 1", f"app: {application}")

    benchmark = document(rendered, "Deployment", "benchmarkservice")
    require(
        benchmark,
        "name: POD_NAME",
        "name: nats-client-config",
        "nats-credentials-benchmarkservice",
        "path: /readyz",
    )
    forbid(
        benchmark,
        "RESULTS_DIR",
        "/var/lib/benchmarkservice",
        "emptyDir:",
        "type: Recreate",
    )
    benchmark_external = document(
        rendered, "Service", "benchmarkservice-external"
    )
    require(
        benchmark_external,
        "app: benchmarkservice",
        "type: LoadBalancer",
        "port: 80",
        "targetPort: http",
    )
    role = document(rendered, "Role", "benchmarkservice-jobs")
    require(role, "resources:", "- jobs", "- create", "- delete")
    runner = document(
        rendered, "ServiceAccount", "benchmark-runner"
    )
    require(runner, "name: benchmark-runner")
    runner_role = document(
        rendered, "ClusterRole", "online-boutique-benchmark-runner"
    )
    require(runner_role, "- nodes", "- nodes/proxy", "- get", "- list")
    runner_binding = document(
        rendered,
        "ClusterRoleBinding",
        "online-boutique-benchmark-runner",
    )
    require(
        runner_binding,
        "name: online-boutique-benchmark-runner",
        "name: benchmark-runner",
    )
    policy = document(rendered, "NetworkPolicy", "benchmark-runner-egress")
    require(
        policy,
        "app: benchmark-runner",
        "port: 4222",
        "port: 7777",
        "port: 80",
        "port: 443",
        "port: 8080",
        "port: 6443",
    )
    nats_policy = document(
        run(
            [
                kubectl,
                "kustomize",
                "kubernetes-manifests/nats/fresh-cluster",
            ]
        ),
        "NetworkPolicy",
        "nats-clients-and-cluster",
    )
    require(
        nats_policy,
        "- benchmark-runner",
        "- benchmarkservice",
        "port: 4222",
        "port: 7777",
    )

    for legacy in (
        "email-data",
        "shipping-data",
        "redis-cart-data",
        "redis-checkout-data",
    ):
        try:
            document(rendered, "PersistentVolumeClaim", legacy)
        except VerificationError:
            continue
        raise VerificationError(
            f"application-owned PersistentVolumeClaim/{legacy} still renders"
        )
    forbid(rendered, "type: Recreate")
    return rendered


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_release_manifests() -> None:
    full = ROOT / "release" / "kubernetes-manifests.yaml"
    no_load = (
        ROOT / "release" / "kubernetes-manifests-no-loadgenerator.yaml"
    )
    benchmarks = tuple(
        ROOT / "benchmark" / name
        for name in (
            "benchmark-nats.yaml",
            "benchmark-nats-hpa.yaml",
            "benchmark-nats-multiple-replicas.yaml",
            "benchmark-nats-with-delay.yaml",
        )
    )
    generated = (full, no_load, *benchmarks)
    before = tuple(digest(path) for path in generated)
    run([sys.executable, "scripts/generate-release-manifests.py"])
    after = tuple(digest(path) for path in generated)
    if before != after:
        raise VerificationError(
            "generated release manifests were not up to date"
        )
    no_load_text = no_load.read_text()
    full_text = full.read_text()
    require(
        no_load_text,
        "Generated by scripts/generate-release-manifests.py",
        "kind: PodDisruptionBudget",
        "name: benchmark-runner",
        "name: redis-cart-cluster",
        "name: redis-checkout-cluster",
    )
    require(full_text, "name: loadgenerator")
    try:
        document(no_load_text, "Deployment", "loadgenerator")
    except VerificationError:
        pass
    else:
        raise VerificationError(
            "no-loadgenerator release contains Deployment/loadgenerator"
        )
    for content in (full_text, no_load_text):
        forbid(
            content,
            "kind: HorizontalPodAutoscaler",
            "type: Recreate",
            "name: email-data",
            "name: shipping-data",
            "name: redis-cart-data",
            "name: redis-checkout-data",
            "RESULTS_DIR",
            "/var/lib/benchmarkservice",
        )
    for path in benchmarks:
        content = path.read_text()
        require(
            content,
            "Generated by scripts/generate-release-manifests.py",
            "Install and wait for NATS before applying this benchmark bundle",
            "A compatible NATS configuration must already be running",
            "kind: PodDisruptionBudget",
            "name: benchmark-runner",
            "name: benchmarkservice-external",
            "name: redis-cart-cluster",
            "name: redis-checkout-cluster",
            "name: benchmarkmetrics-external",
            "type: LoadBalancer",
            "python",
            "metrics_server.py",
        )
        for kind, name in (
            ("Namespace", "nats"),
            ("Service", "nats"),
            ("StatefulSet", "nats"),
            ("Deployment", "nats-setup"),
            ("ConfigMap", "nats-client-config"),
            ("ConfigMap", "nats-ca"),
            ("ConfigMap", "nats-server-config"),
            ("Job", "nats-global-bootstrap"),
            ("Job", "nats-regional-bootstrap"),
            ("Secret", "nats-credentials-benchmarkservice"),
        ):
            forbid_document(content, kind, name)
        metrics = document(content, "Deployment", "benchmarkmetrics")
        require(
            metrics,
            "name: APPLICATION_TYPE",
            "value: NATS",
            "name: BENCHMARK_METRICS_TOKEN",
            "name: benchmark-metrics-auth",
        )
        standalone_redis = path.name == "benchmark-nats.yaml"
        for application in APPLICATIONS:
            deployment = document(content, "Deployment", application)
            require(
                deployment,
                "replicas: 2",
                "type: RollingUpdate",
                "maxUnavailable: 0",
            )
        currency = document(content, "Deployment", "currencyservice")
        require(currency, "cpu: 200m", "cpu: 400m")
        message_operations = document(
            content, "Deployment", "messageoperationsservice"
        )
        require(message_operations, "replicas: 2")
        controller = document(content, "Deployment", "benchmarkservice")
        require(
            controller,
            "name: nats-client-config",
            "name: nats-credentials-benchmarkservice",
            "name: nats-ca",
        )
        if standalone_redis:
            forbid(
                content,
                "kind: HorizontalPodAutoscaler",
                "name: redis-cluster-bootstrap",
                "redis-cli CLUSTER INFO",
                "--cluster-enabled yes",
            )
            checkout = document(content, "Deployment", "checkoutservice")
            require(checkout, "name: CHECKOUT_REDIS_MODE", "value: standalone")
            for redis in (
                "redis-cart-cluster",
                "redis-checkout-cluster",
            ):
                stateful_set = document(content, "StatefulSet", redis)
                require(
                    stateful_set,
                    "replicas: 1",
                    "redis-cli",
                    "- PING",
                )
                forbid(
                    stateful_set,
                    "--cluster-enabled",
                    "cluster-bus",
                    "repair-topology",
                )
        if path.name == "benchmark-nats-hpa.yaml":
            require(content, "kind: HorizontalPodAutoscaler")
        else:
            forbid(content, "kind: HorizontalPodAutoscaler")
        if path.name == "benchmark-nats-with-delay.yaml":
            payment = document(content, "Deployment", "paymentservice")
            shipping = document(content, "Deployment", "shippingservice")
            require(payment, "name: PROCESSING_TIME_MS", 'value: "500"')
            require(shipping, "name: PROCESSING_TIME_MS", 'value: "200"')
        if path.name == "benchmark-nats-hpa.yaml":
            lag_scaled = {
                "adservice",
                "cartservice",
                "checkoutservice",
                "emailservice",
                "paymentservice",
                "recommendationservice",
                "shippingservice",
                "storefrontprojectionservice",
            }
            for application in APPLICATIONS:
                hpa = document(
                    content, "HorizontalPodAutoscaler", application
                )
                require(
                    hpa,
                    "type: Resource",
                    "name: cpu",
                    "averageUtilization: 70",
                )
                if application in lag_scaled:
                    require(
                        hpa,
                        "type: External",
                        "name: boutique_jetstream_consumer_pending",
                        f"service: {application}",
                    )
                else:
                    forbid(hpa, "type: External")
            forbid(
                content,
                "boutique_http_in_flight",
                "boutique_http_p95_latency_seconds",
                "boutique_handler_p95_latency_seconds",
            )
            metrics_server = document(content, "Deployment", "metrics-server")
            require(
                metrics_server,
                "image: registry.k8s.io/metrics-server/metrics-server:v0.9.0",
                "namespace: kube-system",
                "--kubelet-insecure-tls",
            )
            metrics_api = document(
                content, "APIService", "v1beta1.metrics.k8s.io"
            )
            require(metrics_api, "group: metrics.k8s.io")
            prometheus = document(
                content, "Deployment", "benchmark-prometheus"
            )
            require(
                prometheus,
                "namespace: observability",
                "image: prom/prometheus:v3.10.0-distroless",
                "--storage.tsdb.retention.time=2h",
            )
            adapter = document(
                content, "Deployment", "benchmark-prometheus-adapter"
            )
            require(
                adapter,
                "namespace: observability",
                (
                    "image: registry.k8s.io/prometheus-adapter/"
                    "prometheus-adapter:v0.12.0"
                ),
                (
                    "--prometheus-url=http://benchmark-prometheus."
                    "observability.svc:9090"
                ),
            )
            external_api = document(
                content, "APIService", "v1beta1.external.metrics.k8s.io"
            )
            require(
                external_api,
                "group: external.metrics.k8s.io",
                "name: benchmark-prometheus-adapter",
                "namespace: observability",
            )
            adapter_config = document(
                content, "ConfigMap", "benchmark-prometheus-adapter"
            )
            require(
                adapter_config,
                "externalRules:",
                (
                    "seriesQuery: "
                    "'boutique_jetstream_consumer_pending"
                ),
                (
                    "metricsQuery: 'max(<<.Series>>"
                    "{<<.LabelMatchers>>}) by (namespace, service)'"
                ),
            )
            recording_rules = document(
                content, "ConfigMap", "benchmark-prometheus-rules"
            )
            require(
                recording_rules,
                "record: boutique_jetstream_consumer_pending",
                'consumer_name="cart-commands-v1"',
                "service: cartservice",
                "service: checkoutservice",
                "service: recommendationservice",
            )
        else:
            forbid(
                content,
                "image: registry.k8s.io/metrics-server/metrics-server:",
                "name: v1beta1.metrics.k8s.io",
                "name: benchmark-prometheus",
                "name: benchmark-prometheus-adapter",
                "name: v1beta1.external.metrics.k8s.io",
            )
        forbid(
            content,
            "name: redis-cart-data",
            "name: redis-checkout-data",
            "RESULTS_DIR",
            "/var/lib/benchmarkservice",
        )
    original = (ROOT / "benchmark" / "benchmark-original-app.yaml").read_text()
    grpc_controller = document(original, "Deployment", "benchmarkservice")
    require(
        original,
        "NATS configuration before applying this bundle",
        "name: benchmark-runner",
        "name: benchmarkservice-external",
        "name: benchmarkmetrics-external",
        "kind: PodDisruptionBudget",
    )
    for kind, name in (
        ("Namespace", "benchmark-system"),
        ("Service", "benchmark-state"),
        ("Deployment", "benchmark-state"),
        ("Job", "benchmark-state-bootstrap"),
        ("ConfigMap", "nats-client-config"),
        ("ConfigMap", "nats-ca"),
        ("Secret", "nats-credentials-benchmarkservice"),
    ):
        forbid_document(original, kind, name)
    grpc_metrics = document(original, "Deployment", "benchmarkmetrics")
    require(
        grpc_metrics,
        "python",
        "metrics_server.py",
        "name: APPLICATION_TYPE",
        'value: "GRPC"',
        "name: BENCHMARK_METRICS_TOKEN",
        "name: benchmark-metrics-auth",
    )
    require(
        grpc_controller,
        'value: "GRPC"',
        "replicas: 1",
        "type: RollingUpdate",
        "maxUnavailable: 0",
        "name: nats-client-config",
        "nats-credentials-benchmarkservice",
    )
    for application, (request, limit) in COMPARABLE_CPU.items():
        deployment = document(original, "Deployment", application)
        require(
            deployment,
            "replicas: 2",
            f"cpu: {request}",
            f"cpu: {limit}",
        )
    redis_cart = document(original, "Deployment", "redis-cart")
    require(redis_cart, "cpu: 50m", "cpu: 500m")
    try:
        document(original, "HorizontalPodAutoscaler", "benchmarkservice")
    except VerificationError:
        pass
    else:
        raise VerificationError(
            "original application benchmarkservice must not autoscale"
        )
    forbid(
        original,
        "RESULTS_DIR",
        "/var/lib/benchmarkservice",
        "type: Recreate",
    )
    delayed_original = (
        ROOT / "benchmark" / "benchmark-original-app-with-delay.yaml"
    ).read_text()
    require(
        delayed_original,
        "500 ms of payment authorization latency",
        "shipment-creation latency using delay-enabled original application images",
    )
    for application, (request, limit) in COMPARABLE_CPU.items():
        deployment = document(delayed_original, "Deployment", application)
        require(
            deployment,
            "replicas: 2",
            f"cpu: {request}",
            f"cpu: {limit}",
        )
    delayed_redis_cart = document(
        delayed_original, "Deployment", "redis-cart"
    )
    require(delayed_redis_cart, "cpu: 50m", "cpu: 500m")
    delayed_payment = document(
        delayed_original, "Deployment", "paymentservice"
    )
    require(
        delayed_payment,
        "image: matslo123/paymentservice:v0.10.6-delay",
        "name: PROCESSING_TIME_MS",
        'value: "500"',
    )
    delayed_shipping = document(
        delayed_original, "Deployment", "shippingservice"
    )
    require(
        delayed_shipping,
        "image: matslo123/shippingservice:v0.10.6-with-delay",
        "name: PROCESSING_TIME_MS",
        'value: "200"',
    )


def verify_dashboard_and_bootstrap() -> None:
    dashboard = (
        ROOT
        / "kubernetes-manifests"
        / "observability"
        / "stateless-dashboard.yaml"
    ).read_text()
    require(
        dashboard,
        "boutique_cart_command_duration_seconds_sum",
        "boutique_cart_redis_retries_total",
        "boutique_checkout_transition_conflicts_total",
        "boutique_checkout_deadline_oldest_age_seconds",
        "boutique_projection_kv_conflict_retries_total",
        "boutique_projection_stale_events_total",
        "boutique_projection_age_seconds",
        "boutique_result_republishes_total",
        "jetstream_consumer_num_ack_pending",
    )
    bootstrap = (
        ROOT
        / "kubernetes-manifests"
        / "nats"
        / "base"
        / "regional-bootstrap.yaml"
    ).read_text()
    require(
        bootstrap,
        'ensure_kv "${BENCHMARK_RUNS_BUCKET}" 10 0s 0 536870912',
        'ensure_object "${BENCHMARK_ARTIFACTS_BUCKET}"',
        "--replicas=3",
        "nats ${nats_args} object ls",
    )
    forbid(bootstrap, "nats ${nats_args} object list")


def main() -> int:
    try:
        run([sys.executable, "scripts/verify-stateless-phase5.py"])
        verify_benchmark()
        verify_manifests()
        verify_release_manifests()
        verify_dashboard_and_bootstrap()
        if not PROGRESS.is_file():
            raise VerificationError(
                "docs/development/stateless-progress/Phase6.md is missing"
            )
        run(
            [
                "git",
                "check-ignore",
                "--quiet",
                str(PROGRESS.relative_to(ROOT)),
            ]
        )
    except (OSError, VerificationError) as error:
        print(
            f"Stateless Phase 6 verification failed: {error}",
            file=sys.stderr,
        )
        return 1
    print(
        "Stateless Phase 6 Jobs/shared run stores, rolling multi-replica "
        "deployments, HPA/PDB/topology policy, dashboards, generated "
        "releases, and ignored progress documentation verified"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
