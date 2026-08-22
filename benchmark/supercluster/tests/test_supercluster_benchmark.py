from __future__ import annotations

import asyncio
import contextlib
import io
import json
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import supercluster_benchmark as benchmark  # noqa: E402


class MetricSummaryTests(unittest.TestCase):
    def test_reports_median_and_population_standard_deviation(self) -> None:
        summary = benchmark.metric_summary([1.0, 2.0, 3.0], expected=4)

        self.assertEqual(summary["count"], 3)
        self.assertEqual(summary["missing"], 1)
        self.assertEqual(summary["median_ms"], 2.0)
        self.assertEqual(summary["population_stddev_ms"], 0.816497)
        self.assertEqual(summary["p95_ms"], 3.0)

    def test_empty_metric_is_explicit(self) -> None:
        summary = benchmark.metric_summary([], expected=2)

        self.assertEqual(summary["missing"], 2)
        self.assertIsNone(summary["median_ms"])
        self.assertIsNone(summary["population_stddev_ms"])


class InventoryTests(unittest.TestCase):
    def test_loads_exactly_two_regions(self) -> None:
        document = {
            "regions": [
                {
                    "region_id": "a",
                    "k8s_context": "context-a",
                    "nats_cluster_name": "NATS-A",
                },
                {
                    "region_id": "b",
                    "k8s_context": "context-b",
                    "nats_cluster_name": "NATS-B",
                },
            ]
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inventory.json"
            path.write_text(json.dumps(document), encoding="utf-8")

            regions = benchmark.load_regions(path)

        self.assertEqual([region.region_id for region in regions], ["a", "b"])
        self.assertEqual(regions[1].nats_cluster_name, "NATS-B")

    def test_rejects_reused_context(self) -> None:
        document = {
            "regions": [
                {
                    "region_id": "a",
                    "k8s_context": "same",
                    "nats_cluster_name": "NATS-A",
                },
                {
                    "region_id": "b",
                    "k8s_context": "same",
                    "nats_cluster_name": "NATS-B",
                },
            ]
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inventory.json"
            path.write_text(json.dumps(document), encoding="utf-8")

            with self.assertRaises(benchmark.BenchmarkError):
                benchmark.load_regions(path)


class ProtocolTests(unittest.TestCase):
    def test_parses_status_and_regular_headers(self) -> None:
        headers, status, description = benchmark.NatsConnection._parse_headers(
            b"NATS/1.0 503 No Responders\r\nNats-Pending-Messages: 3\r\n\r\n"
        )

        self.assertEqual(status, 503)
        self.assertEqual(description, "No Responders")
        self.assertEqual(headers["Nats-Pending-Messages"], "3")


class ProtocolSendTests(unittest.IsolatedAsyncioTestCase):
    async def test_publish_marks_send_immediately_before_wire_write(self) -> None:
        connection = object.__new__(benchmark.NatsConnection)
        connection.connected = asyncio.Event()
        connection.connected.set()
        connection._write_lock = asyncio.Lock()
        calls: list[str] = []

        class Writer:
            def is_closing(self) -> bool:
                return False

            def write(self, frame: bytes) -> None:
                self.frame = frame
                calls.append("write")

            async def drain(self) -> None:
                calls.append("drain")

        writer = Writer()
        connection._writer = writer

        await connection.publish(
            "events", b"payload", before_send=lambda: calls.append("mark")
        )

        self.assertEqual(calls, ["mark", "write", "drain"])
        self.assertEqual(writer.frame, b"PUB events 7\r\npayload\r\n")


@unittest.skipUnless(shutil.which("openssl"), "openssl is required")
class ProtocolConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_info_then_tls_and_ping(self) -> None:
        try:
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except OSError as error:
            self.skipTest(f"local sockets are unavailable: {error}")
        else:
            probe.close()
        with tempfile.TemporaryDirectory() as directory:
            certificate = Path(directory) / "server.crt"
            key = Path(directory) / "server.key"
            subprocess.run(
                [
                    "openssl",
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-subj",
                    "/CN=nats.nats.svc.cluster.local",
                    "-addext",
                    "subjectAltName=DNS:nats.nats.svc.cluster.local",
                    "-addext",
                    "basicConstraints=critical,CA:TRUE",
                    "-keyout",
                    str(key),
                    "-out",
                    str(certificate),
                ],
                check=True,
                capture_output=True,
            )
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.load_cert_chain(certificate, key)
            handler_done = asyncio.get_running_loop().create_future()

            async def handle(
                reader: asyncio.StreamReader, writer: asyncio.StreamWriter
            ) -> None:
                try:
                    writer.write(
                        b'INFO {"tls_required":true,"headers":true}\r\n'
                    )
                    await writer.drain()
                    await writer.start_tls(server_context)
                    self.assertTrue((await reader.readline()).startswith(b"CONNECT "))
                    self.assertEqual(await reader.readline(), b"PING\r\n")
                    writer.write(b"PONG\r\n")
                    await writer.drain()
                    while line := await reader.readline():
                        if line == b"PING\r\n":
                            writer.write(b"PONG\r\n")
                            await writer.drain()
                    if not handler_done.done():
                        handler_done.set_result(None)
                except BaseException as error:
                    if not handler_done.done():
                        handler_done.set_exception(error)
                finally:
                    writer.close()
                    await writer.wait_closed()

            try:
                server = await asyncio.start_server(handle, "127.0.0.1", 0)
            except OSError as error:
                self.skipTest(f"local sockets are unavailable: {error}")
            port = int(server.sockets[0].getsockname()[1])
            connection = benchmark.NatsConnection(
                name="protocol-test",
                host="127.0.0.1",
                port=port,
                user="user",
                password="password",
                ca=certificate.read_text(encoding="utf-8"),
                tls_server_name="nats.nats.svc.cluster.local",
            )
            try:
                await connection.connect()
                await connection.flush()
            finally:
                await connection.close()
                server.close()
                await server.wait_closed()
            await asyncio.wait_for(handler_done, timeout=2.0)


class ReplicationHealthTests(unittest.TestCase):
    def test_r3_requires_two_current_followers(self) -> None:
        info = {
            "config": {"num_replicas": 3},
            "cluster": {
                "name": "DEST",
                "leader": "nats-0",
                "replicas": [
                    {"name": "nats-1", "current": True, "lag": 0},
                    {"name": "nats-2", "current": True, "lag": 0},
                ],
            },
            "state": {"messages": 5, "bytes": 100},
        }

        health = benchmark.DirectionRun._replication_health(info, "DEST", 3)

        self.assertTrue(health["healthy"])
        self.assertEqual(len(health["followers"]), 2)

    def test_r3_rejects_lagging_follower(self) -> None:
        info = {
            "config": {"num_replicas": 3},
            "cluster": {
                "name": "DEST",
                "leader": "nats-0",
                "replicas": [
                    {"name": "nats-1", "current": True, "lag": 0},
                    {"name": "nats-2", "current": False, "lag": 1},
                ],
            },
        }

        health = benchmark.DirectionRun._replication_health(info, "DEST", 3)

        self.assertFalse(health["healthy"])


class OutagePublishingTests(unittest.IsolatedAsyncioTestCase):
    async def test_jetstream_outage_requests_are_pipelined(self) -> None:
        runner = object.__new__(benchmark.DirectionRun)
        runner.events = 3
        runner.interval = 0.0
        started = 0
        all_started = asyncio.Event()

        async def publish_one(
            phase: str, transport: str, subject: str, index: int
        ) -> int:
            nonlocal started
            self.assertEqual(phase, "connection_drop")
            self.assertEqual(transport, "jetstream_r3")
            self.assertEqual(subject, "events")
            started += 1
            if started == runner.events:
                all_started.set()
            await all_started.wait()
            return index

        runner._publish_one_js = publish_one

        records = await asyncio.wait_for(
            runner._publish_js_pipelined(
                "connection_drop", "jetstream_r3", "events"
            ),
            timeout=0.5,
        )

        self.assertEqual(records, [0, 1, 2])

    async def test_tunnel_restart_uses_disconnect_deadline(self) -> None:
        runner = object.__new__(benchmark.DirectionRun)
        runner.drop_seconds = 0.01
        runner.destination = benchmark.Region("b", "context-b", "NATS-B")

        class Connection:
            def __init__(self) -> None:
                self.reconnected = asyncio.Event()
                self.last_reconnected_ns: int | None = None

        connection = Connection()

        class Forward:
            async def start(self) -> None:
                connection.last_reconnected_ns = benchmark.time.perf_counter_ns()
                connection.reconnected.set()

        runner.destination_connection = connection
        runner.destination_forward = Forward()
        disconnected_ns = benchmark.time.perf_counter_ns()

        restart_started, reconnected_ns = (
            await runner._restart_destination_after_drop(disconnected_ns)
        )

        elapsed_ms = benchmark.milliseconds(restart_started - disconnected_ns)
        self.assertGreaterEqual(elapsed_ms, 8.0)
        self.assertLess(elapsed_ms, 250.0)
        self.assertGreaterEqual(reconnected_ns, restart_started)


class OutputTests(unittest.TestCase):
    def test_writes_event_and_replication_reports(self) -> None:
        metric = benchmark.metric_summary([1.0, 2.0], expected=2)
        healthy = {
            "healthy": True,
            "configured_replicas": 3,
            "cluster": "DEST",
            "leader": "nats-0",
            "followers": [
                {"name": "nats-1"},
                {"name": "nats-2"},
            ],
        }
        r1_healthy = {**healthy, "configured_replicas": 1, "followers": []}
        summary = {
            "direction": "a-to-b",
            "source_region": "a",
            "destination_region": "b",
            "streams": {"r1_control": "R1", "r3": "R3"},
            "stream_placement": "DEST",
            "latency": {
                "baseline": {
                    "core_delivery": metric,
                    "jetstream_r1_delivery": metric,
                    "jetstream_r1_publish_ack": metric,
                    "jetstream_r3_delivery": metric,
                    "jetstream_r3_publish_ack": metric,
                },
                "connection_drop": {
                    "core_delivery": benchmark.metric_summary([], expected=2),
                    "jetstream_r3_queued_age": metric,
                    "jetstream_r3_publish_ack": metric,
                },
                "recovery": {
                    "core_delivery": metric,
                    "jetstream_r1_delivery": metric,
                    "jetstream_r1_publish_ack": metric,
                    "jetstream_r3_delivery": metric,
                    "jetstream_r3_publish_ack": metric,
                },
            },
            "connection_drop": {
                "scheduled_drop_ms": 5000.0,
                "tunnel_restart_started_ms": 5001.0,
                "unavailable_ms": 5000.0,
                "reconnect_after_tunnel_restart_ms": 250.0,
                "jetstream_first_recovered_after_tunnel_restart_ms": 275.0,
                "jetstream_last_recovered_after_tunnel_restart_ms": 300.0,
                "jetstream_backlog_drain_ms": 25.0,
                "jetstream_queued_age": metric,
                "core_lost": 2,
                "core_published": 2,
                "core_published_before_tunnel_restart": 2,
                "jetstream_recovered": 2,
                "jetstream_published": 2,
                "jetstream_published_before_tunnel_restart": 2,
                "jetstream_missing": 0,
                "jetstream_duplicate_deliveries": 0,
            },
            "replication": {
                "r1_control": {
                    "initial": r1_healthy,
                    "source_view_after": r1_healthy,
                    "destination_view_after": r1_healthy,
                },
                "r3": {
                    "initial": healthy,
                    "source_view_after": healthy,
                    "destination_view_after": healthy,
                },
                "factor_impact": {
                    "baseline": {
                        "r3_minus_r1_publish_ack_median_ms": 0.5,
                        "r3_minus_r1_delivery_median_ms": 0.25,
                    }
                },
            },
            "failures": [],
        }
        metadata = {
            "run_id": "test-run",
            "fatal_error": None,
        }
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)

            benchmark.write_outputs(
                output,
                metadata,
                [summary],
                [],
                {"before": {}, "after": {}},
            )

            machine_summary = json.loads(
                (output / "summary.json").read_text(encoding="utf-8")
            )
            report = (output / "report.md").read_text(encoding="utf-8")
            events = (output / "events.csv").read_text(encoding="utf-8")
        self.assertTrue(machine_summary["passed"])
        self.assertIn("R3 minus R1 median", report)
        self.assertIn("JetStream backlog recovery", report)
        self.assertIn("Queued age median ms", report)
        self.assertIn("publish_ack_latency_ms", events)


class ArgumentTests(unittest.TestCase):
    def test_multiple_events_are_required(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                benchmark.parse_args(["--events", "1"])

    def test_outage_publish_window_must_fit_inside_drop(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                benchmark.parse_args(
                    [
                        "--events",
                        "1000",
                        "--interval",
                        "0.01",
                        "--drop-seconds",
                        "5",
                    ]
                )

    def test_valid_outage_publish_window_is_accepted(self) -> None:
        args = benchmark.parse_args(
            [
                "--events",
                "1000",
                "--interval",
                "0.01",
                "--drop-seconds",
                "12",
            ]
        )

        self.assertEqual(args.drop_seconds, 12)


if __name__ == "__main__":
    unittest.main()
