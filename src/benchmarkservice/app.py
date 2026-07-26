# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import threading
import uuid
import zipfile
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from config import BenchmarkConfig, ConfigError, normalize_target_url
from reporting import build_report


SOURCE_DIRECTORY = Path(__file__).resolve().parent
RUN_ID_PATTERN = re.compile(r"^[0-9TZa-f-]{12,64}$")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as target:
        json.dump(value, target, indent=2, sort_keys=True)
        target.write("\n")
    os.replace(temporary, path)


def read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError(f"{path} does not contain a JSON object")
    return value


class RunConflict(RuntimeError):
    pass


class RunNotFound(KeyError):
    pass


class BenchmarkManager:
    def __init__(
        self,
        result_directory: Path,
        application_type: str,
        target_url: str,
    ) -> None:
        self.result_directory = result_directory
        self.result_directory.mkdir(parents=True, exist_ok=True)
        self.application_type = application_type.strip().upper()
        if self.application_type not in {"GRPC", "NATS"}:
            raise ConfigError("APPLICATION_TYPE must be GRPC or NATS")
        self.target_url = normalize_target_url(target_url)
        self.lock = threading.Lock()
        self.process: subprocess.Popen[bytes] | None = None
        self.active_run_id: str | None = None
        self._recover_interrupted_runs()

    def _recover_interrupted_runs(self) -> None:
        for status_path in self.result_directory.glob("*/status.json"):
            try:
                status = read_json(status_path)
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            if status.get("state") in {"starting", "running", "stopping"}:
                status.update(
                    {
                        "state": "interrupted",
                        "ended_at": utc_now(),
                        "message": "controller restarted while this run was active",
                    }
                )
                write_json(status_path, status)

    def _active(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def start(self, values: dict[str, Any]) -> dict[str, Any]:
        config = BenchmarkConfig.from_request(
            values, self.application_type, self.target_url
        )
        with self.lock:
            if self._active():
                raise RunConflict(f"run {self.active_run_id} is still active")
            self.process = None
            self.active_run_id = None

            run_id = (
                datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                + "-"
                + uuid.uuid4().hex[:8]
            )
            run_directory = self.result_directory / run_id
            run_directory.mkdir(mode=0o750)
            write_json(run_directory / "config.json", config.as_dict())
            status = {
                "run_id": run_id,
                "state": "starting",
                "created_at": utc_now(),
                "application_type": config.application_type,
                "workload": config.workload,
            }
            write_json(run_directory / "status.json", status)

            users = config.users if config.workload == "closed" else 1
            spawn_rate = config.spawn_rate if config.workload == "closed" else 1
            user_class = (
                "ClosedLoopUser"
                if config.workload == "closed"
                else "OpenLoopDriver"
            )
            command = [
                sys.executable,
                "-m",
                "locust",
                "-f",
                str(SOURCE_DIRECTORY / "locustfile.py"),
                "--headless",
                "--only-summary",
                "--host",
                config.target_url,
                "--users",
                str(users),
                "--spawn-rate",
                str(spawn_rate),
                "--run-time",
                f"{config.run_seconds}s",
                "--stop-timeout",
                str(config.drain_seconds),
                "--csv",
                str(run_directory / "locust"),
                "--csv-full-history",
                "--exit-code-on-error",
                "0",
                "--loglevel",
                "WARNING",
                user_class,
            ]
            environment = os.environ.copy()
            environment["BENCHMARK_CONFIG_FILE"] = str(
                run_directory / "config.json"
            )
            environment["BENCHMARK_OUTPUT_DIR"] = str(run_directory)
            log = (run_directory / "runner.log").open("wb")
            try:
                process = subprocess.Popen(
                    command,
                    cwd=SOURCE_DIRECTORY,
                    env=environment,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            except Exception:
                log.close()
                status.update(
                    {
                        "state": "failed",
                        "ended_at": utc_now(),
                        "message": "could not start Locust",
                    }
                )
                write_json(run_directory / "status.json", status)
                raise

            self.process = process
            self.active_run_id = run_id
            status.update(
                {"state": "running", "started_at": utc_now(), "pid": process.pid}
            )
            write_json(run_directory / "status.json", status)
            threading.Thread(
                target=self._monitor,
                args=(run_id, process, log),
                name=f"benchmark-{run_id}",
                daemon=True,
            ).start()
            return self.details(run_id)

    def _monitor(
        self,
        run_id: str,
        process: subprocess.Popen[bytes],
        log: Any,
    ) -> None:
        return_code = process.wait()
        log.close()
        run_directory = self.result_directory / run_id
        try:
            status = read_json(run_directory / "status.json")
        except (OSError, ValueError, json.JSONDecodeError):
            status = {"run_id": run_id}

        stop_requested = bool(status.get("stop_requested"))
        state = "stopped" if stop_requested else ("completed" if return_code == 0 else "failed")
        message = None
        try:
            build_report(run_directory)
        except Exception as exc:  # The runner log and raw artifacts remain exportable.
            state = "failed"
            message = f"report generation failed: {exc}"
        status.update(
            {
                "state": state,
                "ended_at": utc_now(),
                "exit_code": return_code,
            }
        )
        if message:
            status["message"] = message
        write_json(run_directory / "status.json", status)
        with self.lock:
            if self.process is process:
                self.process = None
                self.active_run_id = None

    def stop(self, run_id: str) -> dict[str, Any]:
        with self.lock:
            if (
                not self._active()
                or self.active_run_id != run_id
                or self.process is None
            ):
                raise RunConflict("the requested run is not active")
            run_directory = self._run_directory(run_id)
            status = read_json(run_directory / "status.json")
            status.update(
                {
                    "state": "stopping",
                    "stop_requested": True,
                    "stop_requested_at": utc_now(),
                }
            )
            write_json(run_directory / "status.json", status)
            os.killpg(self.process.pid, signal.SIGINT)
        return self.details(run_id)

    def _run_directory(self, run_id: str) -> Path:
        if not RUN_ID_PATTERN.fullmatch(run_id):
            raise RunNotFound(run_id)
        directory = self.result_directory / run_id
        if not directory.is_dir():
            raise RunNotFound(run_id)
        return directory

    def details(self, run_id: str) -> dict[str, Any]:
        directory = self._run_directory(run_id)
        result: dict[str, Any] = {
            "status": read_json(directory / "status.json"),
            "config": read_json(directory / "config.json"),
        }
        summary_path = directory / "summary.json"
        if summary_path.exists():
            result["summary"] = read_json(summary_path)
        return result

    def list_runs(self) -> list[dict[str, Any]]:
        runs: list[dict[str, Any]] = []
        for status_path in sorted(
            self.result_directory.glob("*/status.json"), reverse=True
        ):
            try:
                status = read_json(status_path)
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            summary_path = status_path.parent / "summary.json"
            if summary_path.exists():
                try:
                    summary = read_json(summary_path)
                    business = summary.get("business", {})
                    status["completed_orders"] = business.get("completed")
                    status["p95_ms"] = business.get(
                        "checkout_to_outcome", {}
                    ).get("p95_ms")
                except (OSError, ValueError, json.JSONDecodeError):
                    pass
            runs.append(status)
        return runs

    def create_archive(self, run_id: str) -> Path:
        directory = self._run_directory(run_id)
        archive = directory / f"{run_id}.zip"
        with zipfile.ZipFile(
            archive, "w", compression=zipfile.ZIP_DEFLATED
        ) as target:
            for artifact in sorted(directory.iterdir()):
                if artifact.is_file() and artifact != archive:
                    target.write(artifact, artifact.name)
        return archive

    def shutdown(self) -> None:
        with self.lock:
            if not self._active() or self.process is None:
                return
            process = self.process
            run_id = self.active_run_id
            if run_id:
                status_path = self.result_directory / run_id / "status.json"
                try:
                    status = read_json(status_path)
                    status.update(
                        {
                            "state": "stopping",
                            "stop_requested": True,
                            "stop_requested_at": utc_now(),
                            "message": "controller is shutting down",
                        }
                    )
                    write_json(status_path, status)
                except (OSError, ValueError, json.JSONDecodeError):
                    pass
            os.killpg(process.pid, signal.SIGINT)
        try:
            process.wait(timeout=70)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)


HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Online Boutique benchmark</title>
  <style>
    :root { color-scheme: light; --ink:#17202a; --muted:#617080; --line:#d9e0e7;
      --paper:#fff; --bg:#f3f6f8; --brand:#174ea6; --bad:#b3261e; }
    * { box-sizing:border-box } body { margin:0; font:15px/1.5 system-ui,sans-serif;
      color:var(--ink); background:var(--bg) } main { max-width:1100px; margin:auto; padding:32px 20px }
    h1,h2 { line-height:1.15 } .card { background:var(--paper); border:1px solid var(--line);
      border-radius:12px; padding:20px; margin:16px 0; box-shadow:0 2px 8px #17202a0a }
    .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:14px }
    label { display:flex; flex-direction:column; gap:5px; color:var(--muted); font-size:13px }
    input,select,button { font:inherit; padding:9px 10px; border:1px solid #aab5c0; border-radius:7px }
    button { background:var(--brand); color:white; border:0; cursor:pointer; font-weight:650 }
    button.secondary { background:#5f6b76 } button.danger { background:var(--bad) }
    button:disabled { opacity:.5; cursor:not-allowed } .actions { display:flex; gap:10px; margin-top:18px }
    table { width:100%; border-collapse:collapse } th,td { padding:9px; border-bottom:1px solid var(--line);
      text-align:left } th { color:var(--muted); font-size:12px; text-transform:uppercase }
    code { font-size:12px } .pill { display:inline-block; border-radius:100px; padding:2px 8px;
      background:#e8eef8; font-size:12px } .error { color:var(--bad) } .muted { color:var(--muted) }
    pre { white-space:pre-wrap; overflow:auto; background:#f7f8fa; padding:12px; border-radius:8px }
  </style>
</head>
<body><main>
  <h1>Online Boutique benchmark</h1>
  <p class="muted">Target: <strong id="application"></strong> at <code id="target"></code>.
    Runs start only when requested and execute one at a time.</p>
  <section class="card">
    <h2>New run</h2>
    <form id="run-form"><div class="grid">
      <label>Workload<select name="workload"><option value="closed">Closed-loop users</option>
        <option value="open">Open-loop capacity</option></select></label>
      <label>Warm-up (seconds)<input name="warmup_seconds" type="number" min="0" value="30"></label>
      <label>Steady interval (seconds)<input name="duration_seconds" type="number" min="1" value="120"></label>
      <label>Drain (seconds)<input name="drain_seconds" type="number" min="1" value="60"></label>
      <label>Closed-loop users<input name="users" type="number" min="1" value="10"></label>
      <label>User spawn rate/s<input name="spawn_rate" type="number" min=".01" step=".01" value="1"></label>
      <label>Open-loop orders/s<input name="arrival_rate" type="number" min=".01" step=".01" value="1"></label>
      <label>Outcome timeout (seconds)<input name="outcome_timeout_seconds" type="number" min="1" value="30"></label>
      <label>Settlement timeout (seconds)<input name="settlement_timeout_seconds" type="number" min="1" value="60"></label>
      <label>Resource sample interval<input name="resource_sample_interval_seconds" type="number" min="1" value="5"></label>
      <label>Random seed<input name="seed" type="number" min="0" value="1"></label>
      <label><span>Collection</span><span><input name="collect_resources" type="checkbox" checked>
        Runtime resources</span></label>
    </div><div class="actions"><button id="start" type="submit">Start benchmark</button>
      <button id="stop" class="danger" type="button" disabled>Stop active run</button></div>
    <p id="message" class="muted"></p></form>
  </section>
  <section class="card"><h2>Runs</h2><div id="runs"></div></section>
  <section class="card" id="details-card" hidden><h2>Result</h2>
    <div id="details"></div></section>
</main>
<script>
let activeRun = null;
const message = document.querySelector("#message");
const fields = ["warmup_seconds","duration_seconds","drain_seconds","users","spawn_rate",
  "arrival_rate","outcome_timeout_seconds","settlement_timeout_seconds",
  "resource_sample_interval_seconds","seed"];
async function api(path, options={}) {
  const response = await fetch(path, options);
  const data = await response.json().catch(()=>({error:response.statusText}));
  if (!response.ok) throw new Error(data.error || response.statusText);
  return data;
}
function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, c=>({"&":"&amp;","<":"&lt;",">":"&gt;",
    '"':"&quot;","'":"&#39;"}[c]));
}
async function refresh() {
  const info = await api("/api/info");
  document.querySelector("#application").textContent = info.application_type;
  document.querySelector("#target").textContent = info.target_url;
  const runs = await api("/api/runs");
  activeRun = runs.find(run=>["starting","running","stopping"].includes(run.state))?.run_id || null;
  document.querySelector("#start").disabled = !!activeRun;
  document.querySelector("#stop").disabled = !activeRun;
  document.querySelector("#runs").innerHTML = runs.length ? `<table><thead><tr>
    <th>Run</th><th>Application</th><th>Workload</th><th>Status</th><th>Completed</th><th>p95 outcome</th><th></th>
    </tr></thead><tbody>${runs.map(run=>`<tr><td><code>${esc(run.run_id)}</code></td>
    <td>${esc(run.application_type)}</td><td>${esc(run.workload)}</td>
    <td><span class="pill">${esc(run.state)}</span></td><td>${esc(run.completed_orders)}</td>
    <td>${run.p95_ms == null ? "" : esc(run.p95_ms)+" ms"}</td>
    <td><button class="secondary" onclick="showRun('${esc(run.run_id)}')">View</button></td></tr>`).join("")}
    </tbody></table>` : `<p class="muted">No benchmark has been run.</p>`;
}
async function showRun(id) {
  const run = await api(`/api/runs/${id}`);
  const card = document.querySelector("#details-card"); card.hidden = false;
  const summary = run.summary;
  document.querySelector("#details").innerHTML = summary ? `<div class="actions">
    <a href="/api/runs/${id}/summary.json">Summary JSON</a>
    <a href="/api/runs/${id}/business.csv">Raw business CSV</a>
    <a href="/api/runs/${id}/artifacts.zip">All artifacts</a></div>
    <pre>${esc(JSON.stringify(summary,null,2))}</pre>` :
    `<p>Status: <span class="pill">${esc(run.status.state)}</span></p>
     <pre>${esc(JSON.stringify(run.status,null,2))}</pre>`;
}
document.querySelector("#run-form").addEventListener("submit", async event=>{
  event.preventDefault(); message.textContent = "Starting…"; message.className = "muted";
  const form = new FormData(event.target), payload = {workload:form.get("workload"),
    collect_resources:form.has("collect_resources")};
  fields.forEach(name=>payload[name]=form.get(name));
  try { const run = await api("/api/runs",{method:"POST",headers:{"Content-Type":"application/json"},
      body:JSON.stringify(payload)}); activeRun=run.status.run_id; message.textContent="Benchmark started.";
    await refresh(); await showRun(activeRun);
  } catch(error) { message.textContent=error.message; message.className="error"; }
});
document.querySelector("#stop").addEventListener("click", async ()=>{
  if (!activeRun) return;
  try { await api(`/api/runs/${activeRun}/stop`,{method:"POST"}); message.textContent="Stop requested.";
    await refresh();
  } catch(error) { message.textContent=error.message; message.className="error"; }
});
refresh().catch(error=>{message.textContent=error.message;message.className="error"});
setInterval(()=>refresh().catch(()=>{}), 5000);
</script></body></html>
"""


class BenchmarkHandler(BaseHTTPRequestHandler):
    manager: BenchmarkManager
    server_version = "benchmarkservice/1.0"

    def log_message(self, format_string: str, *args: Any) -> None:
        sys.stdout.write(
            json.dumps(
                {
                    "timestamp": utc_now(),
                    "severity": "INFO",
                    "message": format_string % args,
                    "client": self.client_address[0],
                }
            )
            + "\n"
        )

    def _json(self, status: HTTPStatus, value: Any) -> None:
        body = json.dumps(value, sort_keys=True).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: HTTPStatus, message: str) -> None:
        self._json(status, {"error": message})

    def _send_file(self, path: Path, content_type: str, download_name: str) -> None:
        try:
            size = path.stat().st_size
            source = path.open("rb")
        except OSError:
            self._error(HTTPStatus.NOT_FOUND, "artifact not found")
            return
        with source:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(size))
            self.send_header(
                "Content-Disposition", f'attachment; filename="{download_name}"'
            )
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            while chunk := source.read(64 * 1024):
                self.wfile.write(chunk)

    def _parts(self) -> list[str]:
        return [part for part in urlparse(self.path).path.split("/") if part]

    def do_GET(self) -> None:
        parts = self._parts()
        try:
            if not parts:
                body = HTML.encode()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif parts == ["healthz"] or parts == ["readyz"]:
                self._json(HTTPStatus.OK, {"status": "ok"})
            elif parts == ["api", "info"]:
                self._json(
                    HTTPStatus.OK,
                    {
                        "application_type": self.manager.application_type,
                        "target_url": self.manager.target_url,
                    },
                )
            elif parts == ["api", "runs"]:
                self._json(HTTPStatus.OK, self.manager.list_runs())
            elif len(parts) == 3 and parts[:2] == ["api", "runs"]:
                self._json(HTTPStatus.OK, self.manager.details(parts[2]))
            elif len(parts) == 4 and parts[:2] == ["api", "runs"]:
                run_id, artifact = parts[2], parts[3]
                directory = self.manager._run_directory(run_id)
                if artifact == "summary.json":
                    self._send_file(
                        directory / artifact, "application/json", f"{run_id}-summary.json"
                    )
                elif artifact == "business.csv":
                    self._send_file(
                        directory / artifact, "text/csv", f"{run_id}-business.csv"
                    )
                elif artifact == "artifacts.zip":
                    self._send_file(
                        self.manager.create_archive(run_id),
                        "application/zip",
                        f"{run_id}-artifacts.zip",
                    )
                else:
                    self._error(HTTPStatus.NOT_FOUND, "unknown artifact")
            else:
                self._error(HTTPStatus.NOT_FOUND, "not found")
        except RunNotFound:
            self._error(HTTPStatus.NOT_FOUND, "run not found")
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))

    def do_POST(self) -> None:
        parts = self._parts()
        try:
            if parts == ["api", "runs"]:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > 64 * 1024:
                    raise ConfigError("request body must contain a small JSON object")
                payload = json.loads(self.rfile.read(length))
                if not isinstance(payload, dict):
                    raise ConfigError("request body must be a JSON object")
                self._json(HTTPStatus.CREATED, self.manager.start(payload))
            elif (
                len(parts) == 4
                and parts[:2] == ["api", "runs"]
                and parts[3] == "stop"
            ):
                self._json(HTTPStatus.ACCEPTED, self.manager.stop(parts[2]))
            else:
                self._error(HTTPStatus.NOT_FOUND, "not found")
        except (ConfigError, json.JSONDecodeError) as exc:
            self._error(HTTPStatus.BAD_REQUEST, str(exc))
        except RunConflict as exc:
            self._error(HTTPStatus.CONFLICT, str(exc))
        except RunNotFound:
            self._error(HTTPStatus.NOT_FOUND, "run not found")


def main() -> None:
    manager = BenchmarkManager(
        Path(os.environ.get("RESULTS_DIR", "/var/lib/benchmarkservice")),
        os.environ.get("APPLICATION_TYPE", ""),
        os.environ.get("FRONTEND_ADDR", "frontend:80"),
    )
    BenchmarkHandler.manager = manager
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), BenchmarkHandler)
    print(
        json.dumps(
            {
                "timestamp": utc_now(),
                "severity": "INFO",
                "message": "benchmark controller listening",
                "port": port,
                "application_type": manager.application_type,
                "target_url": manager.target_url,
            }
        ),
        flush=True,
    )

    def request_shutdown(signum: int, frame: Any) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    try:
        server.serve_forever()
    finally:
        manager.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
