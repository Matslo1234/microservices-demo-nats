# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from __future__ import annotations

import json
import os
import signal
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from config import ConfigError
from control import BenchmarkManager, RunConflict, RunNotFound, utc_now
from kubernetes_jobs import KubernetesJobClient
from shared_store import NatsSharedStore


HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Online Boutique benchmark</title>
  <style>
    :root { color-scheme:light; --ink:#17202a; --muted:#617080; --line:#d9e0e7;
      --paper:#fff; --bg:#f3f6f8; --brand:#174ea6; --bad:#b3261e; }
    * { box-sizing:border-box } body { margin:0; font:15px/1.5 system-ui,sans-serif;
      color:var(--ink); background:var(--bg) } main { max-width:1100px; margin:auto;
      padding:32px 20px } h1,h2 { line-height:1.15 } .card { background:var(--paper);
      border:1px solid var(--line); border-radius:12px; padding:20px; margin:16px 0 }
    .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
      gap:14px } label { display:flex; flex-direction:column; gap:5px; color:var(--muted);
      font-size:13px } input,select,button { font:inherit; padding:9px 10px;
      border:1px solid #aab5c0; border-radius:7px } button { background:var(--brand);
      color:#fff; border:0; cursor:pointer; font-weight:650 } button.danger { background:var(--bad) }
    button:disabled { opacity:.5; cursor:not-allowed } .actions { display:flex; gap:12px;
      margin-top:18px } table { width:100%; border-collapse:collapse } th,td { padding:9px;
      border-bottom:1px solid var(--line); text-align:left } th { color:var(--muted);
      font-size:12px; text-transform:uppercase } .pill { display:inline-block;
      border-radius:100px; padding:2px 8px; background:#e8eef8; font-size:12px }
    .error { color:var(--bad) } .muted { color:var(--muted) } pre { white-space:pre-wrap;
      overflow:auto; background:#f7f8fa; padding:12px; border-radius:8px }
  </style>
</head>
<body><main>
  <h1>Online Boutique benchmark</h1>
  <p class="muted">Target: <strong id="application"></strong> at <code id="target"></code>.
    Runs are shared across every API replica and execute in disposable Kubernetes Jobs.</p>
  <section class="card"><h2>New run</h2>
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
  <section class="card" id="details-card" hidden><h2>Result</h2><div id="details"></div></section>
</main>
<script>
let activeRun=null; const message=document.querySelector("#message");
const fields=["warmup_seconds","duration_seconds","drain_seconds","users","spawn_rate",
  "arrival_rate","outcome_timeout_seconds","settlement_timeout_seconds",
  "resource_sample_interval_seconds","seed"];
async function api(path,options={}) {
  const response=await fetch(path,options), data=await response.json().catch(()=>({error:response.statusText}));
  if(!response.ok) throw new Error(data.error||response.statusText); return data;
}
function esc(value) { return String(value??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",
  ">":"&gt;",'"':"&quot;","'":"&#39;"}[c])); }
async function refresh() {
  const info=await api("/api/info"); document.querySelector("#application").textContent=info.application_type;
  document.querySelector("#target").textContent=info.target_url; const runs=await api("/api/runs");
  activeRun=runs.find(run=>["submitted","starting","running","stopping"].includes(run.state))?.run_id||null;
  document.querySelector("#start").disabled=!!activeRun; document.querySelector("#stop").disabled=!activeRun;
  document.querySelector("#runs").innerHTML=runs.length?`<table><thead><tr><th>Run</th>
    <th>Workload</th><th>Status</th><th>Completed</th><th>p95</th><th></th></tr></thead>
    <tbody>${runs.map(run=>`<tr><td><code>${esc(run.run_id)}</code></td><td>${esc(run.workload)}</td>
    <td><span class="pill">${esc(run.state)}</span></td><td>${esc(run.completed_orders)}</td>
    <td>${run.p95_ms==null?"":esc(run.p95_ms)+" ms"}</td><td><button
    onclick="showRun('${esc(run.run_id)}')">View</button></td></tr>`).join("")}</tbody></table>`:
    `<p class="muted">No benchmark has been run.</p>`;
}
async function showRun(id) {
  const run=await api(`/api/runs/${id}`), card=document.querySelector("#details-card"); card.hidden=false;
  document.querySelector("#details").innerHTML=run.summary?`<div class="actions">
    <a href="/api/runs/${id}/summary.json">Summary JSON</a>
    <a href="/api/runs/${id}/business.csv">Business CSV</a>
    <a href="/api/runs/${id}/artifacts.zip">All artifacts</a></div>
    <pre>${esc(JSON.stringify(run.summary,null,2))}</pre>`:
    `<p>Status: <span class="pill">${esc(run.status.state)}</span></p>
    <pre>${esc(JSON.stringify(run.status,null,2))}</pre>`;
}
document.querySelector("#run-form").addEventListener("submit",async event=>{
  event.preventDefault(); message.textContent="Submitting Job…"; const form=new FormData(event.target);
  const payload={workload:form.get("workload"),collect_resources:form.has("collect_resources")};
  fields.forEach(name=>payload[name]=form.get(name));
  try { const run=await api("/api/runs",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify(payload)}); activeRun=run.status.run_id; message.textContent="Benchmark submitted.";
    await refresh(); await showRun(activeRun); } catch(error) { message.textContent=error.message;
    message.className="error"; }
});
document.querySelector("#stop").addEventListener("click",async()=>{
  if(!activeRun)return; try { await api(`/api/runs/${activeRun}/stop`,{method:"POST"});
    message.textContent="Benchmark stopped."; activeRun=null; await refresh(); }
  catch(error) { message.textContent=error.message; message.className="error"; }
});
refresh().catch(error=>{message.textContent=error.message;message.className="error"});
setInterval(()=>refresh().catch(()=>{}),3000);
</script></body></html>"""


class BenchmarkHandler(BaseHTTPRequestHandler):
    manager: BenchmarkManager
    server_version = "benchmarkservice/2.0"

    def log_message(self, format_string: str, *args: Any) -> None:
        print(
            json.dumps(
                {
                    "timestamp": utc_now(),
                    "severity": "INFO",
                    "message": format_string % args,
                    "client": self.client_address[0],
                }
            ),
            flush=True,
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

    def _send_artifact(
        self, run_id: str, artifact: str, content_type: str, name: str
    ) -> None:
        data = self.manager.artifact(run_id, artifact)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Content-Disposition", f'attachment; filename="{name}"')
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

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
            elif parts == ["healthz"]:
                self._json(HTTPStatus.OK, {"status": "ok"})
            elif parts == ["readyz"]:
                ready = self.manager.ready()
                self._json(
                    HTTPStatus.OK if ready else HTTPStatus.SERVICE_UNAVAILABLE,
                    {"status": "ok" if ready else "dependencies unavailable"},
                )
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
                options = {
                    "summary.json": ("application/json", f"{run_id}-summary.json"),
                    "business.csv": ("text/csv", f"{run_id}-business.csv"),
                    "artifacts.zip": ("application/zip", f"{run_id}-artifacts.zip"),
                }
                if artifact not in options:
                    self._error(HTTPStatus.NOT_FOUND, "unknown artifact")
                    return
                self._send_artifact(run_id, artifact, *options[artifact])
            else:
                self._error(HTTPStatus.NOT_FOUND, "not found")
        except RunNotFound:
            self._error(HTTPStatus.NOT_FOUND, "run or artifact not found")
        except Exception as error:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))

    def do_POST(self) -> None:
        parts = self._parts()
        try:
            if parts == ["api", "runs"]:
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0 or length > 64 * 1024:
                    raise ConfigError(
                        "request body must contain a small JSON object"
                    )
                payload = json.loads(self.rfile.read(length))
                if not isinstance(payload, dict):
                    raise ConfigError("request body must be a JSON object")
                self._json(HTTPStatus.CREATED, self.manager.start(payload))
            elif (
                len(parts) == 4
                and parts[:2] == ["api", "runs"]
                and parts[3] == "stop"
            ):
                self._json(
                    HTTPStatus.ACCEPTED, self.manager.stop(parts[2])
                )
            else:
                self._error(HTTPStatus.NOT_FOUND, "not found")
        except (ConfigError, json.JSONDecodeError) as error:
            self._error(HTTPStatus.BAD_REQUEST, str(error))
        except RunConflict as error:
            self._error(HTTPStatus.CONFLICT, str(error))
        except RunNotFound:
            self._error(HTTPStatus.NOT_FOUND, "run not found")
        except Exception as error:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, str(error))


def main() -> None:
    store = NatsSharedStore()
    manager = BenchmarkManager(
        store,
        KubernetesJobClient(),
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
                "message": "stateless benchmark API listening",
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
