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
      color:var(--ink); background:var(--bg) } main { max-width:1240px; margin:auto;
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
    .card-heading { display:flex; align-items:center; justify-content:space-between;
      gap:12px; margin:.83em 0 } .card-heading h2 { margin:0 }
    .run-actions { display:flex; gap:8px } .run-select { width:18px; height:18px;
      padding:0 } #runs { overflow-x:auto }
    .error { color:var(--bad) } .muted { color:var(--muted) } pre { white-space:pre-wrap;
      overflow:auto; background:#f7f8fa; padding:12px; border-radius:8px }
  </style>
</head>
<body><main>
  <h1>Online Boutique benchmark</h1>
  <p class="muted">Application type: <strong id="application"></strong>. Each run targets
    an explicit application location and reads metrics from the cluster being tested.</p>
  <section class="card"><h2>New run</h2>
    <form id="run-form"><div class="grid">
      <label>Target application URL<input name="target_url" type="text"
        inputmode="url" placeholder="local or https://shop.example.com" required></label>
      <label>Target metrics URL<input name="metrics_url" type="text"
        inputmode="url" placeholder="local or https://metrics.example.com/snapshot" required></label>
      <label>Workload<select name="workload"><option value="closed">Closed-loop users</option>
        <option value="open">Open-loop capacity</option>
        <option value="saturation">Open-loop saturation ladder</option></select></label>
      <label>Warm-up (seconds)<input name="warmup_seconds" type="number" min="0" value="30"></label>
      <label>Steady interval (seconds)<input name="duration_seconds" type="number" min="1" value="120"></label>
      <label>Drain (seconds)<input name="drain_seconds" type="number" min="1" value="60"></label>
      <label>Closed-loop users<input name="users" type="number" min="1" value="10"></label>
      <label>User spawn rate/s<input name="spawn_rate" type="number" min=".01" step=".01" value="1"></label>
      <label>Open-loop orders/s<input name="arrival_rate" type="number" min=".01" step=".01" value="1"></label>
      <label>Saturation starting orders/s<input name="saturation_start_rate"
        type="number" min=".01" max="10000" step=".01" value="10"></label>
      <label>Saturation maximum orders/s<input name="saturation_max_rate"
        type="number" min=".01" max="10000" step=".01" value="1000"></label>
      <label>Saturation rung (seconds)<input name="saturation_step_seconds"
        type="number" min="10" max="300" value="30"></label>
      <label>Outcome timeout (seconds)<input name="outcome_timeout_seconds" type="number" min="1" value="30"></label>
      <label>Settlement timeout (seconds)<input name="settlement_timeout_seconds" type="number" min="1" value="60"></label>
      <label>Resource sample interval<input name="resource_sample_interval_seconds" type="number" min="1" value="5"></label>
      <label>Random seed<input name="seed" type="number" min="0" value="1"></label>
      <label>Number of re-runs<input id="rerun-count" name="rerun_count" type="number"
        min="0" step="1" value="0" required></label>
      <label>Delay between re-runs (seconds)<input id="rerun-delay"
        name="rerun_delay_seconds" type="number" min="0" step="1" value="0"
        required disabled></label>
      <label><span>Collection</span><span><input name="collect_resources" type="checkbox" checked>
        Runtime resources</span></label>
    </div><div class="actions"><button id="start" type="submit">Start benchmark</button>
      <button id="stop" class="danger" type="button" disabled>Stop active run</button></div>
    <p class="muted">Use <code>local</code> for either target to use the application
      or collect metrics in the cluster running benchmarkservice.</p>
    <p class="muted">The saturation ladder starts at the configured rate (10 orders/s
      by default) and adds 10 every configured rung interval (30 seconds by default),
      holding at the maximum rate once reached. Backlog growth and falling goodput are
      recorded without ending the run.
      Re-runs use the same settings and produce separate results. Keep this browser tab
      open until the sequence finishes.</p>
    <p id="message" class="muted" role="status" aria-live="polite"></p></form>
  </section>
  <section class="card"><div class="card-heading"><h2>Runs</h2>
    <div class="run-actions"><button id="download-selected" type="button" disabled>
      Download Selected JSONs</button>
      <button id="download-all" type="button" disabled>Download All</button></div></div>
    <div id="runs"></div></section>
  <section class="card" id="details-card" hidden><h2>Result</h2><div id="details"></div></section>
</main>
<script>
const ACTIVE_STATES=new Set(["submitted","starting","running","stopping"]);
let activeRun=null, repeatPlan=null, sequenceRequestInFlight=false, refreshPromise=null,
  selectedDownloadInFlight=false;
const selectedRuns=new Set();
const message=document.querySelector("#message"), startButton=document.querySelector("#start"),
  stopButton=document.querySelector("#stop"), rerunCount=document.querySelector("#rerun-count"),
  rerunDelay=document.querySelector("#rerun-delay"),
  workload=document.querySelector('[name="workload"]'),
  duration=document.querySelector('[name="duration_seconds"]'),
  downloadSelectedButton=document.querySelector("#download-selected"),
  downloadAllButton=document.querySelector("#download-all");
let previousWorkload=workload.value;
workload.addEventListener("change",()=>{
  if(workload.value==="saturation"&&duration.value==="120") duration.value="600";
  if(previousWorkload==="saturation"&&workload.value!=="saturation"&&duration.value==="600")
    duration.value="120";
  previousWorkload=workload.value;
});
const fields=["warmup_seconds","duration_seconds","drain_seconds","users","spawn_rate",
  "arrival_rate","saturation_start_rate","saturation_max_rate","saturation_step_seconds","outcome_timeout_seconds","settlement_timeout_seconds",
  "resource_sample_interval_seconds","seed"];
async function api(path,options={}) {
  const response=await fetch(path,options), data=await response.json().catch(()=>({error:response.statusText}));
  if(!response.ok) {
    const error=new Error(data.error||response.statusText); error.status=response.status; throw error;
  }
  return data;
}
async function summaryArchive(runIds) {
  const response=await fetch("/api/runs/summaries.zip",{method:"POST",
    headers:{"Content-Type":"application/json"},body:JSON.stringify({run_ids:runIds})});
  if(!response.ok) {
    const data=await response.json().catch(()=>({error:response.statusText}));
    const error=new Error(data.error||response.statusText); error.status=response.status; throw error;
  }
  return response.blob();
}
function esc(value) { return String(value??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",
  ">":"&gt;",'"':"&quot;","'":"&#39;"}[c])); }
function setMessage(text,isError=false) {
  message.textContent=text; message.className=isError?"error":"muted";
}
function updateControls() {
  startButton.disabled=!!activeRun||!!repeatPlan||sequenceRequestInFlight;
  stopButton.disabled=!activeRun&&!repeatPlan&&!sequenceRequestInFlight;
  stopButton.textContent=repeatPlan||sequenceRequestInFlight?
    "Stop benchmark sequence":"Stop active run";
  downloadSelectedButton.disabled=selectedRuns.size===0||selectedDownloadInFlight;
}
async function submitSequenceRun() {
  const plan=repeatPlan; if(!plan||sequenceRequestInFlight)return null;
  sequenceRequestInFlight=true; updateControls();
  try {
    const run=await api("/api/runs",{method:"POST",headers:{"Content-Type":"application/json"},
      body:JSON.stringify(plan.payload)});
    activeRun=run.status.run_id;
    if(repeatPlan!==plan) {
      try {
        await api(`/api/runs/${activeRun}/stop`,{method:"POST"});
        activeRun=null; setMessage("Benchmark sequence stopped.");
      } catch(error) {
        setMessage(`Sequence cancelled, but the submitted run could not be stopped: ${error.message}`,true);
      }
      return null;
    }
    plan.currentRunId=activeRun; plan.submittedRuns+=1; plan.nextRunAt=null;
    setMessage(`Run ${plan.submittedRuns} of ${plan.totalRuns} submitted.`);
    showRun(activeRun).catch(()=>{}); return run;
  } finally {
    sequenceRequestInFlight=false; updateControls();
  }
}
async function advanceRepeatPlan(runs) {
  const plan=repeatPlan; if(!plan||sequenceRequestInFlight)return;
  if(plan.currentRunId) {
    const current=runs.find(run=>run.run_id===plan.currentRunId);
    if(!current||ACTIVE_STATES.has(current.state))return;
    plan.currentRunId=null;
    if(current.state==="stopped") {
      repeatPlan=null; setMessage("Benchmark sequence stopped."); return;
    }
    if(plan.submittedRuns>=plan.totalRuns) {
      repeatPlan=null;
      setMessage(`Benchmark sequence finished (${plan.totalRuns} run${plan.totalRuns===1?"":"s"}).`);
      return;
    }
    const endedAt=Date.parse(current.ended_at||"");
    plan.nextRunAt=(Number.isFinite(endedAt)?endedAt:Date.now())+plan.delayMs;
  }
  if(plan.nextRunAt===null)return;
  const secondsLeft=Math.max(0,Math.ceil((plan.nextRunAt-Date.now())/1000));
  if(secondsLeft>0) {
    setMessage(`Run ${plan.submittedRuns} of ${plan.totalRuns} finished. Next re-run in ${secondsLeft}s.`);
    return;
  }
  if(activeRun) {
    setMessage(`Run ${plan.submittedRuns} of ${plan.totalRuns} finished. Waiting for the active benchmark.`);
    return;
  }
  try { await submitSequenceRun(); }
  catch(error) {
    if(repeatPlan!==plan)return;
    if(error.status===409) {
      setMessage("Waiting for the previous benchmark lease to be released."); return;
    }
    repeatPlan=null; updateControls();
    setMessage(`Could not submit the next re-run: ${error.message}`,true);
  }
}
async function refreshPage() {
  const info=await api("/api/info"); document.querySelector("#application").textContent=info.application_type;
  const runs=await api("/api/runs");
  activeRun=runs.find(run=>ACTIVE_STATES.has(run.state))?.run_id||null;
  const selectable=new Set(runs.filter(run=>run.summary_available).map(run=>run.run_id));
  for(const runId of selectedRuns)if(!selectable.has(runId))selectedRuns.delete(runId);
  await advanceRepeatPlan(runs); updateControls();
  downloadAllButton.disabled=!runs.some(run=>run.artifacts_available);
  document.querySelector("#runs").innerHTML=runs.length?`<table><thead><tr><th></th><th>Run</th>
    <th>Target</th><th>Workload</th><th>Status</th><th>Completed</th><th>p95</th><th></th></tr></thead>
    <tbody>${runs.map(run=>`<tr><td><input class="run-select" type="checkbox"
    value="${esc(run.run_id)}" aria-label="Select run ${esc(run.run_id)}"
    ${selectedRuns.has(run.run_id)?"checked":""} ${run.summary_available?"":"disabled"}></td>
    <td><code>${esc(run.run_id)}</code></td><td>${esc(run.target_url)}</td><td>${esc(run.workload)}</td>
    <td><span class="pill">${esc(run.state)}</span></td><td>${esc(run.completed_orders)}</td>
    <td>${run.p95_ms==null?"":esc(run.p95_ms)+" ms"}</td><td><div class="run-actions"><button
    type="button" onclick="showRun('${esc(run.run_id)}')">View</button><button type="button"
    onclick="downloadSummary('${esc(run.run_id)}')" ${run.summary_available?"":"disabled"}
    title="Download summary JSON">JSON</button></div></td></tr>`).join("")}</tbody></table>`:
    `<p class="muted">No benchmark has been run.</p>`;
}
function refresh() {
  if(!refreshPromise)refreshPromise=refreshPage().finally(()=>{refreshPromise=null});
  return refreshPromise;
}
function downloadSummary(id) {
  window.location.assign(`/api/runs/${encodeURIComponent(id)}/summary.json`);
}
async function downloadSelectedSummaries() {
  const runIds=[...selectedRuns]; if(!runIds.length)return;
  selectedDownloadInFlight=true; updateControls();
  try {
    let handle=null;
    if(typeof window.showSaveFilePicker==="function") {
      try {
        handle=await window.showSaveFilePicker({suggestedName:"benchmark-summaries.zip",
          types:[{description:"ZIP archive",accept:{"application/zip":[".zip"]}}]});
      } catch(error) { if(error.name==="AbortError")return; throw error; }
    }
    const archive=await summaryArchive(runIds);
    if(handle) {
      const writable=await handle.createWritable();
      await writable.write(archive); await writable.close();
    } else {
      const url=URL.createObjectURL(archive), link=document.createElement("a");
      link.href=url; link.download="benchmark-summaries.zip"; document.body.appendChild(link);
      link.click(); link.remove(); setTimeout(()=>URL.revokeObjectURL(url),1000);
    }
    setMessage(`Downloaded ${runIds.length} selected summary JSON${runIds.length===1?"":"s"}.`);
  } catch(error) { setMessage(`Could not download selected summaries: ${error.message}`,true); }
  finally { selectedDownloadInFlight=false; updateControls(); }
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
  event.preventDefault(); setMessage("Submitting Job…"); const form=new FormData(event.target);
  const payload={target_url:form.get("target_url"),metrics_url:form.get("metrics_url"),
    workload:form.get("workload"),collect_resources:form.has("collect_resources")};
  fields.forEach(name=>payload[name]=form.get(name));
  const additionalRuns=Number(form.get("rerun_count")||0),
    delaySeconds=Number(form.get("rerun_delay_seconds")||0);
  if(!Number.isInteger(additionalRuns)||additionalRuns<0||
    !Number.isInteger(delaySeconds)||delaySeconds<0) {
    setMessage("Re-runs and delay must be non-negative whole numbers.",true); return;
  }
  const plan={payload,totalRuns:additionalRuns+1,submittedRuns:0,currentRunId:null,
    delayMs:delaySeconds*1000,nextRunAt:null};
  repeatPlan=plan;
  try { await submitSequenceRun(); await refresh(); }
  catch(error) {
    if(repeatPlan===plan) { repeatPlan=null; setMessage(error.message,true); }
    updateControls();
  }
});
stopButton.addEventListener("click",async()=>{
  const plan=repeatPlan, runToStop=plan?plan.currentRunId:activeRun;
  if(!runToStop&&!plan&&!sequenceRequestInFlight)return;
  repeatPlan=null; updateControls();
  try {
    if(runToStop)await api(`/api/runs/${runToStop}/stop`,{method:"POST"});
    setMessage(plan?(runToStop?"Benchmark sequence stopped.":"Pending re-runs cancelled."):
      "Benchmark stopped.");
    if(activeRun===runToStop)activeRun=null; await refresh();
  } catch(error) { setMessage(error.message,true); await refresh().catch(()=>{}); }
});
function updateRerunDelay() { rerunDelay.disabled=Number(rerunCount.value||0)===0; }
document.querySelector("#runs").addEventListener("change",event=>{
  const checkbox=event.target;
  if(!checkbox.classList.contains("run-select"))return;
  if(checkbox.checked)selectedRuns.add(checkbox.value); else selectedRuns.delete(checkbox.value);
  updateControls();
});
downloadSelectedButton.addEventListener("click",downloadSelectedSummaries);
downloadAllButton.addEventListener("click",()=>{
  window.location.assign("/api/runs/artifacts.zip");
});
rerunCount.addEventListener("input",updateRerunDelay); updateRerunDelay(); updateControls();
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

    def _send_download(
        self, data: bytes, content_type: str, name: str
    ) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Content-Disposition", f'attachment; filename="{name}"')
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _send_artifact(
        self, run_id: str, artifact: str, content_type: str, name: str
    ) -> None:
        self._send_download(
            self.manager.artifact(run_id, artifact), content_type, name
        )

    def _parts(self) -> list[str]:
        return [part for part in urlparse(self.path).path.split("/") if part]

    def _read_json_object(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > 64 * 1024:
            raise ConfigError(
                "request body must contain a small JSON object"
            )
        payload = json.loads(self.rfile.read(length))
        if not isinstance(payload, dict):
            raise ConfigError("request body must be a JSON object")
        return payload

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
                    },
                )
            elif parts == ["api", "runs"]:
                self._json(HTTPStatus.OK, self.manager.list_runs())
            elif parts == ["api", "runs", "artifacts.zip"]:
                self._send_download(
                    self.manager.combined_artifacts(),
                    "application/zip",
                    "benchmark-runs.zip",
                )
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
            if parts == ["api", "runs", "summaries.zip"]:
                run_ids = self._read_json_object().get("run_ids")
                if (
                    not isinstance(run_ids, list)
                    or not run_ids
                    or len(run_ids) > 1_000
                    or any(not isinstance(run_id, str) for run_id in run_ids)
                ):
                    raise ConfigError(
                        "run_ids must be a non-empty list of at most 1000 strings"
                    )
                self._send_download(
                    self.manager.combined_summaries(run_ids),
                    "application/zip",
                    "benchmark-summaries.zip",
                )
            elif parts == ["api", "runs"]:
                self._json(
                    HTTPStatus.CREATED,
                    self.manager.start(self._read_json_object()),
                )
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
