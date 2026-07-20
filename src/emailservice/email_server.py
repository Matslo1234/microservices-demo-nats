#!/usr/bin/python
# Copyright 2018 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

import os
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from logger import getJSONLogger
from nats_worker import messaging_ready, start_nats_worker, stop_nats_worker

logger = getJSONLogger("emailservice-server")


class HealthHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    if self.path == "/healthz":
      self._reply(200, "ok\n")
      return
    if self.path == "/readyz":
      self._reply(200 if messaging_ready() else 503,
                  "ok\n" if messaging_ready() else "email NATS consumer is not ready\n")
      return
    if self.path == "/metrics":
      ready = 1 if messaging_ready() else 0
      self._reply(
          200,
          f'boutique_dependency_ready{{service="emailservice",dependency="nats"}} {ready}\n'
          f'boutique_dependency_ready{{service="emailservice",dependency="provider_store"}} {ready}\n',
          "text/plain; version=0.0.4")
      return
    self._reply(404, "not found\n")

  def _reply(self, status, body, content_type="text/plain"):
    encoded = body.encode()
    self.send_response(status)
    self.send_header("Content-Type", content_type)
    self.send_header("Content-Length", str(len(encoded)))
    self.end_headers()
    self.wfile.write(encoded)

  def log_message(self, *_):
    return


def configure_tracing():
  if os.getenv("ENABLE_TRACING") != "1":
    logger.info("tracing disabled")
    return
  endpoint = os.getenv("COLLECTOR_SERVICE_ADDR", "localhost:4317")
  trace.set_tracer_provider(TracerProvider())
  trace.get_tracer_provider().add_span_processor(
      BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))


def start():
  configure_tracing()
  start_nats_worker()
  port = int(os.environ.get("PORT", "8080"))
  server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
  shutdown = threading.Event()

  def stop(*_):
    shutdown.set()
    threading.Thread(target=server.shutdown, daemon=True).start()

  signal.signal(signal.SIGINT, stop)
  signal.signal(signal.SIGTERM, stop)
  logger.info("email event worker health server listening", extra={"port": port})
  try:
    server.serve_forever()
  finally:
    stop_nats_worker()
    server.server_close()


if __name__ == "__main__":
  logger.info("starting the email event worker")
  try:
    start()
  except Exception:
    logger.exception("email service stopped")
    raise
