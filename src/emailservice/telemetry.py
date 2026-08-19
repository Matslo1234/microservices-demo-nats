# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

from contextlib import contextmanager

from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind


_TRACER = trace.get_tracer("online-boutique/messaging")


def _normalized(value):
  return value or "unknown"


def _attributes(subject, kind, envelope):
  return {
      "messaging.system": "nats",
      "messaging.destination.name": subject,
      "messaging.operation.type": kind,
      "messaging.message.id": _normalized(
          getattr(envelope, "message_id", "")),
      "correlation.id": _normalized(
          getattr(envelope, "correlation_id", "")),
  }


@contextmanager
def consumer_span(envelope, subject, kind="event"):
  carrier = {}
  if envelope is not None:
    if envelope.traceparent:
      carrier["traceparent"] = envelope.traceparent
    if envelope.tracestate:
      carrier["tracestate"] = envelope.tracestate
  parent = propagate.extract(carrier)
  with _TRACER.start_as_current_span(
      f"{subject} process",
      context=parent,
      kind=SpanKind.CONSUMER,
      attributes=_attributes(subject, kind, envelope),
  ) as span:
    yield span


def inject_envelope(envelope):
  carrier = {}
  propagate.inject(carrier)
  if carrier.get("traceparent"):
    envelope.traceparent = carrier["traceparent"]
    envelope.tracestate = carrier.get("tracestate", "")
  return envelope

