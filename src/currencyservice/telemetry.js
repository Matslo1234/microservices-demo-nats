/* Copyright 2026 Google LLC. Licensed under the Apache License, Version 2.0. */
'use strict';

const {
  context,
  propagation,
  trace,
  SpanKind,
  SpanStatusCode,
} = require('@opentelemetry/api');

const tracer = trace.getTracer('online-boutique/messaging');

function normalized(value) {
  return value || 'unknown';
}

function messageAttributes({ subject, kind, messageId, correlationId }) {
  return {
    'messaging.system': 'nats',
    'messaging.destination.name': subject,
    'messaging.operation.type': kind,
    'messaging.message.id': normalized(messageId),
    'correlation.id': normalized(correlationId),
  };
}

function extractedContext(envelope = {}) {
  const carrier = {};
  if (envelope.traceparent) carrier.traceparent = envelope.traceparent;
  if (envelope.tracestate) carrier.tracestate = envelope.tracestate;
  return propagation.extract(context.active(), carrier);
}

function recordError(span, error) {
  if (!error) return;
  span.recordException(error);
  span.setStatus({ code: SpanStatusCode.ERROR, message: 'operation failed' });
}

function finishWithinSpan(parentContext, name, options, work) {
  return tracer.startActiveSpan(name, options, parentContext, span => {
    try {
      const result = work(span);
      if (result && typeof result.then === 'function') {
        return result.catch(error => {
          recordError(span, error);
          throw error;
        }).finally(() => span.end());
      }
      span.end();
      return result;
    } catch (error) {
      recordError(span, error);
      span.end();
      throw error;
    }
  });
}

function withConsumerSpan(envelope, options, work) {
  const subject = options.subject;
  return finishWithinSpan(
    extractedContext(envelope),
    `${subject} process`,
    {
      kind: SpanKind.CONSUMER,
      attributes: messageAttributes(options),
    },
    work,
  );
}

function withProducerSpan(options, work) {
  const subject = options.subject;
  return finishWithinSpan(
    context.active(),
    `${subject} publish`,
    {
      kind: SpanKind.PRODUCER,
      attributes: messageAttributes(options),
    },
    work,
  );
}

function injectEnvelope(envelope) {
  const carrier = {};
  propagation.inject(context.active(), carrier);
  if (carrier.traceparent) envelope.traceparent = carrier.traceparent;
  if (carrier.tracestate) envelope.tracestate = carrier.tracestate;
  else if (carrier.traceparent) envelope.tracestate = '';
  return envelope;
}

module.exports = {
  injectEnvelope,
  recordError,
  withConsumerSpan,
  withProducerSpan,
};

