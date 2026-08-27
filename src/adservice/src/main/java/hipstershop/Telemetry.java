/* Copyright 2026 Google LLC.
 * Licensed under the Apache License, Version 2.0 (the "License"); */

package hipstershop;

import boutique.common.v1.MessageEnvelope;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import java.util.List;
import org.apache.logging.log4j.ThreadContext;

final class Telemetry {
  private static boolean enabled;
  private static final TextMapGetter<MessageEnvelope> GETTER =
      new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(MessageEnvelope carrier) {
          return List.of("traceparent", "tracestate");
        }

        @Override
        public String get(MessageEnvelope carrier, String key) {
          if (carrier == null) {
            return null;
          }
          return switch (key) {
            case "traceparent" -> carrier.getTraceparent();
            case "tracestate" -> carrier.getTracestate();
            default -> null;
          };
        }
      };
  private static final TextMapSetter<MessageEnvelope.Builder> SETTER =
      (carrier, key, value) -> {
        if (carrier == null) {
          return;
        }
        if ("traceparent".equals(key)) {
          carrier.setTraceparent(value);
        } else if ("tracestate".equals(key)) {
          carrier.setTracestate(value);
        }
      };

  private static OpenTelemetry openTelemetry = OpenTelemetry.noop();
  private static Tracer tracer = openTelemetry.getTracer("online-boutique/messaging");

  private Telemetry() {}

  static void initialize() {
    if (!"1".equals(System.getenv("ENABLE_TRACING"))) {
      return;
    }
    enabled = true;
    openTelemetry = AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();
    tracer = openTelemetry.getTracer("online-boutique/messaging");
  }

  static MessageSpan consumer(MessageEnvelope envelope, String subject, String kind) {
    if (!enabled) {
      return MessageSpan.noop();
    }
    Context parent =
        openTelemetry.getPropagators().getTextMapPropagator().extract(Context.root(), envelope, GETTER);
    String correlationId = normalized(envelope == null ? "" : envelope.getCorrelationId());
    String messageId = normalized(envelope == null ? "" : envelope.getMessageId());
    Span span =
        tracer
            .spanBuilder(subject + " process")
            .setParent(parent)
            .setSpanKind(SpanKind.CONSUMER)
            .setAttribute("messaging.system", "nats")
            .setAttribute("messaging.destination.name", subject)
            .setAttribute("messaging.operation.type", kind)
            .setAttribute("messaging.message.id", messageId)
            .setAttribute("correlation.id", correlationId)
            .startSpan();
    return new MessageSpan(span);
  }

  static void inject(MessageEnvelope.Builder envelope) {
    if (!enabled) {
      return;
    }
    openTelemetry
        .getPropagators()
        .getTextMapPropagator()
        .inject(Context.current(), envelope, SETTER);
  }

  private static String normalized(String value) {
    return value == null || value.isBlank() ? "unknown" : value;
  }

  static final class MessageSpan implements AutoCloseable {
    private static final MessageSpan NOOP = new MessageSpan();
    private final Span span;
    private final Scope scope;

    private MessageSpan() {
      this.span = null;
      this.scope = null;
    }

    static MessageSpan noop() {
      return NOOP;
    }

    private MessageSpan(Span span) {
      this.span = span;
      this.scope = span.makeCurrent();
      if (span.getSpanContext().isValid()) {
        ThreadContext.put("traceId", span.getSpanContext().getTraceId());
        ThreadContext.put("spanId", span.getSpanContext().getSpanId());
        ThreadContext.put("traceSampled", Boolean.toString(span.getSpanContext().isSampled()));
      }
    }

    void recordError(Throwable error) {
      if (span == null) {
        return;
      }
      span.recordException(error);
      span.setStatus(StatusCode.ERROR, "operation failed");
    }

    @Override
    public void close() {
      if (span == null) {
        return;
      }
      scope.close();
      span.end();
      ThreadContext.remove("traceId");
      ThreadContext.remove("spanId");
      ThreadContext.remove("traceSampled");
    }
  }
}
