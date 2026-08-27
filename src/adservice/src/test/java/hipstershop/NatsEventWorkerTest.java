/*
 * Copyright 2026 Google LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package hipstershop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import boutique.common.v1.MessageEnvelope;
import boutique.events.v1.StorefrontPageViewedEvent;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

final class NatsEventWorkerTest {
  @Test
  void freshestPageViewsDiscardsStaleAndSupersededInputs() {
    Instant now = Instant.parse("2026-08-27T14:00:00Z");
    NatsEventWorker.PageViewInput stale = input("stale", 1, now.minusSeconds(6));
    NatsEventWorker.PageViewInput superseded = input("active", 1, now.minusMillis(2));
    NatsEventWorker.PageViewInput newest = input("active", 2, now.minusMillis(1));
    NatsEventWorker.PageViewInput other = input("other", 1, now);

    NatsEventWorker.FilteredPageViews filtered =
        NatsEventWorker.freshestPageViews(
            List.of(stale, superseded, newest, other), now, Duration.ofSeconds(5));

    assertEquals(List.of(newest, other), filtered.retained());
    assertEquals(List.of(stale, superseded), filtered.discarded());
  }

  @Test
  void freshestPageViewsRetainsMalformedInputForNormalRedelivery() {
    NatsEventWorker.PageViewInput malformed =
        new NatsEventWorker.PageViewInput(
            null, null, null, new IllegalArgumentException("invalid"), 0, Instant.EPOCH);

    NatsEventWorker.FilteredPageViews filtered =
        NatsEventWorker.freshestPageViews(List.of(malformed), Instant.now(), Duration.ofSeconds(5));

    assertEquals(List.of(malformed), filtered.retained());
    assertTrue(filtered.discarded().isEmpty());
  }

  @Test
  void freshestPageViewsRetainsFutureAndTimestampFreeInputs() {
    Instant now = Instant.parse("2026-08-27T14:00:00Z");
    NatsEventWorker.PageViewInput future = input("future", 1, now.plusSeconds(1));
    NatsEventWorker.PageViewInput timestampFree = input("epoch", 1, Instant.EPOCH);

    NatsEventWorker.FilteredPageViews filtered =
        NatsEventWorker.freshestPageViews(
            List.of(future, timestampFree), now, Duration.ofSeconds(5));

    assertEquals(List.of(future, timestampFree), filtered.retained());
    assertTrue(filtered.discarded().isEmpty());
  }

  private static NatsEventWorker.PageViewInput input(
      String sessionId, long version, Instant occurredAt) {
    StorefrontPageViewedEvent pageView =
        StorefrontPageViewedEvent.newBuilder().setSessionId(sessionId).build();
    MessageEnvelope source =
        MessageEnvelope.newBuilder()
            .setMessageId(sessionId + "-" + version)
            .setAggregateVersion(version)
            .setOccurredAt(
                Timestamp.newBuilder()
                    .setSeconds(occurredAt.getEpochSecond())
                    .setNanos(occurredAt.getNano()))
            .setData(Any.pack(pageView))
            .build();
    return new NatsEventWorker.PageViewInput(null, source, pageView, null, version, occurredAt);
  }
}
