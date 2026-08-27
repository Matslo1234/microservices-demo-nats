/*
 * Copyright 2026 Google LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

package hipstershop;

import boutique.common.v1.AdSelection;
import boutique.common.v1.MessageEnvelope;
import boutique.events.v1.AdSelectionGeneratedEvent;
import boutique.events.v1.StorefrontPageViewedEvent;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import hipstershop.Demo.Ad;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

final class NatsEventWorker implements AutoCloseable {
  private static final Logger logger = LogManager.getLogger(NatsEventWorker.class);
  private static final String PAGE_SUBJECT = "boutique.evt.storefront.page-viewed.v1";
  private static final String RESULT_SUBJECT = "boutique.evt.ad.selection-generated.v1";
  private static final String DURABLE = "ad-page-views-v1";
  private static final String CONFIG_REVISION = "static-ads-v1";
  private static final ThreadLocal<MessageDigest> SHA_256 =
      ThreadLocal.withInitial(
          () -> {
            try {
              return MessageDigest.getInstance("SHA-256");
            } catch (Exception exception) {
              throw new IllegalStateException("SHA-256 is unavailable", exception);
            }
          });
  private static final ThreadLocal<MessageDigest> MD5 =
      ThreadLocal.withInitial(
          () -> {
            try {
              return MessageDigest.getInstance("MD5");
            } catch (Exception exception) {
              throw new IllegalStateException("MD5 is unavailable", exception);
            }
          });

  private final AdService service;
  private final boolean required;
  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicBoolean consumerReady = new AtomicBoolean();
  private int batchSize;
  private int concurrency;
  private Duration pageViewMaxAge;
  private Connection connection;
  private ExecutorService eventExecutor;
  private Thread thread;

  NatsEventWorker(AdService service) {
    this.service = service;
    this.required = Boolean.parseBoolean(System.getenv().getOrDefault("NATS_REQUIRED", "false"));
  }

  void start() throws IOException {
    if (!required) {
      return;
    }
    for (String name :
        List.of(
            "NATS_URL",
            "NATS_USER",
            "NATS_PASSWORD",
            "NATS_CA_FILE",
            "REGION_ID",
            "K8S_CLUSTER_NAME")) {
      if (System.getenv(name) == null || System.getenv(name).isBlank()) {
        throw new IOException(name + " is required when NATS_REQUIRED=true");
      }
    }
    batchSize = boundedInteger("NATS_CONSUMER_BATCH_SIZE", 32, 1, 256);
    concurrency = boundedInteger("NATS_CONSUMER_CONCURRENCY", 8, 1, 64);
    pageViewMaxAge = duration("AD_PAGE_VIEW_MAX_AGE", Duration.ofSeconds(5));
    if (pageViewMaxAge.isNegative()) {
      throw new IllegalArgumentException("AD_PAGE_VIEW_MAX_AGE must not be negative");
    }
    AtomicInteger workerNumber = new AtomicInteger();
    eventExecutor =
        Executors.newFixedThreadPool(
            concurrency,
            task -> {
              Thread worker =
                  new Thread(
                      task, "adservice-nats-page-view-worker-" + workerNumber.incrementAndGet());
              worker.setDaemon(true);
              return worker;
            });
    running.set(true);
    thread = new Thread(this::connectAndConsume, "adservice-nats-page-views");
    thread.setDaemon(true);
    thread.start();
  }

  private void connectAndConsume() {
    while (running.get()) {
      try {
        Options options =
            new Options.Builder()
                .server(System.getenv("NATS_URL"))
                .userInfo(System.getenv("NATS_USER"), System.getenv("NATS_PASSWORD"))
                .connectionName(
                    "adservice/phase2/"
                        + System.getenv("REGION_ID")
                        + "/"
                        + System.getenv("K8S_CLUSTER_NAME"))
                .sslContext(trustContext(System.getenv("NATS_CA_FILE")))
                .connectionTimeout(duration("NATS_CONNECT_TIMEOUT", Duration.ofSeconds(2)))
                .reconnectWait(duration("NATS_RECONNECT_WAIT", Duration.ofSeconds(2)))
                .maxReconnects(integer("NATS_MAX_RECONNECTS", -1))
                .pingInterval(duration("NATS_PING_INTERVAL", Duration.ofSeconds(20)))
                .maxPingsOut(integer("NATS_MAX_PINGS_OUT", 2))
                .build();
        connection = Nats.connect(options);
        JetStream jetStream = connection.jetStream();
        ConsumerConfiguration consumer =
            ConsumerConfiguration.builder()
                .durable(DURABLE)
                .filterSubject(PAGE_SUBJECT)
                .deliverPolicy(DeliverPolicy.All)
                .ackPolicy(AckPolicy.Explicit)
                .ackWait(Duration.ofSeconds(30))
                .maxDeliver(10)
                .build();
        PullSubscribeOptions subscribeOptions =
            PullSubscribeOptions.builder().stream("BOUTIQUE_EVENTS")
                .durable(DURABLE)
                .configuration(consumer)
                .build();
        JetStreamSubscription subscription = jetStream.subscribe(PAGE_SUBJECT, subscribeOptions);
        consumerReady.set(true);
        logger.info(
            "NATS page-view consumer is ready batch_size={} concurrency={} max_age={}",
            batchSize,
            concurrency,
            pageViewMaxAge);
        consume(jetStream, subscription);
        consumerReady.set(false);
      } catch (InterruptedException exception) {
        consumerReady.set(false);
        if (!running.get()) {
          Thread.currentThread().interrupt();
          return;
        }
        logger.warn("interrupted while establishing NATS page-view consumer", exception);
      } catch (Exception exception) {
        consumerReady.set(false);
        if (running.get()) {
          logger.warn("NATS page-view consumer is unavailable; retrying", exception);
        }
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
          return;
        }
        connection = null;
      }
      if (running.get()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException exception) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  boolean ready() {
    return !required
        || (running.get()
            && consumerReady.get()
            && connection != null
            && connection.getStatus() == Connection.Status.CONNECTED);
  }

  private void consume(JetStream jetStream, JetStreamSubscription subscription) throws Exception {
    while (running.get()) {
      try {
        Iterator<Message> messages = subscription.iterate(batchSize, Duration.ofSeconds(1));
        List<PageViewInput> fetched = new ArrayList<>();
        while (running.get() && messages.hasNext()) {
          fetched.add(decodeEnvelope(messages.next()));
        }
        FilteredPageViews filtered = freshestPageViews(fetched, Instant.now(), pageViewMaxAge);
        for (PageViewInput discarded : filtered.discarded()) {
          discarded.message().ack();
        }
        if (!filtered.discarded().isEmpty()) {
          logger.debug(
              "discarded stale or superseded page views count={}", filtered.discarded().size());
        }
        List<Future<CompletableFuture<Void>>> dispatches = new ArrayList<>();
        for (PageViewInput input : filtered.retained()) {
          dispatches.add(eventExecutor.submit(() -> processMessage(jetStream, input)));
        }
        List<CompletableFuture<Void>> confirmations = new ArrayList<>(dispatches.size());
        for (Future<CompletableFuture<Void>> dispatch : dispatches) {
          confirmations.add(dispatch.get());
        }
        CompletableFuture.allOf(confirmations.toArray(CompletableFuture[]::new)).get();
      } catch (Exception exception) {
        if (running.get()) {
          logger.warn("failed to fetch page-view events", exception);
          throw exception;
        }
        return;
      }
    }
  }

  private CompletableFuture<Void> processMessage(JetStream jetStream, PageViewInput input) {
    Message message = input.message();
    ThreadContext.put("correlation_id", "unknown");
    ThreadContext.put("message_id", "unknown");
    MessageEnvelope source = input.source();
    if (source != null) {
      if (!source.getCorrelationId().isBlank()) {
        ThreadContext.put("correlation_id", source.getCorrelationId());
      }
      if (!source.getMessageId().isBlank()) {
        ThreadContext.put("message_id", source.getMessageId());
      }
    }
    logger.debug(
        "NATS event received topic={} message_id={} correlation_id={}",
        message.getSubject(),
        ThreadContext.get("message_id"),
        ThreadContext.get("correlation_id"));
    Telemetry.MessageSpan telemetry = Telemetry.consumer(source, message.getSubject(), "event");
    try {
      if (input.decodeException() != null) {
        throw input.decodeException();
      }
      CompletableFuture<?> confirmation = handle(jetStream, input);
      return confirmation.handle(
          (ignored, error) -> {
            if (error == null) {
              message.ack();
            } else {
              logger.warn(
                  "page-view result publish failed topic={} message_id={} correlation_id={}",
                  message.getSubject(),
                  source.getMessageId(),
                  source.getCorrelationId(),
                  error);
              message.nakWithDelay(Duration.ofSeconds(1));
            }
            return null;
          });
    } catch (Exception exception) {
      telemetry.recordError(exception);
      logger.warn(
          "page-view event processing failed topic={} message_id={} correlation_id={}",
          message.getSubject(),
          ThreadContext.get("message_id"),
          ThreadContext.get("correlation_id"),
          exception);
      message.nakWithDelay(Duration.ofSeconds(1));
      return CompletableFuture.completedFuture(null);
    } finally {
      telemetry.close();
      ThreadContext.remove("correlation_id");
      ThreadContext.remove("message_id");
    }
  }

  private CompletableFuture<?> handle(JetStream jetStream, PageViewInput input) throws Exception {
    MessageEnvelope source = input.source();
    StorefrontPageViewedEvent pageView = input.pageView();
    if (pageView == null) {
      pageView = source.getData().unpack(StorefrontPageViewedEvent.class);
    }
    long version = input.version();
    Instant eventTime = input.eventTime();
    List<Ad> selected =
        service.selectAds(
            pageView.getCategoryIdsList(), seed(source.getMessageId() + "\0" + CONFIG_REVISION));
    AdSelectionGeneratedEvent.Builder payload =
        AdSelectionGeneratedEvent.newBuilder()
            .setSessionId(pageView.getSessionId())
            .setTriggeringPageType(pageView.getPageType())
            .setExpiresAt(timestamp(eventTime.plus(Duration.ofMinutes(10))));
    for (Ad ad : selected) {
      payload.addAds(
          AdSelection.newBuilder()
              .setRedirectUrl(ad.getRedirectUrl())
              .setText(ad.getText())
              .build());
    }
    String messageId =
        nameUuid(
                (RESULT_SUBJECT + "\0" + source.getMessageId() + "\0" + CONFIG_REVISION)
                    .getBytes(StandardCharsets.UTF_8))
            .toString();
    MessageEnvelope.Builder result =
        MessageEnvelope.newBuilder()
            .setMessageId(messageId)
            .setMessageType("boutique.ad.SelectionGenerated.v1")
            .setSchemaVersion(1)
            .setOccurredAt(timestamp(eventTime))
            .setProducer("adservice/phase2")
            .setAggregateType("ad-context")
            .setAggregateId(pageView.getSessionId())
            .setAggregateVersion(version)
            .setCorrelationId(source.getCorrelationId())
            .setCausationId(source.getMessageId())
            .setTraceparent(source.getTraceparent())
            .setTracestate(source.getTracestate())
            .setData(Any.pack(payload.build()));
    Telemetry.inject(result);
    CompletableFuture<?> published = jetStream.publishAsync(
        RESULT_SUBJECT,
        result.build().toByteArray(),
        PublishOptions.builder().messageId(messageId).build());
    logger.debug(
        "NATS event sent topic={} message_id={} correlation_id={}",
        RESULT_SUBJECT,
        messageId,
        source.getCorrelationId().isBlank() ? "unknown" : source.getCorrelationId());
    return published;
  }

  static PageViewInput decodeEnvelope(Message message) {
    MessageEnvelope source = null;
    try {
      source = MessageEnvelope.parseFrom(message.getData());
      long version = contextVersion(source);
      Instant eventTime =
          source.hasOccurredAt()
              ? Instant.ofEpochSecond(
                  source.getOccurredAt().getSeconds(), source.getOccurredAt().getNanos())
              : Instant.EPOCH;
      return new PageViewInput(message, source, null, null, version, eventTime);
    } catch (Exception exception) {
      return new PageViewInput(message, source, null, exception, 0, Instant.EPOCH);
    }
  }

  static FilteredPageViews freshestPageViews(
      List<PageViewInput> inputs, Instant now, Duration maxAge) {
    if (maxAge.isNegative()) {
      throw new IllegalArgumentException("page-view max age must not be negative");
    }
    List<PageViewInput> retained = new ArrayList<>();
    List<PageViewInput> discarded = new ArrayList<>();
    Map<String, PageViewInput> newest = new LinkedHashMap<>();
    for (PageViewInput input : inputs) {
      if (input.decodeException() != null
          || input.source() == null
          || input.source().getAggregateId().isBlank()) {
        retained.add(input);
        continue;
      }
      if (!input.eventTime().equals(Instant.EPOCH)
          && Duration.between(input.eventTime(), now).compareTo(maxAge) > 0) {
        discarded.add(input);
        continue;
      }
      String sessionId = input.source().getAggregateId();
      PageViewInput previous = newest.get(sessionId);
      if (previous == null || input.version() >= previous.version()) {
        if (previous != null) {
          discarded.add(previous);
        }
        newest.put(sessionId, input);
      } else {
        discarded.add(input);
      }
    }
    retained.addAll(newest.values());
    return new FilteredPageViews(List.copyOf(retained), List.copyOf(discarded));
  }

  private static long contextVersion(MessageEnvelope source) throws Exception {
    long version = source.getAggregateVersion();
    if (version == 0) {
      version = seed(source.getMessageId()) & Long.MAX_VALUE;
      if (version == 0) {
        version = 1;
      }
    }
    return version;
  }

  record PageViewInput(
      Message message,
      MessageEnvelope source,
      StorefrontPageViewedEvent pageView,
      Exception decodeException,
      long version,
      Instant eventTime) {}

  record FilteredPageViews(List<PageViewInput> retained, List<PageViewInput> discarded) {}

  private static long seed(String messageId) throws Exception {
    MessageDigest digestFunction = SHA_256.get();
    digestFunction.reset();
    byte[] digest = digestFunction.digest(messageId.getBytes(StandardCharsets.UTF_8));
    return ByteBuffer.wrap(digest).getLong();
  }

  private static UUID nameUuid(byte[] name) {
    MessageDigest digestFunction = MD5.get();
    digestFunction.reset();
    byte[] digest = digestFunction.digest(name);
    digest[6] &= 0x0f;
    digest[6] |= 0x30;
    digest[8] &= 0x3f;
    digest[8] |= 0x80;
    ByteBuffer bytes = ByteBuffer.wrap(digest);
    return new UUID(bytes.getLong(), bytes.getLong());
  }

  private static Timestamp timestamp(Instant instant) {
    return Timestamp.newBuilder()
        .setSeconds(instant.getEpochSecond())
        .setNanos(instant.getNano())
        .build();
  }

  private static SSLContext trustContext(String caFile) throws Exception {
    CertificateFactory factory = CertificateFactory.getInstance("X.509");
    Certificate certificate;
    try (FileInputStream input = new FileInputStream(caFile)) {
      certificate = factory.generateCertificate(input);
    }
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null);
    trustStore.setCertificateEntry("nats-ca", certificate);
    TrustManagerFactory manager =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    manager.init(trustStore);
    SSLContext context = SSLContext.getInstance("TLSv1.3");
    context.init(null, manager.getTrustManagers(), null);
    return context;
  }

  private static Duration duration(String name, Duration fallback) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
      return fallback;
    }
    if (value.endsWith("ms")) {
      return Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)));
    }
    if (value.endsWith("s")) {
      return Duration.ofSeconds(Long.parseLong(value.substring(0, value.length() - 1)));
    }
    if (value.endsWith("m")) {
      return Duration.ofMinutes(Long.parseLong(value.substring(0, value.length() - 1)));
    }
    throw new IllegalArgumentException("invalid duration in " + name);
  }

  private static int integer(String name, int fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : Integer.parseInt(value);
  }

  private static int boundedInteger(String name, int fallback, int minimum, int maximum) {
    int value = integer(name, fallback);
    if (value < minimum || value > maximum) {
      throw new IllegalArgumentException(name + " must be between " + minimum + " and " + maximum);
    }
    return value;
  }

  @Override
  public void close() {
    running.set(false);
    consumerReady.set(false);
    if (thread != null) {
      thread.interrupt();
    }
    if (eventExecutor != null) {
      eventExecutor.shutdown();
      try {
        if (!eventExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
          eventExecutor.shutdownNow();
        }
      } catch (InterruptedException exception) {
        eventExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    if (connection != null) {
      try {
        connection.drain(Duration.ofSeconds(10)).get(10, TimeUnit.SECONDS);
      } catch (Exception exception) {
        logger.warn("NATS drain failed during shutdown", exception);
      }
      try {
        connection.close();
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
