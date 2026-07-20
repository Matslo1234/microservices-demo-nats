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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

final class NatsEventWorker implements AutoCloseable {
  private static final Logger logger = LogManager.getLogger(NatsEventWorker.class);
  private static final String PAGE_SUBJECT = "boutique.evt.storefront.page-viewed.v1";
  private static final String RESULT_SUBJECT = "boutique.evt.ad.selection-generated.v1";
  private static final String DURABLE = "ad-page-views-v1";

  private final AdService service;
  private final boolean required;
  private final AtomicBoolean running = new AtomicBoolean();
  private Connection connection;
  private Thread thread;

  NatsEventWorker(AdService service) {
    this.service = service;
    this.required = Boolean.parseBoolean(System.getenv().getOrDefault("NATS_REQUIRED", "false"));
  }

  void start() throws IOException {
    if (!required) {
      return;
    }
    for (String name : List.of("NATS_URL", "NATS_USER", "NATS_PASSWORD", "NATS_CA_FILE")) {
      if (System.getenv(name) == null || System.getenv(name).isBlank()) {
        throw new IOException(name + " is required when NATS_REQUIRED=true");
      }
    }
    try {
      Options options =
          new Options.Builder()
              .server(System.getenv("NATS_URL"))
              .userInfo(System.getenv("NATS_USER"), System.getenv("NATS_PASSWORD"))
              .connectionName("adservice/phase3")
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
          PullSubscribeOptions.builder()
              .stream("BOUTIQUE_EVENTS")
              .durable(DURABLE)
              .configuration(consumer)
              .build();
      JetStreamSubscription subscription = jetStream.subscribe(PAGE_SUBJECT, subscribeOptions);
      running.set(true);
      thread =
          new Thread(
              () -> consume(jetStream, subscription), "adservice-nats-page-views");
      thread.setDaemon(true);
      thread.start();
      logger.info("NATS page-view consumer is ready");
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while connecting to NATS", exception);
    } catch (Exception exception) {
      throw new IOException("could not establish NATS page-view consumer", exception);
    }
  }

  boolean ready() {
    return !required
        || (running.get()
            && connection != null
            && connection.getStatus() == Connection.Status.CONNECTED);
  }

  private void consume(JetStream jetStream, JetStreamSubscription subscription) {
    while (running.get()) {
      try {
        for (Message message : subscription.fetch(32, Duration.ofSeconds(1))) {
          try {
            handle(jetStream, message);
            message.ack();
          } catch (Exception exception) {
            logger.warn("failed to process page-view event", exception);
            message.nakWithDelay(Duration.ofSeconds(1));
          }
        }
      } catch (Exception exception) {
        if (running.get()) {
          logger.warn("failed to fetch page-view events", exception);
        }
      }
    }
  }

  private void handle(JetStream jetStream, Message message) throws Exception {
    MessageEnvelope source = MessageEnvelope.parseFrom(message.getData());
    StorefrontPageViewedEvent pageView = source.getData().unpack(StorefrontPageViewedEvent.class);
    long version = source.getAggregateVersion();
    if (version == 0) {
      version = Instant.now().toEpochMilli() * 1_000_000L;
    }
    List<Ad> selected = service.selectAds(pageView.getCategoryIdsList(), seed(source.getMessageId()));
    AdSelectionGeneratedEvent.Builder payload =
        AdSelectionGeneratedEvent.newBuilder()
            .setSessionId(pageView.getSessionId())
            .setTriggeringPageType(pageView.getPageType())
            .setExpiresAt(timestamp(Instant.now().plus(Duration.ofMinutes(10))));
    for (Ad ad : selected) {
      payload.addAds(
          AdSelection.newBuilder()
              .setRedirectUrl(ad.getRedirectUrl())
              .setText(ad.getText())
              .build());
    }
    String messageId =
        UUID.nameUUIDFromBytes(
                (RESULT_SUBJECT + "\0" + source.getMessageId()).getBytes(StandardCharsets.UTF_8))
            .toString();
    MessageEnvelope result =
        MessageEnvelope.newBuilder()
            .setMessageId(messageId)
            .setMessageType("boutique.ad.SelectionGenerated.v1")
            .setSchemaVersion(1)
            .setOccurredAt(timestamp(Instant.now()))
            .setProducer("adservice/phase3")
            .setAggregateType("ad-context")
            .setAggregateId(pageView.getSessionId())
            .setAggregateVersion(version)
            .setCorrelationId(source.getCorrelationId())
            .setCausationId(source.getMessageId())
            .setTraceparent(source.getTraceparent())
            .setTracestate(source.getTracestate())
            .setData(Any.pack(payload.build()))
            .build();
    jetStream.publish(
        RESULT_SUBJECT,
        result.toByteArray(),
        PublishOptions.builder().messageId(messageId).build());
  }

  private static long seed(String messageId) throws Exception {
    byte[] digest = MessageDigest.getInstance("SHA-256").digest(messageId.getBytes(StandardCharsets.UTF_8));
    return ByteBuffer.wrap(digest).getLong();
  }

  private static Timestamp timestamp(Instant instant) {
    return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
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

  @Override
  public void close() {
    running.set(false);
    if (thread != null) {
      thread.interrupt();
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
