/* Copyright 2018, Google LLC.
 * Licensed under the Apache License, Version 2.0 (the "License"); */

package hipstershop;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import hipstershop.Demo.Ad;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class AdService {
  private static final Logger logger = LogManager.getLogger(AdService.class);
  private static final int MAX_ADS_TO_SERVE = 2;
  private static final AdService service = new AdService();

  private HttpServer healthServer;
  private ExecutorService healthExecutor;
  private NatsEventWorker eventWorker;

  private void start() throws IOException {
    int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
    eventWorker = new NatsEventWorker(service);
    eventWorker.start();

    healthServer = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
    healthExecutor = Executors.newFixedThreadPool(4);
    healthServer.setExecutor(healthExecutor);
    healthServer.createContext("/healthz", exchange -> reply(exchange, 200, "ok\n"));
    healthServer.createContext(
        "/readyz",
        exchange ->
            reply(
                exchange,
                eventWorker.ready() ? 200 : 503,
                eventWorker.ready() ? "ok\n" : "ad NATS consumer is not ready\n"));
    healthServer.createContext(
        "/metrics",
        exchange -> {
          exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4");
          reply(
              exchange,
              200,
              "boutique_dependency_ready{service=\"adservice\",dependency=\"nats\"} "
                  + (eventWorker.ready() ? "1\n" : "0\n"));
        });
    healthServer.start();
    logger.info("Ad health server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "adservice-shutdown"));
  }

  private static void reply(HttpExchange exchange, int status, String body) throws IOException {
    byte[] encoded = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().putIfAbsent("Content-Type", List.of("text/plain"));
    exchange.sendResponseHeaders(status, encoded.length);
    exchange.getResponseBody().write(encoded);
    exchange.close();
  }

  private void stop() {
    if (healthServer != null) {
      healthServer.stop(0);
    }
    if (healthExecutor != null) {
      healthExecutor.shutdown();
    }
    if (eventWorker != null) {
      eventWorker.close();
    }
  }

  private static final Map<String, List<Ad>> adsMap = createAdsMap();

  List<Ad> selectAds(List<String> categories, long seed) {
    List<Ad> selected = new ArrayList<>();
    for (String category : categories) {
      selected.addAll(adsMap.getOrDefault(category, List.of()));
    }
    if (selected.isEmpty()) {
      adsMap.values().forEach(selected::addAll);
    }
    Collections.shuffle(selected, new Random(seed));
    return selected.subList(0, Math.min(MAX_ADS_TO_SERVE, selected.size()));
  }

  private static Map<String, List<Ad>> createAdsMap() {
    Ad hairdryer =
        Ad.newBuilder()
            .setRedirectUrl("/product/2ZYFJ3GM2N")
            .setText("Hairdryer for sale. 50% off.")
            .build();
    Ad tankTop =
        Ad.newBuilder()
            .setRedirectUrl("/product/66VCHSJNUP")
            .setText("Tank top for sale. 20% off.")
            .build();
    Ad candleHolder =
        Ad.newBuilder()
            .setRedirectUrl("/product/0PUK6V6EV0")
            .setText("Candle holder for sale. 30% off.")
            .build();
    Ad bambooGlassJar =
        Ad.newBuilder()
            .setRedirectUrl("/product/9SIQT8TOJO")
            .setText("Bamboo glass jar for sale. 10% off.")
            .build();
    Ad watch =
        Ad.newBuilder()
            .setRedirectUrl("/product/1YMWWN1N4O")
            .setText("Watch for sale. Buy one, get second kit for free")
            .build();
    Ad mug =
        Ad.newBuilder()
            .setRedirectUrl("/product/6E92ZMYYFZ")
            .setText("Mug for sale. Buy two, get third one for free")
            .build();
    Ad loafers =
        Ad.newBuilder()
            .setRedirectUrl("/product/L9ECAV7KIM")
            .setText("Loafers for sale. Buy one, get second one for free")
            .build();
    return Map.of(
        "clothing", List.of(tankTop),
        "accessories", List.of(watch),
        "footwear", List.of(loafers),
        "hair", List.of(hairdryer),
        "decor", List.of(candleHolder),
        "kitchen", List.of(bambooGlassJar, mug));
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    logger.info("AdService NATS worker starting.");
    service.start();
    new CountDownLatch(1).await();
  }
}
