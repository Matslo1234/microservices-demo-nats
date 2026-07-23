/* Copyright 2018 Google LLC.
 * Licensed under the Apache License, Version 2.0 (the "License"); */

'use strict';

const http = require('http');
const pino = require('pino');
const { connectAndPublish } = require('./nats_events');

const logger = pino({
  name: 'currencyservice-server',
  level: 'debug',
  messageKey: 'message',
  formatters: { level (label) { return { severity: label }; } }
});

if (process.env.DISABLE_PROFILER) {
  logger.info('Profiler disabled.');
} else {
  logger.info('Profiler enabled.');
  require('@google-cloud/profiler').start({
    serviceContext: { service: 'currencyservice', version: '1.0.0' }
  });
}

if (process.env.ENABLE_TRACING === '1') {
  const { resourceFromAttributes } = require('@opentelemetry/resources');
  const { ATTR_SERVICE_NAME } = require('@opentelemetry/semantic-conventions');
  const opentelemetry = require('@opentelemetry/sdk-node');
  const { OTLPTraceExporter } = require('@opentelemetry/exporter-otlp-grpc');
  const sdk = new opentelemetry.NodeSDK({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: process.env.OTEL_SERVICE_NAME || 'currencyservice'
    }),
    traceExporter: new OTLPTraceExporter({ url: process.env.COLLECTOR_SERVICE_ADDR })
  });
  sdk.start();
} else {
  logger.info('Tracing disabled.');
}

let natsConnection = null;
let natsReady = process.env.NATS_REQUIRED !== 'true';
let healthServer = null;

function healthHandler (request, response) {
  if (request.url === '/healthz') {
    response.writeHead(200, { 'Content-Type': 'text/plain' });
    response.end('ok\n');
    return;
  }
  if (request.url === '/readyz') {
    response.writeHead(natsReady ? 200 : 503, { 'Content-Type': 'text/plain' });
    response.end(natsReady ? 'ok\n' : 'currency publisher is not ready\n');
    return;
  }
  if (request.url === '/metrics') {
    response.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4' });
    response.end(`boutique_dependency_ready{service="currencyservice",dependency="nats"} ${natsReady ? 1 : 0}\n`);
    return;
  }
  response.writeHead(404);
  response.end();
}

async function main () {
  const currencyData = require('./data/currency_conversion.json');
  natsConnection = await connectAndPublish(currencyData, logger);
  if (natsConnection) {
    natsReady = true;
    (async () => {
      for await (const status of natsConnection.status()) {
        if (status.type === 'disconnect' || status.type === 'error') {
          natsReady = false;
          logger.warn({ type: status.type, data: status.data }, 'NATS connection is not ready');
        } else if (status.type === 'reconnect') {
          natsReady = true;
          logger.info('NATS connection restored');
        }
      }
    })().catch(err => {
      natsReady = false;
      logger.error({ err }, 'NATS status monitor failed');
    });
  }
  const port = Number(process.env.PORT || 8080);
  healthServer = http.createServer(healthHandler);
  healthServer.listen(port, '0.0.0.0', () => logger.info({ port }, 'currency health server started'));
}

async function shutdown () {
  natsReady = false;
  if (healthServer) await new Promise(resolve => healthServer.close(resolve));
  if (natsConnection) await natsConnection.drain();
}

for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => {
    shutdown().then(() => process.exit(0)).catch(err => {
      logger.error({ err }, 'shutdown failed');
      process.exit(1);
    });
  });
}

main().catch(err => {
  logger.fatal({ err, correlation_id: err.correlation_id || 'unknown' }, 'currency service startup failed');
  process.exit(1);
});
