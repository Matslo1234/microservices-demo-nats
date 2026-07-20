// Copyright 2018 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

'use strict';

const http = require('http');
const logger = require('./logger');

class PaymentHealthServer {
  constructor (messaging, port = process.env.PORT || '8080') {
    this.messaging = messaging;
    this.port = Number(port);
    this.server = http.createServer((request, response) => this.handle(request, response));
  }

  handle (request, response) {
    if (request.url === '/healthz') {
      response.writeHead(200, { 'Content-Type': 'text/plain' });
      response.end('ok\n');
      return;
    }
    const ready = this.messaging.ready();
    if (request.url === '/readyz') {
      response.writeHead(ready ? 200 : 503, { 'Content-Type': 'text/plain' });
      response.end(ready ? 'ok\n' : 'payment NATS handlers are not ready\n');
      return;
    }
    if (request.url === '/metrics') {
      response.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4' });
      response.end(
        `boutique_dependency_ready{service="paymentservice",dependency="nats"} ${ready ? 1 : 0}\n` +
        'boutique_dependency_ready{service="paymentservice",dependency="provider_store"} 1\n'
      );
      return;
    }
    response.writeHead(404);
    response.end();
  }

  listen () {
    this.server.listen(this.port, '0.0.0.0', () => {
      logger.info(`Payment health server started on port ${this.port}`);
    });
  }

  close () {
    return new Promise(resolve => this.server.close(resolve));
  }
}

module.exports = PaymentHealthServer;
