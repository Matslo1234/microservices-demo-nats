/*
 * Copyright 2026 Google LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const crypto = require('crypto');
const path = require('path');
const protobuf = require('protobufjs');
const { connect } = require('@nats-io/transport-node');
const { jetstream } = require('@nats-io/jetstream');

const SUBJECT = 'boutique.evt.currency.rates-updated.v1';
const MESSAGE_TYPE = 'boutique.currency.RatesUpdated.v1';
const PAYLOAD_TYPE = 'boutique.events.v1.CurrencyRatesUpdatedEvent';

function parseDurationMs(value, fallbackMs) {
  if (!value) return fallbackMs;
  const match = /^(\d+)(ms|s|m)$/.exec(value);
  if (!match) throw new Error(`invalid duration ${value}`);
  const amount = Number(match[1]);
  return amount * ({ms: 1, s: 1000, m: 60000})[match[2]];
}

function parseInteger(value, fallback) {
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed)) throw new Error(`invalid integer ${value}`);
  return parsed;
}

function contractsRoot() {
  const repositoryRoot = path.resolve(__dirname, '../..');
  const root = new protobuf.Root();
  const protobufRoot = path.dirname(require.resolve('protobufjs/package.json'));
  root.resolvePath = (origin, target) => {
    if (target.startsWith('protos/')) return path.join(repositoryRoot, target);
    if (target.startsWith('google/protobuf/')) return path.join(protobufRoot, target);
    return path.resolve(path.dirname(origin), target);
  };
  root.loadSync([
    path.join(repositoryRoot, 'protos/common/v1/message.proto'),
    path.join(repositoryRoot, 'protos/events/v1/events.proto')
  ], { keepCase: false });
  root.resolveAll();
  return root;
}

function stableRevision(currencyData) {
  const canonical = Object.keys(currencyData)
    .sort()
    .map(code => `${code}=${currencyData[code]}`)
    .join('\n');
  const digest = crypto.createHash('sha256').update(canonical).digest();
  let revision = digest.readBigUInt64BE(0) & 0x7fffffffffffffffn;
  if (revision === 0n) revision = 1n;
  return { revision, checksum: digest.toString('hex') };
}

function deterministicMessageId(...parts) {
  const hash = crypto.createHash('sha256');
  for (const part of parts) {
    hash.update(part);
    hash.update(Buffer.from([0]));
  }
  const bytes = hash.digest().subarray(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function timestampNow() {
  const milliseconds = Date.now();
  return {
    seconds: Math.floor(milliseconds / 1000),
    nanos: (milliseconds % 1000) * 1000000
  };
}

function encodeSnapshot(currencyData) {
  const root = contractsRoot();
  const Event = root.lookupType(PAYLOAD_TYPE);
  const Envelope = root.lookupType('boutique.common.v1.MessageEnvelope');
  const identity = stableRevision(currencyData);
  const occurredAt = timestampNow();
  const rates = Object.keys(currencyData)
    .sort()
    .map(currencyCode => ({
      currencyCode,
      unitsPerBase: Number(currencyData[currencyCode])
    }));
  const eventBytes = Event.encode(Event.create({
    baseCurrencyCode: 'EUR',
    rates,
    effectiveAt: occurredAt,
    rateRevision: identity.revision.toString()
  })).finish();
  const revisionText = identity.revision.toString();
  const messageId = deterministicMessageId(SUBJECT, revisionText, identity.checksum);
  const correlationId = deterministicMessageId('currency-bootstrap', revisionText);
  const envelope = Envelope.create({
    messageId,
    messageType: MESSAGE_TYPE,
    schemaVersion: 1,
    occurredAt,
    producer: 'currencyservice/phase2',
    aggregateType: 'currency-rates',
    aggregateId: 'EUR',
    aggregateVersion: revisionText,
    correlationId,
    data: {
      type_url: `type.googleapis.com/${PAYLOAD_TYPE}`,
      value: eventBytes
    }
  });
  return {
    data: Envelope.encode(envelope).finish(),
    messageId,
    correlationId,
    revision: revisionText,
    count: rates.length,
    checksum: identity.checksum
  };
}

async function connectAndPublish(currencyData, logger) {
  const required = process.env.NATS_REQUIRED === 'true';
  if (!required) return null;
  const requiredVariables = ['NATS_URL', 'NATS_USER', 'NATS_PASSWORD', 'NATS_CA_FILE'];
  for (const name of requiredVariables) {
    if (!process.env[name]) throw new Error(`${name} is required when NATS_REQUIRED=true`);
  }

  const maxReconnectAttempts = parseInteger(process.env.NATS_MAX_RECONNECTS, -1);
  const nc = await connect({
    servers: process.env.NATS_URL,
    user: process.env.NATS_USER,
    pass: process.env.NATS_PASSWORD,
    name: 'currencyservice/phase2',
    tls: { caFile: process.env.NATS_CA_FILE },
    timeout: parseDurationMs(process.env.NATS_CONNECT_TIMEOUT, 2000),
    reconnectTimeWait: parseDurationMs(process.env.NATS_RECONNECT_WAIT, 2000),
    maxReconnectAttempts,
    pingInterval: parseDurationMs(process.env.NATS_PING_INTERVAL, 20000),
    maxPingOut: parseInteger(process.env.NATS_MAX_PINGS_OUT, 2),
    waitOnFirstConnect: true
  });
  const js = jetstream(nc, { timeout: parseDurationMs(process.env.NATS_PUBLISH_TIMEOUT, 5000) });
  const snapshot = encodeSnapshot(currencyData);
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      await js.publish(SUBJECT, snapshot.data, { msgID: snapshot.messageId });
      logger.debug({
        topic: SUBJECT,
        message_kind: 'event',
        message_id: snapshot.messageId,
        correlation_id: snapshot.correlationId,
      }, 'NATS event sent');
      lastError = null;
      break;
    } catch (err) {
      lastError = err;
      logger.warn({
        err,
        attempt,
        topic: SUBJECT,
        message_id: snapshot.messageId,
        correlation_id: snapshot.correlationId,
      }, 'JetStream rate snapshot publish failed; retrying with the same message ID');
    }
  }
  if (lastError) {
    await nc.close();
    throw lastError;
  }
  return nc;
}

module.exports = { connectAndPublish, contractsRoot, encodeSnapshot, stableRevision };
