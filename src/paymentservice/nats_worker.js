/* Copyright 2026 Google LLC
 * Licensed under the Apache License, Version 2.0 (the "License"); */

'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const protobuf = require('protobufjs');
const cardValidator = require('simple-card-validator');
const { connect, consumerOpts, headers } = require('nats');
const logger = require('./logger');

const TOKEN_SUBJECT = 'boutique.qry.payment.tokenize.v1';
const COMMAND_SUBJECT = 'boutique.cmd.payment.>';
const COMMAND_DURABLE = 'payment-commands-v1';
const STATE_BUCKETS = ['tokens', 'tokenKeys', 'outcomes', 'authorizations'];
const PAYMENT_STATE_VERSION = 1;
const COMMAND_PREFETCH = 256;
const COMMAND_BATCH_DELAY_MS = 20;
const COMMAND_PULL_EXPIRES_MS = 1000;
const COMMAND_PULL_REFRESH_MS = 500;
const COMMAND_RESTART_DELAY_MS = 1000;
const TOKEN_BATCH_SIZE = 256;
const TOKEN_BATCH_DELAY_MS = 20;

function stableID(...parts) {
  const digest = crypto.createHash('sha256').update(parts.join('\0')).digest();
  digest[6] = (digest[6] & 0x0f) | 0x50;
  digest[8] = (digest[8] & 0x3f) | 0x80;
  const hex = digest.subarray(0, 16).toString('hex');
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20,32)}`;
}

class PaymentState {
  constructor(file) {
    this.file = file;
    fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o750 });
    this.value = { tokens: {}, tokenKeys: {}, outcomes: {}, authorizations: {} };
    this.pending = Object.fromEntries(STATE_BUCKETS.map(bucket => [bucket, new Set()]));
    if (fs.existsSync(file)) this.load();
    for (const key of STATE_BUCKETS) {
      if (!this.value[key]) this.value[key] = {};
    }
  }

  load() {
    const encoded = fs.readFileSync(this.file, 'utf8');
    if (!encoded.trim()) return;
    let legacy;
    try {
      legacy = JSON.parse(encoded);
    } catch (_) {
      legacy = undefined;
    }
    if (legacy && STATE_BUCKETS.some(bucket => Object.hasOwn(legacy, bucket))) {
      this.value = legacy;
      for (const key of STATE_BUCKETS) {
        if (!this.value[key]) this.value[key] = {};
      }
      this.writeSnapshot();
      return;
    }

    const lines = encoded.split('\n');
    for (let index = 0; index < lines.length; index++) {
      const line = lines[index].trim();
      if (!line) continue;
      let record;
      try {
        record = JSON.parse(line);
      } catch (error) {
        const hasLaterRecord = lines.slice(index + 1).some(candidate => candidate.trim());
        if (!hasLaterRecord) {
          // A crash may leave only the final append incomplete. Remove it so
          // the next append starts at a valid journal boundary.
          const validPrefix = lines.slice(0, index).join('\n');
          const fd = fs.openSync(this.file, 'w', 0o600);
          try {
            fs.writeFileSync(fd, validPrefix ? `${validPrefix}\n` : '');
            fs.fsyncSync(fd);
          } finally {
            fs.closeSync(fd);
          }
          break;
        }
        throw new Error(`invalid payment state journal record ${index + 1}: ${error.message}`);
      }
      this.applyRecord(record);
    }
  }

  applyRecord(record) {
    if (record.version !== PAYMENT_STATE_VERSION) {
      throw new Error(`unsupported payment state version ${record.version}`);
    }
    if (record.snapshot) {
      this.value = record.snapshot;
      for (const key of STATE_BUCKETS) {
        if (!this.value[key]) this.value[key] = {};
      }
      return;
    }
    for (const bucket of STATE_BUCKETS) {
      for (const [key, value] of Object.entries(record.sets?.[bucket] || {})) {
        this.value[bucket][key] = value;
      }
      for (const key of record.deletes?.[bucket] || []) delete this.value[bucket][key];
    }
  }

  writeSnapshot() {
    const temporary = `${this.file}.tmp`;
    const fd = fs.openSync(temporary, 'w', 0o600);
    try {
      fs.writeFileSync(fd, `${JSON.stringify({ version: PAYMENT_STATE_VERSION, snapshot: this.value })}\n`);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(temporary, this.file);
  }

  set(bucket, key, value) {
    if (!this.pending[bucket]) throw new Error(`unknown payment state bucket ${bucket}`);
    this.value[bucket][key] = value;
    this.pending[bucket].add(key);
  }

  persist() {
    const sets = {};
    for (const bucket of STATE_BUCKETS) {
      if (this.pending[bucket].size === 0) continue;
      sets[bucket] = {};
      for (const key of this.pending[bucket]) sets[bucket][key] = this.value[bucket][key];
    }
    if (Object.keys(sets).length === 0) return;

    const fd = fs.openSync(this.file, 'a', 0o600);
    try {
      fs.writeFileSync(fd, `${JSON.stringify({ version: PAYMENT_STATE_VERSION, sets })}\n`);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    for (const bucket of STATE_BUCKETS) this.pending[bucket].clear();
  }
}

async function loadContracts() {
  const contractRoot = fs.existsSync(path.join(__dirname, 'protos')) ? __dirname : path.resolve(__dirname, '../..');
  const root = new protobuf.Root();
  root.resolvePath = function(origin, target) {
    if (target.startsWith('protos/')) return path.join(contractRoot, target);
    return protobuf.util.path.resolve(origin, target);
  };
  await root.load([
    path.join(contractRoot, 'protos/common/v1/message.proto'),
    path.join(contractRoot, 'protos/commands/v1/commands.proto'),
    path.join(contractRoot, 'protos/events/v1/events.proto'),
  ]);
  root.resolveAll();
  return {
    Envelope: root.lookupType('boutique.common.v1.MessageEnvelope'),
    Authorize: root.lookupType('boutique.commands.v1.PaymentAuthorizeCommand'),
    Capture: root.lookupType('boutique.commands.v1.PaymentCaptureCommand'),
    Release: root.lookupType('boutique.commands.v1.PaymentReleaseAuthorizationCommand'),
    Authorized: root.lookupType('boutique.events.v1.PaymentAuthorizedEvent'),
    Declined: root.lookupType('boutique.events.v1.PaymentAuthorizationDeclinedEvent'),
    Captured: root.lookupType('boutique.events.v1.PaymentCapturedEvent'),
    CaptureFailed: root.lookupType('boutique.events.v1.PaymentCaptureFailedEvent'),
    Released: root.lookupType('boutique.events.v1.PaymentAuthorizationReleasedEvent'),
    ReleaseFailed: root.lookupType('boutique.events.v1.PaymentAuthorizationReleaseFailedEvent'),
  };
}

function timestampNow() {
  const milliseconds = Date.now();
  return { seconds: Math.floor(milliseconds / 1000), nanos: (milliseconds % 1000) * 1000000 };
}

function validateCard(request) {
  const number = String(request.credit_card_number || '').replaceAll('-', '').replaceAll(' ', '');
  const details = cardValidator(number).getCardDetails();
  if (!details.valid || !['visa', 'mastercard'].includes(details.card_type)) throw new Error('INVALID_CARD');
  const month = Number(request.credit_card_expiration_month);
  const year = Number(request.credit_card_expiration_year);
  const now = new Date();
  if (month < 1 || month > 12 || year * 12 + month < now.getFullYear() * 12 + now.getMonth() + 1) throw new Error('EXPIRED_CARD');
  if (!/^\d{3,4}$/.test(String(request.credit_card_cvv || ''))) throw new Error('INVALID_CARD');
  return { type: details.card_type, last4: number.slice(-4) };
}

function prepareTokenization(state, request) {
  if (!request.order_id || !request.idempotency_key) throw new Error('INVALID_TOKEN_REQUEST');
  const card = validateCard(request);
  const identity = `${request.order_id}\0${request.idempotency_key}`;
  const existing = state.value.tokenKeys[identity];
  if (existing && state.value.tokens[existing] && state.value.tokens[existing].expiresAt > Date.now()) {
    return { payment_token: existing, expires_at: new Date(state.value.tokens[existing].expiresAt).toISOString() };
  }
  const token = `ptok_${crypto.randomBytes(24).toString('base64url')}`;
  const expiresAt = Date.now() + 15 * 60 * 1000;
  state.set('tokens', token, { orderId: request.order_id, cardType: card.type, last4: card.last4, expiresAt, consumed: false });
  state.set('tokenKeys', identity, token);
  return { payment_token: token, expires_at: new Date(expiresAt).toISOString() };
}

function tokenize(state, request) {
  const result = prepareTokenization(state, request);
  state.persist();
  return result;
}

function processTokenBatch(entries, state) {
  logger.debug({
    topic: TOKEN_SUBJECT,
    message_kind: 'query',
    batch_size: entries.length,
  }, 'NATS query batch received');

  for (const entry of entries) {
    if (entry.error) continue;
    try {
      entry.result = prepareTokenization(state, entry.request);
    } catch (tokenError) {
      entry.error = tokenError;
    }
  }

  try {
    if (entries.some(entry => !entry.error)) state.persist();
  } catch (persistError) {
    for (const entry of entries) {
      if (!entry.error) entry.error = persistError;
    }
  }

  for (const entry of entries) {
    if (!entry.error) {
      entry.message.respond(JSON.stringify(entry.result));
      continue;
    }
    entry.message.respond(JSON.stringify({
      error: entry.error.message,
      safe_message: 'Payment details could not be tokenized.',
    }));
    logger.error({
      topic: entry.message.subject || TOKEN_SUBJECT,
      correlation_id: entry.correlationId,
      error: entry.error.message,
    }, 'payment tokenization query processing failed');
  }
}

function createTokenBatcher(state) {
  let buffered = [];
  let flushTimer;
  const flush = () => {
    if (buffered.length === 0) return;
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = undefined;
    }
    const batch = buffered;
    buffered = [];
    processTokenBatch(batch, state);
  };
  return entry => {
    buffered.push(entry);
    if (buffered.length >= TOKEN_BATCH_SIZE) {
      flush();
    } else if (!flushTimer) {
      flushTimer = setTimeout(flush, TOKEN_BATCH_DELAY_MS);
    }
  };
}

function anyPayload(type, payload) {
  // protobufjs' bundled google.protobuf.Any descriptor preserves the proto
  // field name (`type_url`) even though application messages use camelCase.
  return { type_url: `type.googleapis.com/${type.fullName.slice(1)}`, value: type.encode(payload).finish() };
}

function failure(code, message, retryable = false) {
  return { code, retryable, safeMessage: message };
}

function outcome(contracts, cause, subject, messageType, PayloadType, payload) {
  const messageID = stableID(subject, cause.messageId);
  const envelope = {
    messageId: messageID, messageType, schemaVersion: 1, occurredAt: timestampNow(), producer: 'paymentservice/phase5',
    aggregateType: 'order', aggregateId: cause.aggregateId, aggregateVersion: cause.aggregateVersion,
    correlationId: cause.correlationId, causationId: cause.messageId, traceparent: cause.traceparent,
    tracestate: cause.tracestate, data: anyPayload(PayloadType, payload),
  };
  return { messageID, subject, data: Buffer.from(contracts.Envelope.encode(envelope).finish()).toString('base64') };
}

function processCommand(state, contracts, subject, envelope) {
  const mode = process.env.PAYMENT_FAILURE_MODE || '';
  if (subject === 'boutique.cmd.payment.authorize.v1') {
    const command = contracts.Authorize.decode(envelope.data.value);
    const token = state.value.tokens[command.paymentToken];
    if (mode === 'authorization_declined' || !token || token.orderId !== command.orderId || token.expiresAt <= Date.now() || token.consumed) {
      return outcome(contracts, envelope, 'boutique.evt.payment.authorization-declined.v1', 'boutique.payment.AuthorizationDeclined.v1',
        contracts.Declined, { orderId: command.orderId, declineCategory: mode ? 'TEST_DECLINE' : 'INVALID_OR_EXPIRED_TOKEN' });
    }
    const authorizationID = stableID('authorization', command.idempotencyKey);
    const result = outcome(contracts, envelope, 'boutique.evt.payment.authorized.v1', 'boutique.payment.Authorized.v1', contracts.Authorized,
      { orderId: command.orderId, authorizationId: authorizationID, amount: command.amount });
    state.set('tokens', command.paymentToken, { ...token, consumed: true });
    state.set('authorizations', authorizationID,
      { orderId: command.orderId, amount: command.amount, captured: false, released: false });
    return result;
  }
  if (subject === 'boutique.cmd.payment.capture.v1') {
    const command = contracts.Capture.decode(envelope.data.value);
    const authorization = state.value.authorizations[command.authorizationId];
    if (mode === 'capture_failed' || !authorization || authorization.orderId !== command.orderId || authorization.released) {
      return outcome(contracts, envelope, 'boutique.evt.payment.capture-failed.v1', 'boutique.payment.CaptureFailed.v1', contracts.CaptureFailed,
        { orderId: command.orderId, authorizationId: command.authorizationId, failure: failure('CAPTURE_FAILED', 'Payment capture failed.', true) });
    }
    const result = outcome(contracts, envelope, 'boutique.evt.payment.captured.v1', 'boutique.payment.Captured.v1', contracts.Captured,
      { orderId: command.orderId, transactionId: stableID('capture', command.idempotencyKey), amount: command.amount });
    state.set('authorizations', command.authorizationId, { ...authorization, captured: true });
    return result;
  }
  if (subject === 'boutique.cmd.payment.release-authorization.v1') {
    const command = contracts.Release.decode(envelope.data.value);
    const authorization = state.value.authorizations[command.authorizationId];
    if (mode === 'release_failed' || !authorization || authorization.orderId !== command.orderId) {
      return outcome(contracts, envelope, 'boutique.evt.payment.authorization-release-failed.v1', 'boutique.payment.AuthorizationReleaseFailed.v1', contracts.ReleaseFailed,
        { orderId: command.orderId, authorizationId: command.authorizationId, failure: failure('AUTHORIZATION_RELEASE_FAILED', 'Authorization release requires review.') });
    }
    const result = outcome(contracts, envelope, 'boutique.evt.payment.authorization-released.v1', 'boutique.payment.AuthorizationReleased.v1', contracts.Released,
      { orderId: command.orderId, authorizationId: command.authorizationId });
    state.set('authorizations', command.authorizationId, { ...authorization, released: true });
    return result;
  }
  throw new Error(`unsupported payment command ${subject}`);
}

async function processCommandBatch(messages, state, contracts, js) {
  const commands = messages.map(message => {
    let correlationId = 'unknown';
    let messageId = 'unknown';
    let envelope;
    let error;
    try {
      envelope = contracts.Envelope.decode(message.data);
      correlationId = envelope.correlationId || 'unknown';
      messageId = envelope.messageId || 'unknown';
    } catch (decodeError) {
      error = decodeError;
    }
    return { message, correlationId, messageId, envelope, error };
  });
  logger.debug({
    topic: COMMAND_SUBJECT,
    message_kind: 'command',
    batch_size: commands.length,
  }, 'NATS command batch received');

  for (const command of commands) {
    if (command.error) continue;
    try {
      let result = state.value.outcomes[command.envelope.messageId];
      if (!result) {
        result = processCommand(state, contracts, command.message.subject, command.envelope);
        state.set('outcomes', command.envelope.messageId, result);
      }
      command.result = result;
    } catch (commandError) {
      command.error = commandError;
    }
  }

  try {
    // Persist all provider mutations and idempotent outcomes in one fsync.
    // Nothing is published or acknowledged until the whole batch is durable.
    if (commands.some(command => !command.error)) {
      state.persist();
    }
  } catch (persistError) {
    for (const command of commands) {
      if (!command.error) command.error = persistError;
    }
  }

  await Promise.all(commands.map(async command => {
    if (!command.error) {
      try {
        const publishHeaders = headers();
        publishHeaders.set('Nats-Msg-Id', command.result.messageID);
        await js.publish(command.result.subject, Buffer.from(command.result.data, 'base64'),
          { msgID: command.result.messageID, headers: publishHeaders });
        logger.debug({
          topic: command.result.subject,
          message_kind: 'event',
          message_id: command.result.messageID,
          correlation_id: command.correlationId,
        }, 'NATS event sent');
        command.message.ack();
        return;
      } catch (publishError) {
        command.error = publishError;
      }
    }

    logger.error({
      topic: command.message.subject,
      message_id: command.messageId,
      correlation_id: command.correlationId,
      error: command.error.message,
    }, 'payment command processing failed');
    command.message.nak(1000);
  }));
}

async function runCommandConsumer(commandSubscription, state, contracts, js,
    pullRefreshMs = COMMAND_PULL_REFRESH_MS) {
  // Legacy pull subscriptions do not request messages merely by being
  // iterated. Pull requests can also be lost during a reconnect without
  // closing the subscription, so use expiring credit and refresh it while the
  // iterator is alive. Server-side expiry bounds duplicate watchdog requests.
  const requestPull = () => {
    if (!commandSubscription.isClosed()) {
      commandSubscription.pull({
        batch: COMMAND_PREFETCH,
        expires: COMMAND_PULL_EXPIRES_MS,
        idle_heartbeat: COMMAND_PULL_EXPIRES_MS / 2,
      });
    }
  };
  requestPull();
  const pullWatchdog = setInterval(requestPull, pullRefreshMs);
  pullWatchdog.unref?.();
  let buffered = [];
  let flushTimer;
  let processing = Promise.resolve();

  const flush = () => {
    if (buffered.length === 0) return;
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = undefined;
    }
    const batch = buffered;
    buffered = [];
    processing = processing
      .then(() => processCommandBatch(batch, state, contracts, js))
      .finally(() => {
        requestPull();
      });
  };

  try {
    for await (const message of commandSubscription) {
      buffered.push(message);
      if (buffered.length >= COMMAND_PREFETCH) {
        flush();
      } else if (!flushTimer) {
        flushTimer = setTimeout(flush, COMMAND_BATCH_DELAY_MS);
      }
    }
  } finally {
    clearInterval(pullWatchdog);
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = undefined;
    }
    flush();
    await processing;
  }
}

function commandConsumerOptions() {
  const options = consumerOpts();
  options.durable(COMMAND_DURABLE); options.manualAck(); options.ackExplicit(); options.ackWait(30000);
  options.maxDeliver(10); options.maxAckPending(COMMAND_PREFETCH); options.deliverAll();
  options.bindStream('BOUTIQUE_COMMANDS'); options.filterSubject(COMMAND_SUBJECT);
  return options;
}

async function superviseCommandConsumer(nc, js, state, contracts, workerStatus, initialSubscription,
    restartDelayMs = COMMAND_RESTART_DELAY_MS) {
  let commandSubscription = initialSubscription;
  while (!workerStatus.stopping && !nc.isClosed()) {
    try {
      if (!commandSubscription) {
        commandSubscription = await js.pullSubscribe(COMMAND_SUBJECT, commandConsumerOptions());
      }
      workerStatus.commandSubscription = commandSubscription;
      workerStatus.consumerReady = true;
      await runCommandConsumer(commandSubscription, state, contracts, js);
      if (workerStatus.stopping || nc.isClosed()) return;
      throw new Error('payment command consumer closed unexpectedly');
    } catch (workerError) {
      workerStatus.consumerReady = false;
      if (workerStatus.commandSubscription === commandSubscription) {
        workerStatus.commandSubscription = undefined;
      }
      if (commandSubscription && !commandSubscription.isClosed()) {
        commandSubscription.unsubscribe();
      }
      commandSubscription = undefined;
      if (workerStatus.stopping || nc.isClosed()) return;
      logger.error({ error: workerError.message, retry_delay_ms: restartDelayMs },
        'payment consumer interrupted; retrying');
      await new Promise(resolve => setTimeout(resolve, restartDelayMs));
    }
  }
}

async function startPaymentNATS() {
  for (const key of ['NATS_URL', 'NATS_USER', 'NATS_PASSWORD', 'NATS_CA_FILE']) {
    if (!process.env[key]) throw new Error(`${key} is required`);
  }
  const contracts = await loadContracts();
  const state = new PaymentState(process.env.PAYMENT_STORE_PATH || '/tmp/payment/provider-state.json');
  const nc = await connect({ servers: process.env.NATS_URL, user: process.env.NATS_USER, pass: process.env.NATS_PASSWORD,
    name: 'paymentservice/phase5', tls: { caFile: process.env.NATS_CA_FILE },
    reconnectTimeWait: 2000, maxReconnectAttempts: -1, pingInterval: 20000, maxPingOut: 2 });
  const js = nc.jetstream({ timeout: 5000 });
  const workerStatus = {
    connectionReady: !nc.isClosed(), consumerReady: false, stopping: false, commandSubscription: undefined,
  };
  const enqueueTokenization = createTokenBatcher(state);

  const tokenSubscription = nc.subscribe(TOKEN_SUBJECT, { queue: 'payment-tokenize-v1', callback: (err, message) => {
    if (err) return;
    let correlationId = 'unknown';
    let request;
    let parseError;
    try {
      request = JSON.parse(message.string());
      correlationId = request.correlation_id || request.order_id || 'unknown';
    } catch (error) {
      parseError = error;
    }
    enqueueTokenization({ message, request, correlationId, error: parseError });
  }});

  const commandSubscription = await js.pullSubscribe(COMMAND_SUBJECT, commandConsumerOptions());
  superviseCommandConsumer(nc, js, state, contracts, workerStatus, commandSubscription)
    .catch(workerError => {
      workerStatus.consumerReady = false;
      logger.error({ error: workerError.message }, 'payment consumer stopped');
    });
  (async () => {
    for await (const status of nc.status()) {
      if (status.type === 'disconnect' || status.type === 'error') workerStatus.connectionReady = false;
      if (status.type === 'reconnect') workerStatus.connectionReady = true;
    }
  })().catch(statusError => {
    workerStatus.connectionReady = false;
    logger.error({ error: statusError.message }, 'payment NATS status monitor stopped');
  });
  logger.info('Payment tokenization and durable command handlers are ready');
  return {
    nc,
    tokenSubscription,
    get commandSubscription() { return workerStatus.commandSubscription; },
    ready: () => workerStatus.connectionReady && workerStatus.consumerReady && !nc.isClosed(),
    markNotReady: () => {
      workerStatus.stopping = true;
      workerStatus.connectionReady = false;
      workerStatus.consumerReady = false;
    }
  };
}

module.exports = {
  startPaymentNATS, stableID, validateCard, tokenize, PaymentState, loadContracts,
  processTokenBatch, processCommand, processCommandBatch, runCommandConsumer, superviseCommandConsumer,
};
