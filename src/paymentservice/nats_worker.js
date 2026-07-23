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
    if (fs.existsSync(file)) this.value = JSON.parse(fs.readFileSync(file, 'utf8'));
    for (const key of ['tokens', 'tokenKeys', 'outcomes', 'authorizations']) {
      if (!this.value[key]) this.value[key] = {};
    }
  }

  persist() {
    const temporary = `${this.file}.tmp`;
    const fd = fs.openSync(temporary, 'w', 0o600);
    try {
      fs.writeFileSync(fd, JSON.stringify(this.value));
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(temporary, this.file);
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

function tokenize(state, request) {
  if (!request.order_id || !request.idempotency_key) throw new Error('INVALID_TOKEN_REQUEST');
  const card = validateCard(request);
  const identity = `${request.order_id}\0${request.idempotency_key}`;
  const existing = state.value.tokenKeys[identity];
  if (existing && state.value.tokens[existing] && state.value.tokens[existing].expiresAt > Date.now()) {
    return { payment_token: existing, expires_at: new Date(state.value.tokens[existing].expiresAt).toISOString() };
  }
  const token = `ptok_${crypto.randomBytes(24).toString('base64url')}`;
  const expiresAt = Date.now() + 15 * 60 * 1000;
  state.value.tokens[token] = { orderId: request.order_id, cardType: card.type, last4: card.last4, expiresAt, consumed: false };
  state.value.tokenKeys[identity] = token;
  state.persist();
  return { payment_token: token, expires_at: new Date(expiresAt).toISOString() };
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
    token.consumed = true;
    const authorizationID = stableID('authorization', command.idempotencyKey);
    state.value.authorizations[authorizationID] = { orderId: command.orderId, amount: command.amount, captured: false, released: false };
    return outcome(contracts, envelope, 'boutique.evt.payment.authorized.v1', 'boutique.payment.Authorized.v1', contracts.Authorized,
      { orderId: command.orderId, authorizationId: authorizationID, amount: command.amount });
  }
  if (subject === 'boutique.cmd.payment.capture.v1') {
    const command = contracts.Capture.decode(envelope.data.value);
    const authorization = state.value.authorizations[command.authorizationId];
    if (mode === 'capture_failed' || !authorization || authorization.orderId !== command.orderId || authorization.released) {
      return outcome(contracts, envelope, 'boutique.evt.payment.capture-failed.v1', 'boutique.payment.CaptureFailed.v1', contracts.CaptureFailed,
        { orderId: command.orderId, authorizationId: command.authorizationId, failure: failure('CAPTURE_FAILED', 'Payment capture failed.', true) });
    }
    authorization.captured = true;
    return outcome(contracts, envelope, 'boutique.evt.payment.captured.v1', 'boutique.payment.Captured.v1', contracts.Captured,
      { orderId: command.orderId, transactionId: stableID('capture', command.idempotencyKey), amount: command.amount });
  }
  if (subject === 'boutique.cmd.payment.release-authorization.v1') {
    const command = contracts.Release.decode(envelope.data.value);
    const authorization = state.value.authorizations[command.authorizationId];
    if (mode === 'release_failed' || !authorization || authorization.orderId !== command.orderId) {
      return outcome(contracts, envelope, 'boutique.evt.payment.authorization-release-failed.v1', 'boutique.payment.AuthorizationReleaseFailed.v1', contracts.ReleaseFailed,
        { orderId: command.orderId, authorizationId: command.authorizationId, failure: failure('AUTHORIZATION_RELEASE_FAILED', 'Authorization release requires review.') });
    }
    authorization.released = true;
    return outcome(contracts, envelope, 'boutique.evt.payment.authorization-released.v1', 'boutique.payment.AuthorizationReleased.v1', contracts.Released,
      { orderId: command.orderId, authorizationId: command.authorizationId });
  }
  throw new Error(`unsupported payment command ${subject}`);
}

async function runCommandConsumer(commandSubscription, state, contracts, js) {
  // Legacy pull subscriptions do not request messages merely by being
  // iterated. Keep exactly one outstanding pull so an idle worker receives the
  // next command without polling or overlapping pull requests.
  commandSubscription.pull({ batch: 1 });
  for await (const message of commandSubscription) {
    let correlationId = 'unknown';
    let messageId = 'unknown';
    let envelope;
    let decodeError;
    try {
      envelope = contracts.Envelope.decode(message.data);
      correlationId = envelope.correlationId || 'unknown';
      messageId = envelope.messageId || 'unknown';
    } catch (error) {
      decodeError = error;
    }
    logger.debug({
      topic: message.subject,
      message_kind: 'command',
      message_id: messageId,
      correlation_id: correlationId,
    }, 'NATS command received');
    try {
      if (decodeError) throw decodeError;
      let result = state.value.outcomes[envelope.messageId];
      if (!result) {
        result = processCommand(state, contracts, message.subject, envelope);
        state.value.outcomes[envelope.messageId] = result;
        state.persist();
      }
      const publishHeaders = headers(); publishHeaders.set('Nats-Msg-Id', result.messageID);
      await js.publish(result.subject, Buffer.from(result.data, 'base64'), { msgID: result.messageID, headers: publishHeaders });
      logger.debug({
        topic: result.subject,
        message_kind: 'event',
        message_id: result.messageID,
        correlation_id: correlationId,
      }, 'NATS event sent');
      message.ack();
    } catch (commandError) {
      logger.error({
        topic: message.subject,
        message_id: messageId,
        correlation_id: correlationId,
        error: commandError.message,
      }, 'payment command processing failed');
      message.nak(1000);
    } finally {
      if (!commandSubscription.isClosed()) commandSubscription.pull({ batch: 1 });
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
  const workerStatus = { ready: false };

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
    logger.debug({
      topic: message.subject || TOKEN_SUBJECT,
      message_kind: 'query',
      correlation_id: correlationId,
    }, 'NATS query received');
    try {
      if (parseError) throw parseError;
      message.respond(JSON.stringify(tokenize(state, request)));
    } catch (tokenError) {
      message.respond(JSON.stringify({ error: tokenError.message, safe_message: 'Payment details could not be tokenized.' }));
      logger.error({
        topic: message.subject || TOKEN_SUBJECT,
        correlation_id: correlationId,
        error: tokenError.message,
      }, 'payment tokenization query processing failed');
    }
  }});

  const options = consumerOpts();
  options.durable(COMMAND_DURABLE); options.manualAck(); options.ackExplicit(); options.ackWait(30000);
  options.maxDeliver(10); options.deliverAll(); options.bindStream('BOUTIQUE_COMMANDS'); options.filterSubject(COMMAND_SUBJECT);
  const commandSubscription = await js.pullSubscribe(COMMAND_SUBJECT, options);
  workerStatus.ready = true;
  runCommandConsumer(commandSubscription, state, contracts, js)
    .catch(workerError => {
      workerStatus.ready = false;
      logger.error({ error: workerError.message }, 'payment consumer stopped');
    });
  (async () => {
    for await (const status of nc.status()) {
      if (status.type === 'disconnect' || status.type === 'error') workerStatus.ready = false;
      if (status.type === 'reconnect') workerStatus.ready = true;
    }
  })().catch(statusError => {
    workerStatus.ready = false;
    logger.error({ error: statusError.message }, 'payment NATS status monitor stopped');
  });
  logger.info('Payment tokenization and durable command handlers are ready');
  return {
    nc,
    tokenSubscription,
    commandSubscription,
    ready: () => workerStatus.ready && !nc.isClosed(),
    markNotReady: () => { workerStatus.ready = false; }
  };
}

module.exports = { startPaymentNATS, stableID, validateCard, tokenize, PaymentState, loadContracts, processCommand, runCommandConsumer };
