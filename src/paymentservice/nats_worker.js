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
const PAYMENT_TOKEN_PREFIX = 'ptok_v1';
const AUTHORIZATION_PREFIX = 'pauth_v1';
const PAYMENT_TOKEN_TTL_MS = 15 * 60 * 1000;
const SIGNING_KEY_CONTEXT = 'boutique/payment-provider/v1';
const SIGNING_KEY_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$/;
const RESULT_ID_DOMAIN = 'boutique.result.v1';
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

function deriveResultMessageID(inputMessageID, resultSlot) {
  if (typeof inputMessageID !== 'string' || !inputMessageID ||
      typeof resultSlot !== 'string' || !resultSlot) {
    throw new Error('result input message ID and slot are required');
  }
  const input = Buffer.from(inputMessageID, 'utf8');
  const slot = Buffer.from(resultSlot, 'utf8');
  const inputLength = Buffer.alloc(4);
  const slotLength = Buffer.alloc(4);
  inputLength.writeUInt32BE(input.length);
  slotLength.writeUInt32BE(slot.length);
  const digest = crypto.createHash('sha256')
    .update(Buffer.from(RESULT_ID_DOMAIN, 'utf8'))
    .update(Buffer.from([0]))
    .update(inputLength)
    .update(input)
    .update(slotLength)
    .update(slot)
    .digest('base64url');
  return `br1_${digest}`;
}

// Every payment replica receives the same dedicated signing secret. HKDF
// domain-separates the artifact key from the provisioned root key.
function deriveSigningKey(sharedSecret) {
  if (typeof sharedSecret !== 'string' || sharedSecret.length < 32) {
    throw new Error('payment signing secret must contain at least 32 characters');
  }
  return Buffer.from(crypto.hkdfSync(
    'sha256',
    Buffer.from(sharedSecret, 'utf8'),
    Buffer.alloc(0),
    Buffer.from(SIGNING_KEY_CONTEXT, 'utf8'),
    32,
  ));
}

function createSigningKeyring(activeKeyID, activeSecret, verificationSecrets = {}) {
  if (!SIGNING_KEY_ID_PATTERN.test(activeKeyID || '')) {
    throw new Error('payment active signing key ID is invalid');
  }
  if (verificationSecrets === null || Array.isArray(verificationSecrets) ||
      typeof verificationSecrets !== 'object') {
    throw new Error('payment verification keys must be a JSON object');
  }
  const keys = new Map();
  for (const [keyID, secret] of Object.entries(verificationSecrets)) {
    if (!SIGNING_KEY_ID_PATTERN.test(keyID)) {
      throw new Error(`payment verification key ID ${keyID} is invalid`);
    }
    keys.set(keyID, deriveSigningKey(secret));
  }
  const activeKey = deriveSigningKey(activeSecret);
  if (keys.has(activeKeyID) &&
      !crypto.timingSafeEqual(keys.get(activeKeyID), activeKey)) {
    throw new Error('payment active key conflicts with its verification key');
  }
  keys.set(activeKeyID, activeKey);
  const fingerprintHash = crypto.createHash('sha256');
  for (const keyID of [...keys.keys()].sort()) {
    fingerprintHash
      .update(keyID)
      .update(Buffer.from([0]))
      .update(crypto.createHash('sha256').update(keys.get(keyID)).digest());
  }
  const fingerprint = fingerprintHash.digest('hex').slice(0, 16);
  return { activeKeyID, keys, fingerprint };
}

function loadSigningKeyring(environment = process.env) {
  let verificationSecrets = {};
  if (environment.PAYMENT_VERIFICATION_KEYS) {
    try {
      verificationSecrets = JSON.parse(environment.PAYMENT_VERIFICATION_KEYS);
    } catch (_) {
      throw new Error('PAYMENT_VERIFICATION_KEYS must be valid JSON');
    }
  }
  return createSigningKeyring(
    environment.PAYMENT_SIGNING_KEY_ID,
    environment.PAYMENT_SIGNING_KEY,
    verificationSecrets,
  );
}

function signingKey(keyring, keyID) {
  if (!keyring || !(keyring.keys instanceof Map)) {
    throw new Error('payment signing keyring is required');
  }
  const key = keyring.keys.get(keyID);
  if (!key) throw new Error('UNKNOWN_PAYMENT_SIGNING_KEY');
  return key;
}

function signReference(prefix, payload, keyring, keyID = keyring.activeKeyID) {
  if (!SIGNING_KEY_ID_PATTERN.test(keyID || '')) {
    throw new Error('payment signing key ID is invalid');
  }
  const encodedPayload = Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
  const unsigned = `${prefix}.${keyID}.${encodedPayload}`;
  const signature = crypto.createHmac('sha256', signingKey(keyring, keyID))
    .update(unsigned)
    .digest('base64url');
  return `${unsigned}.${signature}`;
}

function verifyReference(reference, expectedPrefix, keyring) {
  if (typeof reference !== 'string' || reference.length > 2048) {
    throw new Error('INVALID_PAYMENT_REFERENCE');
  }
  const parts = reference.split('.');
  if (parts.length !== 4 || parts[0] !== expectedPrefix ||
      !SIGNING_KEY_ID_PATTERN.test(parts[1] || '') || !parts[2] || !parts[3]) {
    throw new Error('INVALID_PAYMENT_REFERENCE');
  }
  const unsigned = `${parts[0]}.${parts[1]}.${parts[2]}`;
  const actual = Buffer.from(parts[3], 'base64url');
  let verificationKey;
  try {
    verificationKey = signingKey(keyring, parts[1]);
  } catch (_) {
    throw new Error('INVALID_PAYMENT_REFERENCE');
  }
  const expected = crypto.createHmac('sha256', verificationKey).update(unsigned).digest();
  if (actual.length !== expected.length || !crypto.timingSafeEqual(actual, expected)) {
    throw new Error('INVALID_PAYMENT_REFERENCE');
  }
  try {
    return {
      keyID: parts[1],
      payload: JSON.parse(Buffer.from(parts[2], 'base64url').toString('utf8')),
    };
  } catch (_) {
    throw new Error('INVALID_PAYMENT_REFERENCE');
  }
}

function verifyPaymentToken(paymentToken, orderID, keyring, now = Date.now()) {
  const verified = verifyReference(paymentToken, PAYMENT_TOKEN_PREFIX, keyring);
  const payload = verified.payload;
  if (payload.version !== 1 || payload.orderId !== orderID ||
      !Number.isSafeInteger(payload.expiresAt) || payload.expiresAt <= now ||
      typeof payload.nonce !== 'string' || !payload.nonce) {
    throw new Error('INVALID_OR_EXPIRED_TOKEN');
  }
  return verified;
}

function authorizationReference(command, keyring, keyID = keyring.activeKeyID) {
  return signReference(AUTHORIZATION_PREFIX, {
    version: 1,
    orderId: command.orderId,
    idempotencyKey: command.idempotencyKey,
  }, keyring, keyID);
}

function verifyAuthorization(authorizationID, orderID, keyring) {
  const verified = verifyReference(authorizationID, AUTHORIZATION_PREFIX, keyring);
  const payload = verified.payload;
  if (payload.version !== 1 || payload.orderId !== orderID ||
      typeof payload.idempotencyKey !== 'string' || !payload.idempotencyKey) {
    throw new Error('INVALID_AUTHORIZATION');
  }
  return verified;
}

function occurredAtMilliseconds(envelope) {
  if (!envelope.occurredAt) throw new Error('INVALID_COMMAND_TIMESTAMP');
  const seconds = Number(envelope.occurredAt.seconds);
  const nanos = Number(envelope.occurredAt.nanos);
  const milliseconds = seconds * 1000 + Math.floor(nanos / 1000000);
  if (!Number.isSafeInteger(milliseconds) || !Number.isInteger(nanos) ||
      nanos < 0 || nanos >= 1000000000) {
    throw new Error('INVALID_COMMAND_TIMESTAMP');
  }
  return milliseconds;
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

function tokenize(keyring, request, now = Date.now()) {
  if (!request.order_id || !request.idempotency_key) throw new Error('INVALID_TOKEN_REQUEST');
  validateCard(request);
  const expiresAt = now + PAYMENT_TOKEN_TTL_MS;
  const paymentToken = signReference(PAYMENT_TOKEN_PREFIX, {
    version: 1,
    orderId: request.order_id,
    expiresAt,
    nonce: crypto.randomBytes(16).toString('base64url'),
  }, keyring);
  return { payment_token: paymentToken, expires_at: new Date(expiresAt).toISOString() };
}

function processTokenBatch(entries, keyring) {
  logger.debug({
    topic: TOKEN_SUBJECT,
    message_kind: 'query',
    batch_size: entries.length,
  }, 'NATS query batch received');

  for (const entry of entries) {
    if (entry.error) continue;
    try {
      entry.result = tokenize(keyring, entry.request);
    } catch (tokenError) {
      entry.error = tokenError;
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

function createTokenBatcher(keyring) {
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
    processTokenBatch(batch, keyring);
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

function outcome(contracts, cause, resultSlot, subject, messageType, PayloadType, payload) {
  if (!cause.messageId || !cause.correlationId || !cause.aggregateId ||
      !cause.aggregateVersion || !cause.occurredAt) {
    throw new Error('payment command envelope is incomplete');
  }
  const messageID = deriveResultMessageID(cause.messageId, resultSlot);
  const envelope = {
    messageId: messageID, messageType, schemaVersion: 1,
    occurredAt: cause.occurredAt, producer: 'paymentservice/phase3',
    aggregateType: 'order', aggregateId: cause.aggregateId, aggregateVersion: cause.aggregateVersion,
    correlationId: cause.correlationId, causationId: cause.messageId, traceparent: cause.traceparent,
    tracestate: cause.tracestate, data: anyPayload(PayloadType, payload),
  };
  return { messageID, subject, data: Buffer.from(contracts.Envelope.encode(envelope).finish()).toString('base64') };
}

function processCommand(keyring, contracts, subject, envelope) {
  const aggregateVersion = Number(envelope?.aggregateVersion);
  if (!envelope?.messageId || !envelope?.correlationId || !envelope?.aggregateId ||
      !Number.isSafeInteger(aggregateVersion) || aggregateVersion <= 0 ||
      !envelope?.data?.value) {
    throw new Error('payment command envelope is incomplete');
  }
  const commandTime = occurredAtMilliseconds(envelope);
  const mode = process.env.PAYMENT_FAILURE_MODE || '';
  if (subject === 'boutique.cmd.payment.authorize.v1') {
    const command = contracts.Authorize.decode(envelope.data.value);
    let verifiedToken;
    try {
      verifiedToken = verifyPaymentToken(
        command.paymentToken,
        command.orderId,
        keyring,
        commandTime,
      );
    } catch (_) {
      verifiedToken = undefined;
    }
    if (!command.commandId || !command.orderId || !command.idempotencyKey ||
        command.orderId !== envelope.aggregateId) {
      throw new Error('INVALID_AUTHORIZE_COMMAND');
    }
    if (mode === 'authorization_declined' || !verifiedToken) {
      return outcome(contracts, envelope, 'payment.authorize',
        'boutique.evt.payment.authorization-declined.v1', 'boutique.payment.AuthorizationDeclined.v1',
        contracts.Declined, { orderId: command.orderId, declineCategory: mode ? 'TEST_DECLINE' : 'INVALID_OR_EXPIRED_TOKEN' });
    }
    // Bind the authorization to the token's signing key. During a rotation,
    // an old token therefore reproduces the same authorization on old and new
    // replicas while the old key remains in the overlap set.
    const authorizationID = authorizationReference(command, keyring, verifiedToken.keyID);
    return outcome(contracts, envelope, 'payment.authorize',
      'boutique.evt.payment.authorized.v1', 'boutique.payment.Authorized.v1', contracts.Authorized,
      { orderId: command.orderId, authorizationId: authorizationID, amount: command.amount });
  }
  if (subject === 'boutique.cmd.payment.capture.v1') {
    const command = contracts.Capture.decode(envelope.data.value);
    if (!command.commandId || !command.orderId || !command.authorizationId ||
        !command.idempotencyKey || command.orderId !== envelope.aggregateId) {
      throw new Error('INVALID_CAPTURE_COMMAND');
    }
    let validAuthorization = false;
    try {
      verifyAuthorization(command.authorizationId, command.orderId, keyring);
      validAuthorization = true;
    } catch (_) {
      validAuthorization = false;
    }
    if (mode === 'capture_failed' || !validAuthorization) {
      return outcome(contracts, envelope, 'payment.capture',
        'boutique.evt.payment.capture-failed.v1', 'boutique.payment.CaptureFailed.v1', contracts.CaptureFailed,
        { orderId: command.orderId, authorizationId: command.authorizationId, failure: failure('CAPTURE_FAILED', 'Payment capture failed.', true) });
    }
    return outcome(contracts, envelope, 'payment.capture',
      'boutique.evt.payment.captured.v1', 'boutique.payment.Captured.v1', contracts.Captured,
      { orderId: command.orderId, transactionId: stableID('capture', command.idempotencyKey), amount: command.amount });
  }
  if (subject === 'boutique.cmd.payment.release-authorization.v1') {
    const command = contracts.Release.decode(envelope.data.value);
    if (!command.commandId || !command.orderId || !command.authorizationId ||
        !command.idempotencyKey || command.orderId !== envelope.aggregateId) {
      throw new Error('INVALID_RELEASE_COMMAND');
    }
    let validAuthorization = false;
    try {
      verifyAuthorization(command.authorizationId, command.orderId, keyring);
      validAuthorization = true;
    } catch (_) {
      validAuthorization = false;
    }
    if (mode === 'release_failed' || !validAuthorization) {
      return outcome(contracts, envelope, 'payment.release-authorization',
        'boutique.evt.payment.authorization-release-failed.v1', 'boutique.payment.AuthorizationReleaseFailed.v1',
        contracts.ReleaseFailed, { orderId: command.orderId, authorizationId: command.authorizationId, failure: failure('AUTHORIZATION_RELEASE_FAILED', 'Authorization release requires review.') });
    }
    return outcome(contracts, envelope, 'payment.release-authorization',
      'boutique.evt.payment.authorization-released.v1', 'boutique.payment.AuthorizationReleased.v1', contracts.Released,
      { orderId: command.orderId, authorizationId: command.authorizationId });
  }
  throw new Error(`unsupported payment command ${subject}`);
}

async function processCommandBatch(messages, keyring, contracts, js) {
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
      command.result = processCommand(keyring, contracts, command.message.subject, command.envelope);
    } catch (commandError) {
      command.error = commandError;
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

async function runCommandConsumer(commandSubscription, keyring, contracts, js,
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
      .then(() => processCommandBatch(batch, keyring, contracts, js))
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

async function superviseCommandConsumer(nc, js, keyring, contracts, workerStatus, initialSubscription,
    restartDelayMs = COMMAND_RESTART_DELAY_MS) {
  let commandSubscription = initialSubscription;
  while (!workerStatus.stopping && !nc.isClosed()) {
    try {
      if (!commandSubscription) {
        commandSubscription = await js.pullSubscribe(COMMAND_SUBJECT, commandConsumerOptions());
      }
      workerStatus.commandSubscription = commandSubscription;
      workerStatus.consumerReady = true;
      await runCommandConsumer(commandSubscription, keyring, contracts, js);
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
  for (const key of [
    'NATS_URL',
    'NATS_USER',
    'NATS_PASSWORD',
    'NATS_CA_FILE',
    'PAYMENT_SIGNING_KEY_ID',
    'PAYMENT_SIGNING_KEY',
  ]) {
    if (!process.env[key]) throw new Error(`${key} is required`);
  }
  const contracts = await loadContracts();
  const keyring = loadSigningKeyring();
  const nc = await connect({ servers: process.env.NATS_URL, user: process.env.NATS_USER, pass: process.env.NATS_PASSWORD,
    name: 'paymentservice/phase3', tls: { caFile: process.env.NATS_CA_FILE },
    reconnectTimeWait: 2000, maxReconnectAttempts: -1, pingInterval: 20000, maxPingOut: 2 });
  const js = nc.jetstream({ timeout: 5000 });
  const workerStatus = {
    connectionReady: !nc.isClosed(), consumerReady: false, stopping: false, commandSubscription: undefined,
  };
  const enqueueTokenization = createTokenBatcher(keyring);

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
  superviseCommandConsumer(nc, js, keyring, contracts, workerStatus, commandSubscription)
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
  logger.info({
    active_signing_key_id: keyring.activeKeyID,
    signing_key_set_fingerprint: keyring.fingerprint,
  }, 'Stateless payment tokenization and durable command handlers are ready');
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
  startPaymentNATS, stableID, deriveResultMessageID, deriveSigningKey, createSigningKeyring,
  loadSigningKeyring, validateCard, tokenize, verifyPaymentToken, authorizationReference,
  verifyAuthorization, occurredAtMilliseconds, loadContracts,
  processTokenBatch, processCommand, processCommandBatch, runCommandConsumer, superviseCommandConsumer,
};
