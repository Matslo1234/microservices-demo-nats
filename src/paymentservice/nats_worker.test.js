/* Copyright 2026 Google LLC; Licensed under the Apache License, Version 2.0. */
'use strict';

const assert = require('assert');
const {
  createSigningKeyring, deriveResultMessageID, tokenize, verifyPaymentToken,
  loadSigningKeyring, processTokenBatch, loadContracts, processCommand, runCommandConsumer,
  superviseCommandConsumer, processingTimeMs, waitForProcessing, registerTokenizationService,
  superviseTokenizationService,
} = require('./nats_worker');

async function main() {
  assert.equal(processingTimeMs({}), 0);
  assert.equal(processingTimeMs({PROCESSING_TIME_MS: ''}), 0);
  assert.equal(processingTimeMs({PROCESSING_TIME_MS: 'not-a-number'}), 0);
  assert.equal(processingTimeMs({PROCESSING_TIME_MS: '0'}), 0);
  assert.equal(processingTimeMs({PROCESSING_TIME_MS: '-10'}), 0);
  assert.equal(processingTimeMs({PROCESSING_TIME_MS: 'Infinity'}), 0);
  assert.equal(processingTimeMs({PROCESSING_TIME_MS: '12.5'}), 12.5);
  const processingStartedAt = process.hrtime.bigint();
  await waitForProcessing({PROCESSING_TIME_MS: '10'});
  const processingElapsedMs = Number(process.hrtime.bigint() - processingStartedAt) / 1e6;
  assert(processingElapsedMs >= 8, 'configured payment processing time was not observed');

  const sharedCredential = 's0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef';
  const replicaAKeyring = createSigningKeyring('primary-v1', sharedCredential);
  const replicaBKeyring = createSigningKeyring('primary-v1', sharedCredential);
  const unrelatedReplicaKeyring = createSigningKeyring(
    'primary-v1',
    'sffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
  );
  assert.deepEqual(
    replicaAKeyring,
    replicaBKeyring,
    'replicas derived different active signing-key sets',
  );
  assert.notEqual(
    replicaAKeyring.keys.get('primary-v1').toString('hex'),
    unrelatedReplicaKeyring.keys.get('primary-v1').toString('hex'),
    'different credentials derived the same signing key',
  );

  const now = Date.now();
  const request = {
    order_id: 'order-1', idempotency_key: 'checkout-1',
    credit_card_number: '4432801561520454', credit_card_expiration_month: 12,
    credit_card_expiration_year: new Date().getFullYear() + 1, credit_card_cvv: '672',
  };
  const tokenizedByReplicaA = tokenize(replicaAKeyring, request, now);
  const paymentToken = tokenizedByReplicaA.payment_token;
  assert.equal(new Date(tokenizedByReplicaA.expires_at).getTime(), now + 15 * 60 * 1000);
  assert.doesNotThrow(
    () => verifyPaymentToken(paymentToken, request.order_id, replicaBKeyring, now),
    'a second replica could not verify the issued token',
  );
  assert.throws(
    () => verifyPaymentToken(paymentToken, request.order_id, unrelatedReplicaKeyring, now),
    /INVALID_PAYMENT_REFERENCE/,
    'a replica with another credential verified the token',
  );
  assert.throws(
    () => verifyPaymentToken(paymentToken, 'another-order', replicaBKeyring, now),
    /INVALID_OR_EXPIRED_TOKEN/,
    'the payment token was not bound to its order',
  );
  assert.throws(
    () => verifyPaymentToken(paymentToken, request.order_id, replicaBKeyring, now + 15 * 60 * 1000),
    /INVALID_OR_EXPIRED_TOKEN/,
    'an expired token was accepted',
  );
  const tokenParts = paymentToken.split('.');
  tokenParts[3] = `${tokenParts[3][0] === 'A' ? 'B' : 'A'}${tokenParts[3].slice(1)}`;
  const tamperedToken = tokenParts.join('.');
  assert.throws(
    () => verifyPaymentToken(tamperedToken, request.order_id, replicaBKeyring, now),
    /INVALID_PAYMENT_REFERENCE/,
    'a token with a modified signature was accepted',
  );
  assert.equal(paymentToken.split('.')[1], 'primary-v1', 'payment token omitted its signing key ID');
  const tokenPayload = JSON.parse(Buffer.from(paymentToken.split('.')[2], 'base64url').toString('utf8'));
  assert.deepEqual(
    Object.keys(tokenPayload).sort(),
    ['expiresAt', 'nonce', 'orderId', 'version'],
    'payment token contains unexpected card data',
  );

  const oldCredential = 'old0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef';
  const newCredential = 'new0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef';
  const oldKeyring = createSigningKeyring('key-2026-06', oldCredential);
  const overlapKeyring = createSigningKeyring(
    'key-2026-07',
    newCredential,
    {'key-2026-06': oldCredential},
  );
  const newOnlyKeyring = createSigningKeyring('key-2026-07', newCredential);
  const loadedOverlapKeyring = loadSigningKeyring({
    PAYMENT_SIGNING_KEY_ID: 'key-2026-07',
    PAYMENT_SIGNING_KEY: newCredential,
    PAYMENT_VERIFICATION_KEYS: JSON.stringify({'key-2026-06': oldCredential}),
  });
  assert.deepEqual(
    loadedOverlapKeyring,
    overlapKeyring,
    'replicas loaded different key sets from the same rotation configuration',
  );
  assert.throws(
    () => loadSigningKeyring({
      PAYMENT_SIGNING_KEY_ID: 'key-2026-07',
      PAYMENT_SIGNING_KEY: newCredential,
      PAYMENT_VERIFICATION_KEYS: '{',
    }),
    /valid JSON/,
    'malformed overlap configuration was accepted',
  );
  const oldToken = tokenize(oldKeyring, request, now).payment_token;
  assert.equal(oldToken.split('.')[1], 'key-2026-06');
  assert.doesNotThrow(
    () => verifyPaymentToken(oldToken, request.order_id, overlapKeyring, now),
    'the rotation overlap did not retain verification for the old key ID',
  );
  const newToken = tokenize(overlapKeyring, request, now).payment_token;
  assert.equal(newToken.split('.')[1], 'key-2026-07');
  assert.throws(
    () => verifyPaymentToken(newToken, request.order_id, oldKeyring, now),
    /INVALID_PAYMENT_REFERENCE/,
    'an old replica unexpectedly knew the new signing key',
  );
  assert.throws(
    () => verifyPaymentToken(oldToken, request.order_id, newOnlyKeyring, now),
    /INVALID_PAYMENT_REFERENCE/,
    'an expired overlap set continued accepting an old key ID',
  );

  const tokenResponses = [];
  processTokenBatch(Array.from({length: 32}, (_, index) => ({
    request: {...request, order_id: `token-batch-${index}`, idempotency_key: `token-batch-${index}`},
    correlationId: `token-batch-${index}`,
    message: {
      subject: 'boutique.qry.payment.tokenize.v1',
      respond: encoded => tokenResponses.push(JSON.parse(encoded)),
    },
  })), replicaAKeyring);
  assert.equal(tokenResponses.filter(response => response.payment_token).length, 32,
    'token batch did not return every token');
  for (let index = 0; index < tokenResponses.length; index++) {
    assert.doesNotThrow(
      () => verifyPaymentToken(tokenResponses[index].payment_token, `token-batch-${index}`, replicaBKeyring),
      `replica B could not verify token batch item ${index}`,
    );
  }

  let serviceConfig;
  let endpointRegistration;
  let serviceRegistrationFlushed = false;
  const tokenizationService = {
    isStopped: false,
    addEndpoint: (name, options) => {
      endpointRegistration = {name, options};
      return {name, subject: options.subject};
    },
    stop: async () => {},
  };
  const registered = await registerTokenizationService({
    services: {
      add: async config => {
        serviceConfig = config;
        return tokenizationService;
      },
    },
    flush: async () => { serviceRegistrationFlushed = true; },
  }, replicaAKeyring);
  assert.deepEqual(serviceConfig, {
    name: 'PaymentTokenization',
    version: '1.0.0',
    description: 'Ephemeral payment card tokenization',
    queue: 'payment-tokenize-v1',
    statsHandler: serviceConfig.statsHandler,
  });
  assert.equal(typeof serviceConfig.statsHandler, 'function');
  assert.equal(endpointRegistration.name, 'tokenize');
  assert.equal(endpointRegistration.options.subject, 'boutique.qry.payment.tokenize.v1');
  assert.equal(typeof endpointRegistration.options.handler, 'function');
  assert.equal(registered.service, tokenizationService);
  assert(serviceRegistrationFlushed, 'tokenization service registration was not flushed');
  const microResponses = [];
  const microRequest = {
    ...request,
    order_id: 'micro-tokenization-order',
    idempotency_key: 'micro-tokenization-order',
  };
  endpointRegistration.options.handler(null, {
    subject: 'boutique.qry.payment.tokenize.v1',
    string: () => JSON.stringify(microRequest),
    respond: encoded => microResponses.push(JSON.parse(encoded)),
  });
  assert.deepEqual(
    await serviceConfig.statsHandler(),
    {pending_requests: 1},
    'NATS micro stats did not expose queued tokenization requests',
  );
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(microResponses.length, 1, 'NATS micro endpoint did not respond');
  assert.doesNotThrow(
    () => verifyPaymentToken(
      microResponses[0].payment_token, microRequest.order_id, replicaBKeyring,
    ),
    'NATS micro endpoint returned an invalid payment token',
  );

  let stopInitialService;
  const initialService = {
    isStopped: false,
    stopped: new Promise(resolve => { stopInitialService = resolve; }),
  };
  const recoveredService = {
    isStopped: false,
    stopped: Promise.resolve(null),
    addEndpoint: (_name, options) => ({subject: options.subject}),
    stop: async () => {},
  };
  const tokenStatus = {
    service: initialService, endpoint: {}, ready: true, failedAt: 0,
    stopping: false, supervising: false,
  };
  let tokenRegistrations = 0;
  const tokenSupervisor = superviseTokenizationService({
    isClosed: () => false,
    services: {
      add: async () => {
        tokenRegistrations++;
        tokenStatus.stopping = true;
        return recoveredService;
      },
    },
    flush: async () => {},
  }, replicaAKeyring, tokenStatus, {service: initialService, endpoint: {}}, 0);
  stopInitialService(new Error('token endpoint stopped'));
  await tokenSupervisor;
  assert.equal(tokenRegistrations, 1, 'stopped tokenization service was not recreated');
  assert.equal(tokenStatus.service, recoveredService, 'replacement tokenization service was not tracked');
  assert(!tokenStatus.supervising, 'tokenization supervisor did not stop cleanly');

  const contracts = await loadContracts();
  const occurredAt = {seconds: Math.floor(now / 1000), nanos: (now % 1000) * 1000000};
  const authorize = { commandId: 'authorize-1', orderId: 'order-1', paymentToken,
    idempotencyKey: 'order-1/authorize', amount: { currencyCode: 'USD', units: 20, nanos: 0 } };
  const envelope = { messageId: 'authorize-message', aggregateId: 'order-1', aggregateVersion: 2,
    correlationId: 'order-1', occurredAt,
    data: { value: contracts.Authorize.encode(authorize).finish() } };
  assert.equal(
    deriveResultMessageID('event-order-completed-42', 'notification.order-confirmation'),
    'br1_BipmFE_ifI2JqRb67NFrgisjZYeejPTlkKhojRP1Mz8',
    'payment result ID helper diverged from the Phase 0 contract',
  );
  const resultFromReplicaB = processCommand(
    replicaBKeyring, contracts, 'boutique.cmd.payment.authorize.v1', envelope,
  );
  assert.equal(resultFromReplicaB.subject, 'boutique.evt.payment.authorized.v1');
  const repeatedResultFromReplicaA = processCommand(
    replicaAKeyring, contracts, 'boutique.cmd.payment.authorize.v1', envelope,
  );
  assert.deepEqual(
    repeatedResultFromReplicaA,
    resultFromReplicaB,
    'two replicas produced different outcomes for the same authorization command',
  );
  const decodedResult = contracts.Envelope.decode(Buffer.from(resultFromReplicaB.data, 'base64'));
  assert.equal(decodedResult.data.type_url, 'type.googleapis.com/boutique.events.v1.PaymentAuthorizedEvent',
    'payment result omitted the protobuf Any type URL');
  const authorized = contracts.Authorized.decode(decodedResult.data.value);
  assert(authorized.authorizationId.startsWith('pauth_v1.'), 'authorization reference was not signed');
  assert.equal(
    authorized.authorizationId.split('.')[1],
    'primary-v1',
    'authorization reference omitted the signing key ID',
  );

  const rotatingAuthorize = {...authorize, paymentToken: oldToken};
  const rotatingEnvelope = {
    ...envelope,
    messageId: 'rotating-authorize-message',
    data: {value: contracts.Authorize.encode(rotatingAuthorize).finish()},
  };
  const beforeRotation = processCommand(
    oldKeyring,
    contracts,
    'boutique.cmd.payment.authorize.v1',
    rotatingEnvelope,
  );
  const duringOverlap = processCommand(
    overlapKeyring,
    contracts,
    'boutique.cmd.payment.authorize.v1',
    rotatingEnvelope,
  );
  assert.deepEqual(
    beforeRotation,
    duringOverlap,
    'key rotation changed a retried authorization outcome during the overlap window',
  );
  const rotatingResult = contracts.Envelope.decode(
    Buffer.from(duringOverlap.data, 'base64'),
  );
  const rotatingAuthorized = contracts.Authorized.decode(rotatingResult.data.value);
  assert.equal(
    rotatingAuthorized.authorizationId.split('.')[1],
    'key-2026-06',
    'authorization did not retain the token key identity during rotation',
  );

  const wrongCredentialResult = processCommand(
    unrelatedReplicaKeyring, contracts, 'boutique.cmd.payment.authorize.v1', envelope,
  );
  assert.equal(wrongCredentialResult.subject, 'boutique.evt.payment.authorization-declined.v1',
    'a replica with the wrong signing key authorized the token');
  const expiredForCommand = tokenize(replicaAKeyring, request, now - 15 * 60 * 1000).payment_token;
  const expiredCommandResult = processCommand(
    replicaBKeyring, contracts, 'boutique.cmd.payment.authorize.v1',
    {...envelope, messageId: 'expired-token-message',
      data: {value: contracts.Authorize.encode({...authorize, paymentToken: expiredForCommand}).finish()}},
  );
  assert.equal(expiredCommandResult.subject, 'boutique.evt.payment.authorization-declined.v1',
    'a token expired when the command was issued was authorized');
  const delayedToken = tokenize(
    replicaAKeyring, request, now - 15 * 60 * 1000 - 10 * 1000,
  ).payment_token;
  const delayedCommandTime = now - 20 * 1000;
  const delayedCommandResult = processCommand(
    replicaBKeyring, contracts, 'boutique.cmd.payment.authorize.v1',
    {...envelope, messageId: 'delayed-token-message',
      occurredAt: {
        seconds: Math.floor(delayedCommandTime / 1000),
        nanos: (delayedCommandTime % 1000) * 1000000,
      },
      data: {value: contracts.Authorize.encode({...authorize, paymentToken: delayedToken}).finish()}},
  );
  assert.equal(delayedCommandResult.subject, 'boutique.evt.payment.authorized.v1',
    'a delayed command changed outcome after its token expired in wall-clock time');

  const capture = { commandId: 'capture-1', orderId: 'order-1',
    authorizationId: authorized.authorizationId, idempotencyKey: 'order-1/capture',
    amount: authorize.amount };
  const captureEnvelope = {...envelope, messageId: 'capture-message',
    data: { value: contracts.Capture.encode(capture).finish() }};
  const captured = processCommand(
    replicaAKeyring, contracts, 'boutique.cmd.payment.capture.v1', captureEnvelope,
  );
  assert.equal(captured.subject, 'boutique.evt.payment.captured.v1',
    'another replica could not capture the signed authorization');

  const release = { commandId: 'release-1', orderId: 'order-1',
    authorizationId: authorized.authorizationId, idempotencyKey: 'order-1/release' };
  const releaseEnvelope = {...envelope, messageId: 'release-message',
    data: { value: contracts.Release.encode(release).finish() }};
  const released = processCommand(
    replicaBKeyring, contracts, 'boutique.cmd.payment.release-authorization.v1', releaseEnvelope,
  );
  assert.equal(released.subject, 'boutique.evt.payment.authorization-released.v1',
    'another replica could not release the signed authorization');

  const invalidCapture = processCommand(
    replicaAKeyring, contracts, 'boutique.cmd.payment.capture.v1',
    {...captureEnvelope, messageId: 'invalid-capture-message', aggregateId: 'another-order',
      data: {value: contracts.Capture.encode({...capture, orderId: 'another-order'}).finish()}},
  );
  assert.equal(invalidCapture.subject, 'boutique.evt.payment.capture-failed.v1',
    'an authorization was accepted for another order');

  process.env.PAYMENT_FAILURE_MODE = 'authorization_declined';
  const declined = processCommand(
    replicaAKeyring, contracts, 'boutique.cmd.payment.authorize.v1',
    {...envelope, messageId: 'declined-message'},
  );
  assert.equal(declined.subject, 'boutique.evt.payment.authorization-declined.v1');
  process.env.PAYMENT_FAILURE_MODE = 'capture_failed';
  const captureFailed = processCommand(
    replicaAKeyring, contracts, 'boutique.cmd.payment.capture.v1',
    {...captureEnvelope, messageId: 'capture-failed-message'},
  );
  assert.equal(captureFailed.subject, 'boutique.evt.payment.capture-failed.v1');
  process.env.PAYMENT_FAILURE_MODE = 'release_failed';
  const releaseFailed = processCommand(
    replicaAKeyring, contracts, 'boutique.cmd.payment.release-authorization.v1',
    {...releaseEnvelope, messageId: 'release-failed-message'},
  );
  assert.equal(releaseFailed.subject, 'boutique.evt.payment.authorization-release-failed.v1');
  delete process.env.PAYMENT_FAILURE_MODE;

  const pulls = [];
  let acknowledgements = 0;
  const publishedMessageIDs = [];
  const messages = [];
  for (let index = 0; index < 32; index++) {
    const orderID = `order-worker-${index}`;
    const workerRequest = {...request, order_id: orderID, idempotency_key: orderID};
    const workerToken = tokenize(replicaAKeyring, workerRequest).payment_token;
    const workerCommand = {...authorize, commandId: `authorize-worker-${index}`, orderId: orderID,
      paymentToken: workerToken, idempotencyKey: `${orderID}/authorize`};
    const workerEnvelope = contracts.Envelope.encode({messageId: `authorize-worker-message-${index}`,
      aggregateId: orderID, aggregateVersion: 2, correlationId: orderID, occurredAt,
      data: {value: contracts.Authorize.encode(workerCommand).finish()}}).finish();
    messages.push({data: workerEnvelope, subject: 'boutique.cmd.payment.authorize.v1',
      ack: () => { acknowledgements++; },
      nak: () => { throw new Error('worker unexpectedly NAKed the command'); }});
  }
  const subscription = {pull: options => { pulls.push(options); }, isClosed: () => false,
    async *[Symbol.asyncIterator]() {
      for (const message of messages) yield message;
    }};
  await runCommandConsumer(subscription, replicaBKeyring, contracts, {
    publish: async (_subject, _data, options) => { publishedMessageIDs.push(options.msgID); },
  });
  const expectedPull = [{batch: 256, expires: 1000, idle_heartbeat: 500},
    {batch: 256, expires: 1000, idle_heartbeat: 500}];
  assert.deepEqual(pulls, expectedPull, 'worker did not replenish expiring command pull credit');
  assert.equal(acknowledgements, 32, 'worker did not ACK every command in the batch');
  assert.equal(publishedMessageIDs.length, 32, 'worker did not publish every deterministic outcome');
  assert.equal(new Set(publishedMessageIDs).size, 32, 'worker reused an event message ID');

  let activeBatches = 0;
  let maximumActiveBatches = 0;
  let releaseParallelBatches;
  const parallelBatchesReady = new Promise(resolve => { releaseParallelBatches = resolve; });
  const parallelSubscription = {
    pull: () => {},
    isClosed: () => false,
    async *[Symbol.asyncIterator]() {
      yield messages[0];
      await new Promise(resolve => setTimeout(resolve, 30));
      yield messages[1];
      await new Promise(resolve => setTimeout(resolve, 30));
    },
  };
  await runCommandConsumer(
    parallelSubscription,
    replicaAKeyring,
    contracts,
    {publish: async () => {}},
    1000,
    async batch => {
      assert.equal(batch.length, 1, 'parallelism test did not split command batches');
      activeBatches++;
      maximumActiveBatches = Math.max(maximumActiveBatches, activeBatches);
      if (activeBatches === 2) releaseParallelBatches();
      let fallbackTimer;
      await Promise.race([
        parallelBatchesReady,
        new Promise(resolve => { fallbackTimer = setTimeout(resolve, 250); }),
      ]);
      clearTimeout(fallbackTimer);
      activeBatches--;
    },
  );
  assert.equal(maximumActiveBatches, 2,
    'payment command batches were serialized instead of processing in parallel');

  const watchdogPulls = [];
  const waitingSubscription = {
    pull: options => { watchdogPulls.push(options); },
    isClosed: () => false,
    async *[Symbol.asyncIterator]() {
      await new Promise(resolve => setTimeout(resolve, 16));
    },
  };
  await runCommandConsumer(
    waitingSubscription, replicaAKeyring, contracts, {publish: async () => {}}, 5,
  );
  assert(watchdogPulls.length >= 3, 'worker did not refresh pull credit while idle');
  assert(watchdogPulls.every(pull => pull.expires === 1000),
    'worker watchdog created an unbounded pull request');

  let interruptedAcknowledgements = 0;
  let interruptedPublishes = 0;
  const interruptedSubscription = {
    pull: () => {},
    isClosed: () => false,
    async *[Symbol.asyncIterator]() {
      yield {
        ...messages[0],
        ack: () => { interruptedAcknowledgements++; },
      };
      throw new Error('503');
    },
  };
  await assert.rejects(
    runCommandConsumer(
      interruptedSubscription,
      replicaAKeyring,
      contracts,
      {publish: async () => { interruptedPublishes++; }},
    ),
    /503/,
  );
  assert.equal(interruptedAcknowledgements, 1,
    'buffered command was not completed before the failed consumer stopped');
  assert.equal(interruptedPublishes, 1,
    'buffered command outcome was not published before the failed consumer stopped');

  let failedSubscriptionClosed = false;
  const failedSubscription = {
    pull: () => {},
    isClosed: () => failedSubscriptionClosed,
    unsubscribe: () => { failedSubscriptionClosed = true; },
    async *[Symbol.asyncIterator]() {
      const error = new Error('503');
      error.code = '503';
      throw error;
    },
  };
  const recoveredStatus = {
    connectionReady: true, consumerReady: false, stopping: false, commandSubscription: undefined,
  };
  let recreatedSubscriptions = 0;
  const recoveredSubscription = {
    pull: () => {},
    isClosed: () => false,
    unsubscribe: () => {},
    async *[Symbol.asyncIterator]() {
      recoveredStatus.stopping = true;
    },
  };
  await superviseCommandConsumer(
    {isClosed: () => false},
    {
      pullSubscribe: async () => {
        recreatedSubscriptions++;
        return recoveredSubscription;
      },
      publish: async () => {},
    },
    replicaAKeyring,
    contracts,
    recoveredStatus,
    failedSubscription,
    0,
  );
  assert(failedSubscriptionClosed, 'failed command subscription was not closed');
  assert.equal(recreatedSubscriptions, 1, 'failed command subscription was not recreated');
  assert.equal(recoveredStatus.commandSubscription, recoveredSubscription,
    'recreated command subscription was not tracked for readiness');
  assert(recoveredStatus.consumerReady, 'recreated command subscription did not restore readiness');

  console.log('Stateless payment tokenization and cross-replica command tests passed.');
}

main().catch(error => { console.error(error); process.exit(1); });
