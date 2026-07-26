/* Copyright 2026 Google LLC; Licensed under the Apache License, Version 2.0. */
'use strict';

const assert = require('assert');
const {
  deriveSigningKey, tokenize, verifyPaymentToken, processTokenBatch, loadContracts,
  processCommand, runCommandConsumer, superviseCommandConsumer,
} = require('./nats_worker');

async function main() {
  const sharedCredential = 's0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef';
  const replicaAKey = deriveSigningKey(sharedCredential);
  const replicaBKey = deriveSigningKey(sharedCredential);
  const unrelatedReplicaKey = deriveSigningKey(
    'sffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff',
  );
  assert.deepEqual(replicaAKey, replicaBKey, 'replicas derived different signing keys');
  assert.notDeepEqual(replicaAKey, unrelatedReplicaKey, 'different credentials derived the same signing key');

  const now = Date.now();
  const request = {
    order_id: 'order-1', idempotency_key: 'checkout-1',
    credit_card_number: '4432801561520454', credit_card_expiration_month: 12,
    credit_card_expiration_year: new Date().getFullYear() + 1, credit_card_cvv: '672',
  };
  const tokenizedByReplicaA = tokenize(replicaAKey, request, now);
  const paymentToken = tokenizedByReplicaA.payment_token;
  assert.equal(new Date(tokenizedByReplicaA.expires_at).getTime(), now + 15 * 60 * 1000);
  assert.doesNotThrow(
    () => verifyPaymentToken(paymentToken, request.order_id, replicaBKey, now),
    'a second replica could not verify the issued token',
  );
  assert.throws(
    () => verifyPaymentToken(paymentToken, request.order_id, unrelatedReplicaKey, now),
    /INVALID_PAYMENT_REFERENCE/,
    'a replica with another credential verified the token',
  );
  assert.throws(
    () => verifyPaymentToken(paymentToken, 'another-order', replicaBKey, now),
    /INVALID_OR_EXPIRED_TOKEN/,
    'the payment token was not bound to its order',
  );
  assert.throws(
    () => verifyPaymentToken(paymentToken, request.order_id, replicaBKey, now + 15 * 60 * 1000),
    /INVALID_OR_EXPIRED_TOKEN/,
    'an expired token was accepted',
  );
  const tokenParts = paymentToken.split('.');
  tokenParts[2] = `${tokenParts[2][0] === 'A' ? 'B' : 'A'}${tokenParts[2].slice(1)}`;
  const tamperedToken = tokenParts.join('.');
  assert.throws(
    () => verifyPaymentToken(tamperedToken, request.order_id, replicaBKey, now),
    /INVALID_PAYMENT_REFERENCE/,
    'a token with a modified signature was accepted',
  );
  const tokenPayload = JSON.parse(Buffer.from(paymentToken.split('.')[1], 'base64url').toString('utf8'));
  assert.deepEqual(
    Object.keys(tokenPayload).sort(),
    ['expiresAt', 'nonce', 'orderId', 'version'],
    'payment token contains unexpected card data',
  );

  const tokenResponses = [];
  processTokenBatch(Array.from({length: 32}, (_, index) => ({
    request: {...request, order_id: `token-batch-${index}`, idempotency_key: `token-batch-${index}`},
    correlationId: `token-batch-${index}`,
    message: {
      subject: 'boutique.qry.payment.tokenize.v1',
      respond: encoded => tokenResponses.push(JSON.parse(encoded)),
    },
  })), replicaAKey);
  assert.equal(tokenResponses.filter(response => response.payment_token).length, 32,
    'token batch did not return every token');
  for (let index = 0; index < tokenResponses.length; index++) {
    assert.doesNotThrow(
      () => verifyPaymentToken(tokenResponses[index].payment_token, `token-batch-${index}`, replicaBKey),
      `replica B could not verify token batch item ${index}`,
    );
  }

  const contracts = await loadContracts();
  const occurredAt = {seconds: Math.floor(now / 1000), nanos: (now % 1000) * 1000000};
  const authorize = { commandId: 'authorize-1', orderId: 'order-1', paymentToken,
    idempotencyKey: 'order-1/authorize', amount: { currencyCode: 'USD', units: 20, nanos: 0 } };
  const envelope = { messageId: 'authorize-message', aggregateId: 'order-1', aggregateVersion: 2,
    correlationId: 'order-1', occurredAt,
    data: { value: contracts.Authorize.encode(authorize).finish() } };
  const resultFromReplicaB = processCommand(
    replicaBKey, contracts, 'boutique.cmd.payment.authorize.v1', envelope,
  );
  assert.equal(resultFromReplicaB.subject, 'boutique.evt.payment.authorized.v1');
  const repeatedResultFromReplicaA = processCommand(
    replicaAKey, contracts, 'boutique.cmd.payment.authorize.v1', envelope,
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

  const wrongCredentialResult = processCommand(
    unrelatedReplicaKey, contracts, 'boutique.cmd.payment.authorize.v1', envelope,
  );
  assert.equal(wrongCredentialResult.subject, 'boutique.evt.payment.authorization-declined.v1',
    'a replica with the wrong signing key authorized the token');
  const expiredForCommand = tokenize(replicaAKey, request, now - 15 * 60 * 1000).payment_token;
  const expiredCommandResult = processCommand(
    replicaBKey, contracts, 'boutique.cmd.payment.authorize.v1',
    {...envelope, messageId: 'expired-token-message',
      data: {value: contracts.Authorize.encode({...authorize, paymentToken: expiredForCommand}).finish()}},
  );
  assert.equal(expiredCommandResult.subject, 'boutique.evt.payment.authorization-declined.v1',
    'a token expired when the command was issued was authorized');
  const delayedToken = tokenize(
    replicaAKey, request, now - 15 * 60 * 1000 - 10 * 1000,
  ).payment_token;
  const delayedCommandTime = now - 20 * 1000;
  const delayedCommandResult = processCommand(
    replicaBKey, contracts, 'boutique.cmd.payment.authorize.v1',
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
    replicaAKey, contracts, 'boutique.cmd.payment.capture.v1', captureEnvelope,
  );
  assert.equal(captured.subject, 'boutique.evt.payment.captured.v1',
    'another replica could not capture the signed authorization');

  const release = { commandId: 'release-1', orderId: 'order-1',
    authorizationId: authorized.authorizationId, idempotencyKey: 'order-1/release' };
  const releaseEnvelope = {...envelope, messageId: 'release-message',
    data: { value: contracts.Release.encode(release).finish() }};
  const released = processCommand(
    replicaBKey, contracts, 'boutique.cmd.payment.release-authorization.v1', releaseEnvelope,
  );
  assert.equal(released.subject, 'boutique.evt.payment.authorization-released.v1',
    'another replica could not release the signed authorization');

  const invalidCapture = processCommand(
    replicaAKey, contracts, 'boutique.cmd.payment.capture.v1',
    {...captureEnvelope, messageId: 'invalid-capture-message',
      data: {value: contracts.Capture.encode({...capture, orderId: 'another-order'}).finish()}},
  );
  assert.equal(invalidCapture.subject, 'boutique.evt.payment.capture-failed.v1',
    'an authorization was accepted for another order');

  process.env.PAYMENT_FAILURE_MODE = 'authorization_declined';
  const declined = processCommand(
    replicaAKey, contracts, 'boutique.cmd.payment.authorize.v1',
    {...envelope, messageId: 'declined-message'},
  );
  assert.equal(declined.subject, 'boutique.evt.payment.authorization-declined.v1');
  process.env.PAYMENT_FAILURE_MODE = 'capture_failed';
  const captureFailed = processCommand(
    replicaAKey, contracts, 'boutique.cmd.payment.capture.v1',
    {...captureEnvelope, messageId: 'capture-failed-message'},
  );
  assert.equal(captureFailed.subject, 'boutique.evt.payment.capture-failed.v1');
  process.env.PAYMENT_FAILURE_MODE = 'release_failed';
  const releaseFailed = processCommand(
    replicaAKey, contracts, 'boutique.cmd.payment.release-authorization.v1',
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
    const workerToken = tokenize(replicaAKey, workerRequest).payment_token;
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
  await runCommandConsumer(subscription, replicaBKey, contracts, {
    publish: async (_subject, _data, options) => { publishedMessageIDs.push(options.msgID); },
  });
  const expectedPull = [{batch: 256, expires: 1000, idle_heartbeat: 500},
    {batch: 256, expires: 1000, idle_heartbeat: 500}];
  assert.deepEqual(pulls, expectedPull, 'worker did not replenish expiring command pull credit');
  assert.equal(acknowledgements, 32, 'worker did not ACK every command in the batch');
  assert.equal(publishedMessageIDs.length, 32, 'worker did not publish every deterministic outcome');
  assert.equal(new Set(publishedMessageIDs).size, 32, 'worker reused an event message ID');

  const watchdogPulls = [];
  const waitingSubscription = {
    pull: options => { watchdogPulls.push(options); },
    isClosed: () => false,
    async *[Symbol.asyncIterator]() {
      await new Promise(resolve => setTimeout(resolve, 16));
    },
  };
  await runCommandConsumer(
    waitingSubscription, replicaAKey, contracts, {publish: async () => {}}, 5,
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
      replicaAKey,
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
    replicaAKey,
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
