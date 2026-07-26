/* Copyright 2026 Google LLC; Licensed under the Apache License, Version 2.0. */
'use strict';

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const {
  PaymentState, tokenize, processTokenBatch, loadContracts, processCommand, runCommandConsumer,
  superviseCommandConsumer,
} = require('./nats_worker');

async function main() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'payment-phase5-'));
  const filename = path.join(directory, 'state.json');
  const state = new PaymentState(filename);
  const request = {
    order_id: 'order-1', idempotency_key: 'checkout-1',
    credit_card_number: '4432801561520454', credit_card_expiration_month: 12,
    credit_card_expiration_year: new Date().getFullYear() + 1, credit_card_cvv: '672',
  };
  const first = tokenize(state, request);
  const second = tokenize(state, request);
  assert.equal(first.payment_token, second.payment_token, 'tokenization retry changed the token');
  const persisted = fs.readFileSync(filename, 'utf8');
  assert(!persisted.includes(request.credit_card_number), 'PAN was persisted');
  assert(!/"(?:credit_card_cvv|creditCardCvv|cvv)"\s*:/.test(persisted), 'CVV field was persisted');
  assert.deepEqual(Object.keys(state.value.tokens[first.payment_token]).sort(),
    ['cardType', 'consumed', 'expiresAt', 'last4', 'orderId'],
    'persisted token contains unexpected card data');

  const tokenBatchState = new PaymentState(path.join(directory, 'token-batch.json'));
  const tokenResponses = [];
  const tokenPersist = tokenBatchState.persist.bind(tokenBatchState);
  let tokenPersistCalls = 0;
  tokenBatchState.persist = () => {
    tokenPersistCalls++;
    tokenPersist();
  };
  processTokenBatch(Array.from({length: 32}, (_, index) => ({
    request: {...request, order_id: `token-batch-${index}`, idempotency_key: `token-batch-${index}`},
    correlationId: `token-batch-${index}`,
    message: {
      subject: 'boutique.qry.payment.tokenize.v1',
      respond: encoded => tokenResponses.push(JSON.parse(encoded)),
    },
  })), tokenBatchState);
  assert.equal(tokenPersistCalls, 1, 'token batch was not made durable with one persist');
  assert.equal(tokenResponses.filter(response => response.payment_token).length, 32,
    'token batch did not return every token');

  const contracts = await loadContracts();
  const authorize = { commandId: 'authorize-1', orderId: 'order-1', paymentToken: first.payment_token,
    idempotencyKey: 'order-1/authorize', amount: { currencyCode: 'USD', units: 20, nanos: 0 } };
  const envelope = { messageId: 'authorize-message', aggregateId: 'order-1', aggregateVersion: 2,
    correlationId: 'order-1', data: { value: contracts.Authorize.encode(authorize).finish() } };
  const result = processCommand(state, contracts, 'boutique.cmd.payment.authorize.v1', envelope);
  assert.equal(result.subject, 'boutique.evt.payment.authorized.v1');
  const decodedResult = contracts.Envelope.decode(Buffer.from(result.data, 'base64'));
  assert.equal(decodedResult.data.type_url, 'type.googleapis.com/boutique.events.v1.PaymentAuthorizedEvent',
    'payment result omitted the protobuf Any type URL');
  state.set('outcomes', envelope.messageId, result);
  state.persist();
  assert.deepEqual(state.value.outcomes[envelope.messageId], result, 'idempotent outcome was not retained');
  const reopened = new PaymentState(filename);
  assert.deepEqual(reopened.value.outcomes[envelope.messageId], result, 'journaled outcome was not restored');

  process.env.PAYMENT_FAILURE_MODE = 'authorization_declined';
  const declined = processCommand(state, contracts, 'boutique.cmd.payment.authorize.v1', {
    ...envelope, messageId: 'declined-message', data: { value: contracts.Authorize.encode({...authorize, orderId: 'order-declined'}).finish() }
  });
  assert.equal(declined.subject, 'boutique.evt.payment.authorization-declined.v1');
  process.env.PAYMENT_FAILURE_MODE = 'capture_failed';
  const capture = { commandId: 'capture-1', orderId: 'order-1', authorizationId: 'auth-test',
    idempotencyKey: 'order-1/capture', amount: authorize.amount };
  const captureFailed = processCommand(state, contracts, 'boutique.cmd.payment.capture.v1', {
    ...envelope, messageId: 'capture-message', data: { value: contracts.Capture.encode(capture).finish() }
  });
  assert.equal(captureFailed.subject, 'boutique.evt.payment.capture-failed.v1');
  process.env.PAYMENT_FAILURE_MODE = 'release_failed';
  const release = { commandId: 'release-1', orderId: 'order-1', authorizationId: 'auth-test', idempotencyKey: 'order-1/release' };
  const releaseFailed = processCommand(state, contracts, 'boutique.cmd.payment.release-authorization.v1', {
    ...envelope, messageId: 'release-message', data: { value: contracts.Release.encode(release).finish() }
  });
  assert.equal(releaseFailed.subject, 'boutique.evt.payment.authorization-release-failed.v1');
  delete process.env.PAYMENT_FAILURE_MODE;

  const pulls = [];
  let acknowledgements = 0, publishes = 0;
  const messages = [];
  for (let index = 0; index < 32; index++) {
    const orderID = `order-worker-${index}`;
    const workerRequest = {...request, order_id: orderID, idempotency_key: orderID};
    const workerToken = tokenize(state, workerRequest).payment_token;
    const workerCommand = {...authorize, commandId: `authorize-worker-${index}`, orderId: orderID,
      paymentToken: workerToken, idempotencyKey: `${orderID}/authorize`};
    const workerEnvelope = contracts.Envelope.encode({messageId: `authorize-worker-message-${index}`,
      aggregateId: orderID, aggregateVersion: 2, correlationId: orderID,
      data: {value: contracts.Authorize.encode(workerCommand).finish()}}).finish();
    messages.push({data: workerEnvelope, subject: 'boutique.cmd.payment.authorize.v1',
      ack: () => { acknowledgements++; },
      nak: () => { throw new Error('worker unexpectedly NAKed the command'); }});
  }
  const subscription = {pull: options => { pulls.push(options); }, isClosed: () => false,
    async *[Symbol.asyncIterator]() {
      for (const message of messages) yield message;
    }};
  let persistCalls = 0;
  const persist = state.persist.bind(state);
  state.persist = () => {
    persistCalls++;
    persist();
  };
  await runCommandConsumer(subscription, state, contracts, {publish: async () => { publishes++; }});
  const expectedPull = [{batch: 256, expires: 1000, idle_heartbeat: 500},
    {batch: 256, expires: 1000, idle_heartbeat: 500}];
  assert.deepEqual(pulls, expectedPull, 'worker did not replenish expiring command pull credit');
  assert.equal(persistCalls, 1, 'worker did not persist the command batch once');
  assert.equal(acknowledgements, 32, 'worker did not ACK every command in the batch');
  assert.equal(publishes, 32, 'worker did not publish every persisted outcome');

  const watchdogPulls = [];
  const waitingSubscription = {
    pull: options => { watchdogPulls.push(options); },
    isClosed: () => false,
    async *[Symbol.asyncIterator]() {
      await new Promise(resolve => setTimeout(resolve, 16));
    },
  };
  await runCommandConsumer(waitingSubscription, state, contracts, {publish: async () => {}}, 5);
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
      state,
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
    state,
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

  const legacyFilename = path.join(directory, 'legacy.json');
  fs.writeFileSync(legacyFilename, JSON.stringify(state.value), {mode: 0o600});
  const migrated = new PaymentState(legacyFilename);
  assert.deepEqual(migrated.value.outcomes[envelope.messageId], result, 'legacy JSON state was not migrated');
  assert.equal(JSON.parse(fs.readFileSync(legacyFilename, 'utf8').trim()).version, 1,
    'legacy state was not rewritten as a versioned journal snapshot');
  fs.appendFileSync(legacyFilename, '{"version":1,"sets":');
  const recovered = new PaymentState(legacyFilename);
  assert.deepEqual(recovered.value.outcomes[envelope.messageId], result, 'valid state before a torn append was not restored');
  assert(!fs.readFileSync(legacyFilename, 'utf8').includes('{"version":1,"sets":'),
    'torn final journal append was not removed');
  console.log('Payment Phase 5 tokenization and idempotency tests passed.');
}

main().catch(error => { console.error(error); process.exit(1); });
