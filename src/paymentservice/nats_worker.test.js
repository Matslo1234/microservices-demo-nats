/* Copyright 2026 Google LLC; Licensed under the Apache License, Version 2.0. */
'use strict';

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { PaymentState, tokenize, loadContracts, processCommand, runCommandConsumer } = require('./nats_worker');

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
  assert(!persisted.includes(request.credit_card_cvv), 'CVV was persisted');

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
  state.value.outcomes[envelope.messageId] = result;
  state.persist();
  assert.deepEqual(state.value.outcomes[envelope.messageId], result, 'idempotent outcome was not retained');

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

  const workerRequest = {...request, order_id: 'order-worker', idempotency_key: 'order-worker'};
  const workerToken = tokenize(state, workerRequest).payment_token;
  const workerCommand = {...authorize, commandId: 'authorize-worker', orderId: 'order-worker',
    paymentToken: workerToken, idempotencyKey: 'order-worker/authorize'};
  const workerEnvelope = contracts.Envelope.encode({messageId: 'authorize-worker-message', aggregateId: 'order-worker',
    aggregateVersion: 2, correlationId: 'order-worker', data: {value: contracts.Authorize.encode(workerCommand).finish()}}).finish();
  let pulls = 0, acknowledgements = 0, publishes = 0;
  const message = {data: workerEnvelope, subject: 'boutique.cmd.payment.authorize.v1',
    ack: () => { acknowledgements++; }, nak: () => { throw new Error('worker unexpectedly NAKed the command'); }};
  const subscription = {pull: () => { pulls++; }, isClosed: () => false,
    async *[Symbol.asyncIterator]() { yield message; }};
  await runCommandConsumer(subscription, state, contracts, {publish: async () => { publishes++; }});
  assert.equal(pulls, 2, 'worker did not keep a pull request outstanding');
  assert.equal(acknowledgements, 1, 'worker did not ACK the command');
  assert.equal(publishes, 1, 'worker did not publish the persisted outcome');
  console.log('Payment Phase 5 tokenization and idempotency tests passed.');
}

main().catch(error => { console.error(error); process.exit(1); });
