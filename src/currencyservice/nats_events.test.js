// Copyright 2026 Google LLC.
// Licensed under the Apache License, Version 2.0 (the "License");

'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { contractsRoot, encodeSnapshot } = require('./nats_events');

test('currency snapshot identity is deterministic and carries a typed Any payload', () => {
  const first = encodeSnapshot({USD: 1.17, EUR: 1, JPY: 172.4});
  const second = encodeSnapshot({JPY: 172.4, EUR: 1, USD: 1.17});

  assert.equal(first.revision, second.revision);
  assert.equal(first.messageId, second.messageId);
  assert.equal(first.checksum, second.checksum);
  assert.equal(first.count, 3);

  const envelopeType = contractsRoot().lookupType('boutique.common.v1.MessageEnvelope');
  const eventType = contractsRoot().lookupType('boutique.events.v1.CurrencyRatesUpdatedEvent');
  const envelope = envelopeType.decode(first.data);
  assert.equal(envelope.data.type_url,
    'type.googleapis.com/boutique.events.v1.CurrencyRatesUpdatedEvent');

  const event = eventType.decode(envelope.data.value);
  assert.equal(event.baseCurrencyCode, 'EUR');
  assert.deepEqual(event.rates.map(rate => rate.currencyCode), ['EUR', 'JPY', 'USD']);
  assert.equal(event.rateRevision.toString(), first.revision);
});
