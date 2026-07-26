// Copyright 2026 Google LLC.
// Licensed under the Apache License, Version 2.0 (the "License");

'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { acquire, complete, ensureBootstrap } = require('./bootstrap_claim');
const { contractsRoot, encodeSnapshot } = require('./nats_events');

class MemoryKV {
  constructor() {
    this.values = new Map();
    this.revision = 0;
  }

  async get(key) {
    const entry = this.values.get(key);
    return entry && {value: Buffer.from(entry.value), revision: entry.revision};
  }

  async create(key, value) {
    if (this.values.has(key)) throw new Error('wrong last sequence');
    return this.store(key, value);
  }

  async update(key, value, revision) {
    const entry = this.values.get(key);
    if (!entry || entry.revision !== revision) throw new Error('wrong last sequence');
    return this.store(key, value);
  }

  store(key, value) {
    this.revision++;
    this.values.set(key, {value: Buffer.from(value), revision: this.revision});
    return this.revision;
  }
}

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

test('concurrent bootstrap publishers produce one logical snapshot', async () => {
  const kv = new MemoryKV();
  let now = 1000;
  let publishes = 0;
  const options = workerId => ({
    prefix: 'bootstrap.currency',
    workId: 'currency:42',
    workerId,
    durationMs: 100,
    now: () => now,
    wait: async milliseconds => { now += milliseconds; },
  });

  const results = await Promise.all([
    ensureBootstrap(kv, options('replica-a'), async () => { publishes++; }),
    ensureBootstrap(kv, options('replica-b'), async () => { publishes++; }),
    ensureBootstrap(kv, options('replica-c'), async () => { publishes++; }),
  ]);

  assert.equal(publishes, 1);
  assert.equal(results.filter(result => result === 'published').length, 1);
  assert.equal(results.filter(result => result === 'complete').length, 2);
});

test('expired bootstrap owner is fenced and another replica completes', async () => {
  const kv = new MemoryKV();
  const abandoned = await acquire(
    kv, 'bootstrap.currency', 'currency:42', 'replica-a', 1000, 100,
  );
  const replacement = await acquire(
    kv, 'bootstrap.currency', 'currency:42', 'replica-b', 1101, 100,
  );
  assert.equal(replacement.record.token, abandoned.record.token + 1);
  await complete(kv, replacement, 1102);
  await complete(kv, abandoned, 1102);
  const finalEntry = await kv.get(replacement.key);
  const finalRecord = JSON.parse(finalEntry.value.toString());
  assert.equal(finalRecord.owner, 'replica-b');
  assert.equal(finalRecord.completed, true);
});
