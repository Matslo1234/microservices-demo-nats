/*
 * Copyright 2026 Google LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

'use strict';

const crypto = require('crypto');

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function claimKey(prefix, workId) {
  if (!/^[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$/.test(prefix) || !workId) {
    throw new Error('bootstrap claim prefix and work ID are required');
  }
  return `${prefix}.${crypto.createHash('sha256').update(workId).digest('hex')}`;
}

function conflict(error) {
  const message = String(error && error.message || error).toLowerCase();
  return message.includes('wrong last sequence') ||
    message.includes('expected last subject sequence') ||
    error && (error.code === '10071' || error.api_error && error.api_error.err_code === 10071);
}

async function acquire(kv, prefix, workId, workerId, nowMs, durationMs) {
  if (!workerId || !Number.isFinite(nowMs) || durationMs <= 0) {
    throw new Error('worker ID, current time, and positive claim duration are required');
  }
  const key = claimKey(prefix, workId);
  for (let attempt = 0; attempt < 32; attempt++) {
    const entry = await kv.get(key);
    if (!entry) {
      const record = {
        owner: workerId,
        token: 1,
        attempts: 1,
        lease_until_unix_ms: nowMs + durationMs,
        completed: false,
      };
      try {
        await kv.create(key, encoder.encode(JSON.stringify(record)));
        return {key, revision: 0, record};
      } catch (error) {
        if (conflict(error)) {
          await claimBackoff(attempt);
          continue;
        }
        throw error;
      }
    }
    const record = JSON.parse(decoder.decode(entry.value));
    if (record.completed) return {key, revision: entry.revision, record, complete: true};
    if (record.lease_until_unix_ms > nowMs) {
      return {key, revision: entry.revision, record, held: true};
    }
    record.owner = workerId;
    record.token += 1;
    record.attempts += 1;
    record.lease_until_unix_ms = nowMs + durationMs;
    try {
      const revision = await kv.update(
        key,
        encoder.encode(JSON.stringify(record)),
        entry.revision,
      );
      return {key, revision, record};
    } catch (error) {
      if (conflict(error)) {
        await claimBackoff(attempt);
        continue;
      }
      throw error;
    }
  }
  throw new Error('bootstrap claim acquisition exceeded retry limit');
}

async function complete(kv, claim, nowMs) {
  for (let attempt = 0; attempt < 32; attempt++) {
    const entry = await kv.get(claim.key);
    if (!entry) throw new Error('bootstrap claim was lost');
    const record = JSON.parse(decoder.decode(entry.value));
    if (record.completed) return;
    if (record.owner !== claim.record.owner ||
        record.token !== claim.record.token ||
        record.lease_until_unix_ms <= nowMs) {
      throw new Error('bootstrap claim was lost');
    }
    record.completed = true;
    record.completed_at_unix_ms = nowMs;
    record.lease_until_unix_ms = 0;
    try {
      await kv.update(
        claim.key,
        encoder.encode(JSON.stringify(record)),
        entry.revision,
      );
      return;
    } catch (error) {
      if (conflict(error)) {
        await claimBackoff(attempt);
        continue;
      }
      throw error;
    }
  }
  throw new Error('bootstrap claim completion exceeded retry limit');
}

async function claimBackoff(attempt) {
  const base = Math.min(16, 0.25 * (2 ** Math.min(attempt, 6)));
  const jitter = crypto.randomInt(0, 1000) / 1000;
  await new Promise(resolve => setTimeout(resolve, base + jitter));
}

async function ensureBootstrap(kv, options, publish) {
  const durationMs = options.durationMs || 30000;
  while (true) {
    const nowMs = options.now();
    const claim = await acquire(
      kv,
      options.prefix,
      options.workId,
      options.workerId,
      nowMs,
      durationMs,
    );
    if (claim.complete) return 'complete';
    if (claim.held) {
      await options.wait(Math.min(250, Math.max(1, claim.record.lease_until_unix_ms - nowMs)));
      continue;
    }
    await publish();
    await complete(kv, claim, options.now());
    return 'published';
  }
}

module.exports = { acquire, claimKey, complete, conflict, ensureBootstrap };
