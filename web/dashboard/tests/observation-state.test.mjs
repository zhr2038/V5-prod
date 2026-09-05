import assert from 'node:assert/strict';
import test from 'node:test';
import { beijingEpoch, countQualifier, finite, observationState } from '../src/lib/commandFormat.ts';

test('malformed numeric payloads remain unknown while real zero survives', () => {
  for (const value of [[], {}, ' ', false, null, undefined, Infinity, 'NaN']) assert.equal(finite(value), null);
  assert.equal(finite(0), 0);
  assert.equal(finite('0'), 0);
});

test('Beijing account labels parse consistently regardless of browser timezone', () => {
  assert.equal(beijingEpoch('2026-09-05 10:00:00'), Date.parse('2026-09-05T02:00:00Z'));
  assert.equal(beijingEpoch('2026-09-05T02:00:00Z'), Date.parse('2026-09-05T02:00:00Z'));
  assert.equal(beijingEpoch('n/a'), null);
});

test('fresh timestamps cannot override invalid observation status or identity', () => {
  const now = Date.parse('2026-09-05T02:00:00Z');
  assert.equal(observationState('identity_mismatch', now, now, 90000), 'identity_mismatch');
  assert.equal(observationState('unavailable', now, now, 90000), 'unavailable');
  assert.equal(observationState('observed', now + 60000, now, 90000), 'future');
  assert.equal(observationState('observed', null, now, 90000), 'unavailable');
  assert.equal(observationState('healthy', now - 90001, now, 90000), 'stale');
  assert.equal(observationState('observed', now, now, 90000), 'observed');
});

test('partial zero counts always carry an observation qualifier', () => {
  assert.equal(countQualifier('partial'), '部分观测');
  assert.equal(countQualifier('unavailable'), '不可观测');
  assert.equal(countQualifier(undefined), '待确认');
  assert.equal(countQualifier('observed'), '');
});
