import { test } from 'node:test';
import assert from 'node:assert/strict';
import { api, authoritativeTradeList, mergePrimaryDashboard, normalizeEquityHistory, secondaryRefreshState } from '../src/api.ts';
import { finite, ratio, epoch, age, reasonLabel } from '../src/lib/commandFormat.ts';

test('empty authoritative trades replace prior trades; missing response preserves a real fallback', () => {
  const previous = [{ id: 'old-fill' }];
  assert.deepEqual(authoritativeTradeList([], previous), []);
  assert.equal(authoritativeTradeList(undefined, previous), previous);
  assert.deepEqual(authoritativeTradeList(undefined, undefined), []);
});

test('primary positions distinguish a real empty observation from missing or invalid data', async (t) => {
  let payload;
  t.mock.method(globalThis, 'fetch', async () => ({ ok: true, json: async () => payload }));
  for (const positions of [undefined, null, {}, 0, [null], [0], [[]]]) {
    payload = positions === undefined ? { account: { totalEquity: 0 } } : { account: { totalEquity: 0 }, positions };
    const result = await api.dashboard();
    assert.equal(result.positionsObserved, false);
    assert.equal(result.account.totalEquity, 0);
  }
  payload = { account: { totalEquity: 0 }, positions: [] };
  assert.equal((await api.dashboard()).positionsObserved, true);
  assert.deepEqual((await api.dashboard()).positions, []);
});

test('primary refresh preserves trades it intentionally omits and marks retained positions unobserved', () => {
  const previous = {
    positions: [{ symbol: 'BTC/USDT', qty: 1 }], positionsObserved: true,
    trades: [{ id: 'old-fill', timestamp: '2026-09-04T01:00:00Z', symbol: 'BTC/USDT', side: 'buy', price: 10, qty: 1, value: 10, fee: 0 }],
  };
  const missing = mergePrimaryDashboard(previous, { positions: [], positionsObserved: false });
  assert.deepEqual(missing.trades, previous.trades);
  assert.deepEqual(missing.positions, previous.positions);
  assert.equal(missing.positionsObserved, false);
  const observedEmpty = mergePrimaryDashboard(previous, { positions: [], positionsObserved: true, trades: [] });
  assert.deepEqual(observedEmpty.positions, []);
  assert.deepEqual(observedEmpty.trades, []);
  assert.equal(observedEmpty.positionsObserved, true);
});

test('trade endpoint treats absent, malformed and failed responses as unavailable, while an empty array is authoritative', async (t) => {
  let payload;
  let ok = true;
  t.mock.method(globalThis, 'fetch', async () => ({ ok, status: 503, json: async () => payload }));
  t.mock.method(console, 'error', () => {});
  for (payload of [null, {}, { trades: null }, { trades: {} }, { trades: 0 }, { trades: [null] }, { trades: [0] }]) {
    assert.equal(await api.trades(), null);
  }
  payload = { trades: [] };
  assert.deepEqual(await api.trades(), { trades: [] });
  ok = false;
  assert.equal(await api.trades(), null);
});

test('partial secondary refresh cannot refresh the batch receipt or conceal another endpoint failure', () => {
  const previous = { failed: false, receivedAt: 1000, deferredReceivedAt: 1000 };
  const deferred = { timers: { timers: [] } };
  const decision = { run_id: 'real-run' };
  const health = { status: 'healthy' };
  for (const responses of [[null, decision, health], [deferred, null, health], [deferred, decision, null]]) {
    const next = secondaryRefreshState(previous, ...responses, 2000);
    assert.equal(next.failed, true);
    assert.equal(next.receivedAt, 1000);
    assert.equal(next.deferredReceivedAt, responses[0] ? 2000 : 1000);
  }
  assert.deepEqual(secondaryRefreshState(previous, null, null, null, 2000), { ...previous, failed: true });
  assert.deepEqual(secondaryRefreshState({ ...previous, failed: true }, deferred, decision, health, 3000),
    { failed: false, receivedAt: 3000, deferredReceivedAt: 3000 });
});
test('actual equity-history value contract preserves finite zero and timestamps', () => {
  const stamp = '2026-09-05T01:01:42.289407Z';
  assert.deepEqual(normalizeEquityHistory([{ timestamp: stamp, value: 106.8607 }, { timestamp: stamp, value: 0 }]),
    [{ timestamp: stamp, equity: 106.8607 }, { timestamp: stamp, equity: 0 }]);
});
test('missing, failed and invalid history never becomes zero equity', () => {
  assert.equal(normalizeEquityHistory(null), null);
  assert.equal(normalizeEquityHistory({ error: 'timeout' }), null);
  assert.deepEqual(normalizeEquityHistory([]), []);
  const rows = normalizeEquityHistory([{ value: null, equity: 42 }, {}, { value: false }]);
  assert.ok(rows.every(row => Number.isNaN(row.equity)));
});
test('ratio fields are explicit, including 100 percent success and small drawdowns', () => {
  assert.equal(ratio(1, 1), '100.0%');
  assert.equal(ratio(.19118, 2), '19.12%');
  assert.equal(ratio(.003, 2), '0.30%');
  assert.equal(ratio(0), '0.0%');
  assert.equal(ratio(null), '—');
  assert.equal(finite(false), null);
});
test('ambiguous local timestamps are not shifted; future timestamps are not fresh', () => {
  const ts = '2026-09-05T09:00:00+08:00';
  assert.equal(epoch(ts), Date.parse('2026-09-05T01:00:00Z'));
  assert.equal(epoch('2026-09-05 09:00:00'), null);
  assert.equal(age(ts, Date.parse('2026-09-05T00:00:00Z')), '时间异常');
});
test('unknown gate codes remain visible instead of turning into a false success', () => {
  assert.equal(reasonLabel('new_unknown_gate'), 'new_unknown_gate');
  assert.equal(reasonLabel('protect_entry_rsi_confirm_too_weak'), 'RSI 确认不足');
});
