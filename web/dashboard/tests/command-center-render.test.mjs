import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { runInNewContext } from 'node:vm';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import ts from 'typescript';

const require = createRequire(import.meta.url);
const sourceRoot = fileURLToPath(new URL('../src/', import.meta.url));
const NOW = Date.parse('2026-09-05T02:00:00Z');
const STAMP = new Date(NOW).toISOString();
class FixedDate extends Date { static now() { return NOW; } }
const modules = new Map();

// Compile the real TSX and formatters in memory. Deferred charts/export widgets are
// outside these semantic checks; suppressing their lazy imports avoids DOM/network work.
function loadSource(filename) {
  if (modules.has(filename)) return modules.get(filename).exports;
  const module = { exports: {} };
  modules.set(filename, module);
  const code = ts.transpileModule(readFileSync(filename, 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, target: ts.ScriptTarget.ES2022 },
  }).outputText;
  const localRequire = (id) => {
    if (id === 'react') return { ...React, lazy: () => () => null };
    if (id.endsWith('.css')) return {};
    if (!id.startsWith('.')) return require(id);
    const base = resolve(dirname(filename), id);
    const target = [base, `${base}.ts`, `${base}.tsx`].find(existsSync);
    assert.ok(target, `Local source module must exist: ${id}`);
    return loadSource(target);
  };
  runInNewContext(code, { module, exports: module.exports, require: localRequire, Date: FixedDate, Intl, console }, { filename });
  return module.exports;
}

const { CommandCenter } = loadSource(resolve(sourceRoot, 'components/CommandCenter.tsx'));
const metric = (value) => ({ value, status: 'observed', unit: 'observations' });

function fixture() {
  return {
    dashboard: {
      account: { totalEquity: 106.86, cash: 106.86, positionsValue: 0, maxDrawdown: 0 },
      positions: [], positionsObserved: true,
      trades: [{ id: 'retained-fill', timestamp: '2026-09-04T12:34:00Z', symbol: 'ETH/USDT', side: 'buy', price: 100, qty: 0.123456, value: 12.3456, fee: 0 }],
      systemStatus: { isRunning: true, mode: 'live', lastUpdate: '2026-09-05 10:00:00', killSwitch: false, errors: [] },
      timers: { timers: [] }, apiTelemetry: { successRate: 1, p95LatencyMs: 173 },
    },
    command: {
      schema_version: 'v5.command_center.v1', generated_at: STAMP, read_only: true, status: 'observed',
      latest_decision: { status: 'observed', run_id: '20260905_10', window_end_ts: STAMP, regime: 'Trending' },
      candidates: [{ symbol: 'ETH/USDT', alpha_score: 0.058395, target_weight: null, router_reasons: [], selection_reasons: ['invalid_candidate'], reference_price: 2452.585 }],
      window_72h: { start_ts: '2026-09-02T02:00:00Z', end_ts: STAMP, observed_runs: 72, expected_runs: 72, coverage_status: 'complete', selected_candidates: metric(44), generated_orders: metric(0), actual_filled_orders: metric(0) },
      blockers: [], warnings: [],
      health: {
        risk: { status: 'observed', observed_at: STAMP, level: 'PROTECT', dd_pct: 0 },
        kill_switch: { status: 'observed', observed_at: STAMP, enabled: false },
        reconcile: { status: 'observed', observed_at: STAMP, ok: true },
        ledger: { status: 'observed', observed_at: STAMP, ok: true },
      },
      participation: { enabled: true, mode: 'forward_paper', status: 'observed', observed_at: STAMP, live_order_effect: 'none', live_promotion_allowed: false, entry_count: 0, closed_trade_count: 0, net_realized_pnl_usdt: 0, equity_usdt: 106.86, valuation_status: 'flat_cash', curve: [] },
      quant_lab: { mode: 'advisory', permission: 'ABORT', permission_gate_enforced: false },
    },
    commandFailed: false, commandReceivedAt: NOW, primaryReceivedAt: NOW,
    tradesFailed: false, tradesReceivedAt: NOW, secondaryFailed: false, secondaryReceivedAt: NOW, deferredReceivedAt: NOW,
    equity: [], equityLoading: false, equityFailed: false,
    riskGuard: null, decisionAudit: null, marketState: null,
    health: { status: 'healthy', timestamp: STAMP },
    quantLabStatus: null, quantLabPermission: null, quantLabCost: null,
    focusSymbol: 'ETH-USDT', loading: false, refreshFailed: false, updateTime: '10:00:00',
    onRefresh() {}, onSymbolSearch() {},
  };
}

function render(props) { return renderToStaticMarkup(React.createElement(CommandCenter, props)); }
function section(html, id) {
  const match = html.match(new RegExp(`<section id="${id}"[^>]*>[\\s\\S]*?</section>`));
  assert.ok(match, `Section ${id} must be rendered`);
  return match[0];
}
function observationStep(html) {
  const step = [...section(html, 'participation').matchAll(/<li\b[^>]*>[\s\S]*?<\/li>/g)]
    .find(([item]) => item.includes('真实周期观测'))?.[0];
  assert.ok(step, 'The real observation step must be rendered');
  return step;
}

test('real empty positions and observed zeros remain valid without granting live promotion', () => {
  const html = render(fixture());
  assert.match(html, /当前空仓，等待可执行机会/);
  assert.match(html, /近 72 小时真实成交<\/span><strong>0<i>笔订单/);
  assert.match(html, /100\.0%/);
  assert.match(html, /0\.00%/);
  assert.match(observationStep(html), /^<li class="complete">/);
  assert.match(html, /尚未授权，不会自动扩大实盘风险/);
});

test('unobserved positions never claim current flatness, including retained position rows', () => {
  for (const positions of [[], [{ symbol: 'BTC/USDT', qty: 1 }]]) {
    const props = fixture();
    props.dashboard.positionsObserved = false;
    props.dashboard.positions = positions;
    const html = render(props);
    assert.doesNotMatch(html, /当前空仓|当前没有有效持仓/);
    assert.match(section(html, 'positions'), /正在确认真实持仓/);
  }
});

test('failed trade refresh keeps visible historical rows and explicitly labels them', () => {
  const props = fixture();
  props.tradesFailed = true;
  props.tradesReceivedAt = NOW - 3600000;
  const html = section(render(props), 'positions');
  assert.match(html, /成交刷新失败，保留上次可见记录/);
  assert.match(html, /0\.123456/);
  assert.doesNotMatch(html, /暂无该方向的可见成交记录/);
});

test('unread trade history is distinct from a successful empty trade observation', () => {
  const props = fixture();
  props.dashboard.trades = [];
  props.tradesReceivedAt = null;
  assert.match(section(render(props), 'positions'), /成交记录尚未确认/);
  props.tradesReceivedAt = NOW;
  assert.match(section(render(props), 'positions'), /暂无该方向的可见成交记录/);
});

test('the actual selection rejection is visible when router reasons are empty', () => {
  const html = section(render(fixture()), 'opportunities');
  assert.match(html, /title="invalid_candidate"/);
  assert.match(html, /候选未通过有效性筛选/);
  assert.doesNotMatch(html, />目标仓位不可观测</);
});

test('future, partial and unavailable decisions cannot render a normal market status', () => {
  for (const status of ['future', 'partial', 'unavailable', 'observed']) {
    const props = fixture();
    props.command.latest_decision.status = status;
    if (status === 'future' || status === 'observed') props.command.latest_decision.window_end_ts = new Date(NOW + 3600000).toISOString();
    const html = render(props);
    assert.match(html, /决策数据待确认、过期或时间异常/);
    assert.doesNotMatch(section(html, 'opportunities'), /cc-status cc-muted"><i><\/i>趋势行情/);
  }
});

test('future or mismatched paper observations never complete the real observation step', () => {
  for (const status of ['future', 'identity_mismatch', 'unavailable', 'observed']) {
    const props = fixture();
    props.command.participation.status = status;
    if (status === 'future' || status === 'observed') props.command.participation.observed_at = new Date(NOW + 3600000).toISOString();
    const html = render(props);
    assert.match(observationStep(html), /^<li class="current">/);
    assert.match(observationStep(html), /观测待确认/);
    assert.match(html, /尚未授权，不会自动扩大实盘风险/);
  }
});

test('partial zero counts disclose incomplete observation in both headline and funnel', () => {
  const props = fixture();
  props.command.window_72h.actual_filled_orders.status = 'partial';
  const html = render(props);
  assert.match(section(html, 'overview'), /部分观测/);
  assert.match(section(html, 'opportunities'), /部分观测/);
});

test('failed secondary refresh and stale health never show a passed service check', () => {
  for (const failed of [true, false]) {
    const props = fixture();
    props.secondaryFailed = failed;
    if (!failed) props.health.timestamp = new Date(NOW - 3600000).toISOString();
    const html = section(render(props), 'operations');
    assert.doesNotMatch(html, /服务检查通过/);
    assert.match(html, failed ? /检查数据需刷新/ : /数据过期/);
  }
});

test('a fresh HTTP receipt cannot make an old account snapshot current', () => {
  const props = fixture();
  props.dashboard.systemStatus.lastUpdate = '2026-09-04 10:00:00';
  props.primaryReceivedAt = NOW;
  const html = render(props);
  assert.match(html, /账户快照过期、刷新失败或返回异常/);
  assert.match(html, /账户状态需确认/);
  assert.match(section(html, 'positions'), /上次快照未见有效持仓/);
  assert.doesNotMatch(html, /当前空仓|当前没有有效持仓/);
});
