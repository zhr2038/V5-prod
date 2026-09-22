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
const { default: DailyTrendPaper } = loadSource(resolve(sourceRoot, 'components/DailyTrendPaper.tsx'));
const { default: PairedReferencePaper } = loadSource(resolve(sourceRoot, 'components/PairedReferencePaper.tsx'));
const { default: LiveCostEvidence } = loadSource(resolve(sourceRoot, 'components/LiveCostEvidence.tsx'));
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
      daily_trend_paper: {
        status: 'observed', ledger: 'v5-daily-trend-paper-20260914-v1', worker: { ok: true, observed_at: NOW / 1000 },
        report: {
          schema_version: 'v5.daily_trend_paper.v1', experiment_id: 'v5-daily-trend-paper-20260914-v1', identity: 'daily-identity',
          status: 'WAITING_FOR_START', finalized: false, observed_at: NOW / 1000, completed_bar_ts: NOW / 1000 - 86400,
          decision_ts: NOW / 1000, initial_capital_usdt: 100, total_equity_usdt: 100, net_equity_increment_usdt: 0,
          modeled_fee_usdt: 0, explicit_roundtrip_cost_bps: 30, observed_maximum_drawdown_fraction: 0,
          observation_gap_days: 0, cumulative_missed_decision_days: 0, observation_count: 0,
          evidence_status: 'COLLECTING_FORWARD_EVIDENCE',
          start_utc: '2026-09-14T00:00:00Z', review_utc: '2026-12-13T00:00:00Z', paper_only: true,
          live_order_effect: 'none', live_execution_eligible: false, automatic_live_scaling: false,
          actions: [],
          sleeves: Object.fromEntries(['BTC/USDT', 'ETH/USDT'].map(symbol => [symbol, {
            signal: { long: false, close: 100, sma: 101, momentum_return: -0.01, sma_days: 100, momentum_days: 30 },
            portfolio: { equity_usdt: 50, net_equity_increment_usdt: 0, gross_exposure_usdt: 0, cash_usdt: 50, drawdown_fraction: 0 },
            observed_maximum_drawdown_fraction: 0, actual_simulated_fill_count: 0, independent_closed_campaign_count: 0,
          }])),
        },
      },
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

test('the active paper section shows only the frozen daily rule and explicit no-live boundary', () => {
  const html = section(render(fixture()), 'participation');
  assert.match(html, /BTC \/ ETH 日线趋势/);
  assert.match(html, /与同起点持币和现金基准比较/);
  assert.match(html, /日线趋势验证/);
  assert.match(html, /不读取交易密钥、不下单、不改变真实仓位/);
  assert.doesNotMatch(html, /独立 A \/ B \/ C \/ D 对照/);
});

test('daily benchmark separates mark-to-market gains from timing excess and completed campaigns', () => {
  const data = fixture().command.daily_trend_paper;
  const account = { equity_usdt: 112, net_equity_increment_usdt: 12, net_return_fraction: .12,
    realized_pnl_usdt: 0, unrealized_pnl_usdt: 12, observed_maximum_drawdown_fraction: .003,
    independent_closed_campaign_count: 0 };
  data.benchmark = { status: 'observed', strategy: account, passive: account,
    cash: { ...account, equity_usdt: 100, net_equity_increment_usdt: 0, net_return_fraction: 0, unrealized_pnl_usdt: 0 },
    excess_vs_passive_usdt: 0, curve: [] };
  const html = renderToStaticMarkup(React.createElement(DailyTrendPaper, { data, unavailable: false }));
  assert.match(html, /相对持币增益/);
  assert.match(html, /已实现收益/);
  assert.match(html, /浮动收益/);
  assert.match(html, /完整进出闭环为 0/);
  assert.match(html, /尚未产生择时超额/);
  assert.match(html, /未包含完整日内风险/);
  data.benchmark = { status: 'unavailable', reason: 'archive hash mismatch' };
  const invalid = renderToStaticMarkup(React.createElement(DailyTrendPaper, { data, unavailable: false }));
  assert.match(invalid, /基准对照尚不可用/);
  assert.match(invalid, /archive hash mismatch/);
  assert.doesNotMatch(invalid, /尚未产生择时超额/);
});

test('paired account report cannot label price observations or zero closures as live evidence', () => {
  const account = { equity_usdt: 100, cash_usdt: 100, net_equity_increment_usdt: 0,
    realized_pnl_usdt: 0, unrealized_pnl_usdt: 0, maximum_drawdown_fraction: 0,
    actual_simulated_fills: 0, independent_closed_campaign_count: 0 };
  const scenario = { accounts: { A_original_v5: account, B_defer_4h: account }, comparison: {
    net_equity_delta_usdt: 0, avoided_net_loss_usdt: 0, missed_net_profit_usdt: 0,
    completed_matched_veto_campaigns: 0 } };
  const data = { status: 'observed', report: { ...scenario, status: 'COLLECTING_FORWARD_EVIDENCE',
    ledger_start_ts: NOW / 1000, latest_observed_at: NOW / 1000, calendar_days: 0,
    cost_model: { explicit_roundtrip_cost_bps: 30, calibrated_to_real_fills: false, fixed_operating_cost_included: false },
    references: { valid: 0, candidates: 0, deferred: 0, matched_candidates: 0, matched_vetoes: 0, valid_coverage_rate: null },
    coverage: { observed_decision_hours: 0, expected_decision_hours: 0, missed_decision_hours: 0 },
    scenarios: { 30: scenario, 60: scenario, 120: scenario },
    acceptance: { requirements: [{ criterion: 'forward_calendar_days', actual: 0, required: '>=30', result: 'INSUFFICIENT' }] } } };
  const html = renderToStaticMarkup(React.createElement(PairedReferencePaper, { data, unavailable: false }));
  assert.match(html, /配对闭环为 0/);
  assert.match(html, /模型尚未按真实成交校准/);
  assert.match(html, /不是项目最终利润/);
  assert.match(html, /不改变原 V5 实盘运行/);
  assert.match(html, /不会自动晋级实盘/);
  assert.match(html, /120 bps/);
  assert.match(html, /自身模拟风控/);
  assert.match(html, /尚未观测/);
  assert.match(html, /每小时评估风控/);
  assert.match(html, /原实盘外部风控每 30 分钟评估/);
  assert.match(html, /至少三轮历史后才允许恢复/);
  data.report.accounts = {
    A_original_v5: { ...account, risk_snapshot: { current_level: 'PROTECT', metrics: { recovery_evidence_ok: false } } },
    B_defer_4h: { ...account, risk_snapshot: { current_level: 'NEUTRAL', metrics: { recovery_evidence_ok: true } } },
  };
  const risk = renderToStaticMarkup(React.createElement(PairedReferencePaper, { data, unavailable: false }));
  assert.match(risk, /保护/);
  assert.match(risk, /中性/);
  assert.match(risk, /恢复证据不足/);
  assert.doesNotMatch(risk, /尚未观测/);
});

test('cost evidence shows signed one-way measurements and missing expected cost without inventing calibration', () => {
  const data = { status: 'observed', report: { status: 'OBSERVED', generated_at_utc: STAMP, window_days: 30,
    fill_count: 3, qualified_fill_count: 2, qualified_order_count: 1, fee_known_fill_count: 3,
    missing_reason_counts: { ORDER_LINK_MISSING: 1 }, groups: [{ symbol: 'BTC/USDT', side: 'buy',
      origin: 'strategy', qualified_order_count: 1, mean_fee_bps: 10, mean_signed_slippage_bps: -2,
      mean_one_way_cost_bps: 8 }], recent_fills: [{ symbol: 'BTC/USDT', side: 'buy', fill_ts_ms: NOW,
      fee_bps: 10, signed_slippage_bps: -2, expected_one_way_cost_bps: null,
      observed_one_way_cost_bps: 8, cost_error_bps: null, missing_reasons: [] }] } };
  const html = renderToStaticMarkup(React.createElement(LiveCostEvidence, { data, unavailable: false }));
  assert.match(html, /-2\.000/);
  assert.match(html, /缺少关联订单 1/);
  assert.match(html, /预估缺失时保持空缺/);
  assert.match(html, /负滑点表示价格改善/);
  assert.match(html, /不会自动改动实盘费用模型或原 V5 开仓规则/);
  assert.match(html, /不能把有效样本数称为“校准通过”/);
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

test('paper quote execution shows pending intent, cancellation and measured fill latency', () => {
  const props = fixture();
  Object.assign(props.command.participation, {
    quote_execution_enabled: true,
    quote_worker: { status: 'observed', observed_at: STAMP, interval_seconds: 2 },
    signal_observed_at: STAMP,
    pending: { action: 'entry_intent', symbol: 'BNB/USDT', decision_ts: NOW / 1000 },
    events: [
      { observed_ts: STAMP, execution: { action: 'cancel', symbol: 'BNB/USDT', reason: 'entry_price_premium' } },
      { observed_ts: STAMP, execution: { action: 'fill', side: 'buy', symbol: 'SOL/USDT', latency_seconds: 1.8 } },
    ],
  });
  const html = section(render(props), 'participation');
  assert.match(html, /持续报价检查运行中/);
  assert.match(html, /检查间隔 2 秒/);
  assert.match(html, /BNB\/USDT 买入意向/);
  assert.match(html, /意向已取消/);
  assert.match(html, /模拟买入成交/);
  assert.match(html, /信号至成交 1\.80 秒/);
});

test('a stopped quote worker cannot retain a running label after its heartbeat expires', () => {
  const props = fixture();
  Object.assign(props.command.participation, {
    quote_execution_enabled: true,
    quote_worker: { status: 'observed', observed_at: new Date(NOW - 60000).toISOString(), interval_seconds: 2 },
  });
  const html = section(render(props), 'participation');
  assert.match(html, /报价检查状态待确认/);
  assert.doesNotMatch(html, /持续报价检查运行中/);
});
