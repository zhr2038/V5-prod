import { Suspense } from 'react';
import type { ComponentType, ReactNode } from 'react';
import { Activity, Clock, Route } from 'lucide-react';
import { PositionsPanel } from './PositionsPanel';
import { MarketRadar } from './MarketRadar';
import { useDataPulse, usePreviousValue } from '../hooks/useDataPulse';
import { fmtNum, fmtPct, fmtUsd, sideLabels } from '../lib/format';
import type {
  AccountData,
  ApiTelemetryData,
  ApiTelemetrySeriesData,
  ApiTelemetrySeriesSample,
  DecisionAuditData,
  MarketStateData,
  Position,
  QuantLabCostEstimateData,
  SlippageInsightsData,
  TimerData,
  Trade,
  UnknownRecord,
} from '../types';

interface MainTradingGridProps {
  positions: Position[];
  trades: Trade[];
  focusSymbol?: string;
  account?: AccountData | null;
  marketState?: MarketStateData | null;
  slippageInsights?: SlippageInsightsData | null;
  timers?: { timers: TimerData[] } | null;
  decisionAudit?: DecisionAuditData | null;
  apiTelemetry?: ApiTelemetryData | null;
  apiTelemetrySeries?: ApiTelemetrySeriesData | null;
  quantLabCost?: QuantLabCostEstimateData | null;
  showDeferredPanels: boolean;
  fallback: ReactNode;
  ExecutionInsightsPanel: ComponentType<{ slippageInsights?: SlippageInsightsData | null }>;
}

function asRecord(value: unknown): UnknownRecord {
  return value && typeof value === 'object' ? (value as UnknownRecord) : {};
}

function firstText(...values: unknown[]) {
  for (const value of values) {
    const text = String(value ?? '').trim();
    if (text) return text;
  }
  return '';
}

function firstNumber(...values: unknown[]) {
  for (const value of values) {
    if (value === null || value === undefined || value === '') continue;
    const num = Number(value);
    if (Number.isFinite(num)) return num;
  }
  return null;
}

function tradeTimeValue(trade: Trade) {
  const raw = String(trade.timestamp || '').trim();
  if (!raw) return 0;
  const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T');
  const parsed = Date.parse(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
}

function shortTime(value?: string) {
  const text = String(value || '').trim();
  return text ? text.slice(5, 16).replace('T', ' ') : '--';
}

function fmtLatencyMs(value: unknown) {
  const num = firstNumber(value);
  if (!Number.isFinite(num)) return '--';
  return `${Number(num) >= 100 ? Number(num).toFixed(0) : Number(num).toFixed(1)}ms`;
}

function timerProgress(timer: TimerData) {
  if (!timer.active) return 0;
  const countdown = firstNumber(timer.countdown_seconds);
  const intervalMinutes = firstNumber(timer.interval_minutes);
  if (!Number.isFinite(countdown) || !Number.isFinite(intervalMinutes) || Number(intervalMinutes) <= 0) {
    return null;
  }
  const totalSeconds = Number(intervalMinutes) * 60;
  return Math.max(0, Math.min(100, (1 - Number(countdown) / totalSeconds) * 100));
}

function buildTelemetryPath(samples: ApiTelemetrySeriesSample[], field: 'p50_latency_ms' | 'p95_latency_ms') {
  const values = samples
    .map((sample) => Number(sample[field]))
    .filter((value) => Number.isFinite(value));
  if (samples.length < 2 || !values.length) return '';
  const w = 250;
  const h = 82;
  const min = Math.min(0, ...values);
  const max = Math.max(...values, 1);
  const range = Math.max(max - min, 1);
  return samples
    .map((sample, index) => {
      const raw = Number(sample[field]);
      if (!Number.isFinite(raw)) return '';
      const x = (index / Math.max(samples.length - 1, 1)) * w;
      const y = h - ((raw - min) / range) * h;
      return `${index ? 'L' : 'M'} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .filter(Boolean)
    .join(' ');
}

function focusTrades(trades: Trade[]) {
  return [...trades].sort((a, b) => tradeTimeValue(b) - tradeTimeValue(a));
}

function HoldingsFocusPanel({ positions, trades, account }: { positions: Position[]; trades: Trade[]; account?: AccountData | null }) {
  const sortedPositions = [...positions].sort((a, b) => Number(b.value || 0) - Number(a.value || 0));
  const sortedTrades = focusTrades(trades);
  const latestTrade = sortedTrades[0] || null;
  const floatingPnl = sortedPositions.reduce((sum, pos) => sum + Number(pos.pnl || 0), 0);
  const floatingPct = account?.positionsValue ? floatingPnl / Number(account.positionsValue || 1) : null;
  const hasPositions = sortedPositions.length > 0;

  return (
    <section className="design-panel holdings-focus-panel">
      <div className="design-panel-heading">
        <span>{hasPositions ? '持仓聚焦' : '最近成交'}</span>
        <small>{hasPositions ? sortedPositions.length : '更多 >'}</small>
      </div>
      <table className="design-table">
        <thead>
          {hasPositions ? (
            <tr>
              <th>币种</th>
              <th>方向</th>
              <th>数量</th>
              <th>均价</th>
              <th>浮动盈亏(USDT)</th>
              <th>盈亏%</th>
            </tr>
          ) : (
            <tr>
              <th>时间</th>
              <th>币种</th>
              <th>方向</th>
              <th>价格</th>
              <th>数量</th>
            </tr>
          )}
        </thead>
        <tbody>
          {hasPositions
            ? sortedPositions.map((position) => (
                <tr key={position.symbol}>
                  <td>{position.symbol.replace('/USDT', '').replace('-USDT', '')}</td>
                  <td className="text-buy">多</td>
                  <td>{fmtNum(position.qty, 4)}</td>
                  <td>{fmtUsd(position.avgPrice)}</td>
                  <td className={Number(position.pnl || 0) >= 0 ? 'text-buy' : 'text-sell'}>{fmtUsd(position.pnl)}</td>
                  <td className={Number(position.pnlPercent || 0) >= 0 ? 'text-buy' : 'text-sell'}>{fmtPct(position.pnlPercent)}</td>
                </tr>
              ))
            : sortedTrades.slice(0, 9).map((trade) => (
                <tr key={trade.id}>
                  <td>{shortTime(trade.timestamp)}</td>
                  <td>{trade.symbol.replace('/USDT', '').replace('-USDT', '')}</td>
                  <td className={trade.side === 'buy' ? 'text-buy' : 'text-sell'}>{sideLabels[trade.side] || trade.side}</td>
                  <td className={trade.side === 'buy' ? 'text-buy' : 'text-sell'}>{fmtUsd(trade.price)}</td>
                  <td>{fmtNum(trade.qty, 4)}</td>
                </tr>
              ))}
          {!hasPositions && !sortedTrades.length ? (
            <tr>
              <td colSpan={5} className="table-empty">
                暂无成交
              </td>
            </tr>
          ) : null}
        </tbody>
      </table>
      <div className="holdings-footer">
        <span>在仓市值(USDT) <strong>{fmtUsd(account?.positionsValue)}</strong></span>
        <span>浮动盈亏(USDT) <strong className={floatingPnl >= 0 ? 'text-buy' : 'text-sell'}>{fmtUsd(floatingPnl)}</strong></span>
        <span>浮动盈亏% <strong className={floatingPnl >= 0 ? 'text-buy' : 'text-sell'}>{fmtPct(floatingPct)}</strong></span>
      </div>
      {latestTrade && hasPositions ? (
        <div className="latest-trade-strip">
          <span>最近成交</span>
          <strong>{latestTrade.symbol.replace('/USDT', '').replace('-USDT', '')}</strong>
          <em className={latestTrade.side === 'buy' ? 'text-buy' : 'text-sell'}>{sideLabels[latestTrade.side] || latestTrade.side}</em>
          <b>{fmtUsd(latestTrade.price)}</b>
        </div>
      ) : null}
    </section>
  );
}

function QuantLabCostPanel({ cost }: { cost?: QuantLabCostEstimateData | null }) {
  const data = asRecord(cost?.data);
  const selectedCost = firstNumber(
    cost?.selected_total_cost_bps,
    data.selected_total_cost_bps,
    cost?.roundtrip_all_in_cost_bps,
    data.roundtrip_all_in_cost_bps,
    cost?.total_cost_bps,
    data.total_cost_bps,
    cost?.cost_bps,
    data.cost_bps
  );
  const source = firstText(cost?.cost_source, cost?.source, data.cost_source, data.source, cost?.available === false ? 'unavailable' : '');

  return (
    <section className="design-panel ql-cost-panel">
      <div className="design-panel-heading">
        <span>中台成本估算 (quant-lab)</span>
        <small>{firstText(cost?.symbol, data.symbol, '--')} / {firstText(cost?.regime, data.regime, 'normal')}</small>
      </div>
      <div className="cost-metric-row">
        <div><span>手续费</span><strong>{fmtNum(firstNumber(cost?.fee_bps, data.fee_bps), 2)} bps</strong></div>
        <div><span>滑点</span><strong>{fmtNum(firstNumber(cost?.slippage_bps, data.slippage_bps), 2)} bps</strong></div>
        <div><span>价差</span><strong>{fmtNum(firstNumber(cost?.spread_bps, data.spread_bps), 2)} bps</strong></div>
        <div><span>总成本</span><strong>{fmtNum(selectedCost, 2)} bps</strong></div>
      </div>
      <div className="ql-cost-footer">
        <span>Fallback Level <strong>{firstText(cost?.fallback_level, data.fallback_level, 'NONE')}</strong></span>
        <span>数据来源 <strong>{source || '--'}</strong></span>
        <span>样本数 <strong>{fmtNum(firstNumber(cost?.sample_count, data.sample_count), 0)}</strong></span>
        <span>更新时间 <strong>{shortTime(firstText(cost?.as_of_ts, data.as_of_ts))}</strong></span>
      </div>
    </section>
  );
}

function TimerProgressRow({ timer }: { timer: TimerData }) {
  const progress = timerProgress(timer);
  const pulse = useDataPulse(
    `${timer.active ? '1' : '0'}:${timer.next_run || timer.next_trigger || ''}:${timer.countdown_seconds ?? ''}`,
    { durationMs: 500 }
  );
  return (
    <div className={`timer-progress-row ${pulse.className}`} data-pulse={pulse.dataPulse} key={timer.name}>
      <span>{timer.name}</span>
      <strong className={timer.active ? 'text-buy' : 'text-muted'}>{timer.active ? '运行中' : '停止'}</strong>
      <div className="timer-progress-track" title={timer.time_left || shortTime(timer.next_run || timer.next_trigger)}>
        <i style={{ width: `${progress ?? 0}%` }} />
      </div>
      <em>{timer.time_left || shortTime(timer.next_run || timer.next_trigger)}</em>
    </div>
  );
}

function TimersPanel({ timers }: { timers?: { timers: TimerData[] } | null }) {
  return (
    <section className="design-panel compact-ops-panel">
      <div className="design-panel-heading"><span>服务与定时器</span><Clock className="h-4 w-4" /></div>
      <div className="timer-progress-list">
        {(timers?.timers || []).slice(0, 5).map((timer) => <TimerProgressRow timer={timer} key={timer.name} />)}
        {!timers?.timers?.length ? <div className="table-empty">暂无定时器数据</div> : null}
      </div>
    </section>
  );
}

function ExecutionPathPanel({ decisionAudit }: { decisionAudit?: DecisionAuditData | null }) {
  const exec = decisionAudit?.execution_summary || {};
  const rejected = decisionAudit?.rejected_summary || {};
  const orders = decisionAudit?.orders || [];
  const selected = firstNumber(decisionAudit?.counts?.selected) || 0;
  const submitted = firstNumber(exec.submitted, orders.length);
  const filled = firstNumber(exec.filled) || 0;
  const rejectedCount = firstNumber(exec.rejected, rejected.total) || 0;
  const previousFilled = usePreviousValue(filled);
  const previousRejected = usePreviousValue(rejectedCount);
  const flowTone = rejectedCount > Number(previousRejected || 0)
    ? 'rejected'
    : filled > Number(previousFilled || 0)
      ? 'filled'
      : Number(rejected.total || 0) > 0
        ? 'blocked'
        : 'selected';
  const flowPulse = useDataPulse(
    `${selected}:${submitted}:${filled}:${rejectedCount}:${Number(rejected.total || 0)}`,
    { durationMs: 900 }
  );

  return (
    <section className="design-panel compact-ops-panel execution-path-panel">
      <div className="design-panel-heading"><span>执行路径 (今日)</span><Route className="h-4 w-4" /></div>
      <div
        className={`execution-chain ${flowPulse.className}`}
        data-pulse={flowPulse.active ? 'true' : 'false'}
        data-flow={flowTone}
      >
        <div><span>信号生成</span><strong>{fmtNum(selected, 0)}</strong></div>
        <i />
        <div><span>风控检查</span><strong>{fmtNum(Math.max(0, selected - Number(rejected.total || 0)), 0)}</strong></div>
        <i />
        <div><span>订单筛选</span><strong>{fmtNum(orders.length, 0)}</strong></div>
        <i />
        <div><span>交易所提交</span><strong>{fmtNum(submitted, 0)}</strong></div>
      </div>
      <div className="execution-status-row">
        <span className="ok">成交 {fmtNum(filled, 0)}</span>
        <span className="info">部分成交 {fmtNum(exec.partially_filled, 0)}</span>
        <span className="warn">压单 {fmtNum(rejected.total, 0)}</span>
        <span className="danger">拒单 {fmtNum(rejectedCount, 0)}</span>
      </div>
    </section>
  );
}

function ApiTelemetryPanel({
  apiTelemetry,
  apiTelemetrySeries,
}: {
  apiTelemetry?: ApiTelemetryData | null;
  apiTelemetrySeries?: ApiTelemetrySeriesData | null;
}) {
  const samples = Array.isArray(apiTelemetrySeries?.samples) ? apiTelemetrySeries.samples : [];
  const p50Path = buildTelemetryPath(samples, 'p50_latency_ms');
  const p95Path = buildTelemetryPath(samples, 'p95_latency_ms');
  const pulse = useDataPulse(
    `${apiTelemetry?.totalRequests ?? ''}:${apiTelemetry?.p50LatencyMs ?? ''}:${apiTelemetry?.p95LatencyMs ?? ''}:${
      samples.at(-1)?.ts_ms ?? ''
    }`,
    { durationMs: 700 }
  );
  return (
    <section className={`design-panel compact-ops-panel api-telemetry-panel ${pulse.className}`} data-pulse={pulse.dataPulse}>
      <div className="design-panel-heading"><span>API 遥测 (OKX)</span><Activity className="h-4 w-4" /></div>
      <div className="api-telemetry-grid">
        <div><span>请求数</span><strong>{fmtNum(apiTelemetry?.totalRequests, 0)}</strong></div>
        <div><span>成功率</span><strong>{fmtPct(apiTelemetry?.successRate, 2)}</strong></div>
        <div><span>P50</span><strong>{fmtLatencyMs(apiTelemetry?.p50LatencyMs)}</strong></div>
        <div><span>P95</span><strong>{fmtLatencyMs(apiTelemetry?.p95LatencyMs)}</strong></div>
      </div>
      {samples.length >= 2 && p50Path && p95Path ? (
        <svg className="api-telemetry-wave" viewBox="0 0 250 92" role="img" aria-label="API telemetry latency series">
          <defs>
            <linearGradient id="apiWaveFill" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="rgba(37, 231, 255, 0.22)" />
              <stop offset="100%" stopColor="rgba(37, 231, 255, 0)" />
            </linearGradient>
          </defs>
          <path d={`${p95Path} L 250 92 L 0 92 Z`} fill="url(#apiWaveFill)" />
          <path d={p50Path} className="api-wave-line api-wave-line-p50" />
          <path d={p95Path} className="api-wave-line api-wave-line-p95" />
          {samples.some((sample) => Number(sample.error_count || 0) > 0 || Number(sample.rate_limited_count || 0) > 0) ? (
            <circle cx="238" cy="16" r="4" className="api-wave-error-dot" />
          ) : null}
        </svg>
      ) : (
        <div className="api-series-empty">需启用 telemetry series</div>
      )}
      <div className="api-note">{apiTelemetrySeries?.note || apiTelemetry?.note || '暂无 API 遥测数据'}</div>
    </section>
  );
}

export function MainTradingGrid({
  positions,
  trades,
  focusSymbol,
  account,
  marketState,
  slippageInsights,
  timers,
  decisionAudit,
  apiTelemetry,
  apiTelemetrySeries,
  quantLabCost,
  showDeferredPanels,
  fallback,
  ExecutionInsightsPanel,
}: MainTradingGridProps) {
  return (
    <main className="main-trading-grid strict-design-grid">
      <div className="design-top-row">
        <PositionsPanel positions={positions} trades={trades} focusSymbol={focusSymbol} account={account || null} />
        <HoldingsFocusPanel positions={positions} trades={trades} account={account || null} />
      </div>

      <div className="design-diagnostics-row">
        <MarketRadar marketState={marketState || null} />
        {showDeferredPanels ? (
          <Suspense fallback={fallback}>
            <ExecutionInsightsPanel slippageInsights={slippageInsights || null} />
          </Suspense>
        ) : (
          fallback
        )}
        <ApiTelemetryPanel apiTelemetry={apiTelemetry || null} apiTelemetrySeries={apiTelemetrySeries || null} />
        <TimersPanel timers={timers || null} />
        <ExecutionPathPanel decisionAudit={decisionAudit || null} />
      </div>

      <div className="design-ops-row">
        <QuantLabCostPanel cost={quantLabCost || null} />
      </div>
    </main>
  );
}
