import {
  BarChart3,
  CheckCircle2,
  Clock,
  DatabaseZap,
  Gauge,
  GitBranch,
  HeartPulse,
  Receipt,
  Route,
  ShieldCheck,
} from 'lucide-react';
import { motion, useReducedMotion } from 'framer-motion';
import type { ReactNode } from 'react';
import { SignalsPanel } from './SignalsPanel';
import { fmtNum, fmtPct, fmtUsd, sideLabels, statusLabels } from '../lib/format';
import type {
  AlphaScore,
  ApiTelemetryData,
  DecisionAuditData,
  HealthData,
  QuantLabCostEstimateData,
  QuantLabGateDecisionData,
  QuantLabPermissionData,
  TimerData,
  Trade,
  UnknownRecord,
} from '../types';

interface OpsRailProps {
  timers?: { timers: TimerData[] } | null;
  alphaScores?: AlphaScore[];
  trades?: Trade[];
  health?: HealthData | null;
  decisionAudit?: DecisionAuditData | null;
  apiTelemetry?: ApiTelemetryData | null;
  quantLabPermission?: QuantLabPermissionData | null;
  quantLabPermissionDetail?: QuantLabPermissionData | null;
  quantLabCost?: QuantLabCostEstimateData | null;
  quantLabGate?: QuantLabGateDecisionData | null;
  deferredReady?: boolean;
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
    const num = Number(value);
    if (Number.isFinite(num)) return num;
  }
  return null;
}

function Section({ title, icon: Icon, children, tone = 'default' }: {
  title: string;
  icon: typeof ShieldCheck;
  children: ReactNode;
  tone?: string;
}) {
  return (
    <section className="ops-card" data-tone={tone}>
      <div className="ops-card-heading">
        <Icon className="h-4 w-4" />
        <span>{title}</span>
      </div>
      {children}
    </section>
  );
}

function fmtLatencyMs(value: unknown) {
  const num = Number(value);
  if (!Number.isFinite(num)) return '--';
  return `${num >= 100 ? num.toFixed(0) : num.toFixed(1)}ms`;
}

function shortTime(value?: string) {
  const text = String(value || '').trim();
  return text ? text.slice(5, 16).replace('T', ' ') : '--';
}

function permissionValue(permission?: QuantLabPermissionData | null) {
  const data = asRecord(permission?.data);
  return firstText(permission?.permission, permission?.decision, data.permission, data.decision, 'UNKNOWN').toUpperCase();
}

function costValue(cost?: QuantLabCostEstimateData | null) {
  const data = asRecord(cost?.data);
  return firstNumber(
    cost?.selected_total_cost_bps,
    data.selected_total_cost_bps,
    cost?.roundtrip_all_in_cost_bps,
    data.roundtrip_all_in_cost_bps,
    cost?.total_cost_bps,
    data.total_cost_bps,
    cost?.cost_bps,
    data.cost_bps
  );
}

export function OpsRail({
  timers,
  alphaScores = [],
  trades = [],
  health,
  decisionAudit,
  apiTelemetry,
  quantLabPermission,
  quantLabPermissionDetail,
  quantLabCost,
  quantLabGate,
  deferredReady = false,
}: OpsRailProps) {
  const reduceMotion = useReducedMotion();
  const exec = decisionAudit?.execution_summary || {};
  const rejected = decisionAudit?.rejected_summary || {};
  const orders = decisionAudit?.orders || [];
  const costData = asRecord(quantLabCost?.data);
  const permissionDetail = asRecord(quantLabPermissionDetail?.data);
  const gateData = asRecord(quantLabGate?.data);
  const selectedCostBps = costValue(quantLabCost);
  const costSource = firstText(
    quantLabCost?.cost_source,
    quantLabCost?.source,
    costData.cost_source,
    costData.source,
    quantLabCost?.available === false ? 'unavailable' : ''
  );

  return (
    <motion.aside
      className="ops-rail"
      initial={reduceMotion ? false : { opacity: 0, x: 12 }}
      animate={reduceMotion ? undefined : { opacity: 1, x: 0 }}
      transition={{ duration: 0.35, ease: 'easeOut' }}
    >
      <Section icon={ShieldCheck} title="中台权限 (quant-lab)" tone="quant">
        <div className="permission-orbit-panel">
          <div className="permission-orbit" data-state={permissionValue(quantLabPermission).toLowerCase()}>
            <ShieldCheck className="h-7 w-7" />
          </div>
          <div className="permission-copy">
            <strong>{permissionValue(quantLabPermission)}</strong>
            <span>{firstText(quantLabPermission?.status, quantLabPermission?.permission_status, permissionDetail.status, 'shadow')}</span>
          </div>
        </div>
        <div className="kv-grid">
          <span>策略</span><strong>{firstText(quantLabPermission?.strategy, permissionDetail.strategy, 'v5')}</strong>
          <span>版本</span><strong>{firstText(quantLabPermission?.version, permissionDetail.version, 'v1')}</strong>
          <span>新鲜度</span><strong>{fmtNum(firstNumber(quantLabPermission?.freshness_sec, permissionDetail.freshness_sec), 0)}s</strong>
          <span>最大订单</span><strong>{fmtUsd(firstNumber(quantLabPermission?.max_single_order_usdt, permissionDetail.max_single_order_usdt))}</strong>
        </div>
        <div className="detail-line">
          Block 原因 ({Array.isArray(quantLabPermission?.reasons) ? quantLabPermission.reasons.length : 0})
          <span>{firstText((quantLabPermission?.reasons || [])[0], permissionDetail.reason, '无阻断')}</span>
        </div>
      </Section>

      <Section icon={DatabaseZap} title="中台成本估算" tone="cost">
        <div className="cost-kpi-grid">
          <div>
            <span>手续费</span>
            <strong>{fmtNum(firstNumber(quantLabCost?.fee_bps, costData.fee_bps), 2)} bps</strong>
          </div>
          <div>
            <span>滑点</span>
            <strong>{fmtNum(firstNumber(quantLabCost?.slippage_bps, costData.slippage_bps), 2)} bps</strong>
          </div>
          <div>
            <span>价差</span>
            <strong>{fmtNum(firstNumber(quantLabCost?.spread_bps, costData.spread_bps), 2)} bps</strong>
          </div>
          <div>
            <span>总成本</span>
            <strong>{fmtNum(selectedCostBps, 2)} bps</strong>
          </div>
        </div>
        <div className="kv-grid">
          <span>Fallback Level</span><strong>{firstText(quantLabCost?.fallback_level, costData.fallback_level, 'NONE')}</strong>
          <span>数据来源</span><strong>{costSource || '--'}</strong>
          <span>样本数</span><strong>{fmtNum(firstNumber(quantLabCost?.sample_count, costData.sample_count), 0)}</strong>
          <span>更新时间</span><strong>{shortTime(firstText(quantLabCost?.as_of_ts, costData.as_of_ts))}</strong>
        </div>
      </Section>

      <Section icon={BarChart3} title="因子排序 (Top 5)" tone="factor">
        <div className="factor-list">
          {alphaScores.slice(0, 5).map((score) => (
            <div className="factor-row" key={score.symbol}>
              <span>{score.symbol.replace('/USDT', '').replace('-USDT', '')}</span>
              <strong data-side={score.score >= 0 ? 'buy' : 'sell'}>{score.score >= 0 ? '多' : '空'}</strong>
              <em>{fmtNum(score.score, 3)}</em>
            </div>
          ))}
          {!alphaScores.length ? <div className="empty-line">{deferredReady ? '无评分数据' : '加载中...'}</div> : null}
        </div>
      </Section>

      <Section icon={Receipt} title="最近成交" tone="trade">
        <div className="trade-list">
          {trades.slice(0, 5).map((trade, index) => (
            <div className="trade-row" key={trade.id || index}>
              <span>{shortTime(trade.timestamp)}</span>
              <strong>{trade.symbol.replace('/USDT', '').replace('-USDT', '')}</strong>
              <em data-side={trade.side}>{sideLabels[trade.side] || trade.side}</em>
              <b>{fmtNum(trade.qty, 4)}</b>
            </div>
          ))}
          {!trades.length ? <div className="empty-line">{deferredReady ? '暂无成交' : '加载中...'}</div> : null}
        </div>
      </Section>

      <Section icon={Route} title="执行路径 (今日)" tone="route">
        <div className="flow-rail">
          {[
            ['信号生成', decisionAudit?.counts?.selected],
            ['风控检查', Number(decisionAudit?.counts?.selected || 0) - Number(rejected.total || 0)],
            ['订单筛选', orders.length],
            ['交易所提交', exec.submitted],
          ].map(([label, value]) => (
            <div className="flow-node" key={String(label)}>
              <span>{label}</span>
              <strong>{fmtNum(value, 0)}</strong>
            </div>
          ))}
        </div>
        <div className="execution-badges">
          <span data-tone="ok"><CheckCircle2 className="h-3.5 w-3.5" />成交 {fmtNum(exec.filled, 0)}</span>
          <span data-tone="warn">压单 {fmtNum(rejected.total, 0)}</span>
          <span data-tone="danger">拒单 {fmtNum(exec.rejected, 0)}</span>
        </div>
      </Section>

      <Section icon={Gauge} title="API 遥测 (OKX)" tone="api">
        <div className="api-kpis">
          <div><span>请求数</span><strong>{fmtNum(apiTelemetry?.totalRequests, 0)}</strong></div>
          <div><span>成功率</span><strong>{fmtPct(apiTelemetry?.successRate, 2)}</strong></div>
          <div><span>P50</span><strong>{fmtLatencyMs(apiTelemetry?.p50LatencyMs)}</strong></div>
          <div><span>P95</span><strong>{fmtLatencyMs(apiTelemetry?.p95LatencyMs)}</strong></div>
        </div>
        <div className="detail-line">{apiTelemetry?.note || (deferredReady ? '暂无 API 遥测数据' : '加载中...')}</div>
      </Section>

      <Section icon={Clock} title="服务与定时器" tone="timer">
        <div className="timer-list">
          {(timers?.timers || []).slice(0, 6).map((timer) => (
            <div className="timer-row" key={timer.name}>
              <span>{timer.name}</span>
              <strong data-state={timer.active ? 'on' : 'off'}>{timer.active ? '运行中' : '停止'}</strong>
              <em>{shortTime(timer.next_trigger)}</em>
            </div>
          ))}
          {!timers?.timers?.length ? <div className="empty-line">{deferredReady ? '无定时器数据' : '加载中...'}</div> : null}
        </div>
      </Section>

      <Section icon={HeartPulse} title="系统健康" tone="health">
        <div className="health-line">
          <span>{statusLabels[health?.status || ''] || health?.status || '--'}</span>
          <strong>{shortTime(health?.last_update)}</strong>
        </div>
        <div className="health-checks">
          {(health?.checks || []).slice(0, 5).map((check) => (
            <div key={check.name} className="health-check">
              <span>{check.name}</span>
              <strong data-state={check.status}>{check.detail}</strong>
            </div>
          ))}
        </div>
      </Section>

      <Section icon={GitBranch} title="策略信号 (最新)" tone="signal">
        <SignalsPanel decisionAudit={decisionAudit} />
        <div className="detail-line">
          Gate decision
          <span>{firstText(quantLabGate?.decision, quantLabGate?.permission, gateData.decision, gateData.permission, 'not_observable')}</span>
        </div>
      </Section>
    </motion.aside>
  );
}
