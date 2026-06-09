import { Activity, Gauge, ShieldCheck, TrendingUp, WalletCards } from 'lucide-react';
import { motion, useReducedMotion } from 'framer-motion';
import type { CSSProperties } from 'react';
import { useDataPulse } from '../hooks/useDataPulse';
import { fmtNum, fmtPct, fmtUnsignedPct, fmtUsd, riskLabels, stateLabels } from '../lib/format';
import type {
  AccountData,
  MarketStateData,
  QuantLabPermissionData,
  QuantLabStatusData,
  RiskGuardData,
  SystemStatus,
  UnknownRecord,
} from '../types';

interface StatusRibbonProps {
  account?: AccountData | null;
  marketState?: MarketStateData | null;
  riskGuard?: RiskGuardData | null;
  quantLabStatus?: QuantLabStatusData | null;
  quantLabPermission?: QuantLabPermissionData | null;
  systemStatus?: SystemStatus | null;
}

function nestedRecord(value: unknown): UnknownRecord {
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

function secondsToClock(seconds: number | null) {
  if (seconds === null) return '--';
  const safe = Math.max(0, Math.floor(seconds));
  const minutes = Math.floor(safe / 60);
  const rest = safe % 60;
  return `${String(minutes).padStart(2, '0')}:${String(rest).padStart(2, '0')}`;
}

function fmtLatencyMs(value: number | null) {
  if (value === null || !Number.isFinite(value)) return '--';
  return `${value >= 100 ? value.toFixed(0) : value.toFixed(1)}ms`;
}

function compactEndpoint(value: string) {
  const endpoint = value.trim();
  if (!endpoint || endpoint === '--') return '--';
  return endpoint
    .replace('/v1/strategy-opportunity-advisory/v5-compact', '/v1/advisory/v5')
    .replace('/v1/strategy-opportunity-advisory', '/v1/advisory')
    .replace('/v1/risk/live-permission', '/v1/live-permission')
    .replace('/v1/costs/estimate', '/v1/costs');
}

function quantLabRequestReasonLabel(reason: string) {
  if (reason === 'no_recent_quant_lab_requests') return '近窗口无请求';
  if (reason === 'quant_lab_request_log_missing_or_empty') return '无请求日志';
  if (reason === 'quant_lab_disabled') return '中台未启用';
  if (reason === 'local_only_no_upstream_requests') return 'local only';
  if (reason === 'quant_lab_not_configured') return '中台未配置';
  if (reason === 'quant_lab_request_metrics_unavailable') return '延迟不可用';
  return reason || '';
}

function permissionTone(permission: string) {
  if (permission === 'ALLOW') return 'allow';
  if (permission === 'SELL_ONLY') return 'sell';
  if (permission === 'ABORT') return 'abort';
  return 'unknown';
}

function ttlRemaining(permission?: QuantLabPermissionData | null) {
  const data = nestedRecord(permission?.data);
  const explicit = firstNumber(permission?.ttl_remaining_sec, data.ttl_remaining_sec, data.ttl_sec);
  if (explicit !== null) return explicit;
  const expiresAt = firstText(permission?.expires_at, data.expires_at);
  const expiry = Date.parse(expiresAt);
  if (!Number.isFinite(expiry)) return null;
  return Math.max(0, (expiry - Date.now()) / 1000);
}

function RibbonCard({
  icon: Icon,
  label,
  value,
  sub,
  tone = 'neutral',
}: {
  icon: typeof Activity;
  label: string;
  value: string;
  sub: string;
  tone?: string;
}) {
  const pulse = useDataPulse(`${tone}:${value}:${sub}`, { durationMs: 620 });
  return (
    <div className={`status-ribbon-card ${pulse.className}`} data-tone={tone} data-pulse={pulse.dataPulse}>
      <div className="ribbon-icon">
        <Icon className="h-4 w-4" />
      </div>
      <div>
        <div className="ribbon-label">{label}</div>
        <div className="ribbon-value">{value}</div>
        <div className="ribbon-sub">{sub}</div>
      </div>
    </div>
  );
}

export function StatusRibbon({
  account,
  marketState,
  riskGuard,
  quantLabStatus,
  quantLabPermission,
  systemStatus,
}: StatusRibbonProps) {
  const reduceMotion = useReducedMotion();
  const state = firstText(marketState?.state, 'UNKNOWN').toUpperCase();
  const risk = firstText(riskGuard?.current_level, 'UNKNOWN').toUpperCase();
  const positionMultiplier = firstNumber(riskGuard?.config?.position_multiplier, marketState?.position_multiplier);
  const riskConfig = nestedRecord(riskGuard?.config);
  const marketRecord = nestedRecord(marketState);
  const marketMetrics = nestedRecord(marketRecord.metrics);
  const targetMultiplier = firstNumber(
    riskConfig.target_position_multiplier,
    riskConfig.position_multiplier_target,
    riskConfig.pos_mult_trending,
    riskConfig.pos_mult_sideways
  );
  const volatilityPct = firstNumber(marketRecord.volatility_pct, marketMetrics.volatility_pct, marketRecord.volatility);
  const permissionData = nestedRecord(quantLabPermission?.data);
  const permission = firstText(
    quantLabPermission?.permission,
    quantLabPermission?.decision,
    permissionData.permission,
    permissionData.decision,
    'UNKNOWN'
  ).toUpperCase();
  const qlStatusData = nestedRecord(quantLabStatus?.data);
  const qlFreshness = firstText(quantLabStatus?.status, qlStatusData.status, quantLabStatus?.available ? 'ok' : 'degraded');
  const qlRequestMetrics = nestedRecord(quantLabStatus?.request_metrics || qlStatusData.request_metrics);
  const qlRequestTotal = firstNumber(qlRequestMetrics.total);
  const qlRequestSuccess = firstNumber(qlRequestMetrics.success_count);
  const qlLookback = firstNumber(qlRequestMetrics.lookback_minutes);
  const qlP95Latency = firstNumber(qlRequestMetrics.p95_latency_ms);
  const qlLatestLatency = firstNumber(qlRequestMetrics.latest_latency_ms);
  const qlLatestEndpoint = compactEndpoint(firstText(qlRequestMetrics.latest_endpoint));
  const qlRequestReason = quantLabRequestReasonLabel(firstText(qlRequestMetrics.reason));
  const qlApiLatencySummary = qlRequestTotal !== null && qlRequestTotal > 0
    ? `API 近${fmtNum(qlLookback, 0)}m ${fmtNum(qlRequestSuccess, 0)}/${fmtNum(qlRequestTotal, 0)}成功 · P95 ${fmtLatencyMs(qlP95Latency)}`
    : `API 延迟 --${qlRequestReason ? ` · ${qlRequestReason}` : ''}`;
  const ttl = ttlRemaining(quantLabPermission);
  const allowedModesRaw = (
    Array.isArray(quantLabPermission?.allowed_modes)
      ? quantLabPermission?.allowed_modes
      : Array.isArray(permissionData.allowed_modes)
        ? permissionData.allowed_modes
        : Array.isArray(permissionData.allowed_live_modes)
          ? permissionData.allowed_live_modes
          : []
  ) as unknown[];
  const allowedModesText = allowedModesRaw.map((item) => String(item || '').toLowerCase()).filter(Boolean);
  const liveOpenAllowed = allowedModesText.some((item) => item.includes('live') || item.includes('spot') || item.includes('open'));
  const permissionSub = permission === 'ALLOW'
    ? (liveOpenAllowed ? '允许开新仓' : 'ALLOW · live modes 未列明')
    : permission === 'SELL_ONLY'
      ? '仅允许减仓'
      : permission === 'ABORT'
        ? '禁止新风险'
        : '权限不可观测';
  const asOf = Date.parse(firstText(quantLabPermission?.as_of_ts, permissionData.as_of_ts));
  const expiresAt = Date.parse(firstText(quantLabPermission?.expires_at, permissionData.expires_at));
  const ttlWindow = Number.isFinite(asOf) && Number.isFinite(expiresAt) && expiresAt > asOf
    ? Math.max(1, (expiresAt - asOf) / 1000)
    : 5400;
  const ttlProgress = ttl === null ? 0 : Math.max(0, Math.min(1, ttl / ttlWindow));
  const permissionPulse = useDataPulse(`${permission}:${ttl === null ? '' : Math.floor(ttl / 5)}:${allowedModesText.join(',')}`, {
    durationMs: 680,
  });

  return (
    <motion.section
      className="status-ribbon"
      initial={reduceMotion ? false : { opacity: 0, y: 10 }}
      animate={reduceMotion ? undefined : { opacity: 1, y: 0 }}
      transition={{ duration: 0.35, ease: 'easeOut' }}
    >
      <RibbonCard
        icon={TrendingUp}
        label="市场状态"
        value={stateLabels[state] || state}
        sub={`波动率 ${fmtUnsignedPct(volatilityPct, 2)}`}
        tone={state === 'RISK_OFF' ? 'danger' : 'good'}
      />
      <RibbonCard
        icon={ShieldCheck}
        label="风险状态"
        value={riskLabels[risk] || risk}
        sub={riskGuard?.reason || systemStatus?.mode || '--'}
        tone={risk === 'PROTECT' || risk === 'DEFENSE' ? 'warn' : 'good'}
      />
      <RibbonCard
        icon={Gauge}
        label="仓位倍数"
        value={`${fmtNum(positionMultiplier, 2)}x`}
        sub={`目标 ${fmtNum(targetMultiplier, 2)}x`}
        tone="warn"
      />
      <RibbonCard
        icon={WalletCards}
        label="账户资金"
        value={fmtUsd(account?.totalEquity)}
        sub={`现金 ${fmtUsd(account?.cash)} · 在仓 ${fmtUsd(account?.positionsValue)}`}
        tone={Number(account?.totalPnl || 0) < 0 ? 'danger' : 'good'}
      />
      <RibbonCard
        icon={Activity}
        label="今日回撤"
        value={fmtPct(account?.todayPnlPercent, 2)}
        sub={`最大回撤 ${fmtUnsignedPct(account?.maxDrawdown, 2)}`}
        tone={Number(account?.todayPnlPercent || 0) < 0 ? 'danger' : 'good'}
      />
      <div
        className={`status-ribbon-card quant-lab-ribbon ${permissionPulse.className}`}
        data-tone={permissionTone(permission)}
        data-pulse={permissionPulse.dataPulse}
      >
        <div
          className="ql-orbit"
          data-permission={permissionTone(permission)}
          style={{ '--ql-ttl-progress': `${ttlProgress * 100}%` } as CSSProperties}
        >
          <ShieldCheck className="h-5 w-5" />
        </div>
        <div className="min-w-0">
          <div className="ribbon-label">中台权限</div>
          <div className="ribbon-value">{permission}</div>
          <div className="ribbon-sub">{permissionSub} · {qlFreshness}</div>
          <div className="ql-api-latency-line">{qlApiLatencySummary}</div>
        </div>
        <div className="ql-ttl">
          <span>中台新鲜度</span>
          <strong>{secondsToClock(ttl)}</strong>
          <small>TTL 剩余时间</small>
          <div className="ql-api-latest">
            <span>{qlLatestEndpoint}</span>
            <b>{fmtLatencyMs(qlLatestLatency)}</b>
          </div>
        </div>
      </div>
    </motion.section>
  );
}
