import { Activity, Gauge, ShieldCheck, TrendingUp, WalletCards } from 'lucide-react';
import { motion, useReducedMotion } from 'framer-motion';
import { fmtNum, fmtPct, fmtUnsignedPct, riskLabels, stateLabels } from '../lib/format';
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
  return (
    <div className="status-ribbon-card" data-tone={tone}>
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
  const ttl = ttlRemaining(quantLabPermission);
  const permissionSub = permission === 'ALLOW' ? '允许开新仓' : permission === 'SELL_ONLY' ? '仅允许减仓' : permission === 'ABORT' ? '禁止新风险' : '权限不可观测';

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
        label="今日回撤"
        value={fmtPct(account?.todayPnlPercent, 2)}
        sub={`最大回撤 ${fmtUnsignedPct(account?.maxDrawdown, 2)}`}
        tone={Number(account?.todayPnlPercent || 0) < 0 ? 'danger' : 'good'}
      />
      <div className="status-ribbon-card quant-lab-ribbon" data-tone={permissionTone(permission)}>
        <div className="ql-orbit" data-permission={permissionTone(permission)}>
          <ShieldCheck className="h-5 w-5" />
        </div>
        <div className="min-w-0">
          <div className="ribbon-label">中台权限</div>
          <div className="ribbon-value">{permission}</div>
          <div className="ribbon-sub">{permissionSub} · {qlFreshness}</div>
        </div>
        <div className="ql-ttl">
          <span>中台新鲜度</span>
          <strong>{secondsToClock(ttl)}</strong>
          <small>TTL 剩余时间</small>
        </div>
      </div>
    </motion.section>
  );
}
