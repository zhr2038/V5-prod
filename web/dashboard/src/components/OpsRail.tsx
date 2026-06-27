import {
  BarChart3,
  GitBranch,
  Receipt,
  ShieldCheck,
} from 'lucide-react';
import { motion, useReducedMotion } from 'framer-motion';
import type { ReactNode } from 'react';
import { SignalsPanel } from './SignalsPanel';
import { fmtNum, fmtUsd, sideLabels } from '../lib/format';
import type {
  AlphaScore,
  DecisionAuditData,
  QuantLabGateDecisionData,
  QuantLabPermissionData,
  Trade,
  UnknownRecord,
} from '../types';

interface OpsRailProps {
  alphaScores?: AlphaScore[];
  trades?: Trade[];
  decisionAudit?: DecisionAuditData | null;
  quantLabPermission?: QuantLabPermissionData | null;
  quantLabPermissionDetail?: QuantLabPermissionData | null;
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
    if (value === null || value === undefined || value === '') continue;
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

function shortTime(value?: string) {
  const text = String(value || '').trim();
  return text ? text.slice(5, 16).replace('T', ' ') : '--';
}

function permissionValue(permission?: QuantLabPermissionData | null) {
  const data = asRecord(permission?.data);
  return firstText(permission?.permission, permission?.decision, data.permission, data.decision, 'UNKNOWN').toUpperCase();
}

export function OpsRail({
  alphaScores = [],
  trades = [],
  decisionAudit,
  quantLabPermission,
  quantLabPermissionDetail,
  quantLabGate,
  deferredReady = false,
}: OpsRailProps) {
  const reduceMotion = useReducedMotion();
  const permissionDetail = {
    ...asRecord(quantLabPermissionDetail?.permission),
    ...asRecord(quantLabPermissionDetail?.data),
  };
  const gateData = asRecord(quantLabGate?.data);

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
          <span>版本</span><strong>{firstText(quantLabPermission?.version, permissionDetail.version, '5.0.0')}</strong>
          <span>新鲜度</span><strong>{fmtNum(firstNumber(quantLabPermission?.freshness_sec, permissionDetail.freshness_sec), 0)}s</strong>
          <span>最大订单</span><strong>{fmtUsd(firstNumber(quantLabPermission?.max_single_order_usdt, permissionDetail.max_single_order_usdt))}</strong>
        </div>
        <div className="detail-line">
          Block 原因 ({Array.isArray(quantLabPermission?.reasons) ? quantLabPermission.reasons.length : 0})
          <span>{firstText((quantLabPermission?.reasons || [])[0], permissionDetail.reason, '无阻断')}</span>
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
