import { motion } from 'framer-motion';
import { stateLabels, riskLabels, modeLabels, fmtNum, fmtPct } from '../lib/format';
import type { MarketStateData, RiskGuardData, SystemStatus } from '../types';

const stateClasses: Record<string, string> = {
  TRENDING: 'bg-emerald-500/20 text-emerald-300 border-emerald-400/30',
  SIDEWAYS: 'bg-amber-500/20 text-amber-300 border-amber-400/30',
  RISK_OFF: 'bg-rose-500/20 text-rose-300 border-rose-400/30',
};

const riskClasses: Record<string, string> = {
  ATTACK: 'bg-emerald-500/20 text-emerald-300 border-emerald-400/30',
  NEUTRAL: 'bg-sky-500/20 text-sky-300 border-sky-400/30',
  DEFENSE: 'bg-amber-500/20 text-amber-300 border-amber-400/30',
  PROTECT: 'bg-rose-500/20 text-rose-300 border-rose-400/30',
};

interface HeroProps {
  marketState?: MarketStateData | null;
  riskGuard?: RiskGuardData | null;
  systemStatus?: SystemStatus | null;
  updateTime?: string;
}

export function Hero({ marketState, riskGuard, systemStatus, updateTime }: HeroProps) {
  const state = String(marketState?.state || 'SIDEWAYS').toUpperCase();
  const level = String(riskGuard?.current_level || 'NEUTRAL').toUpperCase();
  const mode = systemStatus?.mode || 'unknown';
  const multiplier = marketState?.position_multiplier ?? 1;
  const dd = riskGuard?.metrics?.dd_pct ?? riskGuard?.metrics?.last_dd_pct ?? null;

  return (
    <motion.section
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5 }}
      className="relative z-10 px-6 pt-8 pb-4"
    >
      <div className="max-w-[1780px] mx-auto flex flex-col md:flex-row md:items-end md:justify-between gap-4">
        <div>
          <div className="flex items-center gap-3 mb-2">
            <div className="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-pulse" />
            <span className="text-sm text-[var(--text-dim)] tracking-wide">
              {modeLabels[mode] || mode} · {updateTime || '等待刷新...'}
            </span>
          </div>
          <h1 className="text-3xl md:text-4xl font-semibold text-gradient">
            V5 生产交易看板
          </h1>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <div className={`px-4 py-2 rounded-full border text-sm font-medium ${stateClasses[state] || stateClasses.SIDEWAYS}`}>
            市场: {stateLabels[state] || state}
          </div>
          <div className={`px-4 py-2 rounded-full border text-sm font-medium ${riskClasses[level] || riskClasses.NEUTRAL}`}>
            风险: {riskLabels[level] || level}
          </div>
          <div className="glass-panel px-4 py-2 text-sm">
            仓位倍数 <span className="font-mono text-[var(--accent)]">{fmtNum(multiplier, 2)}x</span>
          </div>
          <div className="glass-panel px-4 py-2 text-sm">
            回撤 <span className="font-mono">{dd == null ? '--' : fmtPct(dd, 1)}</span>
          </div>
        </div>
      </div>
    </motion.section>
  );
}
