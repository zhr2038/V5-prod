import { motion } from 'framer-motion';
import { stateLabels, riskLabels, modeLabels, fmtNum, fmtPct } from '../lib/format';
import type { MarketStateData, RiskGuardData, SystemStatus } from '../types';

const stateClasses: Record<string, string> = {
  TRENDING: 'material-surface material-clear clear-chip tone-sage surface-lift text-emerald-50 border-emerald-300/18',
  SIDEWAYS: 'material-surface material-clear clear-chip tone-amber surface-lift text-amber-50 border-amber-300/18',
  RISK_OFF: 'material-surface material-clear clear-chip tone-rose surface-lift text-rose-50 border-rose-300/20',
};

const riskClasses: Record<string, string> = {
  ATTACK: 'material-surface material-clear clear-chip tone-sage surface-lift text-emerald-50 border-emerald-300/18',
  NEUTRAL: 'material-surface material-clear clear-chip tone-sky surface-lift text-sky-50 border-sky-300/18',
  DEFENSE: 'material-surface material-clear clear-chip tone-amber surface-lift text-amber-50 border-amber-300/18',
  PROTECT: 'material-surface material-clear clear-chip tone-rose surface-lift text-rose-50 border-rose-300/20',
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
      className="relative z-10 px-6 pb-4"
    >
      <div className="max-w-[1780px] mx-auto flex flex-col md:flex-row md:items-end md:justify-between gap-4">
        <div className="hero-safe-copy">
          <div className="flex items-center gap-3 mb-2">
            <div className="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-pulse accent-glow" />
            <span className="text-sm text-[var(--text-dim)] tracking-wide">
              {modeLabels[mode] || mode} · {updateTime || '等待刷新...'}
            </span>
          </div>
          <h1 className="text-3xl md:text-4xl font-semibold text-gradient">
            V5 生产交易看板
          </h1>
        </div>

        <div className="material-surface material-clear tone-pearl control-rail hero-safe-rail self-start md:self-auto">
          <div className={`control-pill ${stateClasses[state] || stateClasses.SIDEWAYS}`}>
            市场: {stateLabels[state] || state}
          </div>
          <div className={`control-pill ${riskClasses[level] || riskClasses.NEUTRAL}`}>
            风险: {riskLabels[level] || level}
          </div>
          <div className="material-surface material-clear clear-chip tone-sky control-pill surface-lift" data-emphasis="soft">
            仓位倍数 <span className="font-mono text-[var(--accent)]">{fmtNum(multiplier, 2)}x</span>
          </div>
          <div className="material-surface material-clear clear-chip tone-coral control-pill surface-lift" data-emphasis="soft">
            回撤 <span className="font-mono">{dd == null ? '--' : fmtPct(dd, 1)}</span>
          </div>
        </div>
      </div>
    </motion.section>
  );
}
