import { motion } from 'framer-motion';
import { stateLabels, riskLabels, modeLabels, fmtNum, fmtPct } from '../lib/format';
import type { MarketStateData, RiskGuardData, SystemStatus } from '../types';

const stateClasses: Record<string, string> = {
  TRENDING: 'liquid-glass-thin clear-chip tone-sage text-emerald-50',
  SIDEWAYS: 'liquid-glass-thin clear-chip tone-amber text-amber-50',
  RISK_OFF: 'liquid-glass-thin clear-chip tone-rose text-rose-50',
};

const riskClasses: Record<string, string> = {
  ATTACK: 'liquid-glass-thin clear-chip tone-sage text-emerald-50',
  NEUTRAL: 'liquid-glass-thin clear-chip tone-sky text-sky-50',
  DEFENSE: 'liquid-glass-thin clear-chip tone-amber text-amber-50',
  PROTECT: 'liquid-glass-thin clear-chip tone-rose text-rose-50',
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
      <div className="max-w-[1780px] mx-auto grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
        <div className="hero-safe-copy min-w-0">
          <div className="flex items-center gap-3 mb-2">
            <div className="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-pulse accent-glow" />
            <span className="text-sm text-[var(--text-dim)] tracking-wide">
              {modeLabels[mode] || mode} · {updateTime || '等待刷新...'}
            </span>
          </div>
          <h1 className="text-3xl sm:text-4xl font-semibold text-gradient">
            V5 生产交易看板
          </h1>
        </div>

        <div className="material-surface material-clear tone-pearl control-rail hero-safe-rail w-full justify-center sm:justify-start lg:w-auto lg:justify-end">
          <div className={`control-pill ${stateClasses[state] || stateClasses.SIDEWAYS}`}>
            市场: {stateLabels[state] || state}
          </div>
          <div className={`control-pill liquid-glass-thin ${riskClasses[level] || riskClasses.NEUTRAL}`}>
            风险: {riskLabels[level] || level}
          </div>
          <div className="liquid-glass-thin clear-chip tone-sky control-pill" data-emphasis="soft">
            仓位倍数 <span className="font-mono text-[var(--accent)]">{fmtNum(multiplier, 2)}x</span>
          </div>
          <div className="liquid-glass-thin clear-chip tone-coral control-pill" data-emphasis="soft">
            回撤 <span className="font-mono">{dd == null ? '--' : fmtPct(dd, 1)}</span>
          </div>
        </div>
      </div>
    </motion.section>
  );
}
