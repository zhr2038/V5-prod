import { motion } from 'framer-motion';
import { Radar } from 'lucide-react';
import { stateLabels } from '../lib/format';
import type { MarketStateData } from '../types';

interface MarketRadarProps {
  marketState?: MarketStateData | null;
}

const stateBg: Record<string, string> = {
  TRENDING: 'bg-emerald-500/15 border-emerald-400/25',
  SIDEWAYS: 'bg-amber-500/15 border-amber-400/25',
  RISK_OFF: 'bg-rose-500/15 border-rose-400/25',
};

function VoteCard({
  title,
  vote,
  showProbs,
  showBars,
}: {
  title: string;
  vote?: import('../types').MarketVote;
  showProbs?: boolean;
  showBars?: boolean;
}) {
  if (!vote) return null;
  const state = String(vote.state || '').toUpperCase();
  const confidence = vote.confidence ?? 0;
  const probs = vote.probs || {};
  const surfaceTone =
    state === 'TRENDING' || state === 'TRENDINGUP'
      ? 'tone-sage'
      : state === 'RISK_OFF' || state === 'TRENDINGDOWN'
      ? 'tone-rose'
      : 'tone-amber';

  const rows = showProbs
    ? [
        { label: '上涨', value: Number(probs.TrendingUp || 0) },
        { label: '震荡', value: Number(probs.Sideways || 0) },
        { label: '避险', value: Number(probs.TrendingDown || 0) },
      ]
    : showBars
    ? [
        { label: '上涨', value: state === 'TRENDING' || state === 'TRENDINGUP' ? confidence : 0 },
        { label: '震荡', value: state === 'SIDEWAYS' ? confidence : 0 },
        { label: '避险', value: state === 'RISK_OFF' || state === 'TRENDINGDOWN' ? confidence : 0 },
      ]
    : [];

  return (
    <div className={`material-surface material-clear clear-control surface-lift ${surfaceTone} p-4 flex flex-col gap-3`}>
      <div className="flex items-center justify-between">
        <span className="text-sm text-[var(--text-dim)]">{title}</span>
        <span className={`text-xs px-2 py-0.5 rounded-full border ${stateBg[state] || 'bg-white/8 border-white/12'}`}>
          {stateLabels[state] || vote.state || '—'}
        </span>
      </div>
      {rows.length > 0 && (
        <div className="flex flex-col gap-2">
          {rows.map((r) => (
            <div key={r.label} className="flex items-center gap-2 text-xs">
              <span className="w-10 text-[var(--text-dim)]">{r.label}</span>
              <div className="flex-1 h-1.5 bg-white/10 rounded-full overflow-hidden">
                <div
                  className="h-full rounded-full bg-[var(--accent)]"
                  style={{ width: `${Math.max(0, Math.min(100, r.value * 100))}%` }}
                />
              </div>
              <span className="w-10 text-right font-mono">{(r.value * 100).toFixed(1)}%</span>
            </div>
          ))}
        </div>
      )}
      {vote.summary && <div className="text-xs text-[var(--text-soft)] line-clamp-2">{vote.summary}</div>}
    </div>
  );
}

export function MarketRadar({ marketState }: MarketRadarProps) {
  const votes = marketState?.votes || {};
  const history = marketState?.history_24h || [];
  const alerts = marketState?.alerts || [];

  return (
    <div className="material-surface material-reading tone-smoke reading-frame p-5 flex flex-col gap-4">
      <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
        <Radar className="w-4 h-4" />
        <span>市场雷达</span>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        <VoteCard title="HMM" vote={votes.hmm} showProbs />
        <VoteCard title="资金费率" vote={votes.funding} showBars />
        <VoteCard title="RSS" vote={votes.rss} showBars />
      </div>

      {alerts.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {alerts.slice(0, 4).map((a, i) => (
            <span key={i} className="text-xs px-2.5 py-1 rounded-full bg-rose-500/15 text-rose-300 border border-rose-400/20">
              {a}
            </span>
          ))}
        </div>
      )}

      <div>
        <div className="text-xs text-[var(--text-dim)] mb-2">24h 投票轨迹</div>
        <div className="flex items-center gap-1 flex-wrap">
          {history.map((h, i) => {
            const final = h.final || {};
            const s = String(final.state || '').toUpperCase();
            const confidence = final.confidence ?? 0.5;
            const color =
              s === 'TRENDING' || s === 'TRENDINGUP'
                ? 'bg-emerald-400'
                : s === 'RISK_OFF' || s === 'TRENDINGDOWN'
                ? 'bg-rose-400'
                : 'bg-amber-400';
            return (
              <motion.div
                key={i}
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: i * 0.01 }}
                title={`${h.label}: ${stateLabels[s] || s}`}
                className={`w-3 h-6 rounded-sm ${color}`}
                style={{ opacity: 0.3 + confidence * 0.7 }}
              />
            );
          })}
          {!history.length && <span className="text-xs text-[var(--text-dim)]">无历史数据</span>}
        </div>
      </div>
    </div>
  );
}
