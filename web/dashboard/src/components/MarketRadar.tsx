import { Radar } from 'lucide-react';
import type { CSSProperties } from 'react';
import { useDataPulse } from '../hooks/useDataPulse';
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
  showFullSummary,
}: {
  title: string;
  vote?: import('../types').MarketVote;
  showProbs?: boolean;
  showBars?: boolean;
  showFullSummary?: boolean;
}) {
  if (!vote) return null;
  const state = String(vote.state || '').toUpperCase();
  const confidence = vote.confidence ?? 0;
  const probs = vote.probs || {};
  const summary = vote.summary || vote.summary_short;
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
    <div className={`liquid-glass-thin ${surfaceTone} p-4 flex flex-col gap-3`}>
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
              <div className="glass-progress-track glass-progress-track--slim flex-1">
                <div
                  className="liquid-progress-fill liquid-progress-fill--mint absolute inset-y-[1px] left-[1px] rounded-full"
                  style={{ width: `${Math.max(0, Math.min(100, r.value * 100))}%` }}
                />
              </div>
              <span className="w-10 text-right font-mono">{(r.value * 100).toFixed(1)}%</span>
            </div>
          ))}
        </div>
      )}
      {summary && (
        <div
          className={`text-xs leading-relaxed text-[var(--text-soft)] break-words ${
            showFullSummary ? 'whitespace-normal' : 'line-clamp-2'
          }`}
        >
          {summary}
        </div>
      )}
    </div>
  );
}

function confidenceOf(vote?: import('../types').MarketVote) {
  const value = Number(vote?.confidence ?? 0);
  return Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : 0;
}

function voteRadarPoints(votes: NonNullable<MarketStateData['votes']>) {
  const items = [
    confidenceOf(votes.hmm),
    confidenceOf(votes.funding),
    confidenceOf(votes.rss),
    Number(votes.hmm?.probs?.TrendingUp || 0),
    Number(votes.rss?.probs?.TrendingDown || 0),
  ].map((value) => Math.max(0.08, Math.min(1, Number(value) || 0)));
  const cx = 58;
  const cy = 58;
  const maxR = 46;
  return items
    .map((value, index) => {
      const angle = -Math.PI / 2 + (index / items.length) * Math.PI * 2;
      const radius = value * maxR;
      return `${(cx + Math.cos(angle) * radius).toFixed(1)},${(cy + Math.sin(angle) * radius).toFixed(1)}`;
    })
    .join(' ');
}

export function MarketRadar({ marketState }: MarketRadarProps) {
  const votes = marketState?.votes || {};
  const history = marketState?.history_24h || [];
  const alerts = marketState?.alerts || [];
  const hasVotes = Boolean(votes.hmm || votes.funding || votes.rss);
  const averageConfidence = hasVotes
    ? [votes.hmm, votes.funding, votes.rss].map(confidenceOf).reduce((sum, value) => sum + value, 0) /
      [votes.hmm, votes.funding, votes.rss].filter(Boolean).length
    : 0;
  const scanDuration = Math.max(2.6, 7.2 - averageConfidence * 4.4);
  const radarPulse = useDataPulse(
    `${marketState?.state || ''}:${averageConfidence.toFixed(3)}:${history.at(-1)?.ts_ms || ''}`,
    { durationMs: 700 }
  );

  return (
    <div className="liquid-glass-thick tone-smoke reading-frame p-5 flex flex-col gap-4 market-radar-panel">
      <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
        <Radar className="w-4 h-4" />
        <span>市场雷达</span>
      </div>

      <div
        className={`market-radar-dial ${radarPulse.className}`}
        data-ready={hasVotes ? 'true' : 'false'}
        data-pulse={radarPulse.dataPulse}
        style={{ '--radar-duration': `${scanDuration}s` } as CSSProperties}
      >
        {hasVotes ? (
          <svg viewBox="0 0 116 116" role="img" aria-label="Market vote radar">
            {[18, 32, 46].map((radius) => (
              <circle key={radius} cx="58" cy="58" r={radius} />
            ))}
            {[0, 1, 2, 3, 4].map((index) => {
              const angle = -Math.PI / 2 + (index / 5) * Math.PI * 2;
              return (
                <line
                  key={index}
                  x1="58"
                  y1="58"
                  x2={(58 + Math.cos(angle) * 48).toFixed(1)}
                  y2={(58 + Math.sin(angle) * 48).toFixed(1)}
                />
              );
            })}
            <polygon points={voteRadarPoints(votes)} />
            <path className="market-radar-scan" d="M58 58 L58 12" />
          </svg>
        ) : (
          <span>数据不足</span>
        )}
        <div>
          <strong>{stateLabels[String(marketState?.state || '').toUpperCase()] || marketState?.state || '--'}</strong>
          <em>confidence {Math.round(averageConfidence * 100)}%</em>
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 market-radar-votes">
        <VoteCard title="HMM" vote={votes.hmm} showProbs />
        <VoteCard title="资金费率" vote={votes.funding} showBars />
        <VoteCard title="RSS" vote={votes.rss} showBars showFullSummary />
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

      <div className="liquid-glass-inset tone-neutral px-3 py-3 market-radar-history">
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
              <div
                key={i}
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
