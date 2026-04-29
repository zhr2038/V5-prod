import { Activity } from 'lucide-react';
import { fmtNum } from '../lib/format';
import type { SlippageInsightsData } from '../types';

interface ExecutionInsightsPanelProps {
  slippageInsights?: SlippageInsightsData | null;
}

function fmtBps(value: unknown, digits = 1) {
  const num = Number(value);
  if (!Number.isFinite(num)) return '--';
  return `${num.toFixed(digits)} bps`;
}

function toneClass(status?: string) {
  if (status === 'healthy') return 'tone-sage';
  if (status === 'warning') return 'tone-amber';
  if (status === 'critical' || status === 'error') return 'tone-coral';
  return 'tone-smoke';
}

function markerTone(status?: string) {
  if (status === 'healthy') return 'text-emerald-300';
  if (status === 'warning') return 'text-amber-300';
  if (status === 'critical' || status === 'error') return 'text-rose-300';
  return 'text-[var(--text-soft)]';
}

function baselineMarkerPercent(slippageInsights?: SlippageInsightsData | null) {
  const baseline = Number(slippageInsights?.baselineBps);
  if (!Number.isFinite(baseline)) return null;
  const min = -20;
  const max = 80;
  const clamped = Math.min(max, Math.max(min, baseline));
  return ((clamped - min) / (max - min)) * 100;
}

export function ExecutionInsightsPanel({ slippageInsights }: ExecutionInsightsPanelProps) {
  const bins = slippageInsights?.bins || [];
  const maxCount = Math.max(1, ...bins.map((bin) => Number(bin.count || 0)));
  const markerPercent = baselineMarkerPercent(slippageInsights);
  const baselineLabel = slippageInsights?.baselineSourceDay
    ? `${slippageInsights?.baselineLabel || '回测基线'} · ${slippageInsights.baselineSourceDay}`
    : slippageInsights?.baselineLabel || '回测基线';

  return (
    <div className={`liquid-glass-thick reading-frame p-5 flex flex-col gap-4 ${toneClass(slippageInsights?.status)}`}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
          <Activity className="w-4 h-4" />
          <span>执行成本</span>
        </div>
        <div className={`text-xs font-medium ${markerTone(slippageInsights?.status)}`}>
          {slippageInsights?.note || '暂无滑点样本'}
        </div>
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <div className="liquid-glass-thin metric-pill tone-coral px-4 py-3">
          <div className="text-xs text-[var(--text-dim)]">样本数</div>
          <div className="text-lg font-semibold">{fmtNum(slippageInsights?.sampleCount, 0)}</div>
        </div>
        <div className="liquid-glass-thin metric-pill tone-sky px-4 py-3">
          <div className="text-xs text-[var(--text-dim)]">实测均值</div>
          <div className="text-lg font-semibold">{fmtBps(slippageInsights?.actualAvgBps)}</div>
        </div>
        <div className="liquid-glass-thin metric-pill tone-amber px-4 py-3">
          <div className="text-xs text-[var(--text-dim)]">P90 / P95</div>
          <div className="text-lg font-semibold">{fmtBps(slippageInsights?.actualP90Bps)} / {fmtBps(slippageInsights?.actualP95Bps)}</div>
        </div>
        <div className="liquid-glass-thin metric-pill tone-plum px-4 py-3">
          <div className="text-xs text-[var(--text-dim)]">回测基线</div>
          <div className="text-lg font-semibold">{fmtBps(slippageInsights?.baselineBps)}</div>
        </div>
      </div>

      <div className="liquid-glass-inset tone-neutral p-4">
        <div className="flex items-center justify-between text-xs text-[var(--text-dim)]">
          <span>实测滑点分布</span>
          <span>{baselineLabel}</span>
        </div>
        <div className="relative mt-4 h-48">
          {[0.25, 0.5, 0.75, 1].map((ratio) => (
            <div
              key={ratio}
              className="absolute inset-x-0 border-t border-dashed border-white/8"
              style={{ bottom: `${ratio * 100}%` }}
            />
          ))}
          {markerPercent !== null ? (
            <div className="absolute inset-y-0 z-10" style={{ left: `${markerPercent}%` }}>
              <div className="absolute -top-2 -translate-x-1/2 rounded-full border border-white/12 bg-white/8 px-2 py-0.5 text-[10px] text-[var(--text-soft)] whitespace-nowrap">
                {fmtBps(slippageInsights?.baselineBps)}
              </div>
              <div className="absolute top-4 bottom-6 left-1/2 w-px -translate-x-1/2 bg-[rgba(255,214,102,0.7)]" />
            </div>
          ) : null}
          <div className="absolute inset-0 flex items-end gap-2 pb-6">
            {bins.map((bin) => {
              const count = Number(bin.count || 0);
              const height = maxCount > 0 ? Math.max((count / maxCount) * 100, count > 0 ? 8 : 0) : 0;
              return (
                <div key={bin.label} className="flex-1 flex flex-col items-center justify-end h-full min-w-0">
                  <div className="glass-column-track">
                    <div
                      className="glass-column-fill"
                      style={{ height: `${height}%` }}
                      title={`${bin.label}: ${count}`}
                    />
                  </div>
                </div>
              );
            })}
          </div>
          <div className="absolute inset-x-0 bottom-0 flex gap-2">
            {bins.map((bin) => (
              <div key={bin.label} className="flex-1 min-w-0 text-center text-[10px] text-[var(--text-dim)]">
                {bin.label}
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 text-xs">
        <div className="liquid-glass-thin list-row tone-pearl px-3 py-2 flex items-center justify-between">
          <span className="text-[var(--text-dim)]">最近成交</span>
          <span className="text-[var(--text-soft)]">{slippageInsights?.lastFillAt ? slippageInsights.lastFillAt.slice(5, 16).replace('T', ' ') : '—'}</span>
        </div>
        <div className="liquid-glass-thin list-row tone-pearl px-3 py-2 flex items-center justify-between">
          <span className="text-[var(--text-dim)]">范围</span>
          <span className="text-[var(--text-soft)]">{fmtBps(slippageInsights?.actualMinBps)} ~ {fmtBps(slippageInsights?.actualMaxBps)}</span>
        </div>
      </div>
    </div>
  );
}
