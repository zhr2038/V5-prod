import { Layers } from 'lucide-react';
import { fmtNum } from '../lib/format';
import type { ShadowMLData } from '../types';

interface ShadowMLPanelProps {
  shadowML?: ShadowMLData | null;
}

export function ShadowMLPanel({ shadowML }: ShadowMLPanelProps) {
  if (!shadowML || !shadowML.available) {
    return (
      <div className="material-surface material-reading tone-neutral reading-frame p-5 flex flex-col gap-4">
        <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
          <Layers className="w-4 h-4" />
          <span>旁路模型叠加（Shadow ML / XGBoost 归因）</span>
        </div>
        <div className="text-sm text-[var(--text-dim)]">
          {shadowML?.error || '未找到旁路调优版 XGBoost 工作区'}
        </div>
      </div>
    );
  }

  const ml = shadowML.ml_signal_overview || {};
  const lastStep = ml.last_step || {};
  const promoted = lastStep.promoted_symbols || [];
  const suppressed = lastStep.suppressed_symbols || [];
  const rolling = ml.rolling_24h || {};
  const impactText: Record<string, string> = {
    positive: '正面',
    negative: '负面',
    mixed: '中性',
    insufficient: '样本不足',
  };

  return (
    <div className="material-surface material-reading tone-neutral reading-frame p-5 flex flex-col gap-4">
      <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
        <Layers className="w-4 h-4" />
        <span>旁路模型叠加（Shadow ML / XGBoost 归因）</span>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div className="material-surface material-clear clear-control tone-sky p-3 text-center">
          <div className="text-xs text-[var(--text-dim)]">状态</div>
          <div className="text-lg font-semibold">{impactText[ml.impact_status || 'insufficient'] || ml.impact_status}</div>
        </div>
        <div className="material-surface material-clear clear-control tone-sage p-3 text-center">
          <div className="text-xs text-[var(--text-dim)]">24h 前N影响</div>
          <div className="text-lg font-semibold">
            {rolling.topn_delta_mean_bps != null ? `${fmtNum(rolling.topn_delta_mean_bps, 1)} 基点` : '--'}
          </div>
        </div>
        <div className="material-surface material-clear clear-control tone-coral p-3 text-center">
          <div className="text-xs text-[var(--text-dim)]">本轮</div>
          <div className="text-lg font-semibold">
            {lastStep.delta_bps != null ? `${fmtNum(lastStep.delta_bps, 1)} 基点` : '--'}
          </div>
        </div>
        <div className="material-surface material-clear clear-control tone-amber p-3 text-center">
          <div className="text-xs text-[var(--text-dim)]">覆盖</div>
          <div className="text-lg font-semibold">{ml.coverage_count || ml.active_symbols || 0} 币</div>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="material-surface material-inset tone-sage p-4">
          <div className="text-xs text-emerald-300 mb-3 font-medium">抬升最多</div>
          <div className="flex flex-col gap-2">
            {promoted.slice(0, 6).map((item, i) => (
              <div key={i} className="flex items-center justify-between text-xs">
                <span className="font-medium">{item.symbol.replace('-USDT', '')}</span>
                <span className="font-mono text-emerald-300">
                  {item.rank_delta != null ? `+${item.rank_delta}` : '--'} 名
                </span>
              </div>
            ))}
            {!promoted.length && <div className="text-xs text-[var(--text-dim)]">这一轮没有明显被抬升的币</div>}
          </div>
        </div>

        <div className="material-surface material-inset tone-rose p-4">
          <div className="text-xs text-rose-300 mb-3 font-medium">压低最多</div>
          <div className="flex flex-col gap-2">
            {suppressed.slice(0, 6).map((item, i) => (
              <div key={i} className="flex items-center justify-between text-xs">
                <span className="font-medium">{item.symbol.replace('-USDT', '')}</span>
                <span className="font-mono text-rose-300">
                  {item.rank_delta != null ? `${item.rank_delta}` : '--'} 名
                </span>
              </div>
            ))}
            {!suppressed.length && <div className="text-xs text-[var(--text-dim)]">这一轮没有明显被压低的币</div>}
          </div>
        </div>
      </div>
    </div>
  );
}
