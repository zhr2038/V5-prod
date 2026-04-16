import { Layers } from 'lucide-react';
import { fmtNum } from '../lib/format';
import type { ShadowMLData } from '../types';

interface ShadowMLPanelProps {
  shadowML?: ShadowMLData | null;
}

export function ShadowMLPanel({ shadowML }: ShadowMLPanelProps) {
  const lift = shadowML?.lift || [];
  const drag = shadowML?.drag || [];

  return (
    <div className="glass-card p-5 flex flex-col gap-4">
      <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
        <Layers className="w-4 h-4" />
        <span>旁路模型叠加（Shadow ML / XGBoost 归因）</span>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="glass-panel p-4">
          <div className="text-xs text-emerald-300 mb-3 font-medium">抬升因子</div>
          <div className="flex flex-col gap-2">
            {lift.slice(0, 6).map((item, i) => (
              <div key={i} className="flex items-center justify-between text-xs">
                <span className="font-medium">{item.symbol}</span>
                <span className="font-mono text-emerald-300">+{fmtNum(item.impact, 3)}</span>
              </div>
            ))}
            {!lift.length && <div className="text-xs text-[var(--text-dim)]">无数据</div>}
          </div>
        </div>

        <div className="glass-panel p-4">
          <div className="text-xs text-rose-300 mb-3 font-medium">压低因子</div>
          <div className="flex flex-col gap-2">
            {drag.slice(0, 6).map((item, i) => (
              <div key={i} className="flex items-center justify-between text-xs">
                <span className="font-medium">{item.symbol}</span>
                <span className="font-mono text-rose-300">{fmtNum(item.impact, 3)}</span>
              </div>
            ))}
            {!drag.length && <div className="text-xs text-[var(--text-dim)]">无数据</div>}
          </div>
        </div>
      </div>

      {shadowML?.summary && (
        <div className="text-xs text-[var(--text-soft)]">{shadowML.summary}</div>
      )}
    </div>
  );
}
