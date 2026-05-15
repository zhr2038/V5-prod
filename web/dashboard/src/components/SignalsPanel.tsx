import { Signal } from 'lucide-react';
import type { DecisionAuditData } from '../types';

interface SignalsPanelProps {
  decisionAudit?: DecisionAuditData | null;
}

export function SignalsPanel({ decisionAudit }: SignalsPanelProps) {
  const strategies = decisionAudit?.strategy_signals || [];
  const counts = decisionAudit?.counts || {};
  const runId = decisionAudit?.run_id;

  return (
    <div className="liquid-glass reading-frame p-4 flex flex-col gap-3 tone-plum">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
          <Signal className="w-4 h-4" />
          <span>策略信号</span>
        </div>
        <div className="text-xs text-[var(--text-dim)]">{runId ? `运行 ${runId}` : '等待策略信号...'}</div>
      </div>

      <div className="grid grid-cols-2 gap-2 text-xs">
        <div className="liquid-glass-thin list-row tone-sage p-2 text-center">
          <div className="text-xs text-[var(--text-dim)]">入池</div>
          <div className="font-semibold">{counts.selected || 0}</div>
        </div>
        <div className="liquid-glass-thin list-row tone-sky p-2 text-center">
          <div className="text-xs text-[var(--text-dim)]">订单</div>
          <div className="font-semibold">{(counts.orders_rebalance || 0) + (counts.orders_exit || 0)}</div>
        </div>
      </div>

      <div className="flex max-h-48 flex-col gap-2 overflow-auto pr-1">
        {strategies.map((s, idx) => (
          <div key={idx} className="liquid-glass-thin list-row tone-pearl px-2 py-2 text-xs">
            <div className="flex items-center justify-between gap-2">
              <span className="font-medium truncate">{s.strategy || '策略'}</span>
              {typeof s.allocation === 'number' && (
                <span className="text-[var(--text-dim)]">配置 {(s.allocation * 100).toFixed(0)}%</span>
              )}
            </div>
            <div className="mt-1 text-[var(--text-soft)]">
              {s.total_signals || 0} 个信号 · 买 {s.buy_signals || 0} / 卖 {s.sell_signals || 0}
            </div>
          </div>
        ))}
        {!strategies.length && (
          <div className="text-center text-xs text-[var(--text-dim)] py-2">当前没有策略信号</div>
        )}
      </div>
    </div>
  );
}
