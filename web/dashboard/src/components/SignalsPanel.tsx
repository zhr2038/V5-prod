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
    <div className="glass-card p-5 flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
          <Signal className="w-4 h-4" />
          <span>策略信号</span>
        </div>
        <div className="text-xs text-[var(--text-dim)]">{runId ? `运行 ${runId}` : '等待策略信号...'}</div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="glass-panel p-3 text-center">
          <div className="text-xs text-[var(--text-dim)]">入池</div>
          <div className="text-xl font-semibold">{counts.selected || 0}</div>
        </div>
        <div className="glass-panel p-3 text-center">
          <div className="text-xs text-[var(--text-dim)]">订单</div>
          <div className="text-xl font-semibold">{(counts.orders_rebalance || 0) + (counts.orders_exit || 0)}</div>
        </div>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {strategies.map((s, idx) => (
          <div key={idx} className="glass-panel p-3 flex flex-col gap-1">
            <div className="text-sm font-medium">{s.strategy || '策略'}</div>
            <div className="text-xs text-[var(--text-soft)]">
              {s.total_signals || 0} 个信号 · 买 {s.buy_signals || 0} / 卖 {s.sell_signals || 0}
            </div>
            {typeof s.allocation === 'number' && (
              <div className="text-xs text-[var(--text-dim)]">配置 {(s.allocation * 100).toFixed(0)}%</div>
            )}
          </div>
        ))}
        {!strategies.length && (
          <div className="col-span-full text-center text-sm text-[var(--text-dim)] py-4">当前没有策略信号</div>
        )}
      </div>
    </div>
  );
}
