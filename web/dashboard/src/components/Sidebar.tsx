import { Clock, Route, BarChart3, Receipt, HeartPulse } from 'lucide-react';
import { fmtUsd, fmtNum, statusLabels, sideLabels } from '../lib/format';
import type { TimerData, AlphaScore, Trade, HealthData, DecisionAuditData } from '../types';

interface SidebarProps {
  timers?: { timers: TimerData[] } | null;
  alphaScores?: AlphaScore[];
  trades?: Trade[];
  health?: HealthData | null;
  decisionAudit?: DecisionAuditData | null;
}

function Section({
  icon: Icon,
  title,
  children,
  tone,
}: {
  icon: any;
  title: string;
  children: React.ReactNode;
  tone?: string;
}) {
  return (
    <div className={`material-surface material-reading reading-frame p-4 flex flex-col gap-3 ${tone || 'tone-smoke'}`}>
      <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
        <Icon className="w-4 h-4" />
        <span>{title}</span>
      </div>
      {children}
    </div>
  );
}

export function Sidebar({ timers, alphaScores = [], trades = [], health, decisionAudit }: SidebarProps) {
  const exec = decisionAudit?.execution_summary || {};
  const rejected = decisionAudit?.rejected_summary || {};
  const orders = decisionAudit?.orders || [];

  return (
    <div className="flex flex-col gap-4">
      <Section icon={Clock} title="服务与定时器" tone="tone-sky">
        <div className="flex flex-col gap-2 max-h-48 overflow-auto pr-1">
          {(timers?.timers || []).map((t) => (
            <div key={t.name} className="material-surface material-clear clear-control tone-pearl flex items-center justify-between text-xs px-2 py-2">
              <span className="text-[var(--text-soft)]">{t.name}</span>
              <span
                className={`px-1.5 py-0.5 rounded border ${
                  t.active
                    ? 'bg-emerald-500/15 text-emerald-300 border-emerald-400/25'
                    : 'bg-white/5 text-[var(--text-dim)] border-white/10'
                }`}
              >
                {t.active ? '运行中' : '停止'}
              </span>
            </div>
          ))}
          {!timers?.timers?.length && <div className="text-xs text-[var(--text-dim)]">无定时器数据</div>}
        </div>
      </Section>

      <Section icon={Route} title="执行路径" tone="tone-sage">
        <div className="grid grid-cols-2 gap-2 text-xs">
          <div className="material-surface material-clear clear-control tone-sky p-2 text-center">
            <div className="text-[var(--text-dim)]">已成交</div>
            <div className="font-semibold">{exec.filled || 0}</div>
          </div>
          <div className="material-surface material-clear clear-control tone-coral p-2 text-center">
            <div className="text-[var(--text-dim)]">已选订单</div>
            <div className="font-semibold">{exec.submitted || orders.length || 0}</div>
          </div>
          <div className="material-surface material-clear clear-control tone-amber p-2 text-center">
            <div className="text-[var(--text-dim)]">被拦截</div>
            <div className="font-semibold">{rejected.total || 0}</div>
          </div>
          <div className="material-surface material-clear clear-control tone-plum p-2 text-center">
            <div className="text-[var(--text-dim)]">本轮订单</div>
            <div className="font-semibold">{orders.length || 0}</div>
          </div>
        </div>
      </Section>

      <Section icon={BarChart3} title="因子排序" tone="tone-amber">
        <div className="flex flex-col gap-2 max-h-56 overflow-auto pr-1">
          {alphaScores.slice(0, 10).map((s) => (
            <div key={s.symbol} className="flex items-center gap-2 text-xs">
              <span className="w-14 font-medium truncate">{s.symbol.replace('-USDT', '')}</span>
              <div className="flex-1 h-1.5 bg-white/10 rounded-full overflow-hidden relative">
                <div
                  className="absolute inset-y-0 left-1/2 bg-[var(--accent)] rounded-full"
                  style={{
                    width: `${Math.min(50, Math.abs(s.score) * 25)}%`,
                    marginLeft: s.score >= 0 ? 0 : `-${Math.min(50, Math.abs(s.score) * 25)}%`,
                  }}
                />
              </div>
              <span className="w-10 text-right font-mono">{fmtNum(s.score, 2)}</span>
            </div>
          ))}
          {!alphaScores.length && <div className="text-xs text-[var(--text-dim)]">无评分数据</div>}
        </div>
      </Section>

      <Section icon={Receipt} title="最近成交" tone="tone-coral">
        <div className="flex flex-col gap-2">
          {trades.slice(0, 6).map((t, i) => (
            <div key={i} className="material-surface material-clear clear-control tone-pearl flex items-center justify-between text-xs px-2 py-2">
              <div className="flex items-center gap-2">
                <span className="font-medium">{t.symbol.replace('-USDT', '')}</span>
                <span className={`px-1.5 rounded border ${t.side === 'buy' ? 'text-emerald-300 border-emerald-400/25' : 'text-rose-300 border-rose-400/25'}`}>
                  {sideLabels[t.side] || t.side}
                </span>
              </div>
              <div className="text-right">
                <div className="font-mono">{fmtUsd(t.value)}</div>
                <div className="text-[var(--text-dim)]">{t.timestamp ? t.timestamp.slice(5, 16).replace('T', ' ') : ''}</div>
              </div>
            </div>
          ))}
          {!trades.length && <div className="text-xs text-[var(--text-dim)]">暂无成交</div>}
        </div>
      </Section>

      <Section icon={HeartPulse} title="健康检查" tone="tone-plum">
        <div className="flex flex-col gap-2">
          <div className="flex items-center gap-2">
            <span
              className={`w-2 h-2 rounded-full ${
                health?.status === 'healthy'
                  ? 'bg-emerald-400'
                  : health?.status === 'warning'
                  ? 'bg-amber-400'
                  : 'bg-rose-400'
              }`}
            />
            <span className="text-sm font-medium">{statusLabels[health?.status || ''] || health?.status || '—'}</span>
            <span className="ml-auto text-xs text-[var(--text-dim)]">{health?.last_update?.slice(5, 16) || ''}</span>
          </div>
          <div className="flex flex-col gap-1.5">
            {(health?.checks || []).map((c, i) => (
              <div key={i} className="material-surface material-clear clear-control tone-smoke flex items-center justify-between text-xs px-2 py-1.5">
                <span className="text-[var(--text-soft)]">{c.name}</span>
                <span
                  className={`${
                    c.status === 'healthy'
                      ? 'text-emerald-300'
                      : c.status === 'warning'
                      ? 'text-amber-300'
                      : 'text-rose-300'
                  }`}
                >
                  {c.detail}
                </span>
              </div>
            ))}
          </div>
        </div>
      </Section>
    </div>
  );
}
