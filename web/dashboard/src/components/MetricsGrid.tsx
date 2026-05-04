import { motion } from 'framer-motion';
import { TrendingUp, Wallet, PieChart, Activity, Target } from 'lucide-react';
import { fmtUsd, fmtPct, fmtUsdt } from '../lib/format';
import type { AccountData, SystemStatus } from '../types';

interface MetricsGridProps {
  account?: AccountData | null;
  systemStatus?: SystemStatus | null;
  focusSymbol?: string;
}

const container = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.06 },
  },
};

const item = {
  hidden: { opacity: 0, y: 12, scale: 0.98 },
  show: { opacity: 1, y: 0, scale: 1, transition: { duration: 0.4 } },
};

export function MetricsGrid({ account, systemStatus, focusSymbol }: MetricsGridProps) {
  const cards = [
    {
      icon: Wallet,
      label: '总权益',
      value: fmtUsd(account?.totalEquity),
      sub: `盈亏 ${fmtUsd(account?.totalPnl)} (${fmtPct(account?.totalPnlPercent)})`,
      tone: (account?.totalPnlPercent || 0) >= 0 ? 'text-emerald-300' : 'text-rose-300',
      surface: 'liquid-glass tone-coral',
    },
    {
      icon: PieChart,
      label: '现金',
      value: fmtUsdt(account?.cash),
      sub: account?.totalEquity
        ? `${((account.cash / account.totalEquity) * 100).toFixed(1)}% 现金`
        : '--',
      tone: 'text-[var(--accent-3)]',
      surface: 'liquid-glass tone-sky',
    },
    {
      icon: TrendingUp,
      label: '在仓市值',
      value: fmtUsd(account?.positionsValue),
      sub: `初始 ${fmtUsd(account?.initialCapital)}`,
      tone: 'text-[var(--accent-2)]',
      surface: 'liquid-glass tone-amber',
    },
    {
      icon: Activity,
      label: '系统健康',
      value: systemStatus?.isRunning ? '运行中' : '停止',
      sub: systemStatus?.errors?.length ? `${systemStatus.errors.length} 个告警` : '无异常',
      tone: systemStatus?.isRunning ? 'text-emerald-300' : 'text-rose-300',
      surface: 'liquid-glass tone-sage',
    },
    {
      icon: Target,
      label: '当前焦点',
      value: focusSymbol || '—',
      sub: '持仓聚焦',
      tone: 'text-[var(--accent)]',
      surface: 'liquid-glass tone-plum',
    },
  ];

  return (
    <div className="relative z-10 px-6 pb-6">
      <motion.div
        variants={container}
        initial="hidden"
        animate="show"
        className="max-w-[1780px] mx-auto grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3 lg:gap-4"
      >
        {cards.map((card, index) => (
          <motion.div
            key={card.label}
            variants={item}
            className={`${card.surface} ${index === cards.length - 1 ? 'col-span-2 md:col-span-1' : ''} p-4 sm:p-5 flex min-w-0 flex-col gap-2`}
          >
            <div className="flex min-w-0 items-center gap-2 text-[var(--text-dim)] text-sm">
              <card.icon className="w-4 h-4" />
              <span className="truncate">{card.label}</span>
            </div>
            <div className="text-xl sm:text-2xl font-semibold tracking-tight">{card.value}</div>
            <div className={`text-xs sm:text-sm font-medium ${card.tone}`}>{card.sub}</div>
          </motion.div>
        ))}
      </motion.div>
    </div>
  );
}
