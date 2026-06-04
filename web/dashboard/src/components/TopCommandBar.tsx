import { Maximize2, RefreshCw, Search, ShieldCheck } from 'lucide-react';
import { motion, useReducedMotion } from 'framer-motion';
import { statusLabels } from '../lib/format';
import type { HealthData, SystemStatus } from '../types';

interface TopCommandBarProps {
  systemStatus?: SystemStatus | null;
  health?: HealthData | null;
  updateTime?: string;
  loading?: boolean;
  onRefresh?: () => void;
  onSymbolSearch?: (symbol: string) => void;
}

function modeLabel(mode?: string) {
  if (!mode) return '状态未知';
  if (mode === 'live') return '实盘运行中';
  if (mode === 'dry_run') return '演练模式';
  if (mode === 'paper') return 'Paper';
  return mode;
}

function normalizeSearchSymbol(value: string) {
  const text = value.trim().toUpperCase().replace(/\s+/g, '');
  if (!text) return '';
  if (text.includes('/USDT') || text.includes('-USDT')) return text.replace('/USDT', '-USDT');
  return `${text.replace(/USDT$/, '')}-USDT`;
}

function toggleFullscreen() {
  if (document.fullscreenElement) {
    void document.exitFullscreen();
    return;
  }
  void document.documentElement.requestFullscreen?.();
}

export function TopCommandBar({
  systemStatus,
  health,
  updateTime,
  loading = false,
  onRefresh,
  onSymbolSearch,
}: TopCommandBarProps) {
  const reduceMotion = useReducedMotion();
  const isRunning = Boolean(systemStatus?.isRunning);

  return (
    <header className="top-command-bar">
      <div className="window-dots" aria-hidden="true">
        <span />
        <span />
        <span />
      </div>
      <div className="top-command-brand">
        <div className="v5-mark" aria-hidden="true">V5</div>
        <div>
          <div className="top-command-title">V5 生产交易看板</div>
          <div className="top-command-subtitle">OKX spot · quant-lab shadow</div>
        </div>
        <span className="run-state-pill" data-state={isRunning ? 'on' : 'off'}>
          <ShieldCheck className="h-3.5 w-3.5" />
          {modeLabel(systemStatus?.mode)}
        </span>
      </div>

      <div className="top-command-meta">
        <span>更新时间 {updateTime || '--'}</span>
        <span>自动刷新 30s</span>
        <motion.span
          className="live-dot"
          animate={reduceMotion || !isRunning ? undefined : { opacity: [0.45, 1, 0.45], scale: [0.9, 1.08, 0.9] }}
          transition={{ duration: 2.4, repeat: Infinity, ease: 'easeInOut' }}
        />
      </div>

      <div className="top-command-actions">
        <form
          className="command-search"
          onSubmit={(event) => {
            event.preventDefault();
            const data = new FormData(event.currentTarget);
            const symbol = normalizeSearchSymbol(String(data.get('symbol') || ''));
            if (symbol) onSymbolSearch?.(symbol);
          }}
        >
          <Search className="h-4 w-4" />
          <input name="symbol" aria-label="搜索币种" placeholder="输入币种切换K线，如 BNB" />
        </form>
        <button className="icon-command" type="button" aria-label="全屏" onClick={toggleFullscreen} title="切换全屏">
          <Maximize2 className="h-4 w-4" />
        </button>
        <button className="icon-command" type="button" aria-label="刷新" onClick={onRefresh} title="立即刷新数据">
          <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
        </button>
        <div className="operator-chip">
          <span className="operator-avatar">tr</span>
          <span>
            trader_v5
            <small>{statusLabels[health?.status || ''] || health?.status || '--'}</small>
          </span>
        </div>
      </div>
    </header>
  );
}
