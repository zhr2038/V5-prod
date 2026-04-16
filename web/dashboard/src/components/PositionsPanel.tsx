import { useEffect, useMemo, useState } from 'react';
// import { motion } from 'framer-motion';
import { CandlestickChart, ChevronLeft, ChevronRight } from 'lucide-react';
import { fmtUsd, fmtNum, fmtPct } from '../lib/format';
import { api } from '../api';
import type { Position, KlineData } from '../types';

interface PositionsPanelProps {
  positions?: Position[];
  account?: import('../types').AccountData | null;
}

const timeframes = [
  { key: '1h', label: '1H' },
  { key: '4h', label: '4H' },
  { key: '1d', label: '1D' },
];

function CandlestickSvg({ data }: { data: KlineData[] }) {
  if (!data.length) {
    return (
      <div className="flex items-center justify-center h-full text-[var(--text-dim)] text-sm">
        暂无数据
      </div>
    );
  }
  const w = 560;
  const h = 200;
  const pad = { t: 10, r: 10, b: 20, l: 10 };
  const chartW = w - pad.l - pad.r;
  const chartH = h - pad.t - pad.b;

  const highs = data.map((d) => d.high);
  const lows = data.map((d) => d.low);
  const max = Math.max(...highs);
  const min = Math.min(...lows);
  const range = Math.max(max - min, 1e-9);

  const band = data[data.length - 1];
  const lastClose = band?.close ?? 0;

  const x = (i: number) => pad.l + (i / (data.length - 1 || 1)) * chartW;
  const y = (v: number) => pad.t + (1 - (v - min) / range) * chartH;

  const bodyWidth = Math.max(2, chartW / data.length * 0.6);

  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full h-full">
      <defs>
        <linearGradient id="area" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="rgba(127,255,212,0.25)" />
          <stop offset="100%" stopColor="rgba(127,255,212,0)" />
        </linearGradient>
      </defs>
      {/* Grid */}
      {[0, 0.25, 0.5, 0.75, 1].map((t) => (
        <line
          key={t}
          x1={pad.l}
          x2={w - pad.r}
          y1={pad.t + t * chartH}
          y2={pad.t + t * chartH}
          stroke="rgba(255,255,255,0.06)"
          strokeDasharray="4 4"
        />
      ))}
      {/* Area under close */}
      <path
        d={[
          `M ${x(0)} ${y(data[0].close)}`,
          ...data.slice(1).map((d, i) => `L ${x(i + 1)} ${y(d.close)}`),
          `L ${x(data.length - 1)} ${h - pad.b}`,
          `L ${x(0)} ${h - pad.b}`,
          'Z',
        ].join(' ')}
        fill="url(#area)"
      />
      {/* Candlesticks */}
      {data.map((d, i) => {
        const cx = x(i);
        const yO = y(d.open);
        const yC = y(d.close);
        const yH = y(d.high);
        const yL = y(d.low);
        const up = d.close >= d.open;
        const color = up ? '#34d399' : '#fb7185';
        const bodyH = Math.max(1, Math.abs(yC - yO));
        const bodyY = Math.min(yO, yC);
        return (
          <g key={i}>
            <line x1={cx} x2={cx} y1={yH} y2={yL} stroke={color} strokeWidth={1} />
            <rect
              x={cx - bodyWidth / 2}
              y={bodyY}
              width={bodyWidth}
              height={bodyH}
              fill={color}
              rx={1}
            />
          </g>
        );
      })}
      {/* Last price dashed line */}
      <line
        x1={pad.l}
        x2={w - pad.r}
        y1={y(lastClose)}
        y2={y(lastClose)}
        stroke="#8dc4ff"
        strokeWidth={1}
        strokeDasharray="6 4"
      />
    </svg>
  );
}

export function PositionsPanel({ positions = [] }: PositionsPanelProps) {
  const sorted = useMemo(
    () => [...positions].sort((a, b) => b.value - a.value),
    [positions]
  );
  const [index, setIndex] = useState(0);
  const [tf, setTf] = useState('1h');
  const [kline, setKline] = useState<KlineData[] | null>(null);

  const spotlight = sorted[index] || null;

  useEffect(() => {
    if (!spotlight) return;
    let mounted = true;
    const klineSymbol = String(spotlight.symbol || '')
      .replace('/USDT', '')
      .replace('-USDT', '');
    api.positionKline(klineSymbol, tf).then((data) => {
      if (mounted) setKline(data);
    });
    return () => {
      mounted = false;
    };
  }, [spotlight?.symbol, tf]);

  return (
    <div className="material-surface material-regular tone-sky reading-frame p-5 flex flex-col gap-5">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
          <CandlestickChart className="w-4 h-4" />
          <span>持仓聚焦</span>
        </div>
        {spotlight && (
          <div className="flex items-center gap-2">
            <button
              className="p-1 rounded-lg hover:bg-white/10 disabled:opacity-30"
              onClick={() => setIndex((i) => Math.max(0, i - 1))}
              disabled={index === 0}
            >
              <ChevronLeft className="w-4 h-4" />
            </button>
            <span className="text-sm font-medium min-w-[5rem] text-center">
              {spotlight.symbol.replace('-USDT', '')}
            </span>
            <button
              className="p-1 rounded-lg hover:bg-white/10 disabled:opacity-30"
              onClick={() => setIndex((i) => Math.min(sorted.length - 1, i + 1))}
              disabled={index >= sorted.length - 1}
            >
              <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        )}
      </div>

      {spotlight ? (
        <>
          <div className="grid grid-cols-1 xl:grid-cols-[1fr_auto] gap-4 items-start">
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              <div className="material-surface material-clear clear-control metric-pill tone-sky px-4 py-3">
                <div className="text-xs text-[var(--text-dim)]">市值</div>
                <div className="text-lg font-semibold">{fmtUsd(spotlight.value)}</div>
              </div>
              <div className="material-surface material-clear clear-control metric-pill tone-sage px-4 py-3">
                <div className="text-xs text-[var(--text-dim)]">数量</div>
                <div className="text-lg font-mono">{fmtNum(spotlight.qty, 4)}</div>
              </div>
              <div className="material-surface material-clear clear-control metric-pill tone-amber px-4 py-3">
                <div className="text-xs text-[var(--text-dim)]">均价</div>
                <div className="text-lg font-mono">{fmtUsd(spotlight.avgPrice)}</div>
              </div>
              <div className="material-surface material-clear clear-control metric-pill tone-coral px-4 py-3">
                <div className="text-xs text-[var(--text-dim)]">现价</div>
                <div className="text-lg font-mono">{fmtUsd(spotlight.currentPrice)}</div>
              </div>
            </div>
            <div className={`xl:text-right px-2 pt-2 ${spotlight.pnlPercent >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
              <div className="text-2xl font-semibold">{fmtPct(spotlight.pnlPercent)}</div>
              <div className="text-sm">{fmtUsd(spotlight.pnl)}</div>
            </div>
          </div>

          <div className="material-surface material-reading reading-surface tone-neutral h-56 w-full p-2">
            <CandlestickSvg data={kline || []} />
          </div>

          <div className="flex gap-2">
            {timeframes.map((t) => (
              <button
                key={t.key}
                onClick={() => setTf(t.key)}
                className={`material-surface material-clear clear-chip control-pill px-3 py-1.5 text-xs transition ${
                  tf === t.key
                    ? 'tone-sky text-white border-white/20'
                    : 'tone-pearl text-[var(--text-dim)] surface-lift'
                }`}
              >
                {t.label}
              </button>
            ))}
          </div>
        </>
      ) : (
        <div className="h-56 flex items-center justify-center text-[var(--text-dim)] text-sm">
          当前无持仓
        </div>
      )}

      <div className="mt-2">
        <div className="text-sm text-[var(--text-dim)] mb-3">持仓清单</div>
        <div className="material-surface material-reading reading-block tone-neutral overflow-x-auto px-3 py-2">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-[var(--text-dim)] text-xs border-b border-white/10">
                <th className="text-left py-2 font-medium">币种</th>
                <th className="text-right py-2 font-medium">市值</th>
                <th className="text-right py-2 font-medium">盈亏</th>
                <th className="text-right py-2 font-medium hidden sm:table-cell">数量</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((pos) => (
                <tr
                  key={pos.symbol}
                  className="border-b border-white/5 hover:bg-white/5 transition cursor-pointer"
                  onClick={() => {
                    const idx = sorted.findIndex((p) => p.symbol === pos.symbol);
                    if (idx >= 0) setIndex(idx);
                  }}
                >
                  <td className="py-2.5 font-medium">{pos.symbol.replace('-USDT', '')}</td>
                  <td className="py-2.5 text-right font-mono">{fmtUsd(pos.value)}</td>
                  <td
                    className={`py-2.5 text-right font-mono ${
                      pos.pnlPercent >= 0 ? 'text-emerald-300' : 'text-rose-300'
                    }`}
                  >
                    {fmtPct(pos.pnlPercent)}
                  </td>
                  <td className="py-2.5 text-right font-mono hidden sm:table-cell">{fmtNum(pos.qty, 4)}</td>
                </tr>
              ))}
              {!sorted.length && (
                <tr>
                  <td colSpan={4} className="py-6 text-center text-[var(--text-dim)]">
                    暂无持仓
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
