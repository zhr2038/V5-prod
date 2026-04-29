import { useEffect, useMemo, useState } from 'react';
// import { motion } from 'framer-motion';
import { CandlestickChart, ChevronLeft, ChevronRight } from 'lucide-react';
import { fmtUsd, fmtNum, fmtPct, sideLabels } from '../lib/format';
import { api } from '../api';
import { useInterval } from '../hooks/useInterval';
import type { Position, KlineData, PositionKlinePayload, Trade } from '../types';

interface PositionsPanelProps {
  positions?: Position[];
  trades?: Trade[];
  account?: import('../types').AccountData | null;
}

const timeframes = [
  { key: '1h', label: '1H' },
  { key: '4h', label: '4H' },
  { key: '1d', label: '1D' },
];

function candleTimestamp(candle: KlineData) {
  const rawNumeric = Number(candle.timestamp ?? candle.ts ?? 0);
  if (Number.isFinite(rawNumeric) && rawNumeric > 0) {
    return rawNumeric > 1e12 ? rawNumeric : rawNumeric * 1000;
  }
  const rawText = String(candle.time || '').trim();
  if (!rawText) return null;
  const normalized = rawText.includes('T') ? rawText : rawText.replace(' ', 'T');
  const parsed = Date.parse(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatChartTime(ts: number | null, timeframe: string) {
  if (!ts) return '--';
  const date = new Date(ts);
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  const hh = String(date.getHours()).padStart(2, '0');
  if (timeframe === '1h') return `${hh}:00`;
  if (timeframe === '4h') return `${mm}-${dd} ${hh}:00`;
  return `${mm}-${dd}`;
}

function formatAxisPrice(value: number) {
  const abs = Math.abs(value);
  if (abs >= 1000) return value.toFixed(0);
  if (abs >= 100) return value.toFixed(2);
  if (abs >= 1) return value.toFixed(3);
  return value.toFixed(4);
}

function formatCompactVolume(value: number) {
  const abs = Math.abs(Number(value) || 0);
  if (!abs) return '--';
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(abs >= 10_000_000 ? 1 : 2)}M`;
  if (abs >= 1_000) return `${(value / 1_000).toFixed(abs >= 10_000 ? 1 : 2)}K`;
  return fmtNum(value, 2);
}

function movingAverage(data: KlineData[], period: number) {
  return data.map((_, index) => {
    if (index < period - 1) return null;
    const window = data.slice(index - period + 1, index + 1);
    const total = window.reduce((sum, candle) => sum + candle.close, 0);
    return total / period;
  });
}

function buildSeriesPath(
  values: Array<number | null>,
  x: (index: number) => number,
  y: (value: number) => number
) {
  let path = '';
  let started = false;
  values.forEach((value, index) => {
    if (!Number.isFinite(value)) return;
    const point = `${x(index)} ${y(Number(value))}`;
    path += `${started ? ' L ' : 'M '}${point}`;
    started = true;
  });
  return path;
}

function CandlestickSvg({
  data,
  timeframe,
  referencePrice,
  referenceLabel,
}: {
  data: KlineData[];
  timeframe: string;
  referencePrice?: number;
  referenceLabel?: string;
}) {
  if (!data.length) {
    return (
      <div className="flex items-center justify-center h-full text-[var(--text-dim)] text-sm">
        暂无数据
      </div>
    );
  }
  const w = 760;
  const h = 292;
  const pad = { t: 14, r: 60, b: 34, l: 14 };
  const volumeHeight = 58;
  const gap = 12;
  const chartW = w - pad.l - pad.r;
  const priceH = h - pad.t - pad.b - volumeHeight - gap;
  const volumeTop = pad.t + priceH + gap;
  const volumeBaseY = volumeTop + volumeHeight;

  const highs = data.map((item) => item.high);
  const lows = data.map((item) => item.low);
  const lineAnchors = [referencePrice || null, ...movingAverage(data, 7), ...movingAverage(data, 20)].filter(
    (value): value is number => Number.isFinite(value)
  );
  const rawMax = Math.max(...highs, ...(lineAnchors.length ? lineAnchors : [highs[0]]));
  const rawMin = Math.min(...lows, ...(lineAnchors.length ? lineAnchors : [lows[0]]));
  const pricePadding = Math.max((rawMax - rawMin) * 0.08, rawMax * 0.002, 0.01);
  const max = rawMax + pricePadding;
  const min = Math.max(0, rawMin - pricePadding);
  const range = Math.max(max - min, 1e-9);

  const lastCandle = data[data.length - 1];
  const lastClose = lastCandle?.close ?? 0;
  const maxVolume = Math.max(...data.map((item) => Number(item.volume || 0)), 1);
  const ma7 = movingAverage(data, 7);
  const ma20 = movingAverage(data, 20);

  const x = (index: number) => pad.l + (index / Math.max(data.length - 1, 1)) * chartW;
  const y = (value: number) => pad.t + (1 - (value - min) / range) * priceH;
  const yVol = (value: number) => volumeTop + (1 - value / maxVolume) * volumeHeight;
  const candleBand = chartW / Math.max(data.length, 1);
  const bodyWidth = Math.max(3, Math.min(11, candleBand * 0.62));
  const priceTicks = Array.from({ length: 5 }, (_, index) => max - (range / 4) * index);
  const timeIndices = Array.from(new Set([0, Math.floor((data.length - 1) * 0.33), Math.floor((data.length - 1) * 0.66), data.length - 1]))
    .filter((index) => index >= 0);
  const highestIndex = highs.findIndex((value) => value === Math.max(...highs));
  const lowestIndex = lows.findIndex((value) => value === Math.min(...lows));
  const lastPriceLabelY = y(lastClose);
  const referencePriceVisible =
    Number.isFinite(referencePrice) &&
    Number(referencePrice) > 0 &&
    Number(referencePrice) <= max &&
    Number(referencePrice) >= min;
  const ma7Path = buildSeriesPath(ma7, x, y);
  const ma20Path = buildSeriesPath(ma20, x, y);

  return (
    <svg viewBox={`0 0 ${w} ${h}`} className="w-full h-full">
      <defs>
        <linearGradient id="volumeFill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="rgba(126, 236, 205, 0.62)" />
          <stop offset="100%" stopColor="rgba(126, 236, 205, 0.12)" />
        </linearGradient>
      </defs>
      {priceTicks.map((tick) => (
        <line
          key={tick}
          x1={pad.l}
          x2={w - pad.r}
          y1={y(tick)}
          y2={y(tick)}
          stroke="rgba(255,255,255,0.06)"
          strokeDasharray="4 4"
        />
      ))}
      {timeIndices.map((index) => (
        <line
          key={`v-${index}`}
          x1={x(index)}
          x2={x(index)}
          y1={pad.t}
          y2={volumeBaseY}
          stroke="rgba(255,255,255,0.035)"
        />
      ))}
      {data.map((candle, index) => {
        const cx = x(index);
        const volume = Number(candle.volume || 0);
        const volTop = yVol(volume);
        return (
          <rect
            key={`vol-${index}`}
            x={cx - bodyWidth / 2}
            y={volTop}
            width={bodyWidth}
            height={Math.max(1, volumeBaseY - volTop)}
            fill="url(#volumeFill)"
            opacity={0.88}
            rx={1.2}
          />
        );
      })}
      {ma20Path ? (
        <path
          d={ma20Path}
          fill="none"
          stroke="rgba(255, 205, 120, 0.9)"
          strokeWidth={1.5}
          strokeLinejoin="round"
          strokeLinecap="round"
        />
      ) : null}
      {ma7Path ? (
        <path
          d={ma7Path}
          fill="none"
          stroke="rgba(141, 196, 255, 0.95)"
          strokeWidth={1.5}
          strokeLinejoin="round"
          strokeLinecap="round"
        />
      ) : null}
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
      <line
        x1={pad.l}
        x2={w - pad.r}
        y1={lastPriceLabelY}
        y2={lastPriceLabelY}
        stroke="#8dc4ff"
        strokeWidth={1}
        strokeDasharray="6 4"
      />
      {referencePriceVisible ? (
        <line
          x1={pad.l}
          x2={w - pad.r}
          y1={y(Number(referencePrice))}
          y2={y(Number(referencePrice))}
          stroke="rgba(255, 205, 120, 0.75)"
          strokeWidth={1}
          strokeDasharray="3 4"
        />
      ) : null}
      {highestIndex >= 0 ? (
        <>
          <line
            x1={x(highestIndex)}
            x2={Math.min(w - pad.r - 36, x(highestIndex) + 28)}
            y1={y(data[highestIndex].high)}
            y2={y(data[highestIndex].high)}
            stroke="rgba(255,255,255,0.32)"
            strokeWidth={1}
          />
          <text
            x={Math.min(w - pad.r - 32, x(highestIndex) + 32)}
            y={y(data[highestIndex].high) - 4}
            fill="rgba(255,255,255,0.76)"
            fontSize="11"
            textAnchor="start"
          >
            H {formatAxisPrice(data[highestIndex].high)}
          </text>
        </>
      ) : null}
      {lowestIndex >= 0 ? (
        <>
          <line
            x1={x(lowestIndex)}
            x2={Math.min(w - pad.r - 36, x(lowestIndex) + 28)}
            y1={y(data[lowestIndex].low)}
            y2={y(data[lowestIndex].low)}
            stroke="rgba(255,255,255,0.28)"
            strokeWidth={1}
          />
          <text
            x={Math.min(w - pad.r - 32, x(lowestIndex) + 32)}
            y={y(data[lowestIndex].low) + 13}
            fill="rgba(255,255,255,0.7)"
            fontSize="11"
            textAnchor="start"
          >
            L {formatAxisPrice(data[lowestIndex].low)}
          </text>
        </>
      ) : null}
      {priceTicks.map((tick) => (
        <text
          key={`price-${tick}`}
          x={w - pad.r + 6}
          y={y(tick) + 4}
          fill="rgba(210,218,232,0.76)"
          fontSize="11"
          textAnchor="start"
        >
          {formatAxisPrice(tick)}
        </text>
      ))}
      {timeIndices.map((index) => (
        <text
          key={`time-${index}`}
          x={x(index)}
          y={h - 8}
          fill="rgba(194,204,224,0.66)"
          fontSize="11"
          textAnchor={index === 0 ? 'start' : index === data.length - 1 ? 'end' : 'middle'}
        >
          {formatChartTime(candleTimestamp(data[index]), timeframe)}
        </text>
      ))}
      <rect
        x={w - pad.r + 4}
        y={lastPriceLabelY - 10}
        width="52"
        height="18"
        rx="9"
        fill="rgba(25, 35, 49, 0.9)"
        stroke="rgba(141, 196, 255, 0.44)"
      />
      <text
        x={w - pad.r + 30}
        y={lastPriceLabelY + 3}
        fill="#9ad2ff"
        fontSize="11"
        textAnchor="middle"
      >
        {formatAxisPrice(lastClose)}
      </text>
      {referencePriceVisible ? (
        <>
          <rect
            x={w - pad.r + 4}
            y={y(Number(referencePrice)) - 10}
            width="52"
            height="18"
            rx="9"
            fill="rgba(48, 40, 18, 0.88)"
            stroke="rgba(255, 205, 120, 0.4)"
          />
          <text
            x={w - pad.r + 30}
            y={y(Number(referencePrice)) + 3}
            fill="#ffcb7f"
            fontSize="11"
            textAnchor="middle"
          >
            {formatAxisPrice(Number(referencePrice))}
          </text>
          {referenceLabel ? (
            <text
              x={w - pad.r - 42}
              y={y(Number(referencePrice)) - 14}
              fill="rgba(255, 205, 120, 0.8)"
              fontSize="10"
              textAnchor="end"
            >
              {referenceLabel}
            </text>
          ) : null}
        </>
      ) : null}
    </svg>
  );
}

function tradeTimeValue(trade: Trade) {
  const raw = String(trade.timestamp || '').trim();
  if (!raw) return 0;
  const normalized = raw.includes('T') ? raw : raw.replace(' ', 'T');
  const ts = Date.parse(normalized);
  return Number.isFinite(ts) ? ts : 0;
}

export function PositionsPanel({ positions = [], trades = [] }: PositionsPanelProps) {
  const [livePositions, setLivePositions] = useState<Position[]>(positions);
  const [liveTrades, setLiveTrades] = useState<Trade[]>(trades);
  const [selectedSymbol, setSelectedSymbol] = useState<string>('');
  const sorted = useMemo(
    () => [...livePositions].sort((a, b) => b.value - a.value),
    [livePositions]
  );
  const latestTrade = useMemo(
    () =>
      [...liveTrades].sort((a, b) => tradeTimeValue(b) - tradeTimeValue(a))[0] || null,
    [liveTrades]
  );
  const [tf, setTf] = useState('1h');
  const [kline, setKline] = useState<PositionKlinePayload | null>(null);

  useEffect(() => {
    setLivePositions(positions);
  }, [positions]);

  useEffect(() => {
    setLiveTrades(trades);
  }, [trades]);

  useEffect(() => {
    if (!sorted.length) {
      setSelectedSymbol('');
      return;
    }
    if (!selectedSymbol || !sorted.some((item) => item.symbol === selectedSymbol)) {
      setSelectedSymbol(sorted[0].symbol);
    }
  }, [sorted, selectedSymbol]);

  const spotlightIndex = selectedSymbol
    ? sorted.findIndex((item) => item.symbol === selectedSymbol)
    : 0;
  const spotlightPosition = sorted[Math.max(0, spotlightIndex)] || null;
  const fallbackTrade = !spotlightPosition ? latestTrade : null;
  const fallbackSymbol = fallbackTrade
    ? String(fallbackTrade.symbol || '').replace('/USDT', '').replace('-USDT', '')
    : '';
  const activeSymbol = spotlightPosition?.symbol || fallbackSymbol;
  const activeReferencePrice =
    spotlightPosition && Number(spotlightPosition.avgPrice) > 0
      ? spotlightPosition.avgPrice
      : (fallbackTrade && Number(fallbackTrade.price) > 0 ? fallbackTrade.price : undefined);
  const activeReferenceLabel = spotlightPosition
    ? '持仓均价'
    : (fallbackTrade && Number(fallbackTrade.price) > 0 ? '成交价' : undefined);
  const spotlightLabel = spotlightPosition
    ? spotlightPosition.symbol.replace('-USDT', '')
    : fallbackSymbol || '—';
  const chartCandles = Array.isArray(kline?.candles) ? kline.candles : [];
  const chartSummary = kline?.summary || null;
  const latestCandle = chartCandles[chartCandles.length - 1] || null;
  const previousCandle = chartCandles[chartCandles.length - 2] || null;
  const periodChange = latestCandle && previousCandle
    ? latestCandle.close - previousCandle.close
    : (Number(chartSummary?.close || 0) - Number(chartSummary?.open || 0));
  const periodChangePct = previousCandle?.close
    ? periodChange / previousCandle.close
    : Number(chartSummary?.change_pct || 0);
  const sessionRangePct =
    Number(chartSummary?.low || 0) > 0
      ? (Number(chartSummary?.high || 0) - Number(chartSummary?.low || 0)) / Number(chartSummary?.low)
      : 0;
  const averageVolume =
    chartCandles.length > 0
      ? chartCandles.reduce((sum, candle) => sum + Number(candle.volume || 0), 0) / chartCandles.length
      : 0;
  const ma7Value =
    chartCandles.length >= 7
      ? chartCandles.slice(-7).reduce((sum, candle) => sum + candle.close, 0) / 7
      : null;
  const ma20Value =
    chartCandles.length >= 20
      ? chartCandles.slice(-20).reduce((sum, candle) => sum + candle.close, 0) / 20
      : null;
  const displayAvgPrice =
    spotlightPosition && Number(spotlightPosition.avgPrice) > 0 ? spotlightPosition.avgPrice : activeReferencePrice;
  const displayCurrentPrice =
    spotlightPosition && Number(spotlightPosition.currentPrice) > 0
      ? spotlightPosition.currentPrice
      : (Number(chartSummary?.close || latestCandle?.close || 0) || 0);

  useEffect(() => {
    if (!activeSymbol) {
      setKline(null);
      return;
    }
    let mounted = true;
    const klineSymbol = String(activeSymbol || '')
      .replace('/USDT', '')
      .replace('-USDT', '');
    api.positionKline(klineSymbol, tf).then((data) => {
      if (mounted) setKline(data);
    });
    return () => {
      mounted = false;
    };
  }, [activeSymbol, tf]);

  useInterval(() => {
    if (document.hidden) return;
    api.positions().then((payload) => {
      const next = Array.isArray(payload?.positions) ? payload.positions : [];
      setLivePositions(next);
    });
  }, 5000);

  useInterval(() => {
    if (document.hidden) return;
    api.trades().then((payload) => {
      const next = Array.isArray(payload?.trades) ? payload.trades : [];
      setLiveTrades(next);
    });
  }, 5000);

  useInterval(() => {
    if (document.hidden || !activeSymbol) return;
    const klineSymbol = String(activeSymbol || '')
      .replace('/USDT', '')
      .replace('-USDT', '');
    api.positionKline(klineSymbol, tf).then((data) => {
      setKline(data);
    });
  }, activeSymbol ? 10000 : null);

  return (
    <div className="liquid-glass-thick tone-sky reading-frame p-5 flex flex-col gap-5">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm text-[var(--text-dim)]">
          <CandlestickChart className="w-4 h-4" />
          <span>持仓聚焦</span>
        </div>
        {(spotlightPosition || fallbackTrade) && (
          <div className="flex items-center gap-2">
            <button
              className="p-1 rounded-lg hover:bg-white/10 disabled:opacity-30"
              onClick={() => {
                const nextIndex = Math.max(0, spotlightIndex - 1);
                setSelectedSymbol(sorted[nextIndex]?.symbol || '');
              }}
              disabled={!spotlightPosition || spotlightIndex <= 0}
            >
              <ChevronLeft className="w-4 h-4" />
            </button>
            <span className="text-sm font-medium min-w-[5rem] text-center">
              {spotlightLabel}
            </span>
            <button
              className="p-1 rounded-lg hover:bg-white/10 disabled:opacity-30"
              onClick={() => {
                const nextIndex = Math.min(sorted.length - 1, spotlightIndex + 1);
                setSelectedSymbol(sorted[nextIndex]?.symbol || '');
              }}
              disabled={!spotlightPosition || spotlightIndex >= sorted.length - 1}
            >
              <ChevronRight className="w-4 h-4" />
            </button>
          </div>
        )}
      </div>

      {spotlightPosition || fallbackTrade ? (
        <>
          {spotlightPosition ? (
            <div className="grid grid-cols-1 xl:grid-cols-[1fr_auto] gap-4 items-start">
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
                <div className="liquid-glass-thin metric-pill tone-sky px-4 py-3">
                  <div className="text-xs text-[var(--text-dim)]">市值</div>
                  <div className="text-lg font-semibold">{fmtUsd(spotlightPosition.value)}</div>
                </div>
                <div className="liquid-glass-thin metric-pill tone-sage px-4 py-3">
                  <div className="text-xs text-[var(--text-dim)]">数量</div>
                  <div className="text-lg font-mono">{fmtNum(spotlightPosition.qty, 4)}</div>
                </div>
                <div className="liquid-glass-thin metric-pill tone-amber px-4 py-3">
                  <div className="text-xs text-[var(--text-dim)]">均价</div>
                  <div className="text-lg font-mono">{fmtUsd(displayAvgPrice)}</div>
                </div>
                <div className="liquid-glass-thin metric-pill tone-coral px-4 py-3">
                  <div className="text-xs text-[var(--text-dim)]">现价</div>
                  <div className="text-lg font-mono">{fmtUsd(displayCurrentPrice)}</div>
                </div>
              </div>
              <div className={`xl:text-right px-2 pt-2 ${spotlightPosition.pnlPercent >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                <div className="text-2xl font-semibold">{fmtPct(spotlightPosition.pnlPercent)}</div>
                <div className="text-sm">{fmtUsd(spotlightPosition.pnl)}</div>
              </div>
            </div>
          ) : fallbackTrade ? (
            <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
              <div className="liquid-glass-thin metric-pill tone-sky px-4 py-3">
                <div className="text-xs text-[var(--text-dim)]">状态</div>
                <div className="text-lg font-semibold">最近成交</div>
              </div>
              <div className="liquid-glass-thin metric-pill tone-sage px-4 py-3">
                <div className="text-xs text-[var(--text-dim)]">方向</div>
                <div className="text-lg font-medium">{sideLabels[fallbackTrade.side] || fallbackTrade.side || '--'}</div>
              </div>
              <div className="liquid-glass-thin metric-pill tone-amber px-4 py-3">
                <div className="text-xs text-[var(--text-dim)]">成交单价</div>
                <div className="text-lg font-mono">{fmtUsd(fallbackTrade.price)}</div>
              </div>
              <div className="liquid-glass-thin metric-pill tone-coral px-4 py-3">
                <div className="text-xs text-[var(--text-dim)]">成交数量</div>
                <div className="text-lg font-mono">{fmtNum(fallbackTrade.qty, 6)}</div>
              </div>
              <div className="liquid-glass-thin metric-pill tone-plum px-4 py-3">
                <div className="text-xs text-[var(--text-dim)]">时间</div>
                <div className="text-sm font-medium">{fallbackTrade.timestamp || '--'}</div>
                <div className="text-[11px] text-[var(--text-dim)] mt-1">额 {fmtUsd(fallbackTrade.value)}</div>
              </div>
            </div>
          ) : null}

          {activeSymbol ? (
            <>
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="flex flex-wrap gap-2">
                  {timeframes.map((t) => (
                    <button
                      key={t.key}
                      onClick={() => setTf(t.key)}
                      className={`liquid-glass-thin clear-chip control-pill px-3 py-1.5 text-xs transition ${
                        tf === t.key
                          ? 'tone-sky text-white border-white/20'
                          : 'tone-pearl text-[var(--text-dim)] surface-lift'
                      }`}
                    >
                      {t.label}
                    </button>
                  ))}
                </div>
                <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-[var(--text-dim)]">
                  <span>数据源 {kline?.source || '--'}</span>
                  <span>K线 {chartSummary?.bars || chartCandles.length || 0} 根</span>
                  <span>更新时间 {chartSummary?.last_time || '--'}</span>
                </div>
              </div>

              <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-2">
                <div className="liquid-glass-inset tone-neutral px-3 py-2">
                  <div className="text-[11px] uppercase tracking-[0.08em] text-[var(--text-dim)]">Open</div>
                  <div className="mt-1 text-sm font-mono text-white">
                    {fmtUsd(Number(chartSummary?.open || latestCandle?.open || 0))}
                  </div>
                </div>
                <div className="liquid-glass-inset tone-neutral px-3 py-2">
                  <div className="text-[11px] uppercase tracking-[0.08em] text-[var(--text-dim)]">High</div>
                  <div className="mt-1 text-sm font-mono text-emerald-200">
                    {fmtUsd(Number(chartSummary?.high || latestCandle?.high || 0))}
                  </div>
                </div>
                <div className="liquid-glass-inset tone-neutral px-3 py-2">
                  <div className="text-[11px] uppercase tracking-[0.08em] text-[var(--text-dim)]">Low</div>
                  <div className="mt-1 text-sm font-mono text-rose-200">
                    {fmtUsd(Number(chartSummary?.low || latestCandle?.low || 0))}
                  </div>
                </div>
                <div className="liquid-glass-inset tone-neutral px-3 py-2">
                  <div className="text-[11px] uppercase tracking-[0.08em] text-[var(--text-dim)]">Close</div>
                  <div className="mt-1 text-sm font-mono text-white">
                    {fmtUsd(Number(chartSummary?.close || latestCandle?.close || 0))}
                  </div>
                </div>
                <div className="liquid-glass-inset tone-neutral px-3 py-2">
                  <div className="text-[11px] uppercase tracking-[0.08em] text-[var(--text-dim)]">振幅 / 量能</div>
                  <div className="mt-1 text-sm font-mono text-white">
                    {fmtPct(sessionRangePct)}
                  </div>
                  <div className="text-[11px] text-[var(--text-dim)]">
                    Vol {formatCompactVolume(Number(chartSummary?.volume || 0))}
                  </div>
                </div>
                <div className="liquid-glass-inset tone-neutral px-3 py-2">
                  <div className="text-[11px] uppercase tracking-[0.08em] text-[var(--text-dim)]">本根变化</div>
                  <div className={`mt-1 text-sm font-mono ${periodChange >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                    {fmtUsd(periodChange)}
                  </div>
                  <div className={`text-[11px] ${periodChangePct >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                    {fmtPct(periodChangePct)}
                  </div>
                </div>
              </div>

              <div className="liquid-glass-inset tone-neutral w-full p-3">
                <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/8 pb-3">
                  <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs">
                    <span className="text-[var(--text-dim)]">MA7 <span className="ml-1 font-mono text-sky-200">{ma7Value ? fmtUsd(ma7Value) : '--'}</span></span>
                    <span className="text-[var(--text-dim)]">MA20 <span className="ml-1 font-mono text-amber-200">{ma20Value ? fmtUsd(ma20Value) : '--'}</span></span>
                    <span className="text-[var(--text-dim)]">均量 <span className="ml-1 font-mono text-white">{formatCompactVolume(averageVolume)}</span></span>
                    <span className="text-[var(--text-dim)]">{activeReferenceLabel || '参考价'} <span className="ml-1 font-mono text-amber-200">{fmtUsd(activeReferencePrice)}</span></span>
                  </div>
                  <div className="text-xs text-[var(--text-dim)]">
                    最近收盘 <span className="ml-1 font-mono text-white">{fmtUsd(Number(chartSummary?.close || latestCandle?.close || 0))}</span>
                  </div>
                </div>
                <div className="mt-3 h-[19rem]">
                  <CandlestickSvg
                    data={chartCandles}
                    timeframe={tf}
                    referencePrice={activeReferencePrice}
                    referenceLabel={activeReferenceLabel}
                  />
                </div>
              </div>
            </>
          ) : null}
        </>
      ) : (
        <div className="h-56 flex items-center justify-center text-[var(--text-dim)] text-sm">
          当前无持仓
        </div>
      )}

      <div className="mt-2">
        <div className="text-sm text-[var(--text-dim)] mb-3">持仓清单</div>
        <div className="liquid-glass-inset tone-neutral overflow-x-auto px-3 py-2">
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
                    setSelectedSymbol(pos.symbol);
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
