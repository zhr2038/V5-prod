import { useCallback, useEffect, useRef, useState } from 'react';
import { CandlestickChart, Maximize2, Minimize2, Minus, Plus, RefreshCw, RotateCcw } from 'lucide-react';
import { CandlestickSeries, ColorType, CrosshairMode, HistogramSeries, LineSeries, LineStyle, createChart } from 'lightweight-charts';
import type { IChartApi, ISeriesApi, LogicalRange, TickMarkType, Time, UTCTimestamp } from 'lightweight-charts';
import { chartNumber, chartSymbols, chartTime, chartTimeframes, chartVolume, marketFreshness,
  movingAverage, normalizeChartSymbol, parseMarketSnapshot } from '../lib/marketChart';
import type { ChartTimeframe, MarketSnapshot } from '../lib/marketChart';
import './exchange-chart.css';

const UP = '#38b997', DOWN = '#e4797e';
const averages = [{ period: 7, color: '#e8b86d' }, { period: 25, color: '#9b9bdb' }, { period: 99, color: '#6bafcb' }];
const freshnessLabels = { observed: '行情正常', missing: '连接行情', failed: '刷新失败', stale: '行情延迟', partial: '报价缺失', future: '行情时间异常' };
interface Props { focusSymbol: string; onSymbolChange: (symbol: string) => void; costPrice?: number | null }
interface CanvasState {
  chart: IChartApi; candle: ISeriesApi<'Candlestick'>; volume: ISeriesApi<'Histogram'>;
  lines: ISeriesApi<'Line'>[]; firstTs: number | null; count: number;
}

export default function ExchangeChart({ focusSymbol, onSymbolChange, costPrice }: Props) {
  const [timeframe, setTimeframe] = useState<ChartTimeframe>('1h');
  const [fullscreen, setFullscreen] = useState(false);
  const shell = useRef<HTMLDivElement>(null);
  const fullButton = useRef<HTMLButtonElement>(null);
  const symbol = normalizeChartSymbol(focusSymbol);
  useEffect(() => {
    if (!fullscreen) return;
    const originalOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    const button = fullButton.current;
    button?.focus();
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setFullscreen(false);
      if (event.key !== 'Tab') return;
      const elements = Array.from(shell.current?.querySelectorAll<HTMLElement>('button:not(:disabled), a[href], [tabindex="0"]') || []);
      const first = elements[0], last = elements.at(-1);
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
    };
    document.addEventListener('keydown', onKey);
    return () => { document.body.style.overflow = originalOverflow; document.removeEventListener('keydown', onKey); button?.focus(); };
  }, [fullscreen]);

  return <div ref={shell} className={`ec-shell${fullscreen ? ' ec-fullscreen' : ''}`} role={fullscreen ? 'dialog' : undefined} aria-modal={fullscreen || undefined} aria-label="市场K线">
    <div className="ec-navigation"><div className="ec-symbols" aria-label="K线交易对">{chartSymbols.map(item => <button key={item} className={symbol === item ? 'is-active' : ''} aria-pressed={symbol === item} onClick={() => onSymbolChange(item)}>{item.replace('-USDT', '')}<small>/ USDT</small></button>)}</div><button ref={fullButton} className="ec-icon" onClick={() => setFullscreen(!fullscreen)} title={fullscreen ? '退出全屏 · Esc' : '全屏查看'} aria-label={fullscreen ? '退出K线全屏' : 'K线全屏'}>{fullscreen ? <Minimize2 size={16} /> : <Maximize2 size={16} />}</button></div>
    <ChartSession key={`${symbol}:${timeframe}`} symbol={symbol} timeframe={timeframe} setTimeframe={setTimeframe} costPrice={costPrice} />
  </div>;
}

function ChartSession({ symbol, timeframe, setTimeframe, costPrice }: { symbol: string; timeframe: ChartTimeframe; setTimeframe: (value: ChartTimeframe) => void; costPrice?: number | null }) {
  const [snapshot, setSnapshot] = useState<{ data: MarketSnapshot | null; error: boolean; busy: boolean }>({ data: null, error: false, busy: true });
  const [now, setNow] = useState(() => Date.now());
  const [hoverTime, setHoverTime] = useState<number | null>(null);
  const [showMA, setShowMA] = useState(true);
  const host = useRef<HTMLDivElement>(null);
  const state = useRef<CanvasState | null>(null);
  const refresh = useRef<() => void>(() => {});
  const { data, error, busy } = snapshot;
  const freshness = marketFreshness(data, now, error);
  const base = symbol.replace('-USDT', '');

  useEffect(() => {
    let active = true, pending = false;
    let controller: AbortController | null = null;
    const fetchData = async () => {
      if (pending || document.hidden) return;
      pending = true;
      controller = new AbortController();
      const timeout = window.setTimeout(() => controller?.abort(), 12000);
      setSnapshot(previous => ({ ...previous, busy: true }));
      try {
        const response = await fetch(`/api/market_chart?symbol=${encodeURIComponent(symbol)}&timeframe=${timeframe}`, { signal: controller.signal, cache: 'no-store' });
        if (!response.ok) throw new Error('行情请求失败');
        const next = parseMarketSnapshot(await response.json(), symbol, timeframe);
        if (active) { setSnapshot({ data: next, error: false, busy: false }); setNow(Date.now()); }
      } catch {
        if (active) setSnapshot(previous => ({ ...previous, error: true, busy: false }));
      } finally { window.clearTimeout(timeout); pending = false; }
    };
    refresh.current = fetchData;
    void fetchData();
    const poll = window.setInterval(() => void fetchData(), 10000);
    const clock = window.setInterval(() => setNow(Date.now()), 1000);
    const resume = () => { if (!document.hidden) void fetchData(); };
    document.addEventListener('visibilitychange', resume);
    return () => { active = false; controller?.abort(); window.clearInterval(poll); window.clearInterval(clock); document.removeEventListener('visibilitychange', resume); };
  }, [symbol, timeframe]);

  useEffect(() => {
    if (!host.current) return;
    const chart = createChart(host.current, {
      autoSize: true, layout: { background: { type: ColorType.Solid, color: '#10171c' }, textColor: '#929fa8', fontFamily: 'Consolas, monospace', fontSize: 11, attributionLogo: true,
        panes: { separatorColor: '#29343c', separatorHoverColor: '#5b594b', enableResize: true } },
      grid: { vertLines: { color: '#1a242c' }, horzLines: { color: '#1c262e' } },
      crosshair: { mode: CrosshairMode.Normal, vertLine: { color: '#8b98a2', labelBackgroundColor: '#35414b' }, horzLine: { color: '#8b98a2', labelBackgroundColor: '#35414b' } },
      rightPriceScale: { borderColor: '#29343c', minimumWidth: 76, scaleMargins: { top: 0.08, bottom: 0.08 } },
      timeScale: { borderColor: '#29343c', timeVisible: timeframe !== '1d', secondsVisible: false, rightOffset: 5, barSpacing: 7, minBarSpacing: 2,
        tickMarkFormatter: (time: Time, type: TickMarkType) => typeof time === 'number' ? chartTime(time * 1000, type <= 2 ? 'date' : 'clock') : null },
      localization: { locale: 'zh-CN', timeFormatter: (time: Time) => typeof time === 'number' ? `${chartTime(time * 1000)} 北京时间` : String(time) },
      handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: false },
      handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: true, axisDoubleClickReset: true },
    });
    const candle = chart.addSeries(CandlestickSeries, { upColor: UP, downColor: DOWN, wickUpColor: UP, wickDownColor: DOWN, borderVisible: false,
      priceFormat: { type: 'price', precision: 2, minMove: 0.01 } });
    const lines = averages.map(({ color }) => chart.addSeries(LineSeries, { color, lineWidth: 1, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false }));
    const volume = chart.addSeries(HistogramSeries, { priceFormat: { type: 'volume' }, priceLineVisible: false, lastValueVisible: false }, 1);
    volume.priceScale().applyOptions({ scaleMargins: { top: 0.18, bottom: 0 }, minimumWidth: 76 });
    chart.panes()[0].setStretchFactor(3.5);
    chart.panes()[1].setStretchFactor(1);
    const cursor = (event: { time?: unknown }) => setHoverTime(typeof event.time === 'number' ? event.time * 1000 : null);
    chart.subscribeCrosshairMove(cursor);
    state.current = { chart, candle, volume, lines, firstTs: null, count: 0 };
    return () => { chart.unsubscribeCrosshairMove(cursor); chart.remove(); state.current = null; };
  }, [timeframe]);

  const reset = useCallback(() => {
    const current = state.current;
    if (!current?.count) return;
    const visibleBars = (host.current?.clientWidth || 900) < 500 ? 45 : 100;
    current.chart.timeScale().setVisibleLogicalRange({ from: Math.max(-1, current.count - visibleBars), to: current.count + 4 });
    current.chart.priceScale('right').applyOptions({ autoScale: true });
  }, []);

  useEffect(() => {
    const current = state.current;
    if (!current || !data) return;
    const previousRange = current.chart.timeScale().getVisibleLogicalRange();
    const initial = current.firstTs === null;
    const shift = initial ? 0 : (data.candles[0].ts - current.firstTs!) / (data.bar_seconds * 1000);
    const following = previousRange && previousRange.to >= current.count - 1;
    current.candle.setData(data.candles.map(c => ({ time: c.ts / 1000 as UTCTimestamp, open: c.open, high: c.high, low: c.low, close: c.close })));
    current.volume.setData(data.candles.map(c => ({ time: c.ts / 1000 as UTCTimestamp, value: c.volume, color: c.close >= c.open ? '#38b99778' : '#e4797e78' })));
    current.lines.forEach((line, index) => line.setData(movingAverage(data.candles, averages[index].period).map(point => ({ ...point, time: point.time as UTCTimestamp }))));
    if (previousRange && !initial) {
      const delta = following ? data.candles.length - current.count : -shift;
      current.chart.timeScale().setVisibleLogicalRange({ from: previousRange.from + delta, to: previousRange.to + delta });
    }
    current.firstTs = data.candles[0].ts;
    current.count = data.candles.length;
    if (initial) reset();
  }, [data, reset]);

  useEffect(() => { state.current?.lines.forEach(line => line.applyOptions({ visible: showMA })); }, [showMA]);
  useEffect(() => {
    const current = state.current;
    if (!current || !costPrice || !Number.isFinite(costPrice) || costPrice <= 0) return;
    const line = current.candle.createPriceLine({ price: costPrice, color: '#e8b86d', lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: '持仓成本' });
    return () => { if (state.current === current) current.candle.removePriceLine(line); };
  }, [costPrice]);

  const zoom = (factor: number) => {
    const chart = state.current?.chart;
    const range = chart?.timeScale().getVisibleLogicalRange();
    if (!chart || !range) return;
    const width = Math.max(12, Math.min(340, (range.to - range.from) * factor));
    chart.timeScale().setVisibleLogicalRange({ from: range.to - width, to: range.to } as LogicalRange);
  };
  const last = data?.candles.at(-1);
  const hovered = data?.candles.find(c => c.ts === hoverTime);
  const candle = hovered || last;
  const ticker = data?.ticker;
  const change = ticker ? (ticker.last / ticker.open_24h - 1) * 100 : null;
  const candleChange = candle ? (candle.close / candle.open - 1) * 100 : null;
  const candleColor = candle && candle.close >= candle.open ? 'ec-up' : 'ec-down';
  const maIndex = candle && data ? data.candles.indexOf(candle) : -1;
  const candleText = candle ? `${chartTime(candle.ts)} 开 ${chartNumber(candle.open)} 高 ${chartNumber(candle.high)} 低 ${chartNumber(candle.low)} ${candle.closed ? '收' : '最新'} ${chartNumber(candle.close)}` : 'K线尚未加载';

  return <>
    <div className="ec-quote"><div className="ec-instrument"><span className={`cc-coin cc-coin-${base.toLowerCase()}`}>{base === 'BTC' ? '₿' : base === 'ETH' ? 'Ξ' : base[0]}</span><div><h3>{base}<span>/ USDT</span></h3><small>OKX 现货</small></div></div><div className="ec-last"><strong className={change !== null && change < 0 ? 'ec-down' : change !== null ? 'ec-up' : ''}>{chartNumber(ticker?.last)}</strong><span className={change !== null && change < 0 ? 'ec-down' : change !== null ? 'ec-up' : ''}>{change === null ? '24h 涨跌待确认' : `${change >= 0 ? '+' : ''}${change.toFixed(2)}%`}<small>{change !== null ? '24h 涨跌' : ''}</small></span></div><dl className="ec-ticker-stats"><div><dt>24h 最高</dt><dd>{chartNumber(ticker?.high_24h)}</dd></div><div><dt>24h 最低</dt><dd>{chartNumber(ticker?.low_24h)}</dd></div><div><dt>24h 成交额 <small>USDT</small></dt><dd>{chartVolume(ticker?.quote_volume_24h)}</dd></div></dl></div>
    <div className="ec-toolbar"><div className="ec-timeframes" aria-label="K线周期">{chartTimeframes.map(item => <button key={item.id} onClick={() => setTimeframe(item.id)} aria-pressed={timeframe === item.id} className={timeframe === item.id ? 'is-active' : ''}>{item.label}</button>)}</div><div className="ec-tools"><button className={`ec-ma-button${showMA ? ' is-active' : ''}`} onClick={() => setShowMA(!showMA)} aria-pressed={showMA} title="显示或隐藏 MA7 / MA25 / MA99">MA</button><button className="ec-icon" onClick={() => zoom(0.75)} aria-label="放大K线" title="放大"><Plus size={15} /></button><button className="ec-icon" onClick={() => zoom(1.3)} aria-label="缩小K线" title="缩小"><Minus size={15} /></button><button className="ec-icon" onClick={reset} aria-label="重置K线视图" title="回到最新行情"><RotateCcw size={14} /></button><button className="ec-icon" onClick={() => refresh.current()} disabled={busy} aria-label="刷新K线行情" title="刷新行情"><RefreshCw size={14} className={busy ? 'cc-spinning' : ''} /></button></div></div>
    {freshness !== 'observed' && <div className="ec-warning" role="status">{freshness === 'missing' ? '正在连接 OKX 行情…' : freshness === 'failed' ? `行情刷新失败。${data ? '保留上次图表，请留意报价时间。' : '暂时没有可显示的K线，请重试。'}` : freshness === 'partial' ? 'K线已加载，最新报价暂不可用。' : freshness === 'future' ? '行情时间异常，请检查数据与设备时间。' : '行情数据已延迟，以下保留的是最后一次观测。'}</div>}
    <div className="ec-legend"><div className="ec-ohlc" aria-label={candleText}><time>{chartTime(candle?.ts)}</time><span className="ec-candle-state">{candle ? candle.closed ? '已收盘' : '未收盘' : '—'}</span>{([['开', candle?.open], ['高', candle?.high], ['低', candle?.low], [candle?.closed ? '收' : '最新', candle?.close]] as const).map(([label, value]) => <span key={label}>{label} <b className={candleColor}>{chartNumber(value)}</b></span>)}<b className={candleColor}>{candleChange === null ? '—' : `${candleChange >= 0 ? '+' : ''}${candleChange.toFixed(2)}%`}</b></div><div className="ec-indicators"><span>VOL <b>{chartVolume(candle?.volume)} {base}</b></span>{showMA && averages.map(({ period, color }) => <span key={period} style={{ color }}>MA{period} <b>{data && maIndex >= period - 1 ? chartNumber(data.candles.slice(maIndex - period + 1, maIndex + 1).reduce((sum, c) => sum + c.close, 0) / period) : '—'}</b></span>)}</div></div>
    <div className="ec-canvas-wrap"><div ref={host} className="ec-canvas" role="img" aria-label={`${symbol} ${timeframe} 蜡烛图和成交量，${data?.candles.length || 0} 根K线。${candleText}`} />{!data && <div className="ec-chart-empty"><CandlestickChart size={34} /><span>{error ? '行情暂不可用' : '载入蜡烛图…'}</span>{error && <button onClick={() => refresh.current()}>重新连接</button>}</div>}</div>
    <footer className="ec-footer"><span className={`ec-feed-state${freshness === 'observed' ? ' ec-up' : ''}`}><i />{freshnessLabels[freshness]}</span><span>报价 {ticker ? chartTime(ticker.ts, 'clock') : '—'}<em>·</em>每 10 秒刷新</span><span className="ec-gesture-hint">滚轮缩放 · 拖动平移 · 长按查看</span><span className="ec-zone">北京时间 UTC+8{timeframe === '1d' ? ' · 日线 08:00 开盘' : ''}</span></footer>
    <div className="ec-source-note"><span>最近 {data?.candles.length || '—'} 根 · 量柱颜色表示该根涨跌</span><a href="https://www.tradingview.com/" target="_blank" rel="noreferrer" title="TradingView Lightweight Charts™ · Copyright (с) 2025 TradingView, Inc.">Charts by TradingView</a></div>
  </>;
}
