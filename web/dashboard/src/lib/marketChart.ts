export const chartSymbols = ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT'] as const;
export const chartTimeframes = [
  { id: '1m', label: '1分', seconds: 60 }, { id: '5m', label: '5分', seconds: 300 },
  { id: '15m', label: '15分', seconds: 900 }, { id: '30m', label: '30分', seconds: 1800 },
  { id: '1h', label: '1小时', seconds: 3600 }, { id: '4h', label: '4小时', seconds: 14400 },
  { id: '1d', label: '日线', seconds: 86400 },
] as const;
export type ChartTimeframe = typeof chartTimeframes[number]['id'];
export interface MarketCandle {
  ts: number; open: number; high: number; low: number; close: number;
  volume: number; quote_volume: number; closed: boolean;
}
export interface MarketTicker {
  ts: number; last: number; open_24h: number; high_24h: number; low_24h: number;
  volume_24h: number; quote_volume_24h: number;
}
export interface MarketSnapshot {
  schema_version: 'v5.market_chart.v1'; symbol: string; timeframe: ChartTimeframe;
  bar_seconds: number; candle_received_at_ms: number; fetched_at_ms: number;
  candles: MarketCandle[]; ticker: MarketTicker | null; ticker_error: string | null;
}
const finite = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value);
const positive = (value: unknown): value is number => finite(value) && value > 0;
const timestamp = (value: unknown): value is number => positive(value) && Number.isInteger(value) && value >= 1e12;
const beijingFormatter = new Intl.DateTimeFormat('en-US', { timeZone: 'Asia/Shanghai', year: 'numeric',
  month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' });

export function normalizeChartSymbol(symbol: string) { return symbol.toUpperCase().replace('/', '-'); }

export function parseMarketSnapshot(value: unknown, symbol: string, timeframe: ChartTimeframe): MarketSnapshot {
  if (!value || typeof value !== 'object') throw new Error('行情响应无效');
  const p = value as MarketSnapshot;
  const seconds = chartTimeframes.find(item => item.id === timeframe)!.seconds;
  if (p.schema_version !== 'v5.market_chart.v1' || p.symbol !== symbol || p.timeframe !== timeframe
      || p.bar_seconds !== seconds || !timestamp(p.candle_received_at_ms) || !timestamp(p.fetched_at_ms)
      || !Array.isArray(p.candles) || !p.candles.length || p.candles.length > 300) throw new Error('行情标的或周期不匹配');
  p.candles.forEach((c, index) => {
    if (!c || !timestamp(c.ts) || c.ts % (seconds * 1000) || ![c.open, c.high, c.low, c.close].every(positive)
        || !finite(c.volume) || c.volume < 0 || !finite(c.quote_volume) || c.quote_volume < 0
        || typeof c.closed !== 'boolean' || c.low > Math.min(c.open, c.close) || c.high < Math.max(c.open, c.close)
        || (index > 0 && c.ts - p.candles[index - 1].ts !== seconds * 1000)) throw new Error('K线数据不完整');
  });
  if (p.ticker !== null && (!p.ticker || !timestamp(p.ticker.ts)
      || ![p.ticker.last, p.ticker.open_24h, p.ticker.high_24h, p.ticker.low_24h].every(positive)
      || !finite(p.ticker.volume_24h) || p.ticker.volume_24h < 0
      || !finite(p.ticker.quote_volume_24h) || p.ticker.quote_volume_24h < 0)) throw new Error('报价数据无效');
  return p;
}

export function marketFreshness(data: MarketSnapshot | null, now: number, failed: boolean) {
  if (failed) return 'failed';
  if (!data) return 'missing';
  const last = data.candles.at(-1)!;
  const clocks = [data.candle_received_at_ms, data.fetched_at_ms, last.ts, ...(data.ticker ? [data.ticker.ts] : [])];
  if (clocks.some(ts => ts > now + 5000)) return 'future';
  if (now - data.candle_received_at_ms > 30000 || now - last.ts > data.bar_seconds * 1000 + 20000
      || (data.ticker && now - data.ticker.ts > 30000)) return 'stale';
  return data.ticker ? 'observed' : 'partial';
}

export function chartTime(ts: number | null | undefined, kind: 'full' | 'clock' | 'date' = 'full') {
  if (!finite(ts)) return '—';
  const parts = Object.fromEntries(beijingFormatter.formatToParts(ts).map(part => [part.type, part.value]));
  const clock = `${parts.hour}:${parts.minute}`, day = `${parts.month}-${parts.day}`;
  return kind === 'clock' ? clock : kind === 'date' ? day : `${parts.year}-${day} ${clock}`;
}
export function chartNumber(value: number | null | undefined, digits = 2) {
  return finite(value) ? value.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits }) : '—';
}
export function chartVolume(value: number | null | undefined) {
  if (!finite(value)) return '—';
  const unit = value >= 1e9 ? [1e9, 'B'] as const : value >= 1e6 ? [1e6, 'M'] as const : value >= 1e3 ? [1e3, 'K'] as const : [1, ''] as const;
  return `${chartNumber(value / unit[0])}${unit[1]}`;
}
export function movingAverage(candles: MarketCandle[], period: number) {
  let sum = 0;
  return candles.flatMap((candle, index) => {
    sum += candle.close;
    if (index >= period) sum -= candles[index - period].close;
    return index >= period - 1 ? [{ time: candle.ts / 1000, value: sum / period }] : [];
  });
}
