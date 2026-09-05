import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';

const source = readFileSync(new URL('../src/lib/marketChart.ts', import.meta.url), 'utf8');
const module = { exports: {} };
runInNewContext(ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText,
  { module, exports: module.exports, Intl, Date });
const { parseMarketSnapshot, marketFreshness, chartTime, chartNumber, chartVolume, movingAverage } = module.exports;
const TS = 1788573600000, NOW = TS + 600000;
const candle = (time, close = 101) => ({ ts: time, open: 100, high: 200, low: 99, close, volume: 0, quote_volume: 0, closed: false });
function fixture() { return { schema_version: 'v5.market_chart.v1', symbol: 'BTC-USDT', timeframe: '1h', bar_seconds: 3600,
  fetched_at_ms: NOW, candle_received_at_ms: NOW, candles: [candle(TS - 3600000), candle(TS)],
  ticker: { ts: NOW, last: 101, open_24h: 100, high_24h: 102, low_24h: 99, volume_24h: 0, quote_volume_24h: 0 }, ticker_error: null }; }
test('never mix previous symbol or timeframe into a new chart', () => {
  for (const [symbol, timeframe] of [['ETH-USDT', '1h'], ['BTC-USDT', '1d']]) {
    assert.throws(() => parseMarketSnapshot(fixture(), symbol, timeframe), /不匹配/);
  }
});
test('reject missing, duplicate, unordered and non-finite chart data', () => {
  for (const mutate of [p => p.candles = [], p => p.candles[0].ts = TS, p => p.candles.reverse(),
    p => p.candles[0].close = null, p => p.candles[0].high = NaN, p => p.candles[0].volume = -1,
    p => p.candles[0].ts /= 1000, p => p.ticker.last = 0]) {
    const value = fixture(); mutate(value);
    assert.throws(() => parseMarketSnapshot(value, 'BTC-USDT', '1h'));
  }
});
test('zero volume is valid, missing prices never become zero', () => {
  assert.equal(parseMarketSnapshot(fixture(), 'BTC-USDT', '1h').candles[0].volume, 0);
  assert.equal(chartNumber(null), '—'); assert.equal(chartNumber(undefined), '—');
  assert.equal(chartVolume(0), '0.00');
});
test('receipt time cannot conceal delayed quote, missing live candle or future data', () => {
  const data = fixture();
  assert.equal(marketFreshness(data, NOW, false), 'observed');
  assert.equal(marketFreshness(data, NOW, true), 'failed');
  assert.equal(marketFreshness(data, NOW + 31000, false), 'stale');
  data.ticker.ts = NOW - 60000;
  assert.equal(marketFreshness(data, NOW, false), 'stale');
  data.ticker = null;
  assert.equal(marketFreshness(data, NOW, false), 'partial');
  data.candles = [candle(TS - 3600000)];
  assert.equal(marketFreshness(data, NOW, false), 'stale');
  data.candles = [candle(TS + 3600000)];
  assert.equal(marketFreshness(data, NOW, false), 'future');
});
test('axis and crosshair use Beijing time regardless of browser timezone', () => {
  assert.equal(chartTime(TS), '2026-09-05 10:00');
  assert.equal(chartTime(TS, 'clock'), '10:00');
  assert.equal(chartTime(TS, 'date'), '09-05');
});
test('moving averages use only the available window and preserve true UTC times', () => {
  const values = [100, 110, 120, 130].map((close, i) => candle(TS + i * 3600000, close));
  const points = movingAverage(values, 3);
  assert.equal(points.length, 2); assert.equal(points[0].value, 110); assert.equal(points[1].value, 120);
  assert.equal(points[0].time, (TS + 2 * 3600000) / 1000);
  assert.equal(movingAverage(values, 7).length, 0);
});
