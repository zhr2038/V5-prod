"""Render the real TSX component with observed/missing market inputs."""
import json
import shutil
import subprocess
from pathlib import Path

import pytest

FRONTEND = Path(__file__).resolve().parents[1] / "web/dashboard"
NODE = shutil.which("node")

RENDER = r"""
const fs = require('node:fs');
const vm = require('node:vm');
const ts = require('typescript');
const React = require('react');
const { renderToStaticMarkup } = require('react-dom/server');
const fixtures = JSON.parse(fs.readFileSync(0, 'utf8'));
const options = { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, target: ts.ScriptTarget.ES2022 };
function compile(path) { return ts.transpileModule(fs.readFileSync(path,'utf8'), { compilerOptions: options }).outputText; }
const format = { exports: {} };
vm.runInNewContext(compile('src/lib/format.ts'), { module: format, exports: format.exports, require });
const component = compile('src/components/PositionsPanel.tsx');
const rendered = fixtures.map(fixture => {
  let seeded = false;
  const react = { ...React, useState(initial) {
    // The first nullable state is the asynchronously loaded Kline payload.
    if (initial === null && !seeded) { seeded = true; return React.useState(fixture.kline); }
    return React.useState(initial);
  }};
  const module = { exports: {} };
  const localRequire = name => {
    if (name === 'react') return react;
    if (name === '../lib/format') return format.exports;
    if (name === '../api') return {
      api: new Proxy({}, { get() { throw new Error('render test cannot request network data'); } }),
      summarizeTradeOrders: rows => [...rows],
    };
    if (name === '../hooks/useInterval') return { useInterval() {} };
    if (name === '../hooks/useDataPulse') return { useDataPulse() { return { className:'', dataPulse:undefined }; } };
    return require(name);
  };
  vm.runInNewContext(component, { module, exports: module.exports, require:localRequire });
  return renderToStaticMarkup(React.createElement(module.exports.PositionsPanel, fixture.props));
});
process.stdout.write(JSON.stringify(rendered));
"""


def candle(close):
    return {"timestamp": 1788573600, "open": close, "high": close + 1, "low": close - 1, "close": close, "volume": 10}


@pytest.fixture(scope="module")
def panels():
    if not NODE or not (FRONTEND / "node_modules/typescript").is_dir():
        pytest.skip("render test requires installed dashboard Node dependencies")
    history = {"id": "old-buy", "timestamp": "2026-02-03 12:00:00", "symbol": "BTC-USDT", "side": "buy",
               "price": 63964.90, "qty": .001, "value": 63.9649, "fee": .01}
    btc = {"symbol": "BTC-USDT", "timeframe": "1h", "candles": [candle(79400), candle(79519.55)]}
    fixtures = [
        {"props": {"focusSymbol": "BTC-USDT"}, "kline": None},
        {"props": {"focusSymbol": "BTC-USDT"}, "kline": {"symbol": "BTC-USDT", "timeframe": "1h", "candles": [],
                                                          "summary": {"open": 0, "close": 0, "high": 0, "low": 0, "change_pct": 0}}},
        {"props": {"focusSymbol": "BTC-USDT", "trades": [history]}, "kline": btc},
        {"props": {"focusSymbol": "BTC-USDT", "trades": [history], "positions": [
            {"symbol": "BTC-USDT", "qty": .001, "value": 79.5, "avgPrice": 0, "currentPrice": 79519.55, "pnl": None, "pnlPercent": None}]}, "kline": btc},
        {"props": {"focusSymbol": "BTC-USDT", "positions": [
            {"symbol": "ETH-USDT", "qty": 1, "value": 2000, "avgPrice": 1900, "currentPrice": 2000}]},
         "kline": {"symbol": "ETH-USDT", "timeframe": "1h", "candles": [candle(2000)]}},
        {"props": {"focusSymbol": "BTC-USDT"}, "kline": {**btc, "timeframe": "1d"}},
        {"props": {"focusSymbol": "BTC-USDT"}, "kline": {"symbol": "BTC-USDT", "timeframe": "1h", "candles": [candle(100), candle(100)]}},
        {"props": {"focusSymbol": "BTC-USDT"}, "kline": {"symbol": "BTC-USDT", "timeframe": "1h", "candles": [{**candle(100), "close": None}]}},
    ]
    result = subprocess.run([NODE, "-e", RENDER], cwd=FRONTEND, input=json.dumps(fixtures),
                            text=True, encoding="utf-8", capture_output=True, timeout=30, check=True)
    return json.loads(result.stdout)


@pytest.mark.parametrize("index", [0, 1, 7])
def test_missing_loading_or_invalid_kline_never_renders_zero_price_or_return(panels, index):
    html = panels[index]
    assert "$0.00" not in html
    assert "0.00%" not in html
    assert "最近收盘" in html and "—" in html
    assert ("行情读取中" if index == 0 else "暂无K线数据") in html


def test_flat_market_history_has_explicit_price_and_time_labels_without_cost_line(panels):
    html = panels[2]
    assert "$79519.55" in html
    assert "历史最近成交价" in html and "$63964.90" in html
    assert "历史成交时间" in html and "2026-02-03 12:00:00" in html
    assert "当前无持仓" in html
    assert "持仓均价" not in html
    assert "kline-reference-line" not in html


def test_unknown_current_position_cost_cannot_fall_back_to_old_buy(panels):
    html = panels[3]
    assert "$79519.55" in html
    assert "$63964.90" not in html
    assert "kline-reference-line" not in html


@pytest.mark.parametrize("index", [4, 5])
def test_another_symbol_or_timeframe_does_not_leak_into_focused_market(panels, index):
    html = panels[index]
    assert "行情读取中" in html
    assert "$2000.00" not in html
    assert "$1900.00" not in html
    assert "$79519.55" not in html
    assert "持仓均价" not in html


def test_real_unchanged_prices_preserve_observed_zero_change(panels):
    html = panels[6]
    assert "$100.00" in html
    assert "$0.00" in html
    assert "0.00%" in html
