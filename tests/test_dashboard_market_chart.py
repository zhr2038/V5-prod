"""Exchange chart integrity and public-only HTTP contract."""
import copy
import importlib.util
from pathlib import Path

import pytest
import requests

from src.reporting import dashboard_market_chart as market

TS = 1788573600000
NOW = TS + 600000


def rows():
    return [[str(TS), "100", "102", "99", "101", "0", "0", "0", "0"],
            [str(TS - 3600000), "100", "102", "99", "100", "5", "500", "500", "1"]]


def ticker(symbol="BTC-USDT"):
    return [{"instType": "SPOT", "instId": symbol, "ts": str(NOW), "last": "101", "open24h": "100",
             "high24h": "102", "low24h": "99", "vol24h": "0", "volCcy24h": "0"}]


@pytest.fixture(autouse=True)
def isolated_cache(monkeypatch):
    monkeypatch.setattr(market, "_CACHE", {})
    monkeypatch.setattr(market.time, "time", lambda: NOW / 1000)


def test_real_exchange_order_is_sorted_without_losing_open_candle_or_zero_volume():
    candles = market.normalize_candles(rows(), "1h", NOW)
    assert [c["ts"] for c in candles] == [TS - 3600000, TS]
    assert candles[-1]["closed"] is False
    assert candles[-1]["volume"] == 0
    assert candles[0]["quote_volume"] == 500


@pytest.mark.parametrize("field,value", [(0, "1788577200000"), (1, "NaN"), (2, "99"), (3, "102"),
                                         (4, "0"), (5, "-1"), (7, None), (8, "unknown")])
def test_invalid_candles_fail_explicitly(field, value):
    bad = rows()
    bad[0][field] = value
    with pytest.raises(market.MarketChartError):
        market.normalize_candles(bad, "1h", NOW)


@pytest.mark.parametrize("replacement", [TS, TS - 7200000])
def test_duplicate_or_missing_candle_never_gets_silently_drawn(replacement):
    bad = rows()
    bad[1][0] = str(replacement)
    with pytest.raises(market.MarketChartError, match="duplicate_or_missing"):
        market.normalize_candles(bad, "1h", NOW)


def test_wrong_instrument_ticker_is_not_attributed_to_current_chart():
    with pytest.raises(market.MarketChartError, match="identity"):
        market.normalize_ticker(ticker("ETH-USDT"), "BTC-USDT", NOW)


def test_cache_is_bounded_to_valid_selections_and_does_not_update_receipt_time(monkeypatch):
    calls = []
    def get(endpoint, params):
        calls.append((endpoint, params))
        return rows() if endpoint == "candles" else ticker()
    monkeypatch.setattr(market, "_public_get", get)
    first = market.get_market_chart("BTC/USDT", "1h")
    first["candles"].clear()  # A caller cannot corrupt the shared cache.
    monkeypatch.setattr(market.time, "time", lambda: NOW / 1000 + 3)
    second = market.get_market_chart("BTC-USDT", "1h")
    assert second["fetched_at_ms"] == NOW
    assert len(second["candles"]) == 2
    assert len(calls) == 2
    assert calls[0] == ("candles", {"instId": "BTC-USDT", "bar": "1H", "limit": "300"})
    for symbol, timeframe in [("DOGE-USDT", "1h"), ("BTC-USDT", "1H"), ("https://other.invalid", "1h")]:
        with pytest.raises(ValueError):
            market.get_market_chart(symbol, timeframe)
    assert len(calls) == 2


def test_ticker_outage_preserves_valid_candles_with_explicit_partial_status(monkeypatch):
    def get(endpoint, params):
        if endpoint == "ticker":
            raise market.MarketChartError("market_http_unavailable")
        return rows()
    monkeypatch.setattr(market, "_public_get", get)
    result = market.get_market_chart("BTC-USDT", "1h")
    assert result["ticker"] is None
    assert result["ticker_error"] == "market_http_unavailable"
    assert len(result["candles"]) == 2


def test_transport_uses_only_unsigned_fixed_public_get_and_hides_upstream_errors(monkeypatch):
    def get(url, **kwargs):
        assert url == "https://www.okx.com/api/v5/market/candles"
        assert kwargs == {"params": {"instId": "BTC-USDT"}, "timeout": (2, 3), "allow_redirects": False}
        raise requests.Timeout("private proxy details")
    monkeypatch.setattr(market.requests, "get", get)
    with pytest.raises(market.MarketChartError, match="^market_transport_unavailable$"):
        market._public_get("candles", {"instId": "BTC-USDT"})


def test_daily_candles_have_utc_boundaries(monkeypatch):
    def get(endpoint, params):
        if endpoint == "ticker":
            return ticker()
        assert params["bar"] == "1Dutc"
        result = [rows()[0]]
        result[0][0] = str(TS // 86400000 * 86400000)
        return result
    monkeypatch.setattr(market, "_public_get", get)
    assert market.get_market_chart("BTC-USDT", "1d")["day_boundary"] == "UTC"


def test_http_route_never_loads_account_config_or_accepts_post(monkeypatch):
    spec = importlib.util.spec_from_file_location("market_chart_route_test", Path(__file__).resolve().parents[1] / "scripts/web_dashboard.py")
    dashboard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dashboard)
    def forbidden(*args, **kwargs):
        raise AssertionError("chart must not load account or trading configuration")
    monkeypatch.setattr(dashboard, "load_config", forbidden)
    for name in ("api_account", "api_auto_risk_guard", "api_decision_audit"):
        monkeypatch.setattr(dashboard, name, forbidden)
    monkeypatch.setattr(market, "_public_get", lambda endpoint, params: copy.deepcopy(rows() if endpoint == "candles" else ticker()))
    client = dashboard.app.test_client()
    response = client.get("/api/market_chart?symbol=BTC-USDT&timeframe=1h")
    assert response.status_code == 200 and response.json["read_only"] is True
    assert "no-store" in response.headers["Cache-Control"]
    assert client.post("/api/market_chart").status_code == 405
    assert client.get("/api/market_chart?symbol=unknown").status_code == 400
    monkeypatch.setattr(market, "_CACHE", {})
    def unavailable(*args):
        raise market.MarketChartError("market_http_unavailable")
    monkeypatch.setattr(market, "_public_get", unavailable)
    failed = client.get("/api/market_chart?symbol=BTC-USDT&timeframe=1h")
    assert failed.status_code == 502 and "candles" not in failed.json
