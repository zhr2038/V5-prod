import importlib.util
import json
import os
import uuid
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "web_dashboard.py"


def load_web_dashboard_module():
    name = f"web_dashboard_test_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_index_renders_monitor_template():
    module = load_web_dashboard_module()
    client = module.app.test_client()

    response = client.get("/")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'id="update-time"' in body
    assert 'id="health-content"' in body
    assert 'id="positions-content"' in body
    assert 'id="ensemble-votes"' in body
    assert "loadAll();" in body


def test_timer_endpoints_degrade_without_systemctl(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()
    monkeypatch.setattr(module, "SYSTEMCTL_BIN", None)

    status_response = client.get("/api/status")
    timer_response = client.get("/api/timer")
    timers_response = client.get("/api/timers")

    assert status_response.status_code == 200
    assert timer_response.status_code == 200
    assert timers_response.status_code == 200

    status_payload = status_response.get_json()
    timer_payload = timer_response.get_json()
    timers_payload = timers_response.get_json()

    assert status_payload["timer_active"] is False
    assert "systemctl" in status_payload["timer_error"]
    assert timer_payload["next_run"] is None
    assert "systemctl" in timer_payload["error"]
    assert timers_payload["timers"]
    timer_names = [timer["name"] for timer in timers_payload["timers"]]
    assert "v5-prod.user.timer" in timer_names
    assert "v5-sentiment-collect.timer" in timer_names
    assert "v5-live-20u.user.timer" not in timer_names
    assert all("error" in timer for timer in timers_payload["timers"])


def test_dashboard_api_uses_expected_payload_shapes(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    positions_rows = [{
        "symbol": "BTC",
        "qty": 0.1,
        "avg_px": 50000.0,
        "last_price": 55000.0,
        "value_usdt": 5500.0,
        "price": 55000.0,
        "value": 5500.0,
        "quantity": 0.1,
        "pnl_pct": 0.1,
    }]
    account_payload = {
        "cash_usdt": 100.0,
        "positions_value_usdt": 5500.0,
        "total_equity_usdt": 5600.0,
        "initial_capital_usdt": 120.0,
        "equity_delta_usdt": 5480.0,
        "total_pnl_pct": 0.25,
        "drawdown_pct": 0.05,
        "realized_pnl": 12.5,
        "total_trades": 7,
        "last_update": "2026-03-08 21:00:00",
    }

    monkeypatch.setattr(module, "api_positions", lambda: module.jsonify(positions_rows))
    monkeypatch.setattr(module, "api_account", lambda: module.jsonify(account_payload))
    monkeypatch.setattr(module, "api_trades", lambda: module.jsonify({"trades": [{
        "time": "2026-03-08 20:00:00",
        "symbol": "BTC-USDT",
        "side": "buy",
        "amount": 100.0,
        "fee": -0.1,
    }]}))
    monkeypatch.setattr(module, "api_scores", lambda: module.jsonify({"scores": [{
        "symbol": "BTC-USDT",
        "score": 0.9,
    }]}))
    monkeypatch.setattr(module, "api_status", lambda: module.jsonify({
        "timer_active": False,
        "dry_run": True,
        "timer_error": "systemctl is not available",
    }))
    monkeypatch.setattr(module, "api_equity_history", lambda: module.jsonify([{
        "timestamp": "2026-03-08T21:00:00",
        "value": 5600.0,
    }]))
    monkeypatch.setattr(module, "api_market_state", lambda: module.jsonify({
        "state": "TRENDING",
        "position_multiplier": 1.2,
    }))
    monkeypatch.setattr(module, "api_timers", lambda: module.jsonify({
        "timers": [{"name": "v5-prod.user.timer", "active": False, "enabled": False, "next_run": None}],
        "next_run": None,
    }))
    monkeypatch.setattr(module, "api_cost_calibration", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ic_diagnostics", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ml_training", lambda: module.jsonify({"status": "idle"}))
    monkeypatch.setattr(module, "api_reflection_reports", lambda: module.jsonify({"reports": []}))

    response = client.get("/api/dashboard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["account"]["totalEquity"] == 5600.0
    assert payload["account"]["totalPnl"] == 5480.0
    assert payload["account"]["realizedPnl"] == 12.5
    assert payload["account"]["initialCapital"] == 120.0
    assert payload["account"]["totalPnlPercent"] == 25.0
    assert payload["account"]["maxDrawdown"] == 5.0
    assert payload["positions"][0]["symbol"] == "BTC"
    assert payload["trades"][0]["symbol"] == "BTC/USDT"
    assert payload["alphaScores"][0]["symbol"] == "BTC/USDT"
    assert payload["systemStatus"]["errors"] == ["systemctl is not available"]


def test_dashboard_positions_prefer_precomputed_pnl(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "api_positions", lambda: module.jsonify([{
        "symbol": "OKB",
        "qty": 1.0,
        "avg_px": 40.0,
        "last_price": 40.0,
        "value_usdt": 40.0,
        "pnl_value": 5.5,
        "pnl_pct": 0.1375,
    }]))
    monkeypatch.setattr(module, "api_account", lambda: module.jsonify({
        "cash_usdt": 10.0,
        "positions_value_usdt": 40.0,
        "total_equity_usdt": 50.0,
        "initial_capital_usdt": 45.0,
        "equity_delta_usdt": 5.0,
        "total_pnl_pct": 0.1111,
        "drawdown_pct": 0.02,
        "realized_pnl": -12.0,
        "total_trades": 3,
        "last_update": "2026-03-09 12:00:00",
    }))
    monkeypatch.setattr(module, "api_trades", lambda: module.jsonify({"trades": []}))
    monkeypatch.setattr(module, "api_scores", lambda: module.jsonify({"scores": []}))
    monkeypatch.setattr(module, "api_status", lambda: module.jsonify({"timer_active": True, "dry_run": False}))
    monkeypatch.setattr(module, "api_equity_history", lambda: module.jsonify([]))
    monkeypatch.setattr(module, "api_market_state", lambda: module.jsonify({"state": "SIDEWAYS", "position_multiplier": 1.0}))
    monkeypatch.setattr(module, "api_timers", lambda: module.jsonify({"timers": [], "next_run": None}))
    monkeypatch.setattr(module, "api_cost_calibration", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ic_diagnostics", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ml_training", lambda: module.jsonify({"status": "idle"}))
    monkeypatch.setattr(module, "api_reflection_reports", lambda: module.jsonify({"reports": []}))

    response = client.get("/api/dashboard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["positions"][0]["pnl"] == 5.5
    assert payload["positions"][0]["pnlPercent"] == 13.75
    assert payload["account"]["totalPnl"] == 5.0


def _write_cache(path: Path, payload: dict, *, age_sec: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    ts = path.stat().st_mtime - age_sec
    os.utime(path, (ts, ts))


def test_market_state_exposes_stale_signal_health(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    cache_dir = tmp_path / "data" / "sentiment_cache"
    _write_cache(cache_dir / "funding_COMPOSITE_20260309_00.json", {"f6_sentiment": -0.1}, age_sec=3600)
    _write_cache(cache_dir / "rss_MARKET_20260309_00.json", {"f6_sentiment": -0.2}, age_sec=3600)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "load_config", lambda: {"regime": {"funding_signal_max_age_minutes": 30, "rss_signal_max_age_minutes": 30}})
    monkeypatch.setattr(module, "calculate_market_indicators", lambda: {"price": 12345.0})
    monkeypatch.setattr(
        module,
        "_load_market_state_snapshot",
        lambda reports_dir: {
            "state": "SIDEWAYS",
            "position_multiplier": 0.8,
            "method": "decision_audit",
            "final_score": 0.12,
            "votes": {"hmm": {"state": "SIDEWAYS", "weight": 0.35, "confidence": 0.6}},
            "alerts": [],
            "monitor": {},
        },
    )

    response = client.get("/api/market_state")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["signal_health"]["funding"]["status"] == "stale"
    assert payload["signal_health"]["rss"]["status"] == "stale"
    assert "funding_signal_stale_or_missing" in payload["alerts"]
    assert "rss_signal_stale_or_missing" in payload["alerts"]
    assert payload["votes"]["funding"]["error"] == "funding_signal_stale_or_missing"
    assert payload["votes"]["rss"]["error"] == "rss_signal_stale_or_missing"


def test_market_state_refreshes_live_votes_when_cache_is_fresh(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    cache_dir = tmp_path / "data" / "sentiment_cache"
    _write_cache(
        cache_dir / "funding_COMPOSITE_20260309_01.json",
        {"f6_sentiment": 0.42, "tier_breakdown": {"BTC": 0.4}},
        age_sec=60,
    )
    _write_cache(
        cache_dir / "rss_MARKET_20260309_01.json",
        {"f6_sentiment": -0.35, "f6_sentiment_summary": "risk off"},
        age_sec=60,
    )

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "regime": {
                "funding_signal_max_age_minutes": 30,
                "rss_signal_max_age_minutes": 30,
                "funding_weight": 0.35,
                "rss_weight": 0.25,
            }
        },
    )
    monkeypatch.setattr(module, "calculate_market_indicators", lambda: {"price": 12345.0})
    monkeypatch.setattr(
        module,
        "_load_market_state_snapshot",
        lambda reports_dir: {
            "state": "SIDEWAYS",
            "position_multiplier": 0.8,
            "method": "decision_audit",
            "final_score": 0.12,
            "votes": {
                "hmm": {"state": "SIDEWAYS", "weight": 0.35, "confidence": 0.6},
                "funding": {"state": None, "weight": 0, "confidence": 0, "error": "funding_signal_stale_or_missing"},
                "rss": {"state": None, "weight": 0, "confidence": 0, "error": "rss_signal_stale_or_missing"},
            },
            "alerts": ["funding_signal_stale_or_missing", "rss_signal_stale_or_missing"],
            "monitor": {},
        },
    )

    response = client.get("/api/market_state")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["signal_health"]["funding"]["status"] == "fresh"
    assert payload["signal_health"]["rss"]["status"] == "fresh"
    assert payload["votes"]["funding"]["state"] == "TRENDING"
    assert payload["votes"]["funding"].get("error") is None
    assert payload["votes"]["rss"]["state"] == "RISK_OFF"
    assert payload["votes"]["rss"].get("error") is None
    assert "funding_signal_stale_or_missing" not in payload["alerts"]
    assert "rss_signal_stale_or_missing" not in payload["alerts"]


def test_health_endpoint_exposes_summary_fields():
    module = load_web_dashboard_module()
    client = module.app.test_client()

    response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert "status" in payload
    assert "last_update" in payload
    assert "warning_count" in payload
    assert "critical_count" in payload
