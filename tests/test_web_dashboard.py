import importlib.util
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "web_dashboard.py"
MONITOR_V2_JS_PATH = REPO_ROOT / "web" / "static" / "js" / "monitor_v2.js"


def load_web_dashboard_module():
    name = f"web_dashboard_test_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _find_headless_browser() -> str | None:
    candidates = [
        shutil.which("chrome"),
        shutil.which("google-chrome"),
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
        shutil.which("msedge"),
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    ]

    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return str(Path(candidate))
    return None


def test_index_renders_monitor_template():
    module = load_web_dashboard_module()
    client = module.app.test_client()

    response = client.get("/")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert 'id="update-time"' in body
    assert 'id="health-content"' in body
    assert 'id="vote-history"' in body
    assert 'id="history-tooltip"' in body
    assert "风险档位（持仓上限）" in body
    assert 'src="/static/js/monitor_v2.js?v=' in body


def test_monitor_v2_static_script_contains_expected_entrypoints():
    body = MONITOR_V2_JS_PATH.read_text(encoding="utf-8")

    assert "renderHmmProbRows" in body
    assert "renderVoteHistory" in body
    assert "showHistoryTooltip" in body
    assert "showHmmProbs:true" in body
    assert "showSummary:true" in body
    assert "浮盈亏 / 收益率" in body
    assert "fmtUsd(pnlValue)" in body
    assert "p.pnl??p.pnl_value||0" not in body
    assert "p.pnlPercent??p.pnl_pct||0" not in body
    assert "loadAll();" in body


def test_monitor_v2_static_script_executes_in_headless_browser(tmp_path):
    browser = _find_headless_browser()
    if not browser:
        pytest.skip("No Chromium-based browser available for JS syntax check")

    script_path = tmp_path / "monitor_v2.js"
    script_path.write_text(MONITOR_V2_JS_PATH.read_text(encoding="utf-8"), encoding="utf-8")

    harness_path = tmp_path / "monitor_v2_harness.html"
    harness_path.write_text(
        f"""<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><title>monitor_v2 harness</title></head>
<body>
<div id="update-time"></div>
<script>
window.fetch = async () => ({{ json: async () => ({{}}) }});
window.setInterval = () => 0;
</script>
<script src="{script_path.as_uri()}"></script>
</body>
</html>
""",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            browser,
            "--headless",
            "--disable-gpu",
            "--allow-file-access-from-files",
            "--enable-logging=stderr",
            "--v=1",
            "--virtual-time-budget=3000",
            "--dump-dom",
            harness_path.as_uri(),
        ],
        capture_output=True,
        text=False,
        timeout=30,
        check=False,
    )

    stdout = (result.stdout or b"").decode("utf-8", errors="ignore")
    stderr = (result.stderr or b"").decode("utf-8", errors="ignore")
    combined_output = "\n".join(part for part in [stdout, stderr] if part)
    assert "SyntaxError" not in combined_output
    assert "Uncaught" not in combined_output
    assert "最近刷新" in stdout


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
        "total_pnl_pct": 0.25,
        "drawdown_pct": 0.05,
        "realized_pnl": 12.5,
        "total_trades": 7,
        "last_update": "2026-03-08 21:00:00",
    }

    monkeypatch.setattr(module, "api_positions", lambda: module.jsonify({"positions": positions_rows}))
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
    assert payload["account"]["totalPnlPercent"] == 0.25
    assert payload["account"]["maxDrawdown"] == 0.05
    assert payload["positions"][0]["symbol"] == "BTC"
    assert payload["positions"][0]["pnlPercent"] == 0.1
    assert payload["trades"][0]["symbol"] == "BTC/USDT"
    assert payload["alphaScores"][0]["symbol"] == "BTC/USDT"
    assert payload["systemStatus"]["errors"] == ["systemctl is not available"]


def test_dashboard_api_keeps_sub_one_percent_pnl_as_ratio(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "api_positions", lambda: module.jsonify({"positions": [{
        "symbol": "OKB",
        "qty": 0.67,
        "avg_px": 97.197842,
        "last_price": 96.67,
        "value_usdt": 65.2311,
        "pnl_value": -0.3562,
        "pnl_pct": -0.0054,
    }]}))
    monkeypatch.setattr(module, "api_account", lambda: module.jsonify({
        "cash_usdt": 100.0,
        "positions_value_usdt": 65.2311,
        "total_equity_usdt": 165.2311,
        "total_pnl_pct": 0.005,
        "drawdown_pct": 0.0075,
        "realized_pnl": 1.25,
        "total_trades": 3,
        "last_update": "2026-03-09 21:00:00",
    }))
    monkeypatch.setattr(module, "api_trades", lambda: module.jsonify({"trades": []}))
    monkeypatch.setattr(module, "api_scores", lambda: module.jsonify({"scores": []}))
    monkeypatch.setattr(module, "api_status", lambda: module.jsonify({}))
    monkeypatch.setattr(module, "api_equity_history", lambda: module.jsonify([]))
    monkeypatch.setattr(module, "api_market_state", lambda: module.jsonify({}))
    monkeypatch.setattr(module, "api_timers", lambda: module.jsonify({"timers": [], "next_run": None}))
    monkeypatch.setattr(module, "api_cost_calibration", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ic_diagnostics", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ml_training", lambda: module.jsonify({"status": "idle"}))
    monkeypatch.setattr(module, "api_reflection_reports", lambda: module.jsonify({"reports": []}))

    response = client.get("/api/dashboard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["account"]["totalPnlPercent"] == 0.005
    assert payload["account"]["maxDrawdown"] == 0.0075
    assert payload["positions"][0]["pnlPercent"] == -0.0054


def test_auto_risk_guard_api_uses_auto_risk_eval_file(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)

    (tmp_path / "auto_risk_eval.json").write_text(
        """
        {
          "ts": "2026-03-09T22:30:02",
          "current_level": "DEFENSE",
          "metrics": {
            "dd_pct": 0.0682,
            "conversion_rate": 0.0
          },
          "reason": "样本不足 (0轮)，维持当前档位"
        }
        """.strip(),
        encoding="utf-8",
    )

    response = client.get("/api/auto_risk_guard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_level"] == "DEFENSE"
    assert payload["metrics"]["dd_pct"] == 0.0682
    assert payload["reason"] == "样本不足 (0轮)，维持当前档位"
    assert payload["config"]["max_positions"] == 3


def test_market_state_backfills_hmm_vote_from_regime_history(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "_load_market_state_snapshot", lambda _: {
        "state": "TRENDING",
        "position_multiplier": 1.2,
        "method": "decision_audit",
        "votes": {"hmm": {"state": "TRENDING", "confidence": 0.0}},
        "alerts": [],
        "monitor": {},
    })
    monkeypatch.setattr(module, "_load_latest_regime_history_snapshot", lambda _: {
        "votes": {
            "hmm": {
                "state": "TRENDING",
                "confidence": 0.62,
                "weight": 0.35,
                "probs": {"TrendingUp": 0.62, "Sideways": 0.28, "TrendingDown": 0.10},
            }
        }
    })
    monkeypatch.setattr(module, "load_config", lambda: {"regime": {"hmm_weight": 0.35, "funding_weight": 0.4, "rss_weight": 0.25}})
    monkeypatch.setattr(module, "_signal_health", lambda *args, **kwargs: {"status": "fresh", "is_fresh": True, "error": None})
    monkeypatch.setattr(module, "_build_live_funding_vote", lambda *args, **kwargs: {})
    monkeypatch.setattr(module, "_build_live_rss_vote", lambda *args, **kwargs: {})
    monkeypatch.setattr(module, "calculate_market_indicators", lambda: {"price": 0.0})

    response = client.get("/api/market_state")

    assert response.status_code == 200
    payload = response.get_json()
    hmm = payload["votes"]["hmm"]
    assert hmm["confidence"] == 0.62
    assert hmm["weight"] == 0.35
    assert hmm["probs"]["TrendingUp"] == 0.62


def test_market_state_returns_vote_history_and_live_rss_summary(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "_load_market_state_snapshot", lambda _: {
        "state": "SIDEWAYS",
        "position_multiplier": 0.8,
        "method": "decision_audit",
        "votes": {
            "hmm": {"state": "SIDEWAYS", "confidence": 0.7, "probs": {"TrendingUp": 0.1, "Sideways": 0.7, "TrendingDown": 0.2}},
            "rss": {"state": "SIDEWAYS", "confidence": 0.1},
        },
        "alerts": [],
        "monitor": {},
    })
    monkeypatch.setattr(module, "_load_latest_regime_history_snapshot", lambda _: {"votes": {}})
    monkeypatch.setattr(module, "_load_market_vote_history", lambda *args, **kwargs: [{
        "label": "03-09 14:00",
        "final": {"state": "SIDEWAYS", "confidence": 0.3, "score": 0.2},
        "votes": {
            "hmm": {"state": "SIDEWAYS", "confidence": 0.7},
            "funding": {"state": "SIDEWAYS", "confidence": 0.2, "sentiment": 0.01},
            "rss": {"state": "RISK_OFF", "confidence": 0.35, "sentiment": -0.3},
        },
    }])
    monkeypatch.setattr(module, "load_config", lambda: {"regime": {"hmm_weight": 0.35, "funding_weight": 0.4, "rss_weight": 0.25}})
    monkeypatch.setattr(module, "_signal_health", lambda *args, **kwargs: {"status": "fresh", "is_fresh": True, "error": None})
    monkeypatch.setattr(module, "_build_live_funding_vote", lambda *args, **kwargs: {})
    monkeypatch.setattr(module, "_build_live_rss_vote", lambda *args, **kwargs: {
        "state": "RISK_OFF",
        "confidence": 0.35,
        "weight": 0.25,
        "sentiment": -0.3,
        "summary_short": "\u65b0\u95fb\u504f\u7a7a\uff0c\u4f46\u672a\u5230\u6781\u7aef",
    })
    monkeypatch.setattr(module, "calculate_market_indicators", lambda: {"price": 0.0})

    response = client.get("/api/market_state")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["history_24h"][0]["votes"]["rss"]["state"] == "RISK_OFF"
    assert payload["history_24h"][0]["votes"]["rss"]["sentiment"] == -0.3
    assert payload["votes"]["rss"]["summary_short"] == "\u65b0\u95fb\u504f\u7a7a\uff0c\u4f46\u672a\u5230\u6781\u7aef"
