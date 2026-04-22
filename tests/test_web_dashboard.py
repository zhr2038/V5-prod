import importlib.util
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
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


def test_resolve_config_path_uses_runtime_helper(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(
        module,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str((tmp_path / "configs" / "runtime.yaml").resolve()),
    )

    assert module._resolve_config_path() == (tmp_path / "configs" / "runtime.yaml").resolve()


def test_load_config_uses_runtime_helper_dynamically(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    cfg_path = tmp_path / "configs" / "runtime.yaml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text("alpha:\n  long_top_pct: 0.42\n", encoding="utf-8")
    monkeypatch.setattr(
        module,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(cfg_path.resolve()),
    )

    payload = module.load_config()

    assert payload["alpha"]["long_top_pct"] == 0.42


def test_load_config_surfaces_invalid_config(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    cfg_path = tmp_path / "configs" / "runtime.yaml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text("symbols:\n  - BTCUSDT\n", encoding="utf-8")
    monkeypatch.setattr(
        module,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(cfg_path.resolve()),
    )
    monkeypatch.setattr(
        module,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str((tmp_path / ".env").resolve()),
    )

    with pytest.raises(Exception, match="invalid symbol format"):
        module.load_config()


def test_multi_strategy_score_transform_uses_dynamic_runtime_config(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    cfg_path = tmp_path / "configs" / "runtime.yaml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        "alpha:\n  multi_strategy_score_transform: clip\n  multi_strategy_score_transform_scale: 2.5\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        module,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(cfg_path.resolve()),
    )

    mode, scale = module._load_multi_strategy_score_transform()

    assert mode == "clip"
    assert scale == 2.5


def _assert_internal_error_hidden(body: str, *fragments: str):
    assert "internal server error" in body
    assert "Traceback" not in body
    for fragment in fragments:
        assert fragment not in body


def _assert_body_hides_internal_details(body: str, *fragments: str):
    assert "Traceback" not in body
    for fragment in fragments:
        assert fragment not in body


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
        if candidate and os.path.exists(candidate):
            return candidate
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
    assert 'id="position-kline-chart"' in body
    assert 'id="position-kline-symbols"' in body
    assert 'id="position-kline-timeframes"' in body
    assert 'id="ml-impact-headline"' in body
    assert 'id="ml-impact-subtitle"' in body
    assert 'id="ml-impact-content"' in body
    assert 'src="/static/js/monitor_v2.js?v=' in body
    assert 'src="/static/js/ml_status_panel.js?v=' in body


def test_static_files_serves_assets_and_spa_fallback(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    build_root = tmp_path / "web" / "dist"
    build_root.mkdir(parents=True)
    (build_root / "app.js").write_text("APP", encoding="utf-8")
    (build_root / "index.html").write_text("INDEX", encoding="utf-8")
    monkeypatch.setattr(module, "REACT_BUILD_PATH", build_root)
    client = module.app.test_client()

    asset_response = client.get("/app.js")
    fallback_response = client.get("/dashboard/settings")

    assert asset_response.status_code == 200
    assert asset_response.get_data(as_text=True) == "APP"
    assert asset_response.headers["Content-Type"].startswith("application/javascript")
    assert fallback_response.status_code == 200
    assert fallback_response.get_data(as_text=True) == "INDEX"


def test_metrics_route_returns_prometheus_payload(monkeypatch):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "render_prometheus_metrics", lambda workspace=None, config=None: "v5_metrics_exporter_up 1\n")
    monkeypatch.setattr(module, "load_config", lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}})
    client = module.app.test_client()

    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["Content-Type"].startswith("text/plain")
    assert response.get_data(as_text=True) == "v5_metrics_exporter_up 1\n"


def test_static_files_rejects_encoded_path_traversal(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    build_root = tmp_path / "web" / "dist"
    build_root.mkdir(parents=True)
    (build_root / "index.html").write_text("INDEX", encoding="utf-8")
    (tmp_path / "web" / "secret-web.txt").write_text("SECRET_WEB", encoding="utf-8")
    (tmp_path / "secret-root.txt").write_text("SECRET_ROOT", encoding="utf-8")
    monkeypatch.setattr(module, "REACT_BUILD_PATH", build_root)
    client = module.app.test_client()

    for url in (
        "/%2e%2e/secret-web.txt",
        "/..%2Fsecret-web.txt",
        "/%2e%2e%2F%2e%2e%2Fsecret-root.txt",
    ):
        response = client.get(url)
        body = response.get_data(as_text=True)
        assert response.status_code == 404
        assert "SECRET_WEB" not in body
        assert "SECRET_ROOT" not in body

    fallback_response = client.get("/not-a-real-route")

    assert fallback_response.status_code == 200
    assert fallback_response.get_data(as_text=True) == "INDEX"


def test_resolve_react_build_path_ignores_legacy_admin_dist(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.delenv("V5_DASHBOARD_DIST", raising=False)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)

    original_exists = module.Path.exists

    def fake_exists(path_obj):
        if str(path_obj).replace("\\", "/") == "/home/admin/v5-trading-dashboard/dist":
            return True
        return original_exists(path_obj)

    monkeypatch.setattr(module.Path, "exists", fake_exists)

    resolved = module._resolve_react_build_path()

    assert resolved == (tmp_path / "web" / "dist")


def test_monitor_v2_static_script_contains_expected_entrypoints():
    body = MONITOR_V2_JS_PATH.read_text(encoding="utf-8")

    assert "renderHmmProbRows" in body
    assert "renderDerivedVoteRows" in body
    assert "renderVoteHistory" in body
    assert "renderMlSignalCard" in body
    assert "showHistoryTooltip" in body
    assert "showHmmProbs:true" in body
    assert "showStateBars:true" in body
    assert "showSummary:true" in body
    assert "renderShadowMlPanel" in body
    assert "/api/shadow_ml_overlay" in body
    assert "buildCandlestickSvg" in body
    assert "syncPositionSpotlight" in body
    assert "/api/position_kline" in body
    assert "position-kline-timeframes" in body
    assert "metrics.conversion_rate??metrics.last_conversion_rate??null" in body
    assert "account?.drawdown_pct??metrics.dd_pct??metrics.last_dd_pct??null" in body
    assert "alphaScores.slice()" in body
    assert "item.display_score ?? item.score ?? 0" in body
    assert "item.raw_score ?? displayScore" in body
    assert "style=\"left:50%;width:${width}%\"" in body
    assert "style=\"right:50%;width:${width}%\"" in body
    assert "原始 ${fmtNum(rawScore, 3)}" in body
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
    assert status_payload["kill_switch"] is False
    assert "systemctl" in status_payload["timer_error"]
    assert timer_payload["next_run"] is None
    assert "systemctl" in timer_payload["error"]
    assert timers_payload["timers"]
    assert all("error" in timer for timer in timers_payload["timers"])


def test_status_api_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/status")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["timer_active"] is False
    assert payload["mode"] == "unknown"
    assert payload["dry_run"] is True
    assert payload["kill_switch"] is False
    assert payload["equity_cap"] == 0
    assert payload["last_check"] == ""
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml", "live_prod.yaml")


def test_status_api_derives_dry_run_from_live_mode_when_flag_missing(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "load_config", lambda: {
        "execution": {
            "mode": "live",
        },
        "budget": {
            "live_equity_cap_usdt": 321.0,
        },
    })
    monkeypatch.setattr(module, "_resolve_dashboard_runtime_paths", lambda _config=None: module.DashboardRuntimePaths(
        reports_dir=Path("/tmp/reports"),
        orders_db=Path("/tmp/reports/orders.sqlite"),
        fills_db=Path("/tmp/reports/fills.sqlite"),
        positions_db=Path("/tmp/reports/positions.sqlite"),
        kill_switch_path=Path("/tmp/reports/kill_switch.json"),
        reconcile_status_path=Path("/tmp/reports/reconcile_status.json"),
        runs_dir=Path("/tmp/reports/runs"),
        auto_risk_guard_path=Path("/tmp/reports/auto_risk_guard.json"),
        auto_risk_eval_path=Path("/tmp/reports/auto_risk_eval.json"),
        telemetry_db=Path("/tmp/reports/api_telemetry.sqlite"),
    ))
    monkeypatch.setattr(module, "_dashboard_kill_switch_enabled", lambda _path: True)
    monkeypatch.setattr(module, "_pick_timer_name", lambda: "v5-prod.user.timer")
    monkeypatch.setattr(module, "_get_timer_state", lambda _name: {"active": True, "error": None})

    response = client.get("/api/status")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["mode"] == "live"
    assert payload["dry_run"] is False
    assert payload["kill_switch"] is True
    assert payload["equity_cap"] == 321.0


def test_status_api_derives_live_mode_from_legacy_dry_run_flag(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "load_config", lambda: {
        "execution": {
            "dry_run": False,
        },
        "budget": {
            "live_equity_cap_usdt": 123.0,
        },
    })
    monkeypatch.setattr(module, "_resolve_dashboard_runtime_paths", lambda _config=None: module.DashboardRuntimePaths(
        reports_dir=Path("/tmp/reports"),
        orders_db=Path("/tmp/reports/orders.sqlite"),
        fills_db=Path("/tmp/reports/fills.sqlite"),
        positions_db=Path("/tmp/reports/positions.sqlite"),
        kill_switch_path=Path("/tmp/reports/kill_switch.json"),
        reconcile_status_path=Path("/tmp/reports/reconcile_status.json"),
        runs_dir=Path("/tmp/reports/runs"),
        auto_risk_guard_path=Path("/tmp/reports/auto_risk_guard.json"),
        auto_risk_eval_path=Path("/tmp/reports/auto_risk_eval.json"),
        telemetry_db=Path("/tmp/reports/api_telemetry.sqlite"),
    ))
    monkeypatch.setattr(module, "_dashboard_kill_switch_enabled", lambda _path: False)
    monkeypatch.setattr(module, "_pick_timer_name", lambda: "v5-prod.user.timer")
    monkeypatch.setattr(module, "_get_timer_state", lambda _name: {"active": True, "error": None})

    response = client.get("/api/status")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["mode"] == "live"
    assert payload["dry_run"] is False
    assert payload["kill_switch"] is False
    assert payload["equity_cap"] == 123.0


def test_status_api_treats_string_false_legacy_dry_run_as_live(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "load_config", lambda: {
        "execution": {
            "dry_run": "false",
        },
    })
    monkeypatch.setattr(module, "_resolve_dashboard_runtime_paths", lambda _config=None: module.DashboardRuntimePaths(
        reports_dir=Path("/tmp/reports"),
        orders_db=Path("/tmp/reports/orders.sqlite"),
        fills_db=Path("/tmp/reports/fills.sqlite"),
        positions_db=Path("/tmp/reports/positions.sqlite"),
        kill_switch_path=Path("/tmp/reports/kill_switch.json"),
        reconcile_status_path=Path("/tmp/reports/reconcile_status.json"),
        runs_dir=Path("/tmp/reports/runs"),
        auto_risk_guard_path=Path("/tmp/reports/auto_risk_guard.json"),
        auto_risk_eval_path=Path("/tmp/reports/auto_risk_eval.json"),
        telemetry_db=Path("/tmp/reports/api_telemetry.sqlite"),
    ))
    monkeypatch.setattr(module, "_dashboard_kill_switch_enabled", lambda _path: False)
    monkeypatch.setattr(module, "_pick_timer_name", lambda: "v5-prod.user.timer")
    monkeypatch.setattr(module, "_get_timer_state", lambda _name: {"active": True, "error": None})

    response = client.get("/api/status")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["mode"] == "live"
    assert payload["dry_run"] is False


def test_dashboard_kill_switch_enabled_treats_string_false_values_as_false(tmp_path):
    module = load_web_dashboard_module()
    kill_switch_path = tmp_path / "kill_switch.json"

    kill_switch_path.write_text(json.dumps({"enabled": "false"}), encoding="utf-8")
    assert module._dashboard_kill_switch_enabled(kill_switch_path) is False

    kill_switch_path.write_text(json.dumps({"kill_switch": {"active": "0"}}), encoding="utf-8")
    assert module._dashboard_kill_switch_enabled(kill_switch_path) is False

    kill_switch_path.write_text(json.dumps({"kill_switch": {"enabled": "true"}}), encoding="utf-8")
    assert module._dashboard_kill_switch_enabled(kill_switch_path) is True


def test_timer_api_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_pick_timer():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml")

    monkeypatch.setattr(module, "_pick_timer_name", raise_pick_timer)

    response = client.get("/api/timer")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["timer_name"] == "v5-prod.user.timer"
    assert payload["next_run"] is None
    assert payload["countdown_seconds"] == 0
    assert payload["interval_minutes"] == 120
    assert payload["last_check"] == ""
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml", "live_prod.yaml")


def test_timers_api_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_runtime(_timer_name):
        raise FileNotFoundError(r"C:\secret\systemd\timers.json")

    monkeypatch.setattr(module, "_get_timer_runtime", raise_runtime)

    response = client.get("/api/timers")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["timers"] == []
    assert payload["last_update"] == ""
    _assert_internal_error_hidden(body, r"C:\secret\systemd\timers.json", "timers.json")


def test_scores_api_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/scores")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["regime"] == "Error"
    assert payload["current_run"] is None
    assert payload["previous_run"] is None
    assert payload["scores"] == []
    assert payload["last_update"] == ""
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml", "live_prod.yaml")


def test_sentiment_api_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    class BrokenWorkspace:
        def __truediv__(self, _other):
            raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/data/sentiment_cache")

    monkeypatch.setattr(module, "WORKSPACE", BrokenWorkspace())

    response = client.get("/api/sentiment")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["overall"]["sentiment"] == 0.0
    assert payload["overall"]["fear_greed"] == 50
    assert payload["by_symbol"] == {}
    assert payload["last_update"] == ""
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/data/sentiment_cache", "sentiment_cache")


def test_sentiment_api_symbol_error_is_sanitized(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)

    cache_dir = tmp_path / "data" / "sentiment_cache"
    cache_dir.mkdir(parents=True)
    (cache_dir / "rss_BTC-USDT_20260410_01.json").write_text("{}", encoding="utf-8")

    def raise_json_load(_fh):
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/data/sentiment_cache/rss_BTC-USDT_20260410_01.json")

    monkeypatch.setattr(module.json, "load", raise_json_load)

    response = client.get("/api/sentiment")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["by_symbol"]["BTC-USDT"]["error"] == "cache_error"
    _assert_body_hides_internal_details(body, "/home/ubuntu/clawd/v5-prod/data/sentiment_cache", "rss_BTC-USDT_20260410_01.json")


def test_sentiment_api_prefers_latest_cache_across_sources_by_timestamp(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(
        module,
        "api_scores",
        lambda: (module.jsonify({"error": "scores unavailable"}), 500),
    )

    cache_dir = tmp_path / "data" / "sentiment_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    stale_rss = cache_dir / "rss_BTC-USDT_20260410_12.json"
    fresh_funding = cache_dir / "funding_BTC-USDT_20260410_13.json"
    stale_rss.write_text(
        json.dumps(
            {
                "f6_sentiment": -0.2,
                "f6_fear_greed_index": 40,
                "f6_market_stage": "risk_off",
                "f6_sentiment_summary": "stale rss",
                "f6_sentiment_source": "rss",
            }
        ),
        encoding="utf-8",
    )
    fresh_funding.write_text(
        json.dumps(
            {
                "f6_sentiment": 0.45,
                "f6_fear_greed_index": 72,
                "f6_market_stage": "trending",
                "f6_sentiment_summary": "fresh funding",
                "f6_sentiment_source": "funding_rate",
            }
        ),
        encoding="utf-8",
    )

    response = client.get("/api/sentiment")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["by_symbol"]["BTC-USDT"]["sentiment"] == 0.45
    assert payload["by_symbol"]["BTC-USDT"]["source"] == "funding_rate"
    assert payload["by_symbol"]["BTC-USDT"]["cache_file"] == "funding_BTC-USDT_20260410_13.json"
    assert payload["by_symbol"]["BTC-USDT"]["cache_mtime"] == "2026-04-10 13:00:00"
    assert payload["last_update"] == "2026-04-10 13:00:00"


def test_dashboard_api_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_dashboard_api(*_args, **_kwargs):
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml")

    monkeypatch.setattr(module, "_call_dashboard_api", raise_dashboard_api)

    response = client.get("/api/dashboard")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["account"]["totalEquity"] == 0.0
    assert payload["positions"] == []
    assert payload["trades"] == []
    assert payload["systemStatus"]["errors"] == ["internal server error"]
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml", "live_prod.yaml")


def test_cost_calibration_api_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/reports/cost_stats_real")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/cost_calibration")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["status"] == "error"
    assert payload["daily_stats"] == []
    assert payload["last_update"] == ""
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/reports/cost_stats_real", "cost_stats_real")


def test_ic_diagnostics_api_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/reports/ic_diagnostics_20260410.json")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/ic_diagnostics")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["status"] == "error"
    assert payload["factors"] == []
    assert payload["last_update"] == ""
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/reports/ic_diagnostics_20260410.json", "ic_diagnostics_20260410.json")


def test_shadow_test_api_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/reports/ab_gate_status.json")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/shadow_test")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["status"] == "error"
    assert payload["window_rounds"] == 0
    assert payload["ab_gate_status"] == "error"
    assert payload["ab_gate_error"] == "internal error"
    assert payload["matrix"] == []
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/reports/ab_gate_status.json", "ab_gate_status.json")


def test_health_api_component_errors_hide_internal_paths(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    class FakeRuntimePaths:
        def __init__(self, orders_db):
            self.orders_db = orders_db

    orders_db = tmp_path / "orders.sqlite"
    orders_db.write_text("", encoding="utf-8")

    monkeypatch.setattr(module, "load_config", lambda: {})
    monkeypatch.setattr(module, "_resolve_dashboard_runtime_paths", lambda _config: FakeRuntimePaths(orders_db))
    monkeypatch.setattr(module, "_pick_timer_name", lambda: "v5-prod.user.timer")
    monkeypatch.setattr(
        module,
        "_get_timer_state",
        lambda _name: {"error": r"C:\secret\systemd\timers.json", "active": False},
    )
    monkeypatch.setattr(
        module.sqlite3,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(FileNotFoundError("/home/ubuntu/clawd/v5-prod/orders.sqlite")),
    )

    def raise_creds():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml")

    monkeypatch.setattr(module, "_load_workspace_exchange_creds", raise_creds)

    class BrokenDisk:
        @staticmethod
        def disk_usage(_path):
            raise FileNotFoundError("/home/ubuntu/clawd/v5-prod")

    monkeypatch.setitem(sys.modules, "shutil", BrokenDisk)

    response = client.get("/api/health")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    payload = response.get_json()
    details = {check["name"]: check["detail"] for check in payload["checks"]}
    assert details["定时任务"] == "timer warning"
    assert details["数据库"] == "database status unavailable"
    assert details["OKX API"] == "okx api unavailable"
    assert details["磁盘空间"] == "disk status unavailable"
    _assert_body_hides_internal_details(
        body,
        r"C:\secret\systemd\timers.json",
        "/home/ubuntu/clawd/v5-prod/orders.sqlite",
        "/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml",
    )


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
        "kill_switch": True,
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
    monkeypatch.setattr(
        module,
        "_load_api_telemetry_summary",
        lambda runtime_paths=None, lookback_hours=24: {
            "status": "warning",
            "lookbackHours": lookback_hours,
            "totalRequests": 42,
            "successRate": 0.976,
            "errorCount": 1,
            "rateLimitedCount": 1,
            "p50LatencyMs": 82.0,
            "p95LatencyMs": 340.0,
            "lastRequestAt": "2026-03-08 21:00:00",
            "lastErrorAt": "2026-03-08 20:55:00",
            "latestError": {"method": "GET", "endpoint": "/api/v5/market/ticker", "okxCode": "50011"},
            "note": "API 出现限流或延迟抬升",
        },
    )
    monkeypatch.setattr(
        module,
        "_load_slippage_insights",
        lambda runtime_paths=None, cfg=None, lookback_days=14: {
            "status": "warning",
            "lookbackDays": lookback_days,
            "sampleCount": 18,
            "actualAvgBps": 6.4,
            "actualP50Bps": 4.8,
            "actualP90Bps": 12.2,
            "actualP95Bps": 16.5,
            "actualMinBps": -1.5,
            "actualMaxBps": 28.0,
            "baselineBps": 5.0,
            "baselineLabel": "回测校准 P90",
            "baselineMode": "calibrated",
            "baselineSourceDay": "20260416",
            "bins": [{"label": "0~5", "startBps": 0.0, "endBps": 5.0, "count": 8}],
            "lastFillAt": "2026-03-08 21:00:00",
            "note": "滑点尾部偏高，需关注执行质量",
        },
    )

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
    assert payload["systemStatus"]["killSwitch"] is True
    assert payload["systemStatus"]["errors"] == ["systemctl is not available"]
    assert payload["apiTelemetry"]["status"] == "warning"
    assert payload["apiTelemetry"]["totalRequests"] == 42
    assert payload["apiTelemetry"]["latestError"]["okxCode"] == "50011"
    assert payload["slippageInsights"]["status"] == "warning"
    assert payload["slippageInsights"]["baselineSourceDay"] == "20260416"
    assert payload["slippageInsights"]["bins"][0]["label"] == "0~5"


def test_load_api_telemetry_summary_reads_runtime_db(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    telemetry_db = tmp_path / "reports" / "api_telemetry.sqlite"
    telemetry_db.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(telemetry_db))
    try:
        conn.execute(
            """
            CREATE TABLE api_request_log (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              exchange TEXT NOT NULL,
              method TEXT NOT NULL,
              endpoint TEXT NOT NULL,
              status_class TEXT NOT NULL,
              http_status INTEGER,
              okx_code TEXT,
              okx_msg TEXT,
              duration_ms REAL NOT NULL,
              rate_limited INTEGER NOT NULL DEFAULT 0,
              attempt INTEGER NOT NULL DEFAULT 1,
              error_type TEXT
            )
            """
        )
        rows = [
            (1760000000000, "okx", "GET", "/api/v5/market/ticker", "2xx", 200, "0", "", 80.0, 0, 1, None),
            (1760000001000, "okx", "GET", "/api/v5/market/ticker", "2xx", 200, "0", "", 120.0, 0, 1, None),
            (1760000002000, "okx", "GET", "/api/v5/trade/order", "429", 200, "50011", "rate limit", 900.0, 1, 2, None),
        ]
        conn.executemany(
            """
            INSERT INTO api_request_log(
              ts_ms, exchange, method, endpoint, status_class, http_status,
              okx_code, okx_msg, duration_ms, rate_limited, attempt, error_type
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    class FakeRuntimePaths:
        def __init__(self, telemetry_db_path: Path):
            self.telemetry_db = telemetry_db_path
            self.orders_db = telemetry_db_path.with_name("orders.sqlite")

    monkeypatch.setattr(module.time, "time", lambda: 1760000000.5)

    summary = module._load_api_telemetry_summary(runtime_paths=FakeRuntimePaths(telemetry_db), lookback_hours=24)

    assert summary["status"] == "critical"
    assert summary["totalRequests"] == 3
    assert summary["rateLimitedCount"] == 1
    assert summary["errorCount"] == 1
    assert summary["p50LatencyMs"] == pytest.approx(120.0)
    assert summary["p95LatencyMs"] == pytest.approx(822.0)
    assert summary["latestError"]["okxCode"] == "50011"


def test_load_slippage_insights_reads_cost_events_and_calibrated_baseline(tmp_path):
    module = load_web_dashboard_module()
    orders_db = tmp_path / "reports" / "orders.sqlite"
    events_dir = tmp_path / "reports" / "cost_events"
    stats_dir = tmp_path / "reports" / "cost_stats"
    events_dir.mkdir(parents=True, exist_ok=True)
    stats_dir.mkdir(parents=True, exist_ok=True)

    (events_dir / "20260416.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"event_type": "fill", "slippage_bps": -2.0, "ts": 1760000000}),
                json.dumps({"event_type": "fill", "slippage_bps": 3.0, "ts": 1760000100}),
                json.dumps({"event_type": "fill", "slippage_bps": 8.0, "ts": 1760000200}),
                json.dumps({"event_type": "fill", "slippage_bps": 25.0, "ts": 1760000300}),
            ]
        ),
        encoding="utf-8",
    )

    day_tag = datetime.now(timezone.utc).strftime("%Y%m%d")
    (stats_dir / f"daily_cost_stats_{day_tag}.json").write_text(
        json.dumps(
            {
                "day": day_tag,
                "coverage": {"fills": 40},
                "buckets": {
                    "ALL|ALL|ALL|ALL": {
                        "count": 40,
                        "slippage_bps": {"p90": 7.5, "p95": 9.1},
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    class FakeRuntimePaths:
        def __init__(self, db_path: Path):
            self.orders_db = db_path

    cfg = {
        "backtest": {
            "slippage_bps": 5,
            "cost_model": "calibrated",
            "slippage_quantile": "p90",
            "cost_stats_dir": str(stats_dir),
            "max_stats_age_days": 7,
            "min_fills_global": 30,
        }
    }

    summary = module._load_slippage_insights(
        runtime_paths=FakeRuntimePaths(orders_db),
        cfg=cfg,
        lookback_days=14,
    )

    assert summary["status"] == "warning"
    assert summary["sampleCount"] == 4
    assert summary["baselineBps"] == pytest.approx(7.5)
    assert summary["baselineMode"] == "calibrated"
    assert summary["actualAvgBps"] == pytest.approx(8.5)
    assert summary["actualP50Bps"] == pytest.approx(5.5)
    assert summary["actualP90Bps"] == pytest.approx(19.9)
    assert any(bin_item["label"] == "20~40" and bin_item["count"] == 1 for bin_item in summary["bins"])


def test_load_slippage_insights_ignores_non_dated_event_files(tmp_path):
    module = load_web_dashboard_module()
    reports_dir = tmp_path / "reports"
    events_dir = reports_dir / "cost_events"
    orders_db = reports_dir / "orders.sqlite"
    events_dir.mkdir(parents=True, exist_ok=True)

    (events_dir / "20260410.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"event_type": "fill", "slippage_bps": 1.0, "ts": 1760000000}),
                json.dumps({"event_type": "fill", "slippage_bps": 3.0, "ts": 1760000100}),
            ]
        ) + "\n",
        encoding="utf-8",
    )
    (events_dir / "latest.jsonl").write_text(
        json.dumps({"event_type": "fill", "slippage_bps": 99.0, "ts": 1760000200}) + "\n",
        encoding="utf-8",
    )

    class FakeRuntimePaths:
        def __init__(self, db_path: Path):
            self.orders_db = db_path

    summary = module._load_slippage_insights(
        runtime_paths=FakeRuntimePaths(orders_db),
        cfg={},
        lookback_days=1,
    )

    assert summary["sampleCount"] == 2
    assert summary["actualAvgBps"] == pytest.approx(2.0)
    assert summary["actualMaxBps"] == pytest.approx(3.0)


def test_dashboard_api_degrades_when_child_endpoint_returns_error_tuple(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "api_account", lambda: module.jsonify({
        "cash_usdt": 100.0,
        "positions_value_usdt": 0.0,
        "total_equity_usdt": 100.0,
        "total_pnl_pct": 0.0,
        "drawdown_pct": 0.0,
        "realized_pnl": 0.0,
        "total_trades": 0,
        "last_update": "2026-04-10 12:00:00",
    }))
    monkeypatch.setattr(module, "api_positions", lambda: (module.jsonify({"error": "positions db locked"}), 500))
    monkeypatch.setattr(module, "api_trades", lambda: module.jsonify({"trades": []}))
    monkeypatch.setattr(module, "api_scores", lambda: module.jsonify({"scores": []}))
    monkeypatch.setattr(module, "api_status", lambda: (module.jsonify({"error": "status unavailable"}), 500))
    monkeypatch.setattr(module, "api_equity_history", lambda: module.jsonify([]))
    monkeypatch.setattr(module, "api_market_state", lambda: module.jsonify({"state": "TRENDING"}))
    monkeypatch.setattr(module, "api_timers", lambda: module.jsonify({"timers": []}))
    monkeypatch.setattr(module, "api_cost_calibration", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ic_diagnostics", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ml_training", lambda: module.jsonify({"status": "idle"}))
    monkeypatch.setattr(module, "api_reflection_reports", lambda: module.jsonify({"reports": []}))

    response = client.get("/api/dashboard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["positions"] == []
    assert payload["account"]["positionsValue"] == 0.0
    assert payload["systemStatus"]["mode"] == "unknown"
    assert payload["systemStatus"]["isRunning"] is False
    assert payload["systemStatus"]["errors"] == [
        "positions: positions db locked",
        "status: status unavailable",
    ]


def test_dashboard_api_deferred_view_keeps_deferred_child_errors(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "api_account", lambda: module.jsonify({
        "cash_usdt": 100.0,
        "positions_value_usdt": 0.0,
        "total_equity_usdt": 100.0,
        "total_pnl_pct": 0.0,
        "drawdown_pct": 0.0,
        "realized_pnl": 0.0,
        "total_trades": 0,
        "last_update": "2026-04-10 12:00:00",
    }))
    monkeypatch.setattr(module, "api_positions", lambda: module.jsonify({"positions": []}))
    monkeypatch.setattr(module, "api_status", lambda: module.jsonify({"timer_active": True, "dry_run": False, "mode": "live"}))
    monkeypatch.setattr(module, "api_market_state", lambda: module.jsonify({"state": "TRENDING"}))
    monkeypatch.setattr(module, "api_ml_training", lambda: module.jsonify({"status": "idle"}))
    monkeypatch.setattr(module, "api_trades", lambda: (module.jsonify({"error": "trades unavailable"}), 500))
    monkeypatch.setattr(module, "api_scores", lambda: module.jsonify({"scores": []}))
    monkeypatch.setattr(module, "api_timers", lambda: module.jsonify({"timers": []}))
    monkeypatch.setattr(module, "_load_api_telemetry_summary", lambda runtime_paths=None, lookback_hours=24: None)
    monkeypatch.setattr(module, "_load_slippage_insights", lambda runtime_paths=None, cfg=None, lookback_days=14: None)

    response = client.get("/api/dashboard?view=deferred")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["systemStatus"]["mode"] == "live"
    assert payload["systemStatus"]["errors"] == ["trades: trades unavailable"]
    assert payload["trades"] == []


def test_dashboard_api_sanitizes_child_error_messages(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "api_account", lambda: module.jsonify({
        "cash_usdt": 100.0,
        "positions_value_usdt": 0.0,
        "total_equity_usdt": 100.0,
        "initial_capital_usdt": 120.0,
        "equity_delta_usdt": -20.0,
        "total_pnl_pct": -0.1667,
        "drawdown_pct": 0.1667,
        "realized_pnl": 0.0,
        "total_trades": 0,
        "last_update": "2026-04-10 12:00:00",
    }))
    monkeypatch.setattr(
        module,
        "api_positions",
        lambda: (module.jsonify({"error": r"C:\secret\positions.sqlite"}), 500),
    )
    monkeypatch.setattr(module, "api_trades", lambda: module.jsonify({"trades": []}))
    monkeypatch.setattr(module, "api_scores", lambda: module.jsonify({"scores": []}))
    monkeypatch.setattr(module, "api_status", lambda: module.jsonify({
        "timer_active": True,
        "dry_run": False,
        "timer_error": "/home/ubuntu/clawd/v5-prod/systemd.timer",
    }))
    monkeypatch.setattr(module, "api_equity_history", lambda: module.jsonify([]))
    monkeypatch.setattr(module, "api_market_state", lambda: module.jsonify({"state": "TRENDING"}))
    monkeypatch.setattr(module, "api_timers", lambda: module.jsonify({"timers": []}))
    monkeypatch.setattr(module, "api_cost_calibration", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ic_diagnostics", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ml_training", lambda: module.jsonify({"status": "idle"}))
    monkeypatch.setattr(module, "api_reflection_reports", lambda: module.jsonify({"reports": []}))

    response = client.get("/api/dashboard")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["positions"] == []
    assert payload["systemStatus"]["errors"] == ["internal error", "positions: internal error"]
    _assert_body_hides_internal_details(
        body,
        r"C:\secret\positions.sqlite",
        "/home/ubuntu/clawd/v5-prod/systemd.timer",
        "positions.sqlite",
        "systemd.timer",
    )


def test_dashboard_api_degrades_when_child_endpoint_raises(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "api_account", lambda: module.jsonify({
        "cash_usdt": 100.0,
        "positions_value_usdt": 0.0,
        "total_equity_usdt": 100.0,
        "initial_capital_usdt": 120.0,
        "equity_delta_usdt": -20.0,
        "total_pnl_pct": -0.1667,
        "drawdown_pct": 0.1667,
        "realized_pnl": 0.0,
        "total_trades": 0,
        "last_update": "2026-04-10 12:00:00",
    }))

    def raise_positions():
        raise RuntimeError("positions exploded")

    monkeypatch.setattr(module, "api_positions", raise_positions)
    monkeypatch.setattr(module, "api_trades", lambda: module.jsonify({"trades": []}))
    monkeypatch.setattr(module, "api_scores", lambda: module.jsonify({"scores": []}))
    monkeypatch.setattr(module, "api_status", lambda: module.jsonify({"timer_active": True, "dry_run": False}))
    monkeypatch.setattr(module, "api_equity_history", lambda: module.jsonify([]))
    monkeypatch.setattr(module, "api_market_state", lambda: module.jsonify({"state": "TRENDING"}))
    monkeypatch.setattr(module, "api_timers", lambda: module.jsonify({"timers": []}))
    monkeypatch.setattr(module, "api_cost_calibration", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ic_diagnostics", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ml_training", lambda: module.jsonify({"status": "idle"}))
    monkeypatch.setattr(module, "api_reflection_reports", lambda: module.jsonify({"reports": []}))

    response = client.get("/api/dashboard")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert "traceback" not in payload
    assert "Traceback" not in body
    assert payload["positions"] == []
    assert payload["systemStatus"]["errors"] == ["positions: positions exploded"]


def test_dashboard_api_reuses_positions_endpoint_within_request(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "load_config", lambda: {})
    monkeypatch.setattr(module, "_load_reconcile_cash_balance", lambda *args, **kwargs: (True, 100.0))
    monkeypatch.setattr(module, "_load_local_account_state", lambda *args, **kwargs: {})
    monkeypatch.setattr(module, "_load_total_fees_from_orders", lambda *args, **kwargs: 0.0)

    calls = {"positions": 0}

    def fake_positions():
        calls["positions"] += 1
        return module.jsonify({
            "positions": [{
                "symbol": "ETH",
                "qty": 2.0,
                "avg_px": 10.0,
                "last_price": 12.5,
                "value_usdt": 25.0,
                "pnl_value": 5.0,
                "pnl_pct": 0.25,
            }]
        })

    monkeypatch.setattr(module, "api_positions", fake_positions)
    monkeypatch.setattr(module, "api_trades", lambda: module.jsonify({"trades": []}))
    monkeypatch.setattr(module, "api_scores", lambda: module.jsonify({"scores": []}))
    monkeypatch.setattr(module, "api_status", lambda: module.jsonify({"timer_active": True, "dry_run": False}))
    monkeypatch.setattr(module, "api_equity_history", lambda: module.jsonify([]))
    monkeypatch.setattr(module, "api_market_state", lambda: module.jsonify({}))
    monkeypatch.setattr(module, "api_timers", lambda: module.jsonify({"timers": []}))
    monkeypatch.setattr(module, "api_cost_calibration", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ic_diagnostics", lambda: module.jsonify({"status": "ok"}))
    monkeypatch.setattr(module, "api_ml_training", lambda: module.jsonify({"status": "idle"}))
    monkeypatch.setattr(module, "api_reflection_reports", lambda: module.jsonify({"reports": []}))

    response = client.get("/api/dashboard")

    assert response.status_code == 200
    payload = response.get_json()
    assert calls["positions"] == 1
    assert payload["positions"][0]["symbol"] == "ETH"
    assert payload["account"]["positionsValue"] == 25.0
    assert payload["account"]["totalEquity"] == 125.0


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


def test_position_kline_api_returns_expected_shape(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    series = module.MarketSeries(
        symbol="BTC/USDT",
        timeframe="1h",
        ts=[1710201600000, 1710205200000],
        open=[100.0, 101.0],
        high=[102.0, 105.0],
        low=[99.5, 100.5],
        close=[101.0, 103.0],
        volume=[10.0, 12.0],
    )
    monkeypatch.setattr(module, "_load_position_market_series", lambda symbol, timeframe, limit: (series, "cache"))

    response = client.get("/api/position_kline?symbol=BTC&timeframe=1h&limit=2")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["symbol"] == "BTC/USDT"
    assert payload["timeframe"] == "1h"
    assert payload["source"] == "cache"
    assert len(payload["candles"]) == 2
    assert payload["candles"][0]["open"] == 100.0
    assert payload["summary"]["bars"] == 2
    assert payload["summary"]["high"] == 105.0
    assert payload["summary"]["low"] == 99.5
    assert payload["summary"]["volume"] == 22.0
    assert payload["summary"]["change_pct"] == pytest.approx(0.03)


def test_position_kline_api_normalizes_second_timestamps(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    series = module.MarketSeries(
        symbol="BTC/USDT",
        timeframe="1h",
        ts=[1710201600, 1710205200],
        open=[100.0, 101.0],
        high=[102.0, 105.0],
        low=[99.5, 100.5],
        close=[101.0, 103.0],
        volume=[10.0, 12.0],
    )
    monkeypatch.setattr(module, "_load_position_market_series", lambda symbol, timeframe, limit: (series, "cache"))

    response = client.get("/api/position_kline?symbol=BTC&timeframe=1h")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["candles"][0]["ts"] == 1710201600000
    assert payload["candles"][0]["time"] == "2024-03-12 00:00"


def test_position_kline_api_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_series(*_args, **_kwargs):
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/reports/position_cache.db")

    monkeypatch.setattr(module, "_load_position_market_series", raise_series)

    response = client.get("/api/position_kline?symbol=BTC&timeframe=1h")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["candles"] == []
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/reports/position_cache.db", "position_cache.db")


def test_load_position_market_series_prefers_okx_when_cache_is_stale(monkeypatch):
    module = load_web_dashboard_module()

    stale_series = module.MarketSeries(
        symbol="BTC/USDT",
        timeframe="1h",
        ts=[1710201600000, 1710205200000],
        open=[100.0, 101.0],
        high=[102.0, 105.0],
        low=[99.5, 100.5],
        close=[101.0, 103.0],
        volume=[10.0, 12.0],
    )
    live_series = module.MarketSeries(
        symbol="BTC/USDT",
        timeframe="1h",
        ts=[1_775_900_400_000, 1_775_904_000_000],
        open=[110.0, 111.0],
        high=[112.0, 115.0],
        low=[109.5, 110.5],
        close=[111.0, 113.0],
        volume=[14.0, 16.0],
    )

    class FakeProvider:
        def fetch_ohlcv(self, symbols, timeframe="1h", limit=0):
            assert symbols == ["BTC/USDT"]
            assert timeframe == "1h"
            return {"BTC/USDT": live_series}

    monkeypatch.setattr(module, "_load_cached_position_market_series", lambda symbol, timeframe, limit: stale_series)
    monkeypatch.setattr(module, "_get_okx_public_provider", lambda: FakeProvider())
    monkeypatch.setattr(module.time, "time", lambda: 1_776_000_000.0)

    series, source = module._load_position_market_series("BTC", "1h", 2)

    assert source == "okx"
    assert series.close[-1] == 113.0


def test_load_position_market_series_falls_back_to_stale_cache_when_okx_unavailable(monkeypatch):
    module = load_web_dashboard_module()

    stale_series = module.MarketSeries(
        symbol="BTC/USDT",
        timeframe="1h",
        ts=[1710201600000, 1710205200000],
        open=[100.0, 101.0],
        high=[102.0, 105.0],
        low=[99.5, 100.5],
        close=[101.0, 103.0],
        volume=[10.0, 12.0],
    )

    class BrokenProvider:
        def fetch_ohlcv(self, symbols, timeframe="1h", limit=0):
            raise RuntimeError("network down")

    monkeypatch.setattr(module, "_load_cached_position_market_series", lambda symbol, timeframe, limit: stale_series)
    monkeypatch.setattr(module, "_get_okx_public_provider", lambda: BrokenProvider())
    monkeypatch.setattr(module.time, "time", lambda: 1_776_000_000.0)

    series, source = module._load_position_market_series("BTC", "1h", 2)

    assert source == "cache_stale"
    assert series.close[-1] == 103.0


def test_cache_json_response_ignores_cachebuster_query_param():
    module = load_web_dashboard_module()
    module._DASHBOARD_ROUTE_CACHE.clear()
    calls = {"count": 0}

    @module._cache_json_response(10.0)
    def sample():
        calls["count"] += 1
        return module.jsonify({"count": calls["count"]})

    with module.app.test_request_context("/api/sample?_=111"):
        payload1, _ = module._extract_endpoint_json(sample())

    with module.app.test_request_context("/api/sample?_=222"):
        payload2, _ = module._extract_endpoint_json(sample())

    assert payload1["count"] == 1
    assert payload2["count"] == 1
    assert calls["count"] == 1


def test_api_scores_exposes_display_score_rank_and_raw_strength(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    previous_run = runs_dir / "20260311_00"
    current_run = runs_dir / "20260311_01"
    previous_run.mkdir(parents=True, exist_ok=True)
    current_run.mkdir(parents=True, exist_ok=True)

    (previous_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": "SIDEWAYS",
                "top_scores": [
                    {"symbol": "HYPE/USDT", "score": 0.91, "display_score": 0.91, "raw_score": 1.82, "rank": 1},
                    {"symbol": "FLOW/USDT", "score": 0.88, "display_score": 0.88, "raw_score": 1.90, "rank": 2},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": "SIDEWAYS",
                "top_scores": [
                    {"symbol": "FLOW/USDT", "score": 0.9987, "display_score": 0.9987, "raw_score": 3.7228, "rank": 1},
                    {"symbol": "HYPE/USDT", "score": 0.9521, "display_score": 0.9521, "raw_score": 0.9521, "rank": 2},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(previous_run, (1, 1))
    os.utime(current_run, (2, 2))
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/scores")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["regime"] == "SIDEWAYS"
    assert payload["last_update"] == datetime.strptime("20260311_01", "%Y%m%d_%H").isoformat()
    first = payload["scores"][0]
    assert first["symbol"] == "FLOW/USDT"
    assert first["rank"] == 1
    assert first["previous_rank"] == 2
    assert first["rank_change"] == 1
    assert first["score"] == 0.9987
    assert first["display_score"] == 0.9987
    assert first["raw_score"] == 3.7228
    assert first["raw_score_change"] == pytest.approx(1.8228)


def test_api_scores_backfills_display_score_for_legacy_raw_values(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260311_01"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": "SIDEWAYS",
                "top_scores": [
                    {"symbol": "FLOW/USDT", "score": 3.7228},
                    {"symbol": "HYPE/USDT", "score": 0.9521},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(current_run, (2, 2))
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/scores")

    assert response.status_code == 200
    payload = response.get_json()
    first = payload["scores"][0]
    assert first["symbol"] == "FLOW/USDT"
    assert first["raw_score"] == 3.7228
    assert 0.99 < first["display_score"] < 1.0
    assert first["score"] == first["display_score"]


def test_api_scores_skips_latest_failed_run(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    valid_run = runs_dir / "20260311_00"
    failed_run = runs_dir / "20260311_01"
    valid_run.mkdir(parents=True, exist_ok=True)
    failed_run.mkdir(parents=True, exist_ok=True)

    (valid_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": "SIDEWAYS",
                "top_scores": [
                    {"symbol": "FLOW/USDT", "score": 0.88, "display_score": 0.88, "raw_score": 1.90, "rank": 1},
                    {"symbol": "HYPE/USDT", "score": 0.86, "display_score": 0.86, "raw_score": 1.70, "rank": 2},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (failed_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": "Unknown",
                "top_scores": [],
                "counts": {"universe": 0, "scored": 0},
                "notes": ["No market data returned from provider"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(valid_run, (1, 1))
    os.utime(failed_run, (2, 2))
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/scores")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_run"] == "20260311_00"
    assert payload["regime"] == "SIDEWAYS"
    assert payload["scores"][0]["symbol"] == "FLOW/USDT"


def test_api_scores_falls_back_to_alpha_snapshot_when_runs_empty(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    failed_run = runs_dir / "20260311_01"
    failed_run.mkdir(parents=True, exist_ok=True)
    (failed_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": "Unknown",
                "top_scores": [],
                "counts": {"universe": 0, "scored": 0},
                "notes": ["No market data returned from provider"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "alpha_snapshot.json").write_text(
        json.dumps(
            {
                "scores": {
                    "SOL/USDT": 0.95,
                    "BTC/USDT": 0.75,
                    "ETH/USDT": -0.10,
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(reports_dir / "alpha_snapshot.json", (1_710_100_000, 1_710_100_000))
    (reports_dir / "regime.json").write_text(
        json.dumps({"state": "Trending", "multiplier": 1.2, "votes": {}}, ensure_ascii=False),
        encoding="utf-8",
    )
    os.utime(failed_run, (2, 2))
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/scores")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_run"] == "alpha_snapshot"
    assert payload["regime"] == "Trending"
    assert payload["last_update"] == datetime.fromtimestamp(1_710_100_000).isoformat()
    assert [item["symbol"] for item in payload["scores"][:2]] == ["SOL/USDT", "BTC/USDT"]


def test_api_scores_prefers_alpha_snapshot_runtime_ts_over_file_mtime(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    failed_run = runs_dir / "20260311_01"
    failed_run.mkdir(parents=True, exist_ok=True)
    (failed_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": "Unknown",
                "top_scores": [],
                "counts": {"universe": 0, "scored": 0},
                "notes": ["No market data returned from provider"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    alpha_snapshot_path = reports_dir / "alpha_snapshot.json"
    alpha_snapshot_path.write_text(
        json.dumps(
            {
                "scores": {"SOL/USDT": 0.95},
                "ml_runtime": {"ts": "2026-03-10T12:05:00Z"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(alpha_snapshot_path, (1_999_999_999, 1_999_999_999))
    (reports_dir / "regime.json").write_text(
        json.dumps({"state": "Trending", "multiplier": 1.2, "votes": {}}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/scores")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_run"] == "alpha_snapshot"
    assert payload["last_update"] == datetime.fromtimestamp(module._coerce_timestamp_epoch("2026-03-10T12:05:00Z")).isoformat()


def test_api_scores_uses_active_runtime_runs_dir(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    root_run = reports_dir / "runs" / "20260311_02"
    runtime_run = runtime_dir / "runs" / "20260311_01"
    root_run.mkdir(parents=True, exist_ok=True)
    runtime_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"},
            "rebalance": {"deadband_sideways": 0.07},
        },
    )

    (root_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": "SIDEWAYS",
                "top_scores": [
                    {"symbol": "BTC/USDT", "score": 0.95, "display_score": 0.95, "raw_score": 1.9, "rank": 1},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": "TRENDING",
                "top_scores": [
                    {"symbol": "ETH/USDT", "score": 0.88, "display_score": 0.88, "raw_score": 1.4, "rank": 1},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(root_run, (20, 20))
    os.utime(runtime_run, (10, 10))

    response = client.get("/api/scores")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_run"] == "20260311_01"
    assert payload["regime"] == "TRENDING"
    assert payload["scores"][0]["symbol"] == "ETH/USDT"


def test_api_scores_limits_recent_decision_audit_scan(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setenv("V5_DASHBOARD_SCORE_AUDIT_SCAN_LIMIT", "4")

    for hour in range(20):
        run_dir = runs_dir / f"20260312_{hour:02d}"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "decision_audit.json").write_text(
            json.dumps(
                {
                    "regime": "TRENDING",
                    "top_scores": [
                        {
                            "symbol": f"COIN{hour}/USDT",
                            "score": 0.5 + hour / 100,
                            "display_score": 0.5 + hour / 100,
                            "raw_score": 0.5 + hour / 100,
                            "rank": 1,
                        }
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    original_load_json_payload = module._load_json_payload
    reads = {"decision_audit": 0}

    def counting_load_json_payload(path):
        if Path(path).name == "decision_audit.json":
            reads["decision_audit"] += 1
        return original_load_json_payload(path)

    monkeypatch.setattr(module, "_load_json_payload", counting_load_json_payload)

    response = client.get("/api/scores")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_run"] == "20260312_19"
    assert payload["previous_run"] == "20260312_18"
    assert payload["scores"][0]["symbol"] == "COIN19/USDT"
    assert reads["decision_audit"] <= 4


def test_api_scores_falls_back_to_active_runtime_alpha_snapshot(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    root_run = reports_dir / "runs" / "20260311_02"
    runtime_run = runtime_dir / "runs" / "20260311_01"
    root_run.mkdir(parents=True, exist_ok=True)
    runtime_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"},
            "rebalance": {"deadband_sideways": 0.07},
        },
    )

    failed_payload = json.dumps(
        {
            "regime": "Unknown",
            "top_scores": [],
            "counts": {"universe": 0, "scored": 0},
            "notes": ["No market data returned from provider"],
        },
        ensure_ascii=False,
    )
    (root_run / "decision_audit.json").write_text(failed_payload, encoding="utf-8")
    (runtime_run / "decision_audit.json").write_text(failed_payload, encoding="utf-8")
    (reports_dir / "alpha_snapshot.json").write_text(
        json.dumps({"scores": {"BTC/USDT": 0.91}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "regime.json").write_text(
        json.dumps({"state": "Sideways", "multiplier": 0.8, "votes": {}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (runtime_dir / "alpha_snapshot.json").write_text(
        json.dumps({"scores": {"ETH/USDT": 0.97, "SOL/USDT": 0.75}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (runtime_dir / "regime.json").write_text(
        json.dumps({"state": "Trending", "multiplier": 1.2, "votes": {}}, ensure_ascii=False),
        encoding="utf-8",
    )
    os.utime(root_run, (20, 20))
    os.utime(runtime_run, (10, 10))

    response = client.get("/api/scores")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_run"] == "alpha_snapshot"
    assert payload["regime"] == "Trending"
    assert [item["symbol"] for item in payload["scores"][:2]] == ["ETH/USDT", "SOL/USDT"]


def test_api_decision_audit_exposes_ml_signal_overview(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260312_01"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260312_01",
                "regime": "TRENDING",
                "counts": {"selected": 2, "orders_rebalance": 1, "orders_exit": 0},
                "top_scores": [
                    {"symbol": "BTC/USDT", "score": 0.93, "display_score": 0.93, "raw_score": 1.44, "rank": 1},
                    {"symbol": "ETH/USDT", "score": 0.84, "display_score": 0.84, "raw_score": 0.88, "rank": 2},
                ],
                "targets_pre_risk": {"BTC/USDT": 0.35, "ETH/USDT": 0.25},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "ml_runtime_status.json").write_text(
        json.dumps(
            {
                "configured_enabled": True,
                "promotion_passed": True,
                "used_in_latest_snapshot": True,
                "prediction_count": 3,
                "ml_weight": 0.2,
                "configured_ml_weight": 0.2,
                "effective_ml_weight": 0.08,
                "overlay_mode": "downweighted",
                "online_control_reason": "rolling_24h_negative",
                "reason": "ok",
                "ts": "2026-03-12T02:01:08Z",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "alpha_snapshot.json").write_text(
        json.dumps(
            {
                "raw_factors": {
                    "BTC/USDT": {"ml_pred_raw": 0.061, "ml_overlay_score": 0.52, "ml_base_score": 0.78},
                    "ETH/USDT": {"ml_pred_raw": -0.018, "ml_overlay_score": -0.24, "ml_base_score": 0.91},
                    "SOL/USDT": {"ml_pred_raw": 0.074, "ml_overlay_score": 0.68, "ml_base_score": 0.55},
                },
                "z_factors": {
                    "BTC/USDT": {"ml_pred_zscore": 0.82, "ml_overlay_score": 0.52},
                    "ETH/USDT": {"ml_pred_zscore": -0.31, "ml_overlay_score": -0.24},
                    "SOL/USDT": {"ml_pred_zscore": 1.12, "ml_overlay_score": 0.68},
                },
                "base_scores": {
                    "BTC/USDT": 0.78,
                    "ETH/USDT": 0.91,
                    "SOL/USDT": 0.55,
                },
                "scores": {
                    "BTC/USDT": 0.93,
                    "ETH/USDT": 0.84,
                    "SOL/USDT": 0.65,
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "ml_overlay_impact.json").write_text(
        json.dumps(
            {
                "last_step": {
                    "top_n": 3,
                    "delta_bps": 14.2,
                    "status": "positive",
                },
                "rolling_24h": {
                    "points": 6,
                    "topn_delta_mean_bps": 8.4,
                    "status": "positive",
                },
                "rolling_48h": {
                    "points": 12,
                    "topn_delta_mean_bps": -3.1,
                    "status": "mixed",
                },
                "overlay_mode": "downweighted",
                "online_control_reason": "rolling_24h_negative",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(current_run, (2, 2))
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    ml = payload["ml_signal_overview"]
    assert ml["configured_enabled"] is True
    assert ml["promoted"] is True
    assert ml["live_active"] is True
    assert ml["prediction_count"] == 3
    assert ml["ml_weight"] == 0.2
    assert ml["configured_ml_weight"] == 0.2
    assert ml["effective_ml_weight"] == 0.08
    assert ml["overlay_mode"] == "downweighted"
    assert ml["online_control_reason"] == "rolling_24h_negative"
    assert ml["impact_status"] == "positive"
    assert ml["last_step"]["delta_bps"] == 14.2
    assert ml["rolling_24h"]["topn_delta_mean_bps"] == 8.4
    assert ml["rolling_48h"]["topn_delta_mean_bps"] == -3.1
    assert [item["symbol"] for item in ml["top_contributors"]] == ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    assert ml["top_contributors"][0]["ml_zscore"] == 0.82
    assert ml["top_promoted"][0]["symbol"] == "BTC/USDT"
    assert ml["top_suppressed"][0]["symbol"] == "ETH/USDT"


def test_api_decision_audit_ml_signal_overview_treats_string_false_as_false(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260312_02"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260312_02",
                "regime": "TRENDING",
                "counts": {"selected": 0, "orders_rebalance": 0, "orders_exit": 0},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "ml_runtime_status.json").write_text(
        json.dumps(
            {
                "configured_enabled": "false",
                "promotion_passed": "false",
                "used_in_latest_snapshot": "false",
                "prediction_count": 0,
                "reason": "disabled",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "model_promotion_decision.json").write_text(
        json.dumps({"passed": "false"}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    ml = response.get_json()["ml_signal_overview"]
    assert ml["configured_enabled"] is False
    assert ml["promoted"] is False
    assert ml["live_active"] is False


def test_api_decision_audit_uses_prefixed_runtime_ml_signal_overview(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260312_03"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260312_03",
                "regime": "TRENDING",
                "counts": {"selected": 1, "orders_rebalance": 1, "orders_exit": 0},
                "top_scores": [{"symbol": "BTC/USDT", "score": 0.91, "display_score": 0.91, "raw_score": 1.21, "rank": 1}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "alpha_snapshot.json").write_text(
        json.dumps(
            {
                "raw_factors": {"BTC/USDT": {"ml_pred_raw": 0.071, "ml_overlay_score": 0.44, "ml_base_score": 0.77}},
                "z_factors": {"BTC/USDT": {"ml_pred_zscore": 0.93, "ml_overlay_score": 0.44}},
                "base_scores": {"BTC/USDT": 0.77},
                "scores": {"BTC/USDT": 0.91},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "shadow_ml_runtime_status.json").write_text(
        json.dumps(
            {
                "configured_enabled": True,
                "promotion_passed": True,
                "used_in_latest_snapshot": True,
                "prediction_count": 5,
                "reason": "shadow-runtime",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "shadow_model_promotion_decision.json").write_text(
        json.dumps({"passed": True}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "shadow_ml_overlay_impact.json").write_text(
        json.dumps({"rolling_24h": {"topn_delta_mean_bps": 6.5, "status": "positive"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "load_config", lambda: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}})

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    ml = response.get_json()["ml_signal_overview"]
    assert ml["configured_enabled"] is True
    assert ml["promoted"] is True
    assert ml["live_active"] is True
    assert ml["prediction_count"] == 5
    assert ml["reason"] == "shadow-runtime"
    assert ml["rolling_24h"]["topn_delta_mean_bps"] == 6.5
    assert ml["top_contributors"][0]["symbol"] == "BTC/USDT"


def test_api_decision_audit_uses_suffixed_runtime_ml_signal_overview(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260312_04"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260312_04",
                "regime": "TRENDING",
                "counts": {"selected": 1, "orders_rebalance": 1, "orders_exit": 0},
                "top_scores": [{"symbol": "ETH/USDT", "score": 0.89, "display_score": 0.89, "raw_score": 1.11, "rank": 1}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "alpha_snapshot.json").write_text(
        json.dumps(
            {
                "raw_factors": {"ETH/USDT": {"ml_pred_raw": -0.022, "ml_overlay_score": -0.19, "ml_base_score": 0.95}},
                "z_factors": {"ETH/USDT": {"ml_pred_zscore": -0.37, "ml_overlay_score": -0.19}},
                "base_scores": {"ETH/USDT": 0.95},
                "scores": {"ETH/USDT": 0.89},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "ml_runtime_status_accelerated.json").write_text(
        json.dumps(
            {
                "configured_enabled": True,
                "promotion_passed": False,
                "used_in_latest_snapshot": False,
                "prediction_count": 4,
                "reason": "accelerated-runtime",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "model_promotion_decision_accelerated.json").write_text(
        json.dumps({"passed": False}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "ml_overlay_impact_accelerated.json").write_text(
        json.dumps({"rolling_24h": {"topn_delta_mean_bps": -2.4, "status": "mixed"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "load_config", lambda: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}})

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    ml = response.get_json()["ml_signal_overview"]
    assert ml["configured_enabled"] is True
    assert ml["promoted"] is False
    assert ml["live_active"] is False
    assert ml["prediction_count"] == 4
    assert ml["reason"] == "accelerated-runtime"
    assert ml["rolling_24h"]["topn_delta_mean_bps"] == -2.4
    assert ml["top_contributors"][0]["symbol"] == "ETH/USDT"


def test_api_decision_audit_prefers_current_run_embedded_strategy_signals(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260313_14"
    stale_run = runs_dir / "20260313_13"
    current_run.mkdir(parents=True, exist_ok=True)
    stale_run.mkdir(parents=True, exist_ok=True)

    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260313_14",
                "regime": "SIDEWAYS",
                "counts": {"selected": 1, "orders_rebalance": 0, "orders_exit": 0},
                "strategy_signals": [
                    {
                        "strategy": "MeanReversion",
                        "allocation": 0.25,
                        "total_signals": 1,
                        "buy_signals": 0,
                        "sell_signals": 1,
                        "signals": [{"symbol": "SUI/USDT", "side": "sell", "score": 0.61}],
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (stale_run / "decision_audit.json").write_text(
        json.dumps({"run_id": "20260313_13", "regime": "SIDEWAYS"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (stale_run / "strategy_signals.json").write_text(
        json.dumps(
            {
                "strategies": [
                    {
                        "strategy": "TrendFollowing",
                        "allocation": 0.2,
                        "total_signals": 2,
                        "buy_signals": 2,
                        "sell_signals": 0,
                        "signals": [{"symbol": "BTC/USDT", "side": "buy", "score": 0.9}],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(stale_run, (1, 1))
    os.utime(current_run, (2, 2))
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["strategy_signal_source"] == "decision_audit"
    assert payload["strategy_run_id"] == "20260313_14"
    assert payload["strategy_signals"][0]["strategy"] == "MeanReversion"


def test_api_decision_audit_falls_back_to_previous_run_strategy_signals(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260313_15"
    stale_run = runs_dir / "20260313_14"
    current_run.mkdir(parents=True, exist_ok=True)
    stale_run.mkdir(parents=True, exist_ok=True)

    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {"run_id": "20260313_15", "regime": "TRENDING", "counts": {"selected": 0, "orders_rebalance": 0, "orders_exit": 0}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (stale_run / "decision_audit.json").write_text(
        json.dumps({"run_id": "20260313_14", "regime": "SIDEWAYS"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (stale_run / "strategy_signals.json").write_text(
        json.dumps(
            {
                "strategies": [
                    {
                        "strategy": "TrendFollowing",
                        "allocation": 0.2,
                        "total_signals": 1,
                        "buy_signals": 1,
                        "sell_signals": 0,
                        "signals": [{"symbol": "BTC/USDT", "side": "buy", "score": 0.9}],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(stale_run, (1, 1))
    os.utime(current_run, (2, 2))
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["strategy_signal_source"] == "previous_run_strategy_file"
    assert payload["strategy_run_id"] == "20260313_14"
    assert payload["strategy_signals"][0]["strategy"] == "TrendFollowing"
    assert payload["fused_source_is_fallback"] is True
    assert payload["timestamp"] == datetime.strptime("20260313_15", "%Y%m%d_%H").timestamp()


def test_api_decision_audit_timestamp_stays_on_latest_run_when_strategy_file_mtime_is_newer(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260313_15"
    current_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "load_config", lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}})

    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {"run_id": "20260313_15", "regime": "TRENDING", "counts": {"selected": 1, "orders_rebalance": 0, "orders_exit": 0}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    strategy_file = current_run / "strategy_signals.json"
    strategy_file.write_text(
        json.dumps(
            {
                "strategies": [
                    {
                        "strategy": "TrendFollowing",
                        "allocation": 0.2,
                        "total_signals": 1,
                        "buy_signals": 1,
                        "sell_signals": 0,
                        "signals": [{"symbol": "BTC/USDT", "side": "buy", "score": 0.9}],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(strategy_file, (9_999_999_999, 9_999_999_999))

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["strategy_signal_source"] == "strategy_file"
    assert payload["strategy_run_id"] == "20260313_15"
    assert payload["timestamp"] == datetime.strptime("20260313_15", "%Y%m%d_%H").timestamp()


def test_api_decision_audit_tolerates_non_numeric_selected_order_notional(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260313_15"
    current_run.mkdir(parents=True, exist_ok=True)

    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260313_15",
                "regime": "TRENDING",
                "counts": {"selected": 1, "orders_rebalance": 1, "orders_exit": 0},
                "router_decisions": [
                    {
                        "action": "create",
                        "symbol": "BTC/USDT",
                        "side": "buy",
                        "reason": "rebalance",
                        "notional": "n/a",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["selected_orders"] == [
        {
            "symbol": "BTC/USDT",
            "side": "buy",
            "reason": "rebalance",
            "notional": 0.0,
        }
    ]


def test_api_decision_audit_limits_recent_run_scan(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setenv("V5_DASHBOARD_DECISION_AUDIT_SCAN_LIMIT", "4")

    for hour in range(20):
        run_dir = runs_dir / f"20260313_{hour:02d}"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "decision_audit.json").write_text(
            json.dumps(
                {
                    "run_id": f"20260313_{hour:02d}",
                    "regime": "TRENDING",
                    "counts": {"selected": 1, "orders_rebalance": 0, "orders_exit": 0},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        if hour == 16:
            (run_dir / "strategy_signals.json").write_text(
                json.dumps(
                    {
                        "strategies": [
                            {
                                "strategy": "WithinLimit",
                                "allocation": 0.3,
                                "total_signals": 1,
                                "buy_signals": 1,
                                "sell_signals": 0,
                                "signals": [{"symbol": "ETH/USDT", "side": "buy", "score": 0.8}],
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
        if hour == 10:
            (run_dir / "strategy_signals.json").write_text(
                json.dumps(
                    {
                        "strategies": [
                            {
                                "strategy": "OutsideLimit",
                                "allocation": 0.3,
                                "total_signals": 1,
                                "buy_signals": 1,
                                "sell_signals": 0,
                                "signals": [{"symbol": "BTC/USDT", "side": "buy", "score": 0.9}],
                            }
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

    original_load_json_payload = module._load_json_payload
    reads = {"decision_audit": 0}

    def counting_load_json_payload(path):
        if Path(path).name == "decision_audit.json":
            reads["decision_audit"] += 1
        return original_load_json_payload(path)

    monkeypatch.setattr(module, "_load_json_payload", counting_load_json_payload)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["run_id"] == "20260313_19"
    assert payload["strategy_run_id"] == "20260313_16"
    assert payload["strategy_signal_source"] == "previous_run_strategy_file"
    assert payload["strategy_signals"][0]["strategy"] == "WithinLimit"
    assert reads["decision_audit"] <= 4


def test_api_decision_audit_recent_fill_summary_prefers_fill_timestamps(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260313_16"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260313_16",
                "regime": "TRENDING",
                "counts": {"selected": 1, "orders_rebalance": 1, "orders_exit": 0},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(current_run, (2, 2))

    orders_db = reports_dir / "orders.sqlite"
    conn = sqlite3.connect(str(orders_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            created_ts INTEGER,
            updated_ts INTEGER,
            run_id TEXT,
            inst_id TEXT,
            side TEXT,
            intent TEXT,
            state TEXT,
            notional_usdt REAL,
            last_error_code TEXT,
            last_error_msg TEXT,
            ord_id TEXT,
            cl_ord_id TEXT
        )
        """
    )
    cur.execute(
        """
        INSERT INTO orders(created_ts, updated_ts, run_id, inst_id, side, intent, state, notional_usdt, last_error_code, last_error_msg, ord_id, cl_ord_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1_710_000_000_000,
            1_710_000_000_000,
            "20260313_16",
            "BTC-USDT",
            "buy",
            "rebalance",
            "FILLED",
            100.0,
            "",
            "",
            "ord-1",
            "cl-1",
        ),
    )
    conn.commit()
    conn.close()

    fills_db = reports_dir / "fills.sqlite"
    conn = sqlite3.connect(str(fills_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE fills (
            ts_ms INTEGER,
            ord_id TEXT,
            cl_ord_id TEXT,
            inst_id TEXT,
            side TEXT,
            created_ts_ms INTEGER,
            trade_id TEXT
        )
        """
    )
    cur.execute(
        """
        INSERT INTO fills(ts_ms, ord_id, cl_ord_id, inst_id, side, created_ts_ms, trade_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1_710_088_200_000,
            "ord-1",
            "cl-1",
            "BTC-USDT",
            "buy",
            1_710_088_200_000,
            "trade-1",
        ),
    )
    conn.commit()
    conn.close()

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromtimestamp(1_710_090_000, tz=tz)

    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "datetime", _FrozenDateTime)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["recent_fill_summary"]["count_60m"] == 1
    assert payload["recent_fill_summary"]["count_24h"] == 1
    assert payload["recent_fill_summary"]["latest_fill"]["created_ts"] == 1_710_088_200_000
    assert payload["recent_fill_summary"]["latest_fill"]["run_id"] == "20260313_16"
    assert payload["recent_fill_summary"]["latest_fill"]["intent"] == "rebalance"


def test_api_decision_audit_recent_fill_summary_fallback_prefers_updated_ts(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260313_16"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260313_16",
                "regime": "TRENDING",
                "counts": {"selected": 1, "orders_rebalance": 1, "orders_exit": 0},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(current_run, (2, 2))

    orders_db = reports_dir / "orders.sqlite"
    conn = sqlite3.connect(str(orders_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            created_ts INTEGER,
            updated_ts INTEGER,
            run_id TEXT,
            inst_id TEXT,
            side TEXT,
            intent TEXT,
            state TEXT,
            notional_usdt REAL,
            ord_id TEXT
        )
        """
    )
    cur.executemany(
        """
        INSERT INTO orders(created_ts, updated_ts, run_id, inst_id, side, intent, state, notional_usdt, ord_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                1_710_000_000_000,
                1_710_088_200_000,
                "20260313_16",
                "BTC-USDT",
                "buy",
                "rebalance",
                "FILLED",
                100.0,
                "ord-1",
            ),
            (
                1_710_050_000_000,
                1_710_050_000_000,
                "20260313_15",
                "ETH-USDT",
                "buy",
                "rebalance",
                "FILLED",
                90.0,
                "ord-2",
            ),
        ],
    )
    conn.commit()
    conn.close()

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromtimestamp(1_710_090_000, tz=tz)

    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "datetime", _FrozenDateTime)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["recent_fill_summary"]["count_60m"] == 1
    assert payload["recent_fill_summary"]["count_24h"] == 2
    assert payload["recent_fill_summary"]["latest_fill"]["created_ts"] == 1_710_088_200_000
    assert payload["recent_fill_summary"]["latest_fill"]["run_id"] == "20260313_16"
    assert payload["recent_fill_summary"]["latest_fill"]["ord_id"] == "ord-1"


def test_api_decision_audit_recent_fill_summary_uses_newer_order_event_when_fill_store_lags(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260313_16"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260313_16",
                "regime": "TRENDING",
                "counts": {"selected": 1, "orders_rebalance": 1, "orders_exit": 0},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(current_run, (2, 2))

    orders_db = reports_dir / "orders.sqlite"
    conn = sqlite3.connect(str(orders_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            created_ts INTEGER,
            updated_ts INTEGER,
            run_id TEXT,
            inst_id TEXT,
            side TEXT,
            intent TEXT,
            state TEXT,
            notional_usdt REAL,
            ord_id TEXT,
            cl_ord_id TEXT
        )
        """
    )
    cur.execute(
        """
        INSERT INTO orders(created_ts, updated_ts, run_id, inst_id, side, intent, state, notional_usdt, ord_id, cl_ord_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1_710_070_000_000,
            1_710_088_200_000,
            "20260313_16",
            "BTC-USDT",
            "buy",
            "rebalance",
            "FILLED",
            100.0,
            "ord-1",
            "cl-1",
        ),
    )
    conn.commit()
    conn.close()

    fills_db = reports_dir / "fills.sqlite"
    conn = sqlite3.connect(str(fills_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE fills (
            ts_ms INTEGER,
            ord_id TEXT,
            cl_ord_id TEXT,
            inst_id TEXT,
            side TEXT,
            created_ts_ms INTEGER,
            trade_id TEXT
        )
        """
    )
    cur.execute(
        """
        INSERT INTO fills(ts_ms, ord_id, cl_ord_id, inst_id, side, created_ts_ms, trade_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1_710_079_200_000,
            "ord-1",
            "cl-1",
            "BTC-USDT",
            "buy",
            1_710_079_200_000,
            "trade-1",
        ),
    )
    conn.commit()
    conn.close()

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromtimestamp(1_710_090_000, tz=tz)

    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "datetime", _FrozenDateTime)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["recent_fill_summary"]["count_60m"] == 1
    assert payload["recent_fill_summary"]["count_24h"] == 2
    assert payload["recent_fill_summary"]["latest_fill"]["created_ts"] == 1_710_088_200_000
    assert payload["recent_fill_summary"]["latest_fill"]["run_id"] == "20260313_16"
    assert payload["recent_fill_summary"]["latest_fill"]["ord_id"] == "ord-1"


def test_api_decision_audit_run_orders_prefers_updated_ts_sort(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260313_16"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260313_16",
                "regime": "TRENDING",
                "counts": {"selected": 1, "orders_rebalance": 2, "orders_exit": 0},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(current_run, (2, 2))

    orders_db = reports_dir / "orders.sqlite"
    conn = sqlite3.connect(str(orders_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            created_ts INTEGER,
            updated_ts INTEGER,
            run_id TEXT,
            inst_id TEXT,
            side TEXT,
            intent TEXT,
            state TEXT,
            notional_usdt REAL,
            last_error_code TEXT,
            last_error_msg TEXT,
            ord_id TEXT
        )
        """
    )
    cur.executemany(
        """
        INSERT INTO orders(created_ts, updated_ts, run_id, inst_id, side, intent, state, notional_usdt, last_error_code, last_error_msg, ord_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                1_710_000_000_000,
                1_710_088_200_000,
                "20260313_16",
                "BTC-USDT",
                "buy",
                "rebalance",
                "REJECTED",
                100.0,
                "500",
                "late reject",
                "ord-1",
            ),
            (
                1_710_050_000_000,
                1_710_050_000_000,
                "20260313_16",
                "ETH-USDT",
                "buy",
                "rebalance",
                "FILLED",
                90.0,
                "",
                "",
                "ord-2",
            ),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["execution_summary"]["total"] == 2
    assert payload["execution_summary"]["negative_expectancy_penalty_count"] == 0
    assert payload["execution_summary"]["negative_expectancy_cooldown_count"] == 0
    assert payload["run_orders"][0]["ord_id"] == "ord-1"
    assert payload["run_orders"][0]["created_ts"] == 1_710_088_200_000
    assert payload["run_orders"][1]["ord_id"] == "ord-2"


def test_api_decision_audit_execution_summary_counts_all_orders_beyond_preview_limit(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260313_16"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260313_16",
                "regime": "TRENDING",
                "counts": {
                    "selected": 101,
                    "orders_rebalance": 101,
                    "orders_exit": 0,
                    "negative_expectancy_score_penalty": 7,
                    "negative_expectancy_cooldown": 8,
                    "negative_expectancy_open_block": 9,
                    "negative_expectancy_fast_fail_open_block": 10,
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(current_run, (2, 2))

    orders_db = reports_dir / "orders.sqlite"
    conn = sqlite3.connect(str(orders_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            created_ts INTEGER,
            updated_ts INTEGER,
            run_id TEXT,
            inst_id TEXT,
            side TEXT,
            intent TEXT,
            state TEXT,
            notional_usdt REAL,
            last_error_code TEXT,
            last_error_msg TEXT,
            ord_id TEXT
        )
        """
    )
    rows = [
        (
            1_710_000_000_000,
            1_710_000_000_000,
            "20260313_16",
            "DOGE-USDT",
            "buy",
            "rebalance",
            "REJECTED",
            10.0,
            "500",
            "old reject",
            "ord-reject",
        )
    ]
    for idx in range(100):
        ts_ms = 1_710_100_000_000 + idx * 1_000
        rows.append(
            (
                ts_ms,
                ts_ms,
                "20260313_16",
                "BTC-USDT",
                "buy",
                "rebalance",
                "FILLED",
                100.0,
                "",
                "",
                f"ord-fill-{idx}",
            )
        )
    cur.executemany(
        """
        INSERT INTO orders(created_ts, updated_ts, run_id, inst_id, side, intent, state, notional_usdt, last_error_code, last_error_msg, ord_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["execution_summary"]["total"] == 101
    assert payload["execution_summary"]["filled"] == 100
    assert payload["execution_summary"]["rejected"] == 1
    assert payload["execution_summary"]["negative_expectancy_penalty_count"] == 7
    assert payload["execution_summary"]["negative_expectancy_cooldown_count"] == 8
    assert payload["execution_summary"]["negative_expectancy_open_block_count"] == 9
    assert payload["execution_summary"]["negative_expectancy_fast_fail_open_block_count"] == 10
    assert payload["execution_summary"]["negative_expectancy_probation_release_count"] == 0
    assert payload["execution_summary"]["reject_reasons"]["500"] == 1
    assert len(payload["run_orders"]) == 30


def test_api_decision_audit_latest_ordered_run_summary_prefers_updated_ts(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    current_run = runs_dir / "20260313_16"
    current_run.mkdir(parents=True, exist_ok=True)
    (current_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260313_16",
                "regime": "TRENDING",
                "counts": {"selected": 2, "orders_rebalance": 2, "orders_exit": 0},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(current_run, (2, 2))

    orders_db = reports_dir / "orders.sqlite"
    conn = sqlite3.connect(str(orders_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            created_ts INTEGER,
            updated_ts INTEGER,
            run_id TEXT,
            inst_id TEXT,
            side TEXT,
            intent TEXT,
            state TEXT,
            notional_usdt REAL,
            last_error_code TEXT,
            last_error_msg TEXT,
            ord_id TEXT
        )
        """
    )
    cur.executemany(
        """
        INSERT INTO orders(created_ts, updated_ts, run_id, inst_id, side, intent, state, notional_usdt, last_error_code, last_error_msg, ord_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                1_710_000_000_000,
                1_710_088_200_000,
                "20260313_14",
                "BTC-USDT",
                "buy",
                "rebalance",
                "FILLED",
                100.0,
                "",
                "",
                "ord-1",
            ),
            (
                1_710_050_000_000,
                1_710_050_000_000,
                "20260313_15",
                "ETH-USDT",
                "buy",
                "rebalance",
                "FILLED",
                90.0,
                "",
                "",
                "ord-2",
            ),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["latest_ordered_run_summary"]["run_id"] == "20260313_14"
    assert payload["latest_ordered_run_summary"]["last_ts"] == 1_710_088_200_000


def test_api_decision_audit_uses_active_runtime_paths(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    root_runs_dir = reports_dir / "runs"
    runtime_runs_dir = runtime_dir / "runs"
    root_run = root_runs_dir / "20260408_02"
    runtime_run = runtime_runs_dir / "20260408_01"
    root_run.mkdir(parents=True, exist_ok=True)
    runtime_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {
                "order_store_path": "reports/shadow_runtime/orders.sqlite",
                "reconcile_status_path": "reports/shadow_runtime/reconcile_status.json",
            }
        },
    )

    (root_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260408_02",
                "regime": "SIDEWAYS",
                "counts": {"selected": 1, "orders_rebalance": 1, "orders_exit": 0},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260408_01",
                "regime": "TRENDING",
                "counts": {"selected": 2, "orders_rebalance": 2, "orders_exit": 0},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (root_run / "strategy_signals.json").write_text(
        json.dumps(
            {
                "strategies": [
                    {
                        "strategy": "RootStrategy",
                        "signals": [
                            {"symbol": "ETH/USDT", "side": "buy", "score": 0.9},
                            {"symbol": "BTC/USDT", "side": "sell", "score": 0.8},
                        ],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_run / "strategy_signals.json").write_text(
        json.dumps(
            {
                "strategies": [
                    {
                        "strategy": "ShadowStrategy",
                        "signals": [
                            {"symbol": "BTC/USDT", "side": "buy", "score": 0.9},
                            {"symbol": "ETH/USDT", "side": "sell", "score": 0.8},
                        ],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(root_run, (20, 20))
    os.utime(runtime_run, (10, 10))

    root_positions_db = reports_dir / "positions.sqlite"
    conn = sqlite3.connect(str(root_positions_db))
    cur = conn.cursor()
    cur.execute("CREATE TABLE positions (symbol TEXT, qty REAL, avg_px REAL, last_mark_px REAL)")
    cur.execute("INSERT INTO positions VALUES ('BTC/USDT', 1.0, 30000.0, 30000.0)")
    conn.commit()
    conn.close()

    runtime_positions_db = runtime_dir / "positions.sqlite"
    conn = sqlite3.connect(str(runtime_positions_db))
    cur = conn.cursor()
    cur.execute("CREATE TABLE positions (symbol TEXT, qty REAL, avg_px REAL, last_mark_px REAL)")
    cur.execute("INSERT INTO positions VALUES ('ETH/USDT', 2.0, 1800.0, 2000.0)")
    conn.commit()
    conn.close()

    root_orders_db = reports_dir / "orders.sqlite"
    conn = sqlite3.connect(str(root_orders_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            created_ts INTEGER,
            updated_ts INTEGER,
            run_id TEXT,
            inst_id TEXT,
            side TEXT,
            intent TEXT,
            state TEXT,
            notional_usdt REAL,
            last_error_code TEXT,
            last_error_msg TEXT,
            ord_id TEXT,
            cl_ord_id TEXT
        )
        """
    )
    cur.execute(
        """
        INSERT INTO orders(created_ts, updated_ts, run_id, inst_id, side, intent, state, notional_usdt, last_error_code, last_error_msg, ord_id, cl_ord_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1_710_000_000_000,
            1_710_099_000_000,
            "20260408_02",
            "SOL-USDT",
            "buy",
            "rebalance",
            "FILLED",
            90.0,
            "",
            "",
            "root-ord",
            "root-cl",
        ),
    )
    conn.commit()
    conn.close()

    runtime_orders_db = runtime_dir / "orders.sqlite"
    conn = sqlite3.connect(str(runtime_orders_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            created_ts INTEGER,
            updated_ts INTEGER,
            run_id TEXT,
            inst_id TEXT,
            side TEXT,
            intent TEXT,
            state TEXT,
            notional_usdt REAL,
            last_error_code TEXT,
            last_error_msg TEXT,
            ord_id TEXT,
            cl_ord_id TEXT
        )
        """
    )
    cur.execute(
        """
        INSERT INTO orders(created_ts, updated_ts, run_id, inst_id, side, intent, state, notional_usdt, last_error_code, last_error_msg, ord_id, cl_ord_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1_710_050_000_000,
            1_710_088_200_000,
            "20260408_01",
            "ETH-USDT",
            "sell",
            "rebalance",
            "FILLED",
            120.0,
            "",
            "",
            "shadow-ord",
            "shadow-cl",
        ),
    )
    conn.commit()
    conn.close()

    root_fills_db = reports_dir / "fills.sqlite"
    conn = sqlite3.connect(str(root_fills_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE fills (
            ts_ms INTEGER,
            ord_id TEXT,
            cl_ord_id TEXT,
            inst_id TEXT,
            side TEXT,
            created_ts_ms INTEGER,
            trade_id TEXT
        )
        """
    )
    cur.execute(
        "INSERT INTO fills(ts_ms, ord_id, cl_ord_id, inst_id, side, created_ts_ms, trade_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (1_710_099_000_000, "root-ord", "root-cl", "SOL-USDT", "buy", 1_710_099_000_000, "root-trade"),
    )
    conn.commit()
    conn.close()

    runtime_fills_db = runtime_dir / "fills.sqlite"
    conn = sqlite3.connect(str(runtime_fills_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE fills (
            ts_ms INTEGER,
            ord_id TEXT,
            cl_ord_id TEXT,
            inst_id TEXT,
            side TEXT,
            created_ts_ms INTEGER,
            trade_id TEXT
        )
        """
    )
    cur.execute(
        "INSERT INTO fills(ts_ms, ord_id, cl_ord_id, inst_id, side, created_ts_ms, trade_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (1_710_088_200_000, "shadow-ord", "shadow-cl", "ETH-USDT", "sell", 1_710_088_200_000, "shadow-trade"),
    )
    conn.commit()
    conn.close()

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromtimestamp(1_710_100_000, tz=tz)

    monkeypatch.setattr(module, "datetime", _FrozenDateTime)

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["run_id"] == "20260408_01"
    assert payload["strategy_run_id"] == "20260408_01"
    assert payload["latest_ordered_run_summary"]["run_id"] == "20260408_01"
    assert payload["recent_fill_summary"]["latest_fill"]["run_id"] == "20260408_01"
    assert payload["actionable_signals"]["held_symbols"] == ["ETH/USDT"]
    assert [row["symbol"] for row in payload["actionable_signals"]["buy_candidates"]] == ["BTC/USDT"]
    assert [row["symbol"] for row in payload["actionable_signals"]["sell_candidates"]] == ["ETH/USDT"]


def test_api_decision_audit_prefers_decision_audit_file_mtime_over_run_dir_mtime(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    stale_run = runs_dir / "20260408_02"
    fresh_run = runs_dir / "20260408_01"
    stale_run.mkdir(parents=True, exist_ok=True)
    fresh_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "load_config", lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}})

    stale_audit = stale_run / "decision_audit.json"
    fresh_audit = fresh_run / "decision_audit.json"
    stale_audit.write_text(json.dumps({"run_id": "20260408_02", "regime": "SIDEWAYS", "counts": {"selected": 1}}), encoding="utf-8")
    fresh_audit.write_text(json.dumps({"run_id": "20260408_01", "regime": "TRENDING", "counts": {"selected": 2}}), encoding="utf-8")
    os.utime(stale_audit, (100, 100))
    os.utime(fresh_audit, (200, 200))
    os.utime(stale_run, (500, 500))
    os.utime(fresh_run, (50, 50))

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["run_id"] == "20260408_01"


def test_api_decision_audit_scan_limit_prefers_sorted_epoch_over_file_mtime(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    older_run = runs_dir / "20260408_01"
    newer_run = runs_dir / "20260408_02"
    older_run.mkdir(parents=True, exist_ok=True)
    newer_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "load_config", lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}})
    monkeypatch.setenv("V5_DASHBOARD_DECISION_AUDIT_SCAN_LIMIT", "1")

    older_audit = older_run / "decision_audit.json"
    newer_audit = newer_run / "decision_audit.json"
    older_audit.write_text(json.dumps({"run_id": "20260408_01", "regime": "SIDEWAYS", "counts": {"selected": 1}}), encoding="utf-8")
    newer_audit.write_text(json.dumps({"run_id": "20260408_02", "regime": "TRENDING", "counts": {"selected": 2}}), encoding="utf-8")
    os.utime(older_audit, (200, 200))
    os.utime(newer_audit, (100, 100))

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["run_id"] == "20260408_02"


def test_api_decision_audit_prefers_sorted_epoch_over_file_mtime_without_scan_limit(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    older_run = runs_dir / "20260408_01"
    newer_run = runs_dir / "20260408_02"
    older_run.mkdir(parents=True, exist_ok=True)
    newer_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "load_config", lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}})

    older_audit = older_run / "decision_audit.json"
    newer_audit = newer_run / "decision_audit.json"
    older_audit.write_text(json.dumps({"run_id": "20260408_01", "regime": "SIDEWAYS", "counts": {"selected": 1}}), encoding="utf-8")
    newer_audit.write_text(json.dumps({"run_id": "20260408_02", "regime": "TRENDING", "counts": {"selected": 2}}), encoding="utf-8")
    os.utime(older_audit, (200, 200))
    os.utime(newer_audit, (100, 100))

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["run_id"] == "20260408_02"


def test_api_decision_audit_timestamp_prefers_sorted_epoch_over_file_mtime(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    older_run = runs_dir / "20260408_01"
    newer_run = runs_dir / "20260408_02"
    older_run.mkdir(parents=True, exist_ok=True)
    newer_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "load_config", lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}})

    older_audit = older_run / "decision_audit.json"
    newer_audit = newer_run / "decision_audit.json"
    older_audit.write_text(json.dumps({"run_id": "20260408_01", "counts": {"selected": 1}}), encoding="utf-8")
    newer_audit.write_text(json.dumps({"run_id": "20260408_02", "counts": {"selected": 2}}), encoding="utf-8")
    os.utime(older_audit, (200, 200))
    os.utime(newer_audit, (100, 100))

    response = client.get("/api/decision_audit")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["run_id"] == "20260408_02"
    assert payload["timestamp"] == datetime.strptime("20260408_02", "%Y%m%d_%H").timestamp()


def test_shadow_ml_overlay_api_reads_shadow_workspace(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    shadow_workspace = tmp_path / "v5-shadow-tuned-xgboost"
    reports_dir = shadow_workspace / "reports"
    stale_run_dir = reports_dir / "runs" / "shadow_tuned_xgboost_20260318_22"
    run_dir = reports_dir / "runs" / "shadow_tuned_xgboost_20260404_14"
    runtime_dir = reports_dir / "shadow_tuned_xgboost"
    stale_run_dir.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    (stale_run_dir / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "shadow_tuned_xgboost_20260318_22",
                "ml_signal_overview": {
                    "configured_enabled": True,
                    "live_active": True,
                    "prediction_count": None,
                    "overlay_score_max_abs": None,
                    "impact_status": None,
                    "rolling_24h": {"points": 25, "topn_delta_mean_bps": 15.88, "status": "positive"},
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (run_dir / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "shadow_tuned_xgboost_20260404_14",
                "ml_signal_overview": {
                    "configured_enabled": True,
                    "live_active": False,
                    "prediction_count": None,
                    "impact_status": None,
                    "top_contributors": [
                        {
                            "symbol": "OKB/USDT",
                            "ml_zscore": 2.61,
                            "ml_overlay_score": 1.48,
                            "score_delta": 0.29,
                            "base_rank": 16,
                            "final_rank": 10,
                            "rank_delta": 6,
                        }
                    ],
                    "top_promoted": [
                        {
                            "symbol": "HYPE/USDT",
                            "score_delta": 0.20,
                            "base_rank": 9,
                            "final_rank": 2,
                            "rank_delta": 7,
                        }
                    ],
                    "top_suppressed": [
                        {
                            "symbol": "PIXEL/USDT",
                            "score_delta": -0.25,
                            "base_rank": 3,
                            "final_rank": 4,
                            "rank_delta": -1,
                        }
                    ],
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(stale_run_dir, (9_999_999_999, 9_999_999_999))
    os.utime(run_dir, (1, 1))
    (runtime_dir / "ml_runtime_status.json").write_text(
        json.dumps(
            {
                "configured_enabled": "false",
                "promotion_passed": "false",
                "used_in_latest_snapshot": "false",
                "prediction_count": 11,
                "ml_weight": 0.08,
                "overlay_score_max_abs": 1.4569,
                "ts": "2026-04-04T07:33:09.185251Z",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_dir / "ml_overlay_impact.json").write_text(
        json.dumps(
            {
                "last_step": {},
                "rolling_24h": {"points": 25, "topn_delta_mean_bps": -4.68, "status": "mixed"},
                "rolling_48h": {"points": 49, "topn_delta_mean_bps": -1.01, "status": "mixed"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(module, "_resolve_shadow_workspace", lambda: shadow_workspace)

    response = client.get("/api/shadow_ml_overlay")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["available"] is True
    assert payload["workspace"] == str(shadow_workspace)
    assert payload["run_id"] == "shadow_tuned_xgboost_20260404_14"
    assert payload["ml_signal_overview"]["configured_enabled"] is False
    assert payload["ml_signal_overview"]["promoted"] is False
    assert payload["ml_signal_overview"]["live_active"] is False
    assert payload["ml_signal_overview"]["prediction_count"] == 11
    assert payload["ml_signal_overview"]["overlay_score_max_abs"] == 1.4569
    assert payload["ml_signal_overview"]["rolling_24h"]["topn_delta_mean_bps"] == -4.68
    assert payload["ml_signal_overview"]["rolling_48h"]["topn_delta_mean_bps"] == -1.01
    assert payload["impact_status"] == "mixed"
    assert payload["last_updated"] == "2026-04-04T07:33:09.185251Z"
    assert payload["ml_signal_overview"]["top_contributors"][0]["symbol"] == "OKB/USDT"


def test_shadow_ml_overlay_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_shadow_workspace():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-shadow-tuned-xgboost/reports")

    monkeypatch.setattr(module, "_resolve_shadow_workspace", raise_shadow_workspace)

    response = client.get("/api/shadow_ml_overlay")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["available"] is False
    assert "v5-shadow-tuned-xgboost" not in body
    assert "/home/ubuntu/clawd" not in body
    assert "Traceback" not in body


def test_signal_health_prefers_latest_file_by_filename_timestamp_not_mtime(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now()
    older_name = f"rss_MARKET_{(now - timedelta(hours=1)).strftime('%Y%m%d_%H')}.json"
    newer_name = f"rss_MARKET_{now.strftime('%Y%m%d_%H')}.json"
    older = cache_dir / older_name
    newer = cache_dir / newer_name
    older.write_text("{}", encoding="utf-8")
    newer.write_text("{}", encoding="utf-8")

    os.utime(older, (200, 200))
    os.utime(newer, (100, 100))

    latest = module._latest_signal_file(cache_dir, ["rss_MARKET_*.json"])
    health = module._signal_health(cache_dir, ["rss_MARKET_*.json"], 120, "rss_signal_stale_or_missing")

    assert latest is not None
    assert latest.name == newer_name
    assert health["status"] == "fresh"
    assert health["last_mtime"] == datetime.fromtimestamp(module._signal_file_epoch(newer)).strftime('%Y-%m-%d %H:%M:%S')


def test_shadow_ml_overlay_timestamp_prefers_sort_epoch_over_file_mtime(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    shadow_workspace = tmp_path / "v5-shadow-tuned-xgboost"
    reports_dir = shadow_workspace / "reports"
    stale_run_dir = reports_dir / "runs" / "shadow_tuned_xgboost_20260318_22"
    fresh_run_dir = reports_dir / "runs" / "shadow_tuned_xgboost_20260404_14"
    runtime_dir = reports_dir / "shadow_tuned_xgboost"
    stale_run_dir.mkdir(parents=True, exist_ok=True)
    fresh_run_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    stale_audit = stale_run_dir / "decision_audit.json"
    fresh_audit = fresh_run_dir / "decision_audit.json"
    stale_audit.write_text(json.dumps({"run_id": "shadow_tuned_xgboost_20260318_22", "ml_signal_overview": {}}), encoding="utf-8")
    fresh_audit.write_text(json.dumps({"run_id": "shadow_tuned_xgboost_20260404_14", "ml_signal_overview": {}}), encoding="utf-8")

    os.utime(stale_audit, (100, 100))
    os.utime(fresh_audit, (200, 200))
    os.utime(stale_run_dir, (9_999_999_999, 9_999_999_999))
    os.utime(fresh_run_dir, (1, 1))
    (runtime_dir / "ml_runtime_status.json").write_text(json.dumps({"configured_enabled": True}), encoding="utf-8")

    monkeypatch.setattr(module, "_resolve_shadow_workspace", lambda: shadow_workspace)

    response = client.get("/api/shadow_ml_overlay")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["run_id"] == "shadow_tuned_xgboost_20260404_14"
    assert payload["timestamp"] == module._run_id_epoch("shadow_tuned_xgboost_20260404_14")


def test_smart_alerts_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    class _BrokenEngine:
        def run_all_checks(self):
            raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/reports/alert_rules.yaml")

    fake_module = type(sys)("src.monitoring.smart_alert")
    fake_module.SmartAlertEngine = _BrokenEngine
    monkeypatch.setitem(sys.modules, "src.monitoring.smart_alert", fake_module)

    response = client.get("/api/smart_alerts")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["alerts"] == []
    assert payload["status"] == "error"
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/reports/alert_rules.yaml", "alert_rules.yaml")


def test_ml_training_api_reports_four_stage_chain(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    models_dir = module.WORKSPACE / "models"
    reports_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    db_path = reports_dir / "ml_training_data.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE feature_snapshots(id INTEGER PRIMARY KEY, label_filled INTEGER NOT NULL)")
    cur.executemany(
        "INSERT INTO feature_snapshots(label_filled) VALUES (?)",
        [(1,), (1,), (0,), (1,)],
    )
    conn.commit()
    conn.close()

    (reports_dir / "ml_training_history.json").write_text(
        """
        [
          {
            "timestamp": "2026-03-10T00:30:00Z",
            "valid_ic": 0.12,
            "gate": {"passed": true}
          }
        ]
        """.strip(),
        encoding="utf-8",
    )
    (reports_dir / "model_promotion_decision.json").write_text(
        """
        {
          "ts": "2026-03-10T00:40:00Z",
          "passed": true,
          "fail_reasons": []
        }
        """.strip(),
        encoding="utf-8",
    )
    (reports_dir / "ml_runtime_status.json").write_text(
        """
        {
          "ts": "2026-03-10T01:00:00Z",
          "used_in_latest_snapshot": true,
          "reason": "ok",
          "prediction_count": 3
        }
        """.strip(),
        encoding="utf-8",
    )
    (models_dir / "ml_factor_model.pkl").write_bytes(b"test")
    (models_dir / "ml_factor_model_config.json").write_text("{}", encoding="utf-8")
    (models_dir / "ml_factor_model_active.txt").write_text("models/ml_factor_model", encoding="utf-8")

    response = client.get("/api/ml_training")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["phase"] == "live_active"
    assert payload["status"] == "live_active"
    assert payload["display_status"] == "采样中 是 / 已训练 是 / 已通过门控 是 / 已被实盘使用 是"
    assert payload["stages"] == {
        "sampling": True,
        "trained": True,
        "promoted": True,
        "liveActive": True,
    }
    assert payload["latest_model"] in {"ml_factor_model.pkl", "ml_factor_model_config.json"}
    assert payload["runtime_prediction_count"] == 3
    assert payload["last_update"] == "2026-03-10 01:00:00"


def test_ml_training_api_prefers_model_artifact_over_newer_config_file(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    models_dir = module.WORKSPACE / "models"
    reports_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    db_path = reports_dir / "ml_training_data.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE feature_snapshots(id INTEGER PRIMARY KEY, label_filled INTEGER NOT NULL)")
    cur.execute("INSERT INTO feature_snapshots(label_filled) VALUES (1)")
    conn.commit()
    conn.close()

    (reports_dir / "ml_training_history.json").write_text(
        json.dumps([{"timestamp": "2026-03-10T00:30:00Z", "valid_ic": 0.12, "gate": {"passed": True}}], ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "model_promotion_decision.json").write_text(
        json.dumps({"ts": "2026-03-10T00:40:00Z", "passed": True, "fail_reasons": []}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "ml_runtime_status.json").write_text(
        json.dumps({"ts": "2026-03-10T01:00:00Z", "used_in_latest_snapshot": True, "reason": "ok", "prediction_count": 1}, ensure_ascii=False),
        encoding="utf-8",
    )
    model_file = models_dir / "ml_factor_model.pkl"
    config_file = models_dir / "ml_factor_model_config.json"
    model_file.write_bytes(b"model")
    config_file.write_text("{}", encoding="utf-8")
    os.utime(model_file, (1_000_000_000, 1_000_000_000))
    os.utime(config_file, (2_000_000_000, 2_000_000_000))
    (models_dir / "ml_factor_model_active.txt").write_text("models/ml_factor_model", encoding="utf-8")

    response = client.get("/api/ml_training")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["latest_model"] == "ml_factor_model.pkl"
    assert payload["model_date"] == "2001-09-09 09:46"


def test_ml_training_api_treats_string_false_flags_as_false(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    models_dir = module.WORKSPACE / "models"
    reports_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    db_path = reports_dir / "ml_training_data.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE feature_snapshots(id INTEGER PRIMARY KEY, label_filled INTEGER NOT NULL)")
    cur.execute("INSERT INTO feature_snapshots(label_filled) VALUES (1)")
    conn.commit()
    conn.close()

    (reports_dir / "ml_training_history.json").write_text(
        json.dumps(
            [
                {
                    "timestamp": "2026-03-10T00:30:00Z",
                    "valid_ic": 0.12,
                    "gate": {"passed": "false"},
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "model_promotion_decision.json").write_text(
        json.dumps(
            {
                "ts": "2026-03-10T00:40:00Z",
                "passed": "false",
                "fail_reasons": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "ml_runtime_status.json").write_text(
        json.dumps(
            {
                "ts": "2026-03-10T01:00:00Z",
                "used_in_latest_snapshot": "false",
                "reason": "disabled",
                "prediction_count": 0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (models_dir / "ml_factor_model.pkl").write_bytes(b"test")
    (models_dir / "ml_factor_model_config.json").write_text("{}", encoding="utf-8")
    (models_dir / "ml_factor_model_active.txt").write_text("models/ml_factor_model", encoding="utf-8")

    response = client.get("/api/ml_training")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["phase"] == "trained"
    assert payload["stages"] == {
        "sampling": True,
        "trained": True,
        "promoted": False,
        "liveActive": False,
    }
    assert payload["last_training_gate_passed"] is False


def test_ml_training_api_uses_active_runtime_reports_dir(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    runtime_dir = reports_dir / "shadow_runtime"
    models_dir = module.WORKSPACE / "models"
    reports_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    root_db_path = reports_dir / "ml_training_data.db"
    conn = sqlite3.connect(str(root_db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE feature_snapshots(id INTEGER PRIMARY KEY, label_filled INTEGER NOT NULL)")
    cur.executemany(
        "INSERT INTO feature_snapshots(label_filled) VALUES (?)",
        [(1,), (0,), (1,), (0,), (1,)],
    )
    conn.commit()
    conn.close()

    runtime_db_path = runtime_dir / "ml_training_data.db"
    conn = sqlite3.connect(str(runtime_db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE feature_snapshots(id INTEGER PRIMARY KEY, label_filled INTEGER NOT NULL)")
    cur.executemany(
        "INSERT INTO feature_snapshots(label_filled) VALUES (?)",
        [(1,), (1,), (0,)],
    )
    conn.commit()
    conn.close()

    (reports_dir / "ml_training_history.json").write_text(
        json.dumps(
            [
                {
                    "timestamp": "2026-03-09T00:00:00Z",
                    "valid_ic": 0.01,
                    "gate": {"passed": False},
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_dir / "ml_training_history.json").write_text(
        json.dumps(
            [
                {
                    "timestamp": "2026-03-10T12:30:00Z",
                    "valid_ic": 0.18,
                    "gate": {"passed": True},
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "model_promotion_decision.json").write_text(
        json.dumps(
            {
                "ts": "2026-03-09T00:10:00Z",
                "passed": False,
                "fail_reasons": ["root"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_dir / "model_promotion_decision.json").write_text(
        json.dumps(
            {
                "ts": "2026-03-10T12:45:00Z",
                "passed": True,
                "fail_reasons": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "ml_runtime_status.json").write_text(
        json.dumps(
            {
                "ts": "2026-03-09T01:00:00Z",
                "used_in_latest_snapshot": False,
                "reason": "root-runtime",
                "prediction_count": 0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_dir / "ml_runtime_status.json").write_text(
        json.dumps(
            {
                "ts": "2026-03-10T13:00:00Z",
                "used_in_latest_snapshot": True,
                "reason": "runtime-ok",
                "prediction_count": 7,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (models_dir / "ml_factor_model.pkl").write_bytes(b"test")
    (models_dir / "ml_factor_model_config.json").write_text("{}", encoding="utf-8")
    (models_dir / "ml_factor_model_active.txt").write_text("models/ml_factor_model", encoding="utf-8")

    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"},
            "rebalance": {"deadband_sideways": 0.07},
        },
    )

    def _raise_load_app_config(*args, **kwargs):
        raise RuntimeError("skip structured config for test")

    monkeypatch.setattr(module, "load_app_config", _raise_load_app_config)

    response = client.get("/api/ml_training")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["total_samples"] == 3
    assert payload["labeled_samples"] == 2
    assert payload["last_training_ts"] == "2026-03-10T12:30:00Z"
    assert payload["last_training_gate_passed"] is True
    assert payload["last_promotion_ts"] == "2026-03-10T12:45:00Z"
    assert payload["runtime_reason"] == "runtime-ok"
    assert payload["runtime_prediction_count"] == 7
    assert payload["last_update"] == "2026-03-10 13:00:00"


def test_ml_training_api_treats_legacy_default_ml_paths_as_runtime_paths(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    runtime_dir = reports_dir / "shadow_runtime"
    models_dir = module.WORKSPACE / "models"
    reports_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    runtime_db_path = runtime_dir / "ml_training_data.db"
    conn = sqlite3.connect(str(runtime_db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE feature_snapshots(id INTEGER PRIMARY KEY, label_filled INTEGER NOT NULL)")
    cur.executemany("INSERT INTO feature_snapshots(label_filled) VALUES (?)", [(1,), (1,), (0,)])
    conn.commit()
    conn.close()

    (runtime_dir / "ml_training_history.json").write_text(
        json.dumps(
            [
                {
                    "timestamp": "2026-03-10T12:30:00Z",
                    "valid_ic": 0.18,
                    "gate": {"passed": True},
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "model_promotion_decision.json").write_text(
        json.dumps(
            {
                "ts": "2026-03-09T00:10:00Z",
                "passed": False,
                "fail_reasons": ["root"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_dir / "model_promotion_decision.json").write_text(
        json.dumps(
            {
                "ts": "2026-03-10T12:45:00Z",
                "passed": True,
                "fail_reasons": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "ml_runtime_status.json").write_text(
        json.dumps(
            {
                "ts": "2026-03-09T01:00:00Z",
                "used_in_latest_snapshot": False,
                "reason": "root-runtime",
                "prediction_count": 0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_dir / "ml_runtime_status.json").write_text(
        json.dumps(
            {
                "ts": "2026-03-10T13:00:00Z",
                "used_in_latest_snapshot": True,
                "reason": "runtime-ok",
                "prediction_count": 7,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (models_dir / "ml_factor_model.pkl").write_bytes(b"test")
    (models_dir / "ml_factor_model_config.json").write_text("{}", encoding="utf-8")
    (models_dir / "ml_factor_model_active.txt").write_text("models/ml_factor_model", encoding="utf-8")

    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"},
            "rebalance": {"deadband_sideways": 0.07},
        },
    )

    class _Cfg:
        class _Alpha:
            class _ML:
                enabled = True
                model_path = "models/ml_factor_model"
                active_model_pointer_path = "models/ml_factor_model_active.txt"
                promotion_decision_path = "reports/model_promotion_decision.json"
                runtime_status_path = "reports/ml_runtime_status.json"

            ml_factor = _ML()

        alpha = _Alpha()

    monkeypatch.setattr(module, "load_app_config", lambda *args, **kwargs: _Cfg())

    response = client.get("/api/ml_training")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["last_promotion_ts"] == "2026-03-10T12:45:00Z"
    assert payload["runtime_reason"] == "runtime-ok"
    assert payload["runtime_prediction_count"] == 7


def test_resolve_dashboard_runtime_artifact_path_uses_prefixed_runtime_name(monkeypatch, tmp_path):
    module = load_web_dashboard_module()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    orders_db = module.REPORTS_DIR / "shadow_orders.sqlite"

    path = module._resolve_dashboard_runtime_artifact_path(
        orders_db,
        None,
        "reports/model_promotion_decision.json",
    )

    assert path == module.REPORTS_DIR / "shadow_model_promotion_decision.json"


def test_resolve_dashboard_runtime_artifact_path_uses_suffixed_runtime_name(monkeypatch, tmp_path):
    module = load_web_dashboard_module()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    orders_db = module.REPORTS_DIR / "orders_accelerated.sqlite"

    path = module._resolve_dashboard_runtime_artifact_path(
        orders_db,
        None,
        "reports/ml_runtime_status.json",
    )

    assert path == module.REPORTS_DIR / "ml_runtime_status_accelerated.json"


def test_ml_training_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_ml_training():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/reports/ml_training_data.db")

    monkeypatch.setattr(module, "_api_ml_training_v2", raise_ml_training)

    response = client.get("/api/ml_training")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["status"] == "error"
    assert payload["phase"] == "error"
    assert payload["configured_enabled"] is False
    assert payload["stages"] == {
        "sampling": False,
        "trained": False,
        "promoted": False,
        "liveActive": False,
    }
    assert payload["runtime_prediction_count"] == 0
    assert payload["last_update"] == ""
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/reports/ml_training_data.db", "ml_training_data.db")


def test_ml_training_api_uses_prefixed_runtime_artifacts(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    models_dir = module.WORKSPACE / "models"
    reports_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    runtime_db_path = reports_dir / "ml_training_data_prefixed.db"
    conn = sqlite3.connect(str(runtime_db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE feature_snapshots(id INTEGER PRIMARY KEY, label_filled INTEGER NOT NULL)")
    cur.executemany("INSERT INTO feature_snapshots(label_filled) VALUES (?)", [(1,), (0,), (1,)])
    conn.commit()
    conn.close()

    (reports_dir / "shadow_ml_training_data.db").write_bytes(runtime_db_path.read_bytes())
    runtime_db_path.unlink()

    (reports_dir / "shadow_ml_training_history.json").write_text(
        json.dumps(
            [{"timestamp": "2026-03-11T08:00:00Z", "valid_ic": 0.21, "gate": {"passed": True}}],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "shadow_model_promotion_decision.json").write_text(
        json.dumps({"ts": "2026-03-11T08:30:00Z", "passed": True, "fail_reasons": []}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "shadow_ml_runtime_status.json").write_text(
        json.dumps(
            {"ts": "2026-03-11T09:00:00Z", "used_in_latest_snapshot": True, "reason": "shadow-runtime", "prediction_count": 9},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (models_dir / "ml_factor_model.pkl").write_bytes(b"test")
    (models_dir / "ml_factor_model_config.json").write_text("{}", encoding="utf-8")
    (models_dir / "ml_factor_model_active.txt").write_text("models/ml_factor_model", encoding="utf-8")

    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )
    monkeypatch.setattr(module, "load_app_config", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("skip")))

    response = client.get("/api/ml_training")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["total_samples"] == 3
    assert payload["labeled_samples"] == 2
    assert payload["last_training_ts"] == "2026-03-11T08:00:00Z"
    assert payload["last_promotion_ts"] == "2026-03-11T08:30:00Z"
    assert payload["runtime_reason"] == "shadow-runtime"
    assert payload["runtime_prediction_count"] == 9


def test_ml_training_api_uses_suffixed_runtime_artifacts(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    models_dir = module.WORKSPACE / "models"
    reports_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    runtime_db_path = reports_dir / "ml_training_data_accelerated.db"
    conn = sqlite3.connect(str(runtime_db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE feature_snapshots(id INTEGER PRIMARY KEY, label_filled INTEGER NOT NULL)")
    cur.executemany("INSERT INTO feature_snapshots(label_filled) VALUES (?)", [(1,), (1,), (1,), (0,)])
    conn.commit()
    conn.close()

    (reports_dir / "ml_training_history_accelerated.json").write_text(
        json.dumps(
            [{"timestamp": "2026-03-12T10:00:00Z", "valid_ic": 0.33, "gate": {"passed": True}}],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "model_promotion_decision_accelerated.json").write_text(
        json.dumps({"ts": "2026-03-12T10:20:00Z", "passed": True, "fail_reasons": []}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "ml_runtime_status_accelerated.json").write_text(
        json.dumps(
            {"ts": "2026-03-12T10:40:00Z", "used_in_latest_snapshot": True, "reason": "accelerated-runtime", "prediction_count": 13},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (models_dir / "ml_factor_model.pkl").write_bytes(b"test")
    (models_dir / "ml_factor_model_config.json").write_text("{}", encoding="utf-8")
    (models_dir / "ml_factor_model_active.txt").write_text("models/ml_factor_model", encoding="utf-8")

    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )
    monkeypatch.setattr(module, "load_app_config", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("skip")))

    response = client.get("/api/ml_training")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["total_samples"] == 4
    assert payload["labeled_samples"] == 3
    assert payload["last_training_ts"] == "2026-03-12T10:00:00Z"
    assert payload["last_promotion_ts"] == "2026-03-12T10:20:00Z"
    assert payload["runtime_reason"] == "accelerated-runtime"
    assert payload["runtime_prediction_count"] == 13
    assert payload["last_update"] == "2026-03-12 10:40:00"


def test_ml_training_api_prefers_latest_history_entry_by_timestamp_when_history_is_unsorted(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    models_dir = module.WORKSPACE / "models"
    reports_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    db_path = reports_dir / "ml_training_data.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE feature_snapshots(id INTEGER PRIMARY KEY, label_filled INTEGER NOT NULL)")
    cur.executemany("INSERT INTO feature_snapshots(label_filled) VALUES (?)", [(1,), (1,)])
    conn.commit()
    conn.close()

    (reports_dir / "ml_training_history.json").write_text(
        json.dumps(
            [
                {
                    "run_id": "20260310_11",
                    "timestamp": "2026-03-10T11:00:00Z",
                    "valid_ic": 0.12,
                    "gate": {"passed": True},
                },
                {
                    "run_id": "20260310_10",
                    "timestamp": "2026-03-10T10:00:00Z",
                    "valid_ic": 0.01,
                    "gate": {"passed": False},
                },
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "model_promotion_decision.json").write_text(
        json.dumps({"ts": "2026-03-10T12:00:00Z", "passed": True, "fail_reasons": []}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "ml_runtime_status.json").write_text(
        json.dumps(
            {"ts": "2026-03-10T12:05:00Z", "used_in_latest_snapshot": True, "reason": "ok", "prediction_count": 2},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (models_dir / "ml_factor_model.pkl").write_bytes(b"test")
    (models_dir / "ml_factor_model_active.txt").write_text("models/ml_factor_model", encoding="utf-8")

    response = client.get("/api/ml_training")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["last_training_ts"] == "2026-03-10T11:00:00Z"
    assert payload["last_ic"] == 0.12
    assert payload["last_training_gate_passed"] is True


def test_api_reflection_reports_uses_active_runtime_reflection_dir(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    root_reflection_dir = reports_dir / "reflection"
    runtime_reflection_dir = reports_dir / "shadow_runtime" / "reflection"
    root_reflection_dir.mkdir(parents=True, exist_ok=True)
    runtime_reflection_dir.mkdir(parents=True, exist_ok=True)

    (root_reflection_dir / "reflection_20260408_010000.json").write_text(
        json.dumps(
            {
                "summary": {
                    "total_realized_pnl": -99.0,
                    "total_trades": 9,
                    "total_symbols": 4,
                },
                "alerts": [{"level": "critical"}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_reflection_dir / "reflection_20260408_020000.json").write_text(
        json.dumps(
            {
                "summary": {
                    "total_realized_pnl": 12.34,
                    "total_trades": 2,
                    "total_symbols": 1,
                },
                "alerts": [{"level": "warning"}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"},
            "rebalance": {"deadband_sideways": 0.07},
        },
    )

    response = client.get("/api/reflection_reports")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["total_reports"] == 1
    assert len(payload["reports"]) == 1
    assert payload["reports"][0]["filename"] == "reflection_20260408_020000.json"
    assert payload["reports"][0]["total_pnl"] == 12.34
    assert payload["reports"][0]["trade_count"] == 2
    assert payload["reports"][0]["high_priority"] == 0
    assert payload["reports"][0]["medium_priority"] == 1
    assert payload["last_update"] == "2026-04-08 02:00:00"


def test_api_reflection_reports_uses_prefixed_runtime_reflection_dir(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    root_reflection_dir = reports_dir / "reflection"
    runtime_reflection_dir = reports_dir / "shadow_reflection"
    root_reflection_dir.mkdir(parents=True, exist_ok=True)
    runtime_reflection_dir.mkdir(parents=True, exist_ok=True)

    (root_reflection_dir / "reflection_20260408_010000.json").write_text(
        json.dumps({"summary": {"total_realized_pnl": -50.0, "total_trades": 9, "total_symbols": 4}, "alerts": [{"level": "critical"}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (runtime_reflection_dir / "reflection_20260408_020000.json").write_text(
        json.dumps({"summary": {"total_realized_pnl": 8.76, "total_trades": 3, "total_symbols": 2}, "alerts": [{"level": "warning"}]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )

    response = client.get("/api/reflection_reports")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["total_reports"] == 1
    assert payload["reports"][0]["filename"] == "reflection_20260408_020000.json"
    assert payload["reports"][0]["total_pnl"] == 8.76
    assert payload["reports"][0]["trade_count"] == 3
    assert payload["last_update"] == "2026-04-08 02:00:00"


def test_api_reflection_reports_uses_suffixed_runtime_reflection_dir(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    root_reflection_dir = reports_dir / "reflection"
    runtime_reflection_dir = reports_dir / "reflection_accelerated"
    root_reflection_dir.mkdir(parents=True, exist_ok=True)
    runtime_reflection_dir.mkdir(parents=True, exist_ok=True)

    (root_reflection_dir / "reflection_20260408_010000.json").write_text(
        json.dumps({"summary": {"total_realized_pnl": -50.0, "total_trades": 9, "total_symbols": 4}, "alerts": [{"level": "critical"}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (runtime_reflection_dir / "reflection_20260408_020000.json").write_text(
        json.dumps({"summary": {"total_realized_pnl": 6.54, "total_trades": 4, "total_symbols": 1}, "alerts": [{"level": "warning"}]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )

    response = client.get("/api/reflection_reports")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["total_reports"] == 1
    assert payload["reports"][0]["filename"] == "reflection_20260408_020000.json"
    assert payload["reports"][0]["total_pnl"] == 6.54
    assert payload["reports"][0]["trade_count"] == 4
    assert payload["last_update"] == "2026-04-08 02:00:00"


def test_api_reflection_reports_ignores_non_dated_name_precedence(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    reflection_dir = reports_dir / "reflection"
    reflection_dir.mkdir(parents=True, exist_ok=True)

    (reflection_dir / "reflection_20260408_020000.json").write_text(
        json.dumps({"summary": {"total_realized_pnl": 6.54, "total_trades": 4, "total_symbols": 1}, "alerts": [{"level": "warning"}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reflection_dir / "reflection_latest.json").write_text(
        json.dumps({"summary": {"total_realized_pnl": 999.0, "total_trades": 99, "total_symbols": 9}, "alerts": [{"level": "critical"}]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}},
    )

    response = client.get("/api/reflection_reports")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["reports"][0]["filename"] == "reflection_20260408_020000.json"
    assert payload["reports"][0]["total_pnl"] == 6.54
    assert payload["last_update"] == "2026-04-08 02:00:00"


def test_reflection_reports_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/reflection_reports")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["reports"] == []
    assert payload["total_reports"] == 0
    assert payload["last_update"] == ""
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml", "live_prod.yaml")


def test_api_ic_diagnostics_uses_active_runtime_reports_dir(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path / "reports")

    reports_dir = module.REPORTS_DIR
    runtime_dir = reports_dir / "shadow_runtime"
    reports_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    runtime_file = runtime_dir / "ic_diagnostics_20260408.json"
    root_file = reports_dir / "ic_diagnostics_20260409.json"
    runtime_file.write_text(
        json.dumps(
            {
                "overall_tradable": {
                    "ic": {
                        "factor_runtime": {
                            "mean": 0.11,
                            "p50": 0.1,
                            "p75": 0.2,
                            "p25": 0.0,
                            "count": 10,
                        }
                    },
                    "used_points": 10,
                    "used_timestamps": 5,
                },
                "by_regime": {},
                "lookback_days": 30,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    root_file.write_text(
        json.dumps(
            {
                "overall_tradable": {
                    "ic": {
                        "factor_root": {
                            "mean": -0.22,
                            "p50": -0.2,
                            "p75": -0.1,
                            "p25": -0.3,
                            "count": 12,
                        }
                    },
                    "used_points": 12,
                    "used_timestamps": 6,
                },
                "by_regime": {},
                "lookback_days": 30,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(root_file, (runtime_file.stat().st_mtime + 100, runtime_file.stat().st_mtime + 100))

    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"},
            "rebalance": {"deadband_sideways": 0.07},
        },
    )

    response = client.get("/api/ic_diagnostics")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "ready"
    assert payload["source_file"] == "ic_diagnostics_20260408.json"
    assert payload["overall_ic"] == 0.11
    assert payload["sample_count"] == 10
    assert payload["factors"][0]["name"] == "factor_runtime"


def test_api_ic_diagnostics_uses_prefixed_runtime_file(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    runtime_file = reports_dir / "shadow_ic_diagnostics_20260408.json"
    root_file = reports_dir / "ic_diagnostics_20260409.json"
    runtime_file.write_text(
        json.dumps(
            {
                "overall_tradable": {
                    "ic": {
                        "factor_prefixed": {
                            "mean": 0.14,
                            "p50": 0.12,
                            "p75": 0.2,
                            "p25": 0.0,
                            "count": 8,
                        }
                    },
                    "used_points": 8,
                    "used_timestamps": 4,
                },
                "by_regime": {},
                "lookback_days": 30,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    root_file.write_text(
        json.dumps(
            {
                "overall_tradable": {
                    "ic": {
                        "factor_root": {
                            "mean": -0.3,
                            "p50": -0.2,
                            "p75": -0.1,
                            "p25": -0.4,
                            "count": 12,
                        }
                    },
                    "used_points": 12,
                    "used_timestamps": 6,
                },
                "by_regime": {},
                "lookback_days": 30,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )

    response = client.get("/api/ic_diagnostics")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["source_file"] == "shadow_ic_diagnostics_20260408.json"
    assert payload["overall_ic"] == 0.14
    assert payload["sample_count"] == 8
    assert payload["factors"][0]["name"] == "factor_prefixed"


def test_api_ic_diagnostics_uses_suffixed_runtime_file(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    runtime_file = reports_dir / "ic_diagnostics_20260408_accelerated.json"
    root_file = reports_dir / "ic_diagnostics_20260409.json"
    runtime_file.write_text(
        json.dumps(
            {
                "overall_tradable": {
                    "ic": {
                        "factor_accelerated": {
                            "mean": 0.19,
                            "p50": 0.17,
                            "p75": 0.25,
                            "p25": 0.1,
                            "count": 6,
                        }
                    },
                    "used_points": 6,
                    "used_timestamps": 3,
                },
                "by_regime": {},
                "lookback_days": 30,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    root_file.write_text(
        json.dumps(
            {
                "overall_tradable": {
                    "ic": {
                        "factor_root": {
                            "mean": -0.2,
                            "p50": -0.1,
                            "p75": 0.0,
                            "p25": -0.3,
                            "count": 12,
                        }
                    },
                    "used_points": 12,
                    "used_timestamps": 6,
                },
                "by_regime": {},
                "lookback_days": 30,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )

    response = client.get("/api/ic_diagnostics")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["source_file"] == "ic_diagnostics_20260408_accelerated.json"
    assert payload["overall_ic"] == 0.19
    assert payload["sample_count"] == 6
    assert payload["factors"][0]["name"] == "factor_accelerated"


def test_api_ic_diagnostics_prefers_filename_date_over_mtime(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    older_file = reports_dir / "ic_diagnostics_20260408.json"
    newer_file = reports_dir / "ic_diagnostics_20260409.json"
    older_file.write_text(
        json.dumps(
            {
                "overall_tradable": {
                    "ic": {
                        "factor_old": {
                            "mean": 0.11,
                            "p50": 0.1,
                            "p75": 0.2,
                            "p25": 0.0,
                            "count": 10,
                        }
                    },
                    "used_points": 10,
                    "used_timestamps": 5,
                },
                "by_regime": {},
                "lookback_days": 30,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    newer_file.write_text(
        json.dumps(
            {
                "overall_tradable": {
                    "ic": {
                        "factor_new": {
                            "mean": 0.23,
                            "p50": 0.2,
                            "p75": 0.3,
                            "p25": 0.1,
                            "count": 12,
                        }
                    },
                    "used_points": 12,
                    "used_timestamps": 6,
                },
                "by_regime": {},
                "lookback_days": 30,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(older_file, (2_000_000_000, 2_000_000_000))
    os.utime(newer_file, (1_000_000_000, 1_000_000_000))

    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}},
    )

    response = client.get("/api/ic_diagnostics")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["source_file"] == "ic_diagnostics_20260409.json"
    assert payload["overall_ic"] == 0.23
    assert payload["last_update"] == "2026-04-09 00:00:00"


def test_account_api_sanitizes_corrupted_low_peak(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "get_db_connection", lambda: None)
    monkeypatch.setattr(module, "load_config", lambda: {})
    monkeypatch.setattr(module, "api_positions", lambda: module.jsonify({"positions": [{
        "symbol": "OKB",
        "value_usdt": 115.6686,
    }]}))

    reconcile_path = tmp_path / "reconcile_status.json"
    reconcile_path.write_text(
        """
        {
          "exchange_snapshot": {
            "ccy_cashBal": {
              "USDT": 11.48
            }
          }
        }
        """.strip(),
        encoding="utf-8",
    )
    os.utime(reconcile_path, (1_710_100_000, 1_710_100_000))

    db_path = tmp_path / "positions.sqlite"
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE account_state (
          k TEXT PRIMARY KEY,
          cash_usdt REAL NOT NULL,
          equity_peak_usdt REAL NOT NULL,
          scale_basis_usdt REAL DEFAULT 0.0
        )
        """
    )
    cur.execute(
        "INSERT INTO account_state(k, cash_usdt, equity_peak_usdt, scale_basis_usdt) VALUES ('default', 11.48, 11.48, 0.0)"
    )
    con.commit()
    con.close()

    response = client.get("/api/account")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["cash_usdt"] == 11.48
    assert payload["total_equity_usdt"] == 127.1486
    assert payload["peak_equity_usdt"] == 127.15
    assert payload["drawdown_pct"] == 0.0
    assert payload["last_update"] == "2024-03-11 03:46:40"


def test_account_api_error_response_does_not_expose_traceback(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/account")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["cash_usdt"] == 0.0
    assert payload["total_equity_usdt"] == 0.0
    assert payload["initial_capital_usdt"] == 120.0
    assert payload["last_update"] == ""
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml", "live_prod.yaml")


def test_positions_api_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError(r"C:\secret\configs\live_prod.yaml")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/positions")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["positions"] == []
    assert "live_prod.yaml" not in body
    assert "C:\\secret" not in body
    assert "Traceback" not in body


def test_account_api_ignores_ambient_live_creds_by_default(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "get_db_connection", lambda: None)
    monkeypatch.setattr(module, "load_config", lambda: {})
    monkeypatch.setattr(module, "api_positions", lambda: module.jsonify({"positions": []}))
    monkeypatch.delenv("V5_DASHBOARD_ALLOW_LIVE_OKX_ACCOUNT", raising=False)
    monkeypatch.delenv("V5_DASHBOARD_ALLOW_LIVE_OKX", raising=False)
    monkeypatch.setenv("EXCHANGE_API_KEY", "ambient-key")
    monkeypatch.setenv("EXCHANGE_API_SECRET", "ambient-secret")
    monkeypatch.setenv("EXCHANGE_PASSPHRASE", "ambient-passphrase")

    def _unexpected_request(*args, **kwargs):
        raise AssertionError("api/account should not call live OKX without explicit enable flag")

    monkeypatch.setattr("requests.get", _unexpected_request)

    (tmp_path / "reconcile_status.json").write_text(
        """
        {
          "exchange_snapshot": {
            "ccy_cashBal": {
              "USDT": 11.48
            }
          }
        }
        """.strip(),
        encoding="utf-8",
    )

    response = client.get("/api/account")

    assert response.status_code == 200
    assert response.get_json()["cash_usdt"] == 11.48


def test_load_workspace_exchange_creds_uses_runtime_env_path(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "resolve_runtime_env_path", lambda raw_env_path=None, project_root=None: str(tmp_path / ".env.runtime"))
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)

    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "EXCHANGE_API_KEY=root-key",
                "EXCHANGE_API_SECRET=root-secret",
                "EXCHANGE_PASSPHRASE=root-pass",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / ".env.runtime").write_text(
        "\n".join(
            [
                "EXCHANGE_API_KEY=runtime-key",
                "EXCHANGE_API_SECRET=runtime-secret",
                "EXCHANGE_PASSPHRASE=runtime-pass",
            ]
        ),
        encoding="utf-8",
    )

    assert module._load_workspace_exchange_creds() == ("runtime-key", "runtime-secret", "runtime-pass")


def test_account_api_uses_active_runtime_paths(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    reports_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {
                "order_store_path": "reports/shadow_runtime/orders.sqlite",
                "reconcile_status_path": "reports/shadow_runtime/reconcile_status.json",
            }
        },
    )
    monkeypatch.setattr(module, "api_positions", lambda: module.jsonify({"positions": [{"symbol": "OKB", "value_usdt": 100.0}]}))

    (reports_dir / "reconcile_status.json").write_text(
        json.dumps({"exchange_snapshot": {"ccy_cashBal": {"USDT": 11.0}}}),
        encoding="utf-8",
    )
    (runtime_dir / "reconcile_status.json").write_text(
        json.dumps({"exchange_snapshot": {"ccy_cashBal": {"USDT": 50.0}}}),
        encoding="utf-8",
    )
    os.utime(runtime_dir / "reconcile_status.json", (1_710_200_000, 1_710_200_000))

    root_positions_db = reports_dir / "positions.sqlite"
    con = sqlite3.connect(str(root_positions_db))
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE account_state (k TEXT PRIMARY KEY, cash_usdt REAL NOT NULL, equity_peak_usdt REAL NOT NULL, scale_basis_usdt REAL DEFAULT 0.0)"
    )
    cur.execute("INSERT INTO account_state VALUES ('default', 11.0, 151.0, 0.0)")
    con.commit()
    con.close()

    runtime_positions_db = runtime_dir / "positions.sqlite"
    con = sqlite3.connect(str(runtime_positions_db))
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE account_state (k TEXT PRIMARY KEY, cash_usdt REAL NOT NULL, equity_peak_usdt REAL NOT NULL, scale_basis_usdt REAL DEFAULT 0.0)"
    )
    cur.execute("INSERT INTO account_state VALUES ('default', 50.0, 170.0, 0.0)")
    con.commit()
    con.close()

    root_orders_db = reports_dir / "orders.sqlite"
    con = sqlite3.connect(str(root_orders_db))
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE orders (inst_id TEXT, side TEXT, notional_usdt REAL, fee TEXT, state TEXT, avg_px TEXT)"
    )
    cur.execute("INSERT INTO orders VALUES ('BTC-USDT', 'buy', 90.0, '0', 'FILLED', '45000')")
    con.commit()
    con.close()

    runtime_orders_db = runtime_dir / "orders.sqlite"
    con = sqlite3.connect(str(runtime_orders_db))
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE orders (inst_id TEXT, side TEXT, notional_usdt REAL, fee TEXT, state TEXT, avg_px TEXT)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("BTC-USDT", "buy", 100.0, "0", "FILLED", "50000"),
            ("ETH-USDT", "sell", 120.0, "0", "FILLED", "2500"),
        ],
    )
    con.commit()
    con.close()

    response = client.get("/api/account")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["cash_usdt"] == 50.0
    assert payload["total_equity_usdt"] == 150.0
    assert payload["total_trades"] == 2
    assert payload["peak_equity_usdt"] == 170.0
    assert payload["last_update"] == "2024-03-12 07:33:20"


def test_account_api_degrades_when_positions_endpoint_returns_error_tuple(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "load_config", lambda: {})
    monkeypatch.setattr(
        module,
        "api_positions",
        lambda: (module.jsonify({"error": "positions unavailable"}), 500),
    )

    (tmp_path / "reconcile_status.json").write_text(
        json.dumps({"exchange_snapshot": {"ccy_cashBal": {"USDT": 25.0}}}),
        encoding="utf-8",
    )

    response = client.get("/api/account")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["cash_usdt"] == 25.0
    assert payload["positions_value_usdt"] == 0.0
    assert payload["total_equity_usdt"] == 25.0


def test_trades_api_ignores_ambient_live_creds_by_default(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "get_db_connection", lambda: None)
    monkeypatch.delenv("V5_DASHBOARD_ALLOW_LIVE_OKX_ACCOUNT", raising=False)
    monkeypatch.delenv("V5_DASHBOARD_ALLOW_LIVE_OKX", raising=False)
    monkeypatch.setenv("EXCHANGE_API_KEY", "ambient-key")
    monkeypatch.setenv("EXCHANGE_API_SECRET", "ambient-secret")
    monkeypatch.setenv("EXCHANGE_PASSPHRASE", "ambient-passphrase")

    def _unexpected_request(*args, **kwargs):
        raise AssertionError("api/trades should not call live OKX without explicit enable flag")

    monkeypatch.setattr("requests.get", _unexpected_request)

    response = client.get("/api/trades")

    assert response.status_code == 200
    assert response.get_json()["trades"] == []


def test_positions_api_ignores_ambient_live_creds_by_default(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.delenv("V5_DASHBOARD_ALLOW_LIVE_OKX_ACCOUNT", raising=False)
    monkeypatch.delenv("V5_DASHBOARD_ALLOW_LIVE_OKX", raising=False)
    monkeypatch.setenv("EXCHANGE_API_KEY", "ambient-key")
    monkeypatch.setenv("EXCHANGE_API_SECRET", "ambient-secret")
    monkeypatch.setenv("EXCHANGE_PASSPHRASE", "ambient-passphrase")

    def _unexpected_request(*args, **kwargs):
        raise AssertionError("api/positions should not call live OKX without explicit enable flag")

    monkeypatch.setattr("requests.get", _unexpected_request)

    response = client.get("/api/positions")

    assert response.status_code == 200
    assert response.get_json()["positions"] == []


def test_health_api_ignores_ambient_live_creds_by_default(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "_pick_timer_name", lambda: "v5-prod.user.timer")
    monkeypatch.setattr(module, "_get_timer_state", lambda _name: {"active": True, "error": None})
    monkeypatch.delenv("V5_DASHBOARD_ALLOW_LIVE_OKX_ACCOUNT", raising=False)
    monkeypatch.delenv("V5_DASHBOARD_ALLOW_LIVE_OKX", raising=False)
    monkeypatch.setenv("EXCHANGE_API_KEY", "ambient-key")
    monkeypatch.setenv("EXCHANGE_API_SECRET", "ambient-secret")
    monkeypatch.setenv("EXCHANGE_PASSPHRASE", "ambient-passphrase")

    conn = sqlite3.connect(str(tmp_path / "orders.sqlite"))
    cur = conn.cursor()
    cur.execute("CREATE TABLE orders (inst_id TEXT)")
    cur.execute("INSERT INTO orders(inst_id) VALUES ('BTC-USDT')")
    conn.commit()
    conn.close()

    def _unexpected_request(*args, **kwargs):
        raise AssertionError("api/health should not call live OKX without explicit enable flag")

    monkeypatch.setattr("requests.get", _unexpected_request)

    response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "warning"
    assert any(check.get("name") == "OKX API" and check.get("status") == "warning" for check in payload["checks"])


def test_health_api_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/health")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["status"] == "error"
    assert payload["error"] == "internal server error"
    assert payload["checks"] == []
    assert payload["warning_count"] == 0
    assert payload["critical_count"] == 0
    assert "live_prod.yaml" not in body
    assert "/home/ubuntu/clawd/v5-prod" not in body
    assert "Traceback" not in body


def test_sentiment_api_degrades_when_scores_endpoint_returns_error_tuple(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(
        module,
        "api_scores",
        lambda: (module.jsonify({"error": "scores unavailable"}), 500),
    )

    cache_dir = tmp_path / "data" / "sentiment_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    sentiment_payload = {
        "f6_sentiment": 0.3,
        "f6_fear_greed_index": 61,
        "f6_market_stage": "neutral",
        "f6_sentiment_summary": "stable",
        "f6_sentiment_source": "rss",
    }
    for symbol in ("BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT"):
        (cache_dir / f"rss_{symbol}_20260410_13.json").write_text(
            json.dumps(sentiment_payload),
            encoding="utf-8",
        )

    response = client.get("/api/sentiment")

    assert response.status_code == 200
    payload = response.get_json()
    assert set(payload["by_symbol"].keys()) == {"BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT"}


def test_health_api_marks_warning_when_database_probe_errors(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "orders.sqlite").mkdir()

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.setattr(module, "_pick_timer_name", lambda: "v5-prod.user.timer")
    monkeypatch.setattr(module, "_get_timer_state", lambda _name: {"active": True, "error": None})
    monkeypatch.setattr(module, "_dashboard_live_account_enabled", lambda: False)

    response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "warning"
    assert payload["warning_count"] >= 1
    assert any(
        check.get("status") == "warning" and "unable to open database file" in str(check.get("detail", ""))
        for check in payload["checks"]
    )


def test_health_api_promotes_database_warning_even_when_other_checks_are_healthy(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "orders.sqlite").mkdir()

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.setattr(module, "_pick_timer_name", lambda: "v5-prod.user.timer")
    monkeypatch.setattr(module, "_get_timer_state", lambda _name: {"active": True, "error": None})
    monkeypatch.setattr(module, "_dashboard_live_account_enabled", lambda: True)
    monkeypatch.setattr(module, "_load_workspace_exchange_creds", lambda: ("k", "s", "p"))
    monkeypatch.setattr(module, "_load_okx_account_balance", lambda *_args: {"code": "0", "data": []})

    response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "warning"
    assert payload["warning_count"] >= 1
    assert any(
        check.get("name") == "数据库"
        and check.get("status") == "warning"
        and "unable to open database file" in str(check.get("detail", ""))
        for check in payload["checks"]
    )
    assert any(check.get("name") == "OKX API" and check.get("status") == "healthy" for check in payload["checks"])


def test_health_api_uses_active_runtime_orders_db(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    reports_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "SYSTEMCTL_BIN", None)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.delenv("V5_DASHBOARD_ALLOW_LIVE_OKX_ACCOUNT", raising=False)
    monkeypatch.delenv("V5_DASHBOARD_ALLOW_LIVE_OKX", raising=False)
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)

    root_orders_db = reports_dir / "orders.sqlite"
    conn = sqlite3.connect(str(root_orders_db))
    cur = conn.cursor()
    cur.execute("CREATE TABLE orders (inst_id TEXT)")
    cur.execute("INSERT INTO orders(inst_id) VALUES ('BTC-USDT')")
    conn.commit()
    conn.close()

    response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "critical"
    assert any(
        check.get("status") == "critical" and "orders.sqlite" in str(check.get("detail", ""))
        for check in payload["checks"]
    )


def test_health_api_reuses_recent_okx_probe_result(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "SYSTEMCTL_BIN", None)
    monkeypatch.setattr(module, "load_config", lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}})
    monkeypatch.setattr(module, "_pick_timer_name", lambda: "v5-prod.user.timer")
    monkeypatch.setattr(module, "_get_timer_state", lambda _name: {"active": True, "error": None})
    monkeypatch.setattr(module, "_dashboard_live_account_enabled", lambda: True)
    monkeypatch.setattr(module, "_load_workspace_exchange_creds", lambda: ("key", "sec", "pp"))
    monkeypatch.setenv("V5_DASHBOARD_HEALTH_OKX_CACHE_TTL_SECONDS", "60")
    module._OKX_HEALTH_CHECK_CACHE.clear()

    conn = sqlite3.connect(str(reports_dir / "orders.sqlite"))
    cur = conn.cursor()
    cur.execute("CREATE TABLE orders (inst_id TEXT)")
    cur.execute("INSERT INTO orders(inst_id) VALUES ('BTC-USDT')")
    conn.commit()
    conn.close()

    calls = {"count": 0}

    def _fake_balance_loader(key, sec, pp):
        calls["count"] += 1
        return {"code": "0", "data": [{"details": []}]}

    monkeypatch.setattr(module, "_load_okx_account_balance", _fake_balance_loader)

    first = client.get("/api/health")
    second = client.get("/api/health")

    assert first.status_code == 200
    assert second.status_code == 200
    assert calls["count"] == 1
    second_payload = second.get_json()
    assert any(
        check.get("name") == "OKX API" and check.get("status") == "healthy" and "cached" in str(check.get("detail", ""))
        for check in second_payload["checks"]
    )


def test_api_equity_history_uses_active_runtime_runs_dir(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    root_run = reports_dir / "runs" / "20260408_02"
    runtime_run = runtime_dir / "runs" / "20260408_01"
    root_run.mkdir(parents=True, exist_ok=True)
    runtime_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    (root_run / "equity.jsonl").write_text(
        json.dumps({"ts": "2026-04-08T10:00:00", "equity": 999.0}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (runtime_run / "equity.jsonl").write_text(
        json.dumps({"ts": "2026-04-08T11:00:00", "equity": 123.0}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    response = client.get("/api/equity_history")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload == [{"timestamp": "2026-04-08T11:00:00", "value": 123.0}]


def test_api_equity_history_prefers_logically_newer_run_for_duplicate_timestamp(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    stale_run = runs_dir / "stale"
    fresh_run = runs_dir / "fresh"
    stale_run.mkdir(parents=True, exist_ok=True)
    fresh_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}},
    )

    stale_equity = stale_run / "equity.jsonl"
    fresh_equity = fresh_run / "equity.jsonl"
    stale_equity.write_text(
        json.dumps({"ts": "2026-04-08T10:00:00", "equity": 111.0}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    fresh_equity.write_text(
        json.dumps({"ts": "2026-04-08T10:00:00", "equity": 222.0}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    os.utime(stale_equity, (100, 100))
    os.utime(fresh_equity, (200, 200))

    response = client.get("/api/equity_history")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload == [{"timestamp": "2026-04-08T10:00:00", "value": 222.0}]


def test_load_equity_points_limits_recent_equity_file_reads_before_parsing(tmp_path, monkeypatch):
    module = load_web_dashboard_module()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    runtime_paths = module.DashboardRuntimePaths(
        reports_dir=reports_dir,
        orders_db=reports_dir / "orders.sqlite",
        fills_db=reports_dir / "fills.sqlite",
        positions_db=reports_dir / "positions.sqlite",
        kill_switch_path=reports_dir / "kill_switch.json",
        reconcile_status_path=reports_dir / "reconcile_status.json",
        runs_dir=runs_dir,
        auto_risk_guard_path=reports_dir / "auto_risk_guard.json",
        auto_risk_eval_path=reports_dir / "auto_risk_eval.json",
        telemetry_db=reports_dir / "api_telemetry.sqlite",
    )

    now = datetime.now()
    recent_hours = {19, 18, 17, 16}
    for hour in range(20):
        day_offset = 0 if hour in recent_hours else 10
        run_dt = now - timedelta(days=day_offset, hours=19 - hour)
        run_name = run_dt.strftime("%Y%m%d_%H")
        run_dir = runs_dir / run_name
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "equity.jsonl").write_text(
            json.dumps(
                {
                    "ts": (run_dt + timedelta(minutes=30)).isoformat(),
                    "equity": 100.0 + hour,
                },
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )

    original_open = Path.open
    reads = {"equity": 0}

    def counting_open(self: Path, *args, **kwargs):
        if self.name == "equity.jsonl":
            reads["equity"] += 1
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counting_open)

    points = module._load_equity_points(limit=4, runtime_paths=runtime_paths)

    assert len(points) == 4
    assert reads["equity"] <= 4


def test_equity_history_error_response_preserves_list_shape(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/equity_history")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload == []
    _assert_body_hides_internal_details(body, "/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml", "live_prod.yaml")


def test_equity_curve_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError(r"C:\secret\configs\live_prod.yaml")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/equity_curve")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["dates"] == []
    assert payload["values"] == []
    assert payload["pnl"] == []
    assert payload["initial"] == 0
    assert payload["current"] == 0
    assert payload["total_return"] == 0
    assert payload["days"] == 0
    _assert_internal_error_hidden(body, r"C:\secret\configs\live_prod.yaml", "live_prod.yaml")


def test_auto_risk_guard_api_uses_auto_risk_eval_file(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "load_config", lambda: {})

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


def test_auto_risk_guard_api_uses_active_runtime_eval_path(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    (reports_dir / "auto_risk_eval.json").write_text(
        json.dumps({"current_level": "ATTACK", "metrics": {"dd_pct": 0.01}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (runtime_dir / "auto_risk_eval.json").write_text(
        json.dumps({"current_level": "PROTECT", "metrics": {"dd_pct": 0.25}}, ensure_ascii=False),
        encoding="utf-8",
    )

    response = client.get("/api/auto_risk_guard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_level"] == "PROTECT"
    assert payload["metrics"]["dd_pct"] == 0.25


def test_auto_risk_guard_api_uses_prefixed_runtime_eval_path(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )

    (reports_dir / "auto_risk_eval.json").write_text(
        json.dumps({"current_level": "ATTACK", "metrics": {"dd_pct": 0.01}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "shadow_auto_risk_eval.json").write_text(
        json.dumps({"current_level": "PROTECT", "metrics": {"dd_pct": 0.33}}, ensure_ascii=False),
        encoding="utf-8",
    )

    response = client.get("/api/auto_risk_guard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_level"] == "PROTECT"
    assert payload["metrics"]["dd_pct"] == 0.33


def test_auto_risk_guard_api_uses_suffixed_runtime_eval_path(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )

    (reports_dir / "auto_risk_eval.json").write_text(
        json.dumps({"current_level": "ATTACK", "metrics": {"dd_pct": 0.01}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "auto_risk_eval_accelerated.json").write_text(
        json.dumps({"current_level": "DEFENSE", "metrics": {"dd_pct": 0.41}}, ensure_ascii=False),
        encoding="utf-8",
    )

    response = client.get("/api/auto_risk_guard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_level"] == "DEFENSE"
    assert payload["metrics"]["dd_pct"] == 0.41


def test_auto_risk_guard_api_prefers_newer_guard_state_over_stale_eval(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    (runtime_dir / "auto_risk_eval.json").write_text(
        json.dumps(
            {
                "ts": "2026-04-19T13:00:00",
                "current_level": "PROTECT",
                "config": {"max_positions": 1},
                "metrics": {"dd_pct": 0.25},
                "reason": "stale eval snapshot",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_dir / "auto_risk_guard.json").write_text(
        json.dumps(
            {
                "current_level": "DEFENSE",
                "current_config": {"max_positions": 3},
                "metrics": {"last_dd_pct": 0.12},
                "history": [{"to": "DEFENSE", "reason": "newer guard state", "ts": "2026-04-19T14:00:00"}],
                "last_update": "2026-04-19T14:05:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    response = client.get("/api/auto_risk_guard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_level"] == "DEFENSE"
    assert payload["config"]["max_positions"] == 3
    assert payload["reason"] == "newer guard state"
    assert payload["last_update"] == "2026-04-19T14:05:00"


def test_auto_risk_guard_api_prefers_latest_eval_history_ts_when_eval_history_is_unsorted(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    (runtime_dir / "auto_risk_eval.json").write_text(
        json.dumps(
            {
                "current_level": "PROTECT",
                "config": {"max_positions": 1},
                "metrics": {"dd_pct": 0.25},
                "reason": "newer eval history",
                "history": [
                    {"ts": "2026-04-19T15:05:00", "to": "PROTECT"},
                    {"ts": "2026-04-19T13:00:00", "to": "DEFENSE"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_dir / "auto_risk_guard.json").write_text(
        json.dumps(
            {
                "current_level": "DEFENSE",
                "current_config": {"max_positions": 3},
                "metrics": {"last_dd_pct": 0.12},
                "last_update": "2026-04-19T14:05:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    response = client.get("/api/auto_risk_guard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_level"] == "PROTECT"
    assert payload["config"]["max_positions"] == 1
    assert payload["reason"] == "newer eval history"


def test_calculate_market_indicators_prefers_latest_cache_file_by_filename_timestamp(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    older = cache_dir / "BTC_USDT_1H_20260408_00.csv"
    newer = cache_dir / "BTC_USDT_1H_20260408_01.csv"

    def _write_csv(path: Path, close_value: float) -> None:
        rows = ["timestamp,open,high,low,close,volume"]
        for idx in range(60):
            base = 100 + idx
            rows.append(f"{idx},{base},{base + 1},{base - 1},{close_value if idx == 59 else base},1")
        path.write_text("\n".join(rows), encoding="utf-8")

    _write_csv(older, 999)
    _write_csv(newer, 111)
    os.utime(older, (200, 200))
    os.utime(newer, (100, 100))

    monkeypatch.setattr(module, "CACHE_DIR", cache_dir)

    indicators = module.calculate_market_indicators()

    assert indicators["price"] == 111


def test_auto_risk_guard_api_falls_back_to_runtime_guard_path_when_eval_missing(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    (reports_dir / "auto_risk_guard.json").write_text(
        json.dumps({"current_level": "ATTACK", "metrics": {"last_dd_pct": 0.01}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (runtime_dir / "auto_risk_guard.json").write_text(
        json.dumps(
            {
                "current_level": "PROTECT",
                "current_config": {"max_positions": 1},
                "metrics": {"last_dd_pct": 0.25},
                "history": [{"to": "PROTECT", "reason": "runtime guard fallback", "ts": "2026-04-19T13:00:00"}],
                "last_update": "2026-04-19T13:05:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    response = client.get("/api/auto_risk_guard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_level"] == "PROTECT"
    assert payload["config"]["max_positions"] == 1
    assert payload["metrics"]["last_dd_pct"] == 0.25
    assert payload["reason"] == "runtime guard fallback"
    assert payload["last_update"] == "2026-04-19T13:05:00"

    (runtime_dir / "auto_risk_guard.json").write_text(
        json.dumps(
            {
                "current_level": "DEFENSE",
                "current_config": {"max_positions": 3},
                "metrics": {"last_dd_pct": 0.12},
                "history": [{"to": "DEFENSE", "reason": "recovered from protect", "ts": "2026-04-19T14:00:00"}],
                "last_update": "2026-04-19T14:05:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    response = client.get("/api/auto_risk_guard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_level"] == "DEFENSE"
    assert payload["config"]["max_positions"] == 3
    assert payload["metrics"]["last_dd_pct"] == 0.12
    assert payload["reason"] == "recovered from protect"
    assert payload["last_update"] == "2026-04-19T14:05:00"


def test_auto_risk_guard_api_accepts_legacy_guard_level_field(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    (runtime_dir / "auto_risk_guard.json").write_text(
        json.dumps(
            {
                "level": "PROTECT",
                "current_config": {"max_positions": 1},
                "metrics": {"last_dd_pct": 0.25},
                "history": [{"to": "PROTECT", "reason": "legacy guard schema", "ts": "2026-04-19T13:00:00"}],
                "last_update": "2026-04-19T13:05:00",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    response = client.get("/api/auto_risk_guard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_level"] == "PROTECT"
    assert payload["config"]["max_positions"] == 1
    assert payload["reason"] == "legacy guard schema"


def test_auto_risk_guard_api_prefers_latest_matching_history_ts_when_history_is_unsorted(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    (runtime_dir / "auto_risk_guard.json").write_text(
        json.dumps(
            {
                "current_level": "DEFENSE",
                "current_config": {"max_positions": 3},
                "metrics": {"last_dd_pct": 0.12},
                "history": [
                    {"to": "DEFENSE", "reason": "newest defense", "ts": "2026-04-19T14:05:00"},
                    {"to": "PROTECT", "reason": "older protect", "ts": "2026-04-19T14:00:00"},
                    {"to": "DEFENSE", "reason": "older defense", "ts": "2026-04-19T13:55:00"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    response = client.get("/api/auto_risk_guard")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["current_level"] == "DEFENSE"
    assert payload["reason"] == "newest defense"
    assert payload["last_update"] == "2026-04-19T14:05:00"


def test_auto_risk_guard_api_sorts_history_tail_by_ts(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    history = [
        {"to": "DEFENSE", "reason": "14:03", "ts": "2026-04-19T14:03:00"},
        {"to": "DEFENSE", "reason": "13:59", "ts": "2026-04-19T13:59:00"},
        {"to": "DEFENSE", "reason": "14:05", "ts": "2026-04-19T14:05:00"},
        {"to": "DEFENSE", "reason": "14:01", "ts": "2026-04-19T14:01:00"},
        {"to": "DEFENSE", "reason": "14:04", "ts": "2026-04-19T14:04:00"},
        {"to": "DEFENSE", "reason": "14:02", "ts": "2026-04-19T14:02:00"},
    ]
    (runtime_dir / "auto_risk_guard.json").write_text(
        json.dumps(
            {
                "current_level": "DEFENSE",
                "current_config": {"max_positions": 3},
                "metrics": {"last_dd_pct": 0.12},
                "history": history,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    response = client.get("/api/auto_risk_guard")

    assert response.status_code == 200
    payload = response.get_json()
    assert [item["ts"] for item in payload["history"]] == [
        "2026-04-19T14:01:00",
        "2026-04-19T14:02:00",
        "2026-04-19T14:03:00",
        "2026-04-19T14:04:00",
        "2026-04-19T14:05:00",
    ]


def test_auto_risk_guard_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/auto_risk_guard")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["current_level"] == "UNKNOWN"
    assert payload["config"] == {}
    assert payload["history"] == []
    assert payload["metrics"] == {}
    assert "live_prod.yaml" not in body
    assert "/home/ubuntu/clawd/v5-prod" not in body
    assert "Traceback" not in body


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


def test_market_state_prefers_live_funding_vote_over_snapshot(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    monkeypatch.setattr(module, "_load_market_state_snapshot", lambda _: {
        "state": "SIDEWAYS",
        "position_multiplier": 0.8,
        "method": "decision_audit",
        "votes": {
            "hmm": {"state": "SIDEWAYS", "confidence": 0.7, "probs": {"TrendingUp": 0.1, "Sideways": 0.7, "TrendingDown": 0.2}},
            "funding": {"state": "SIDEWAYS", "confidence": 0.54, "sentiment": -0.027},
        },
        "alerts": [],
        "monitor": {},
    })
    monkeypatch.setattr(module, "_load_latest_regime_history_snapshot", lambda _: {"votes": {}})
    monkeypatch.setattr(module, "_load_market_vote_history", lambda *args, **kwargs: [])
    monkeypatch.setattr(module, "load_config", lambda: {"regime": {"hmm_weight": 0.35, "funding_weight": 0.4, "rss_weight": 0.25}})
    monkeypatch.setattr(module, "_signal_health", lambda *args, **kwargs: {"status": "fresh", "is_fresh": True, "error": None})
    monkeypatch.setattr(module, "_build_live_funding_vote", lambda *args, **kwargs: {
        "state": "TRENDING",
        "confidence": 0.18,
        "weight": 0.4,
        "sentiment": 0.09,
        "composite": True,
        "details": {"large": {"avg": 0.1, "count": 2}},
        "raw_state": "TRENDING",
    })
    monkeypatch.setattr(module, "_build_live_rss_vote", lambda *args, **kwargs: {})
    monkeypatch.setattr(module, "calculate_market_indicators", lambda: {"price": 0.0})

    response = client.get("/api/market_state")

    assert response.status_code == 200
    payload = response.get_json()
    funding = payload["votes"]["funding"]
    assert funding["state"] == "TRENDING"
    assert funding["confidence"] == 0.18
    assert funding["sentiment"] == 0.09
    assert funding["composite"] is True


def test_api_market_state_uses_active_runtime_reports_dir(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    seen: dict[str, Path] = {}

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"},
            "regime": {"hmm_weight": 0.35, "funding_weight": 0.4, "rss_weight": 0.25},
        },
    )

    def fake_snapshot(path):
        seen["snapshot"] = path
        return {
            "state": "TRENDING",
            "position_multiplier": 1.2,
            "method": "decision_audit",
            "ts": 1_710_100_000.0,
            "votes": {"hmm": {"state": "TRENDING", "confidence": 0.7}},
            "alerts": [],
            "monitor": {},
        }

    def fake_history_snapshot(path):
        seen["history_snapshot"] = path
        return {"votes": {}}

    def fake_history(path, hours=24, max_points=24):
        seen["history_24h"] = path
        return [
            {
                "ts_ms": 1_710_000_000_000,
                "label": "04-08 10:00",
                "final": {"state": "TRENDING", "confidence": 0.6, "score": 0.4},
                "votes": {
                    "hmm": {"state": "TRENDING", "confidence": 0.7},
                    "funding": {"state": "SIDEWAYS", "confidence": 0.2, "sentiment": 0.01},
                    "rss": {"state": "TRENDING", "confidence": 0.3, "sentiment": 0.2},
                },
            }
        ]

    monkeypatch.setattr(module, "_load_market_state_snapshot", fake_snapshot)
    monkeypatch.setattr(module, "_load_latest_regime_history_snapshot", fake_history_snapshot)
    monkeypatch.setattr(module, "_load_market_vote_history", fake_history)
    monkeypatch.setattr(module, "_signal_health", lambda *args, **kwargs: {"status": "fresh", "is_fresh": True, "error": None})
    monkeypatch.setattr(module, "_build_live_funding_vote", lambda *args, **kwargs: {})
    monkeypatch.setattr(module, "_build_live_rss_vote", lambda *args, **kwargs: {})
    monkeypatch.setattr(module, "calculate_market_indicators", lambda: {"price": 0.0})

    response = client.get("/api/market_state")

    assert response.status_code == 200
    assert seen["snapshot"] == runtime_dir
    assert seen["history_snapshot"] == runtime_dir
    assert seen["history_24h"] == runtime_dir
    payload = response.get_json()
    assert payload["state"] == "TRENDING"
    assert payload["history_24h"][0]["final"]["state"] == "TRENDING"
    assert payload["last_update"] == datetime.fromtimestamp(1_710_100_000).strftime("%Y-%m-%d %H:%M:%S")


def test_market_state_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError(r"C:\secret\reports\market_state.json")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/market_state")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["state"] == "UNKNOWN"
    assert payload["position_multiplier"] == 0.0
    assert payload["votes"] == {}
    assert payload["alerts"] == []
    assert payload["history_24h"] == []
    assert "market_state.json" not in body
    assert "C:\\secret" not in body
    assert "Traceback" not in body


def test_api_cost_calibration_uses_active_runtime_reports_dir(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    root_cost_dir = reports_dir / "cost_stats_real"
    runtime_cost_dir = runtime_dir / "cost_stats_real"
    root_cost_dir.mkdir(parents=True, exist_ok=True)
    runtime_cost_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    (root_cost_dir / "daily_cost_stats_20260407.json").write_text(
        json.dumps(
            {
                "buckets": {
                    "all": {
                        "slippage_bps": {"mean": 9.0, "count": 3},
                        "fee_bps": {"mean": 4.0, "count": 3},
                    }
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_cost_dir / "daily_cost_stats_20260408.json").write_text(
        json.dumps(
            {
                "buckets": {
                    "all": {
                        "slippage_bps": {"mean": 1.5, "count": 2},
                        "fee_bps": {"mean": 0.5, "count": 2},
                    }
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    response = client.get("/api/cost_calibration")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "calibrating"
    assert payload["total_days"] == 1
    assert payload["avg_slippage_bps"] == 1.5
    assert payload["avg_fee_bps"] == 0.5
    assert payload["avg_total_cost_bps"] == 2.0
    assert payload["daily_stats"][0]["date"] == "20260408"


def test_api_cost_calibration_uses_prefixed_runtime_cost_stats_dir(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    root_cost_dir = reports_dir / "cost_stats_real"
    runtime_cost_dir = reports_dir / "shadow_cost_stats_real"
    root_cost_dir.mkdir(parents=True, exist_ok=True)
    runtime_cost_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )

    (root_cost_dir / "daily_cost_stats_20260407.json").write_text(
        json.dumps(
            {"buckets": {"all": {"slippage_bps": {"mean": 9.0, "count": 3}, "fee_bps": {"mean": 4.0, "count": 3}}}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_cost_dir / "daily_cost_stats_20260408.json").write_text(
        json.dumps(
            {"buckets": {"all": {"slippage_bps": {"mean": 1.2, "count": 2}, "fee_bps": {"mean": 0.3, "count": 2}}}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    response = client.get("/api/cost_calibration")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["total_days"] == 1
    assert payload["avg_slippage_bps"] == 1.2
    assert payload["avg_fee_bps"] == 0.3
    assert payload["avg_total_cost_bps"] == 1.5
    assert payload["daily_stats"][0]["date"] == "20260408"
    assert payload["last_update"] == "2026-04-08 00:00:00"


def test_api_cost_calibration_ignores_non_dated_stats_files(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    cost_dir = reports_dir / "cost_stats_real"
    cost_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}},
    )

    (cost_dir / "daily_cost_stats_20260408.json").write_text(
        json.dumps(
            {"buckets": {"all": {"slippage_bps": {"mean": 1.2, "count": 2}, "fee_bps": {"mean": 0.3, "count": 2}}}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (cost_dir / "daily_cost_stats_latest.json").write_text(
        json.dumps(
            {"buckets": {"all": {"slippage_bps": {"mean": 9.9, "count": 2}, "fee_bps": {"mean": 9.9, "count": 2}}}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    response = client.get("/api/cost_calibration")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["total_days"] == 1
    assert payload["avg_slippage_bps"] == 1.2
    assert payload["avg_fee_bps"] == 0.3
    assert payload["avg_total_cost_bps"] == 1.5
    assert payload["daily_stats"][0]["date"] == "20260408"


def test_api_cost_calibration_uses_suffixed_runtime_cost_events_dir(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    root_events_dir = reports_dir / "cost_events"
    runtime_events_dir = reports_dir / "cost_events_accelerated"
    root_events_dir.mkdir(parents=True, exist_ok=True)
    runtime_events_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )

    (root_events_dir / "20260407.jsonl").write_text(
        json.dumps({"slippage_bps": 9.0, "fee_bps": 4.0}) + "\n",
        encoding="utf-8",
    )
    (runtime_events_dir / "20260408.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"slippage_bps": 1.1, "fee_bps": 0.4}, ensure_ascii=False),
                json.dumps({"slippage_bps": 0.9, "fee_bps": 0.6}, ensure_ascii=False),
            ]
        ) + "\n",
        encoding="utf-8",
    )

    response = client.get("/api/cost_calibration")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["data_source"] == "events"
    assert payload["total_days"] == 1
    assert payload["event_files"] == 1
    assert payload["avg_slippage_bps"] == 1.0
    assert payload["avg_fee_bps"] == 0.5
    assert payload["avg_total_cost_bps"] == 1.5
    assert payload["daily_stats"][0]["date"] == "20260408"
    assert payload["last_update"] == "2026-04-08 00:00:00"


def test_api_cost_calibration_ignores_non_dated_event_files(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    events_dir = reports_dir / "cost_events"
    events_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}},
    )

    (events_dir / "20260408.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"slippage_bps": 1.1, "fee_bps": 0.4}, ensure_ascii=False),
                json.dumps({"slippage_bps": 0.9, "fee_bps": 0.6}, ensure_ascii=False),
            ]
        ) + "\n",
        encoding="utf-8",
    )
    (events_dir / "latest.jsonl").write_text(
        json.dumps({"slippage_bps": 99.0, "fee_bps": 99.0}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    response = client.get("/api/cost_calibration")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["data_source"] == "events"
    assert payload["total_days"] == 1
    assert payload["avg_slippage_bps"] == 1.0
    assert payload["avg_fee_bps"] == 0.5
    assert payload["avg_total_cost_bps"] == 1.5
    assert payload["daily_stats"][0]["date"] == "20260408"


def test_market_state_snapshot_falls_back_to_regime_json_after_failed_run(tmp_path):
    module = load_web_dashboard_module()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    failed_run = runs_dir / "20260312_00"
    failed_run.mkdir(parents=True, exist_ok=True)
    (failed_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": "Unknown",
                "top_scores": [],
                "counts": {"universe": 0, "scored": 0},
                "notes": ["No market data returned from provider"],
                "regime_details": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "regime.json").write_text(
        json.dumps(
            {
                "state": "Trending",
                "multiplier": 1.2,
                "final_score": 0.42,
                "votes": {"hmm": {"state": "TRENDING", "confidence": 0.81}},
                "alerts": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(reports_dir / "regime.json", (1_710_200_000, 1_710_200_000))
    os.utime(failed_run, (2, 2))

    snapshot = module._load_market_state_snapshot(reports_dir)

    assert snapshot["state"] == "Trending"
    assert snapshot["method"] == "regime_json"
    assert snapshot["final_score"] == 0.42
    assert snapshot["ts"] == 1_710_200_000
    assert snapshot["votes"]["hmm"]["state"] == "TRENDING"


def test_market_state_snapshot_limits_recent_decision_audit_scan(monkeypatch, tmp_path):
    module = load_web_dashboard_module()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("V5_DASHBOARD_MARKET_AUDIT_SCAN_LIMIT", "4")

    for hour in range(20):
        run_dir = runs_dir / f"20260312_{hour:02d}"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "decision_audit.json").write_text(
            json.dumps(
                {
                    "regime": "SIDEWAYS" if hour < 19 else "TRENDING",
                    "regime_multiplier": 0.8 if hour < 19 else 1.2,
                    "final_score": hour / 100,
                    "regime_details": {
                        "final_state": "SIDEWAYS" if hour < 19 else "TRENDING",
                        "method": "decision_audit",
                        "votes": {"hmm": {"state": "TRENDING", "confidence": 0.7}},
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    original_load_json_payload = module._load_json_payload
    reads = {"decision_audit": 0}

    def counting_load_json_payload(path):
        if Path(path).name == "decision_audit.json":
            reads["decision_audit"] += 1
        return original_load_json_payload(path)

    monkeypatch.setattr(module, "_load_json_payload", counting_load_json_payload)

    snapshot = module._load_market_state_snapshot(reports_dir)

    assert snapshot["state"] == "TRENDING"
    assert snapshot["position_multiplier"] == 1.2
    assert snapshot["final_score"] == pytest.approx(0.19)
    assert snapshot["ts"] == datetime.strptime("20260312_19", "%Y%m%d_%H").timestamp()
    assert reads["decision_audit"] <= 4


def test_decision_chain_legacy_utc_run_time_is_not_double_shifted(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    legacy_run = runs_dir / "20260311_00"
    legacy_run.mkdir(parents=True, exist_ok=True)
    (legacy_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260311_00",
                "now_ts": 3600,
                "regime": "SIDEWAYS",
                "top_scores": [{"symbol": "BTC/USDT", "score": 0.5, "rank": 1}],
                "counts": {"selected": 1, "targets_pre_risk": 1, "orders_rebalance": 0, "orders_exit": 0},
                "router_decisions": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(legacy_run / "decision_audit.json", (0, 0))
    os.utime(legacy_run, (0, 0))
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)

    class _UtcNaiveDateTime(datetime):
        @classmethod
        def fromtimestamp(cls, ts, tz=None):
            if tz is None:
                return cls.utcfromtimestamp(ts)
            return super().fromtimestamp(ts, tz=tz)

    monkeypatch.setattr(module, "datetime", _UtcNaiveDateTime)

    response = client.get("/api/decision_chain")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["rounds"][0]["run_id"] == "20260311_00"
    assert payload["rounds"][0]["time"] == "1970-01-01 09:00:00"


def test_decision_chain_uses_active_runtime_runs_dir(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    root_run = reports_dir / "runs" / "20260408_02"
    runtime_run = runtime_dir / "runs" / "20260408_01"
    root_run.mkdir(parents=True, exist_ok=True)
    runtime_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    (root_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260408_02",
                "now_ts": 1_710_000_000,
                "regime": "SIDEWAYS",
                "top_scores": [{"symbol": "BTC/USDT", "score": 0.5, "rank": 1}],
                "counts": {"selected": 1, "targets_pre_risk": 1, "orders_rebalance": 0, "orders_exit": 0},
                "router_decisions": [{"reason": "deadband", "symbol": "BTC/USDT", "drift": 0.02, "deadband": 0.04}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260408_01",
                "now_ts": 1_710_000_600,
                "regime": "TRENDING",
                "top_scores": [{"symbol": "ETH/USDT", "score": 0.9, "rank": 1}],
                "counts": {
                    "selected": 2,
                    "targets_pre_risk": 2,
                    "orders_rebalance": 1,
                    "orders_exit": 0,
                    "negative_expectancy_score_penalty": 7,
                    "negative_expectancy_cooldown": 1,
                    "negative_expectancy_open_block": 2,
                    "negative_expectancy_fast_fail_open_block": 3,
                },
                "router_decisions": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(root_run, (20, 20))
    os.utime(runtime_run, (10, 10))

    response = client.get("/api/decision_chain")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["rounds"][0]["run_id"] == "20260408_01"
    assert payload["rounds"][0]["strategy_signals"][0]["symbol"] == "ETH/USDT"
    assert payload["rounds"][0]["execution_result"]["orders_rebalance"] == 1
    assert payload["rounds"][0]["execution_result"]["negative_expectancy_score_penalty"] == 7
    assert payload["rounds"][0]["execution_result"]["negative_expectancy_cooldown"] == 1
    assert payload["rounds"][0]["execution_result"]["negative_expectancy_open_block"] == 2
    assert payload["rounds"][0]["execution_result"]["negative_expectancy_fast_fail_open_block"] == 3
    assert payload["last_update"] == datetime.fromtimestamp(1_710_000_600).strftime("%Y-%m-%d %H:%M:%S")


def test_decision_chain_prefers_decision_audit_file_mtime_over_run_dir_mtime(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    stale_run = runs_dir / "20260408_02"
    fresh_run = runs_dir / "20260408_01"
    stale_run.mkdir(parents=True, exist_ok=True)
    fresh_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "load_config", lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}})

    stale_audit = stale_run / "decision_audit.json"
    fresh_audit = fresh_run / "decision_audit.json"
    stale_audit.write_text(json.dumps({"run_id": "20260408_02", "counts": {"selected": 1}}), encoding="utf-8")
    fresh_audit.write_text(json.dumps({"run_id": "20260408_01", "counts": {"selected": 2}}), encoding="utf-8")
    os.utime(stale_audit, (100, 100))
    os.utime(fresh_audit, (200, 200))
    os.utime(stale_run, (500, 500))
    os.utime(fresh_run, (50, 50))

    response = client.get("/api/decision_chain")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["rounds"][0]["run_id"] == "20260408_01"


def test_decision_chain_scan_limit_prefers_sorted_epoch_over_file_mtime(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    older_run = runs_dir / "20260408_01"
    newer_run = runs_dir / "20260408_02"
    older_run.mkdir(parents=True, exist_ok=True)
    newer_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "load_config", lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}})
    monkeypatch.setenv("V5_DASHBOARD_DECISION_CHAIN_SCAN_LIMIT", "1")

    older_audit = older_run / "decision_audit.json"
    newer_audit = newer_run / "decision_audit.json"
    older_audit.write_text(json.dumps({"run_id": "20260408_01", "counts": {"selected": 1}}), encoding="utf-8")
    newer_audit.write_text(json.dumps({"run_id": "20260408_02", "counts": {"selected": 2}}), encoding="utf-8")
    os.utime(older_audit, (200, 200))
    os.utime(newer_audit, (100, 100))

    response = client.get("/api/decision_chain")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["rounds"][0]["run_id"] == "20260408_02"
    assert payload["last_update"] == datetime.strptime("20260408_02", "%Y%m%d_%H").strftime("%Y-%m-%d %H:%M:%S")


def test_decision_chain_prefers_sorted_epoch_over_file_mtime_without_scan_limit(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    older_run = runs_dir / "20260408_01"
    newer_run = runs_dir / "20260408_02"
    older_run.mkdir(parents=True, exist_ok=True)
    newer_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "load_config", lambda: {"execution": {"order_store_path": "reports/orders.sqlite"}})

    older_audit = older_run / "decision_audit.json"
    newer_audit = newer_run / "decision_audit.json"
    older_audit.write_text(json.dumps({"run_id": "20260408_01", "counts": {"selected": 1}}), encoding="utf-8")
    newer_audit.write_text(json.dumps({"run_id": "20260408_02", "counts": {"selected": 2}}), encoding="utf-8")
    os.utime(older_audit, (200, 200))
    os.utime(newer_audit, (100, 100))

    response = client.get("/api/decision_chain")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["rounds"][0]["run_id"] == "20260408_02"


def test_iter_decision_audits_applies_scan_limit_after_sort_epoch(tmp_path):
    import scripts.web_dashboard as module

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    older_run = runs_dir / "20260408_01"
    newer_run = runs_dir / "20260408_02"
    older_run.mkdir(parents=True, exist_ok=True)
    newer_run.mkdir(parents=True, exist_ok=True)

    older_audit = older_run / "decision_audit.json"
    newer_audit = newer_run / "decision_audit.json"
    older_audit.write_text(json.dumps({"run_id": "20260408_01"}), encoding="utf-8")
    newer_audit.write_text(json.dumps({"run_id": "20260408_02"}), encoding="utf-8")

    # Make the older run look newer by file mtime, so pre-limit-on-mtime would choose the wrong one.
    os.utime(older_audit, (200, 200))
    os.utime(newer_audit, (100, 100))

    entries = module._iter_decision_audits(reports_dir, scan_limit=1)

    assert len(entries) == 1
    assert entries[0]["run_dir"].name == "20260408_02"


def test_decision_chain_error_response_hides_internal_paths(monkeypatch):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    def raise_load_config():
        raise FileNotFoundError("/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml")

    monkeypatch.setattr(module, "load_config", raise_load_config)

    response = client.get("/api/decision_chain")

    assert response.status_code == 500
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["error"] == "internal server error"
    assert payload["rounds"] == []
    assert payload["last_update"] == ""
    _assert_internal_error_hidden(body, "/home/ubuntu/clawd/v5-prod/configs/live_prod.yaml", "live_prod.yaml")


def test_decision_chain_run_parse_error_is_sanitized(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_run = runtime_dir / "runs" / "20260408_01"
    runtime_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    (runtime_run / "decision_audit.json").write_text('{"regime": ', encoding="utf-8")

    response = client.get("/api/decision_chain")

    assert response.status_code == 200
    body = response.get_data(as_text=True)
    payload = response.get_json()
    assert payload["rounds"][0]["run_id"] == "20260408_01"
    assert payload["rounds"][0]["error"] == "internal parse error"
    assert "JSONDecodeError" not in body
    assert "Expecting value" not in body


def test_api_shadow_test_uses_active_runtime_paths(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    root_run = reports_dir / "runs" / "20260408_02"
    runtime_run = runtime_dir / "runs" / "20260408_01"
    root_run.mkdir(parents=True, exist_ok=True)
    runtime_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"},
            "rebalance": {"deadband_sideways": 0.07},
        },
    )

    (root_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "counts": {"selected": 100, "orders_rebalance": 40, "orders_exit": 5},
                "router_decisions": [{"reason": "deadband", "drift": 0.035}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (runtime_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "counts": {
                    "selected": 10,
                    "orders_rebalance": 2,
                    "orders_exit": 1,
                    "negative_expectancy_score_penalty": 3,
                    "negative_expectancy_cooldown": 4,
                    "negative_expectancy_open_block": 5,
                    "negative_expectancy_fast_fail_open_block": 6,
                },
                "router_decisions": [{"reason": "deadband", "drift": 0.035}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(root_run, (20, 20))
    os.utime(runtime_run, (10, 10))

    (runtime_dir / "ab_gate_status.json").write_text(
        json.dumps(
            {
                "window_runs": 1,
                "current": {"conversion": 0.2},
                "candidate": {"conversion": 0.3},
                "decision": {"switch_recommended": True},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (reports_dir / "ab_gate_status.json").write_text(
        json.dumps(
            {
                "window_runs": 99,
                "current": {"conversion": 0.4},
                "candidate": {"conversion": 0.5},
                "decision": {"switch_recommended": False},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    response = client.get("/api/shadow_test")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["window_rounds"] == 1
    assert payload["comparison"]["current"]["avg_selected_per_round"] == 10.0
    assert payload["comparison"]["current"]["avg_rebalance_per_round"] == 2.0
    assert payload["comparison"]["current"]["negative_expectancy_score_penalty_count"] == 3
    assert payload["comparison"]["current"]["negative_expectancy_cooldown_count"] == 4
    assert payload["comparison"]["current"]["negative_expectancy_open_block_count"] == 5
    assert payload["comparison"]["current"]["negative_expectancy_fast_fail_open_block_count"] == 6
    assert payload["ab_gate"]["window_runs"] == 1
    assert payload["ab_gate"]["decision"]["switch_recommended"] is True
    assert payload["ab_gate_status"] == "fresh"
    assert payload["current_params"]["deadband_sideways"] == 0.07
    assert payload["proposed_params"]["deadband_sideways"] == 0.06
    assert payload["matrix"][0]["name"] == "A(当前)"
    assert payload["matrix"][0]["params"]["deadband_sideways"] == payload["current_params"]["deadband_sideways"]
    assert payload["last_update"] == datetime.fromtimestamp(module._run_id_epoch("20260408_01")).strftime("%Y-%m-%d %H:%M:%S")


def test_api_shadow_test_prefers_sorted_epoch_over_file_mtime_when_limited_to_50_runs(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runs_dir = runtime_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"},
            "rebalance": {"deadband_sideways": 0.07},
        },
    )

    for idx in range(50):
        run_name = f"20260408_{idx:02d}"
        run_dir = runs_dir / run_name
        run_dir.mkdir(parents=True, exist_ok=True)
        audit_path = run_dir / "decision_audit.json"
        audit_path.write_text(
            json.dumps(
                {"run_id": run_name, "counts": {"selected": 1, "orders_rebalance": 0, "orders_exit": 0}, "router_decisions": []},
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        os.utime(audit_path, (200 + idx, 200 + idx))

    latest_run_name = "20260409_00"
    latest_run_dir = runs_dir / latest_run_name
    latest_run_dir.mkdir(parents=True, exist_ok=True)
    latest_audit = latest_run_dir / "decision_audit.json"
    latest_audit.write_text(
        json.dumps(
            {
                "run_id": latest_run_name,
                "counts": {"selected": 999, "orders_rebalance": 9, "orders_exit": 0},
                "router_decisions": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    # Misleadingly old file mtime: pre-fix code would exclude this run from the top-50 window.
    os.utime(latest_audit, (100, 100))

    response = client.get("/api/shadow_test")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["window_rounds"] == 50
    assert payload["comparison"]["current"]["avg_selected_per_round"] > 20
    assert payload["last_update"] == datetime.fromtimestamp(module._run_id_epoch("20260409_00")).strftime("%Y-%m-%d %H:%M:%S")


def test_api_shadow_test_does_not_refresh_stale_ab_gate_in_request(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_run = runtime_dir / "runs" / "20260408_01"
    runtime_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    (runtime_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "counts": {"selected": 10, "orders_rebalance": 2, "orders_exit": 1},
                "router_decisions": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    gate_path = runtime_dir / "ab_gate_status.json"
    gate_path.write_text(
        json.dumps({"window_runs": 1, "decision": {"switch_recommended": False}}, ensure_ascii=False),
        encoding="utf-8",
    )
    os.utime(gate_path, (1, 1))

    def fail_if_refreshed(*args, **kwargs):
        raise AssertionError("api/shadow_test must not refresh A/B gate synchronously")

    monkeypatch.setattr(module.subprocess, "run", fail_if_refreshed)

    response = client.get("/api/shadow_test")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ab_gate"]["window_runs"] == 1
    assert payload["ab_gate_status"] == "stale"
    assert payload["ab_gate_age_sec"] > 1800


def test_api_shadow_test_prefers_ab_gate_ts_over_file_mtime_for_freshness(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_run = runtime_dir / "runs" / "20260408_01"
    runtime_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )

    (runtime_run / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "20260408_01",
                "counts": {"selected": 10, "orders_rebalance": 2, "orders_exit": 1},
                "router_decisions": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    gate_path = runtime_dir / "ab_gate_status.json"
    gate_path.write_text(
        json.dumps(
            {
                "ts": "2026-04-08T10:00:00Z",
                "window_runs": 1,
                "decision": {"switch_recommended": False},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    os.utime(gate_path, (1_999_999_999, 1_999_999_999))

    response = client.get("/api/shadow_test")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ab_gate"]["window_runs"] == 1
    assert payload["ab_gate_status"] == "stale"
    assert payload["ab_gate_age_sec"] > 1800


def test_api_shadow_test_uses_prefixed_runtime_ab_gate_file(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    run_dir = runs_dir / "20260408_01"
    run_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_orders.sqlite"}},
    )

    (run_dir / "decision_audit.json").write_text(
        json.dumps({"counts": {"selected": 8, "orders_rebalance": 1, "orders_exit": 0}, "router_decisions": []}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "shadow_ab_gate_status.json").write_text(
        json.dumps({"window_runs": 3, "decision": {"switch_recommended": True}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "ab_gate_status.json").write_text(
        json.dumps({"window_runs": 99, "decision": {"switch_recommended": False}}, ensure_ascii=False),
        encoding="utf-8",
    )

    response = client.get("/api/shadow_test")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ab_gate"]["window_runs"] == 3
    assert payload["ab_gate"]["decision"]["switch_recommended"] is True


def test_api_shadow_test_uses_suffixed_runtime_ab_gate_file(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    client = module.app.test_client()

    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    run_dir = runs_dir / "20260408_01"
    run_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/orders_accelerated.sqlite"}},
    )

    (run_dir / "decision_audit.json").write_text(
        json.dumps({"counts": {"selected": 8, "orders_rebalance": 1, "orders_exit": 0}, "router_decisions": []}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "ab_gate_status_20260407.json").write_text(
        json.dumps({"window_runs": 99, "decision": {"switch_recommended": False}}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "ab_gate_status_accelerated.json").write_text(
        json.dumps({"window_runs": 4, "decision": {"switch_recommended": True}}, ensure_ascii=False),
        encoding="utf-8",
    )

    response = client.get("/api/shadow_test")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["ab_gate"]["window_runs"] == 4
    assert payload["ab_gate"]["decision"]["switch_recommended"] is True


class _DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def test_okx_account_balance_cache_reuses_recent_payload(monkeypatch):
    module = load_web_dashboard_module()
    monkeypatch.setenv("V5_DASHBOARD_OKX_BALANCE_CACHE_TTL_SECONDS", "2")

    calls = {"get": 0}

    def fake_get(url, *args, **kwargs):
        calls["get"] += 1
        assert url.endswith("/api/v5/account/balance")
        return _DummyResponse({"code": "0", "data": [{"details": []}]})

    monkeypatch.setattr(module.requests, "get", fake_get)

    first = module._load_okx_account_balance("k", "s", "p")
    second = module._load_okx_account_balance("k", "s", "p")

    assert first == second
    assert calls["get"] == 1


def test_okx_account_balance_cache_can_be_disabled(monkeypatch):
    module = load_web_dashboard_module()
    monkeypatch.setenv("V5_DASHBOARD_OKX_BALANCE_CACHE_TTL_SECONDS", "0")

    calls = {"get": 0}

    def fake_get(url, *args, **kwargs):
        calls["get"] += 1
        return _DummyResponse({"code": "0", "data": [{"details": []}], "call": calls["get"]})

    monkeypatch.setattr(module.requests, "get", fake_get)

    module._load_okx_account_balance("k", "s", "p")
    module._load_okx_account_balance("k", "s", "p")

    assert calls["get"] == 2


def test_api_positions_prefers_cash_balance_over_avail_balance(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setenv("V5_DASHBOARD_ALLOW_LIVE_OKX", "1")
    monkeypatch.setenv("EXCHANGE_API_KEY", "k")
    monkeypatch.setenv("EXCHANGE_API_SECRET", "s")
    monkeypatch.setenv("EXCHANGE_PASSPHRASE", "p")
    monkeypatch.setattr(module, "_load_avg_cost_from_fills", lambda *args, **kwargs: None)

    def fake_get(url, *args, **kwargs):
        if "account/balance" in url:
            return _DummyResponse({
                "code": "0",
                "data": [{
                    "details": [{
                        "ccy": "ETH",
                        "cashBal": "0.1",
                        "availBal": "2.0",
                        "spotBal": "",
                        "eq": "0.1",
                        "eqUsd": "200.0",
                    }]
                }]
            })
        if "market/ticker" in url:
            return _DummyResponse({"code": "0", "data": [{"last": "2000"}]})
        raise AssertionError(url)

    monkeypatch.setattr(module.requests, "get", fake_get)

    with module.app.app_context():
        payload = module.api_positions().get_json()

    assert payload["positions"][0]["symbol"] == "ETH"
    assert payload["positions"][0]["qty"] == 0.1
    assert payload["positions"][0]["value_usdt"] == 200.0


def test_api_positions_does_not_fallback_to_sqlite_when_reconcile_snapshot_confirms_flat(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setenv("V5_DASHBOARD_ALLOW_LIVE_OKX", "1")
    monkeypatch.setenv("EXCHANGE_API_KEY", "k")
    monkeypatch.setenv("EXCHANGE_API_SECRET", "s")
    monkeypatch.setenv("EXCHANGE_PASSPHRASE", "p")
    monkeypatch.setattr(module, "_load_avg_cost_from_fills", lambda *args, **kwargs: None)

    db_path = tmp_path / "positions.sqlite"
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute("CREATE TABLE positions (symbol TEXT, qty REAL, avg_px REAL, last_mark_px REAL)")
    cur.execute("INSERT INTO positions VALUES ('ETH/USDT', 1.0, 2000.0, 2000.0)")
    con.commit()
    con.close()

    (tmp_path / "reconcile_status.json").write_text(
        json.dumps({
            "exchange_snapshot": {
                "ccy_cashBal": {"USDT": "100.0"},
                "ccy_eqUsd": {"USDT": "100.0"},
            }
        }),
        encoding="utf-8",
    )

    def fake_get(url, *args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(module.requests, "get", fake_get)

    with module.app.app_context():
        payload = module.api_positions().get_json()

    assert payload["positions"] == []


def test_api_positions_uses_runtime_runs_and_fill_db(monkeypatch, tmp_path):
    module = load_web_dashboard_module()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    root_runs_dir = reports_dir / "runs" / "20260408_00"
    runtime_runs_dir = runtime_dir / "runs" / "20260408_01"
    reports_dir.mkdir(parents=True, exist_ok=True)
    runtime_runs_dir.mkdir(parents=True, exist_ok=True)
    root_runs_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {
                "order_store_path": "reports/shadow_runtime/orders.sqlite",
                "reconcile_status_path": "reports/shadow_runtime/reconcile_status.json",
            }
        },
    )

    root_positions_db = reports_dir / "positions.sqlite"
    con = sqlite3.connect(str(root_positions_db))
    cur = con.cursor()
    cur.execute("CREATE TABLE positions (symbol TEXT, qty REAL, avg_px REAL, last_mark_px REAL)")
    cur.execute("INSERT INTO positions VALUES ('BTC/USDT', 1.0, 30000.0, 30000.0)")
    con.commit()
    con.close()

    (root_runs_dir / "positions.jsonl").write_text(
        json.dumps({"symbol": "BTC/USDT", "qty": 1.0, "mark_px": 30000.0, "avg_px": 30000.0}) + "\n",
        encoding="utf-8",
    )
    (runtime_runs_dir / "positions.jsonl").write_text(
        json.dumps({"symbol": "ETH/USDT", "qty": 2.0, "mark_px": 2000.0, "avg_px": 1800.0}) + "\n",
        encoding="utf-8",
    )

    seen: dict[str, Path] = {}

    def fake_load_avg_cost(symbol, current_qty, reports_dir=None, fills_db=None):
        seen["fills_db"] = fills_db
        return None

    monkeypatch.setattr(module, "_load_avg_cost_from_fills", fake_load_avg_cost)
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("network down")))

    with module.app.app_context():
        payload = module.api_positions().get_json()

    assert [row["symbol"] for row in payload["positions"]] == ["ETH"]
    assert seen["fills_db"] == runtime_dir / "fills.sqlite"


def test_api_positions_fallback_prefers_positions_file_mtime_over_run_dir_mtime(monkeypatch, tmp_path):
    module = load_web_dashboard_module()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    stale_run = runtime_dir / "runs" / "20260408_00"
    fresh_run = runtime_dir / "runs" / "20260408_01"
    stale_run.mkdir(parents=True, exist_ok=True)
    fresh_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {
                "order_store_path": "reports/shadow_runtime/orders.sqlite",
                "reconcile_status_path": "reports/shadow_runtime/reconcile_status.json",
            }
        },
    )
    monkeypatch.setattr(module, "_load_avg_cost_from_fills", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("network down")))

    stale_positions = stale_run / "positions.jsonl"
    stale_positions.write_text(
        json.dumps({"symbol": "BTC/USDT", "qty": 1.0, "mark_px": 30000.0, "avg_px": 30000.0}) + "\n",
        encoding="utf-8",
    )
    fresh_positions = fresh_run / "positions.jsonl"
    fresh_positions.write_text(
        json.dumps({"symbol": "ETH/USDT", "qty": 2.0, "mark_px": 2000.0, "avg_px": 1800.0}) + "\n",
        encoding="utf-8",
    )

    os.utime(stale_positions, (100, 100))
    os.utime(fresh_positions, (200, 200))
    os.utime(stale_run, (300, 300))
    os.utime(fresh_run, (50, 50))

    with module.app.app_context():
        payload = module.api_positions().get_json()

    assert [row["symbol"] for row in payload["positions"]] == ["ETH"]


def test_api_positions_fallback_prefers_run_id_epoch_when_positions_file_mtime_is_misleading(monkeypatch, tmp_path):
    module = load_web_dashboard_module()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    older_run = runtime_dir / "runs" / "20260408_00"
    newer_run = runtime_dir / "runs" / "20260408_01"
    older_run.mkdir(parents=True, exist_ok=True)
    newer_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {
                "order_store_path": "reports/shadow_runtime/orders.sqlite",
                "reconcile_status_path": "reports/shadow_runtime/reconcile_status.json",
            }
        },
    )
    monkeypatch.setattr(module, "_load_avg_cost_from_fills", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("network down")))

    older_positions = older_run / "positions.jsonl"
    older_positions.write_text(
        json.dumps({"symbol": "BTC/USDT", "qty": 1.0, "mark_px": 30000.0, "avg_px": 30000.0}) + "\n",
        encoding="utf-8",
    )
    newer_positions = newer_run / "positions.jsonl"
    newer_positions.write_text(
        json.dumps({"symbol": "ETH/USDT", "qty": 2.0, "mark_px": 2000.0, "avg_px": 1800.0}) + "\n",
        encoding="utf-8",
    )

    os.utime(older_positions, (200, 200))
    os.utime(newer_positions, (100, 100))

    with module.app.app_context():
        payload = module.api_positions().get_json()

    assert [row["symbol"] for row in payload["positions"]] == ["ETH"]


def test_api_positions_derives_runtime_reconcile_status_when_config_uses_legacy_default(monkeypatch, tmp_path):
    module = load_web_dashboard_module()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    reports_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {
            "execution": {
                "order_store_path": "reports/shadow_runtime/orders.sqlite",
            }
        },
    )
    monkeypatch.setattr(module, "_load_avg_cost_from_fills", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("network down")))

    (reports_dir / "reconcile_status.json").write_text(
        json.dumps(
            {
                "exchange_snapshot": {
                    "ccy_cashBal": {"BTC": "1.0"},
                    "ccy_eqUsd": {"BTC": "100.0"},
                }
            }
        ),
        encoding="utf-8",
    )
    (runtime_dir / "reconcile_status.json").write_text(
        json.dumps(
            {
                "exchange_snapshot": {
                    "ccy_cashBal": {"ETH": "2.0"},
                    "ccy_eqUsd": {"ETH": "400.0"},
                }
            }
        ),
        encoding="utf-8",
    )

    with module.app.app_context():
        payload = module.api_positions().get_json()

    assert [row["symbol"] for row in payload["positions"]] == ["ETH"]
    assert payload["positions"][0]["qty"] == 2.0
    assert payload["positions"][0]["value_usdt"] == 400.0


def test_api_positions_prefers_fresh_local_price_cache_before_public_ticker(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "_load_avg_cost_from_fills", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.time, "time", lambda: datetime(2026, 4, 10, 0, 5).timestamp())

    db_path = tmp_path / "positions.sqlite"
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute("CREATE TABLE positions (symbol TEXT, qty REAL, avg_px REAL, last_mark_px REAL)")
    cur.execute("INSERT INTO positions VALUES ('ETH/USDT', 2.0, 100.0, 0.0)")
    con.commit()
    con.close()

    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / "ETH_USDT_1H_20260410.csv"
    cache_file.write_text("ts,open,high,low,close,volume\n1,100,101,99,123.45,10\n", encoding="utf-8")

    def fail_if_called(*args, **kwargs):
        raise AssertionError("public ticker should not be called when fresh cache is present")

    monkeypatch.setattr(module.requests, "get", fail_if_called)

    with module.app.app_context():
        payload = module.api_positions().get_json()

    assert payload["positions"][0]["symbol"] == "ETH"
    assert payload["positions"][0]["last_price"] == 123.45
    assert payload["positions"][0]["value_usdt"] == 246.9


def test_api_positions_prefers_latest_cache_file_by_logical_suffix_timestamp(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "_load_avg_cost_from_fills", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.time, "time", lambda: datetime(2026, 4, 12, 0, 5).timestamp())

    db_path = tmp_path / "positions.sqlite"
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute("CREATE TABLE positions (symbol TEXT, qty REAL, avg_px REAL, last_mark_px REAL)")
    cur.execute("INSERT INTO positions VALUES ('ETH/USDT', 2.0, 100.0, 0.0)")
    con.commit()
    con.close()

    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    older_daily = cache_dir / "ETH_USDT_1H_20260410.csv"
    newer_range = cache_dir / "ETH_USDT_1H_2026-04-10_2026-04-12.csv"
    older_daily.write_text("ts,open,high,low,close,volume\n1,100,101,99,111.11,10\n", encoding="utf-8")
    newer_range.write_text("ts,open,high,low,close,volume\n1,100,101,99,222.22,10\n", encoding="utf-8")

    def fail_if_called(*args, **kwargs):
        raise AssertionError("public ticker should not be called when fresh local cache is present")

    monkeypatch.setattr(module.requests, "get", fail_if_called)

    with module.app.app_context():
        payload = module.api_positions().get_json()

    assert payload["positions"][0]["symbol"] == "ETH"
    assert payload["positions"][0]["last_price"] == 222.22
    assert payload["positions"][0]["value_usdt"] == 444.44


def test_api_positions_ignores_stale_cache_even_when_file_mtime_is_recent(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "_load_avg_cost_from_fills", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.time, "time", lambda: datetime(2026, 4, 22, 0, 5).timestamp())

    db_path = tmp_path / "positions.sqlite"
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute("CREATE TABLE positions (symbol TEXT, qty REAL, avg_px REAL, last_mark_px REAL)")
    cur.execute("INSERT INTO positions VALUES ('ETH/USDT', 2.0, 100.0, 0.0)")
    con.commit()
    con.close()

    cache_dir = tmp_path / "data" / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    stale_cache = cache_dir / "ETH_USDT_1H_20260410.csv"
    stale_cache.write_text("ts,open,high,low,close,volume\n1,100,101,99,111.11,10\n", encoding="utf-8")
    os.utime(stale_cache, (2_000_000_000, 2_000_000_000))

    class _Response:
        def json(self):
            return {"code": "0", "data": [{"last": "234.56"}]}

    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: _Response())

    with module.app.app_context():
        payload = module.api_positions().get_json()

    assert payload["positions"][0]["symbol"] == "ETH"
    assert payload["positions"][0]["last_price"] == 234.56


def test_api_positions_reuses_recent_public_ticker_cache(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "_load_avg_cost_from_fills", lambda *args, **kwargs: None)
    monkeypatch.setenv("V5_DASHBOARD_PUBLIC_TICKER_CACHE_TTL_SECONDS", "60")
    module._OKX_PUBLIC_TICKER_CACHE.clear()

    db_path = tmp_path / "positions.sqlite"
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute("CREATE TABLE positions (symbol TEXT, qty REAL, avg_px REAL, last_mark_px REAL)")
    cur.execute("INSERT INTO positions VALUES ('ETH/USDT', 2.0, 100.0, 0.0)")
    con.commit()
    con.close()

    calls = {"count": 0}

    class _Response:
        def json(self):
            return {"code": "0", "data": [{"last": "234.56"}]}

    def fake_get(url, timeout=0, **kwargs):
        assert "market/ticker" in url
        calls["count"] += 1
        return _Response()

    monkeypatch.setattr(module.requests, "get", fake_get)

    with module.app.app_context():
        first = module.api_positions().get_json()
        second = module.api_positions().get_json()

    assert calls["count"] == 1
    assert first["positions"][0]["symbol"] == "ETH"
    assert first["positions"][0]["last_price"] == 234.56
    assert second["positions"][0]["last_price"] == 234.56


def test_api_account_converts_json_fee_maps_to_signed_usdt(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "_load_reconcile_cash_balance", lambda: (True, 100.0))
    monkeypatch.setattr(module, "api_positions", lambda: module.jsonify({"positions": []}))

    conn = sqlite3.connect(str(tmp_path / "orders.sqlite"))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            inst_id TEXT,
            side TEXT,
            notional_usdt REAL,
            fee TEXT,
            state TEXT,
            avg_px TEXT
        )
        """
    )
    cur.executemany(
        "INSERT INTO orders(inst_id, side, notional_usdt, fee, state, avg_px) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("BTC-USDT", "buy", 100.0, '{"BTC":"-0.001"}', "FILLED", "50000"),
            ("BTC-USDT", "sell", 120.0, "0", "FILLED", "50000"),
        ],
    )
    conn.commit()
    conn.close()

    with module.app.app_context():
        payload = module.api_account().get_json()

    assert payload["total_trades"] == 2
    assert payload["total_fees"] == pytest.approx(-50.0)
    assert payload["realized_pnl"] == pytest.approx(-30.0)


def test_api_account_prefers_live_okx_equsd_for_total_equity(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "load_config", lambda: {})
    monkeypatch.setenv("V5_DASHBOARD_ALLOW_LIVE_OKX_ACCOUNT", "1")
    monkeypatch.setenv("EXCHANGE_API_KEY", "k")
    monkeypatch.setenv("EXCHANGE_API_SECRET", "s")
    monkeypatch.setenv("EXCHANGE_PASSPHRASE", "p")
    monkeypatch.setattr(module, "_load_reconcile_cash_balance", lambda *args, **kwargs: (True, 50.0))
    monkeypatch.setattr(module, "_load_local_account_state", lambda *args, **kwargs: {})
    monkeypatch.setattr(module, "_load_total_fees_from_orders", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(module, "api_positions", lambda: module.jsonify({"positions": []}))
    monkeypatch.setattr(module, "_load_okx_account_balance", lambda *_args: {
        "code": "0",
        "data": [{
            "details": [
                {"ccy": "USDT", "cashBal": "91.0", "eq": "91.0", "eqUsd": "91.2"},
                {"ccy": "ZEC", "cashBal": "0.04", "eq": "0.04", "eqUsd": "16.3"},
                {"ccy": "ETH", "cashBal": "0.0001", "eq": "0.0001", "eqUsd": "0.001"},
                {"ccy": "PEPE", "cashBal": "1000", "eq": "1000", "eqUsd": "0.2"},
            ]
        }],
    })

    with module.app.app_context():
        payload = module.api_account().get_json()

    assert payload["cash_usdt"] == pytest.approx(91.0)
    assert payload["positions_value_usdt"] == pytest.approx(16.3)
    assert payload["total_equity_usdt"] == pytest.approx(107.701)
    assert payload["equity_source"] == "okx_live"


def test_api_account_uses_reconcile_equsd_total_when_live_unavailable(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setattr(module, "load_config", lambda: {})
    monkeypatch.delenv("V5_DASHBOARD_ALLOW_LIVE_OKX_ACCOUNT", raising=False)
    monkeypatch.delenv("V5_DASHBOARD_ALLOW_LIVE_OKX", raising=False)
    monkeypatch.setattr(module, "_load_local_account_state", lambda *args, **kwargs: {})
    monkeypatch.setattr(module, "_load_total_fees_from_orders", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(module, "api_positions", lambda: module.jsonify({"positions": []}))
    (tmp_path / "reconcile_status.json").write_text(
        json.dumps({
            "exchange_snapshot": {
                "ccy_cashBal": {"USDT": "91.0", "ZEC": "0.04", "ETH": "0.0001"},
                "ccy_eqUsd": {"USDT": "91.2", "ZEC": "16.3", "ETH": "0.001", "PEPE": "0.2"},
            }
        }),
        encoding="utf-8",
    )

    with module.app.app_context():
        payload = module.api_account().get_json()

    assert payload["cash_usdt"] == pytest.approx(91.0)
    assert payload["positions_value_usdt"] == pytest.approx(16.3)
    assert payload["total_equity_usdt"] == pytest.approx(107.701)
    assert payload["equity_source"] == "reconcile"


def test_api_trades_converts_live_base_fee_to_signed_usdt(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.setenv("V5_DASHBOARD_ALLOW_LIVE_OKX", "1")
    monkeypatch.setenv("EXCHANGE_API_KEY", "k")
    monkeypatch.setenv("EXCHANGE_API_SECRET", "s")
    monkeypatch.setenv("EXCHANGE_PASSPHRASE", "p")

    def fake_get(url, *args, **kwargs):
        assert "trade/fills" in url
        return _DummyResponse({
            "code": "0",
            "data": [
                {
                    "instId": "BTC-USDT",
                    "ts": "1710000000000",
                    "fillPx": "50000",
                    "fillSz": "0.002",
                    "fee": "-0.0001",
                    "feeCcy": "BTC",
                    "side": "buy",
                }
            ],
        })

    monkeypatch.setattr(module.requests, "get", fake_get)

    with module.app.app_context():
        payload = module.api_trades().get_json()

    assert len(payload["trades"]) == 1
    assert payload["trades"][0]["price"] == pytest.approx(50000.0)
    assert payload["trades"][0]["qty"] == pytest.approx(0.002)
    assert payload["trades"][0]["amount"] == pytest.approx(100.0)
    assert payload["trades"][0]["fee"] == pytest.approx(-5.0)


def test_api_trades_db_fallback_converts_json_fee_maps_to_signed_usdt(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)

    conn = sqlite3.connect(str(tmp_path / "orders.sqlite"))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            inst_id TEXT,
            side TEXT,
            notional_usdt REAL,
            fee TEXT,
            state TEXT,
            avg_px TEXT,
            created_ts INTEGER
        )
        """
    )
    cur.execute(
        """
        INSERT INTO orders(inst_id, side, notional_usdt, fee, state, avg_px, created_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("BTC-USDT", "buy", 100.0, '{"BTC":"-0.001"}', "FILLED", "50000", 1710000000000),
    )
    conn.commit()
    conn.close()

    with module.app.app_context():
        payload = module.api_trades().get_json()

    assert len(payload["trades"]) == 1
    assert payload["trades"][0]["symbol"] == "BTC-USDT"
    assert payload["trades"][0]["price"] == pytest.approx(50000.0)
    assert payload["trades"][0]["qty"] == pytest.approx(0.002)
    assert payload["trades"][0]["amount"] == pytest.approx(100.0)
    assert payload["trades"][0]["fee"] == pytest.approx(-50.0)


def test_api_trades_db_fallback_prefers_updated_ts_for_trade_time(monkeypatch, tmp_path):
    module = load_web_dashboard_module()
    monkeypatch.setattr(module, "REPORTS_DIR", tmp_path)
    monkeypatch.setattr(module, "WORKSPACE", tmp_path)
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)

    conn = sqlite3.connect(str(tmp_path / "orders.sqlite"))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            inst_id TEXT,
            side TEXT,
            notional_usdt REAL,
            fee TEXT,
            state TEXT,
            avg_px TEXT,
            created_ts INTEGER,
            updated_ts INTEGER
        )
        """
    )
    cur.executemany(
        """
        INSERT INTO orders(inst_id, side, notional_usdt, fee, state, avg_px, created_ts, updated_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("BTC-USDT", "buy", 100.0, "0", "FILLED", "50000", 1710000000000, 1710003600000),
            ("ETH-USDT", "buy", 90.0, "0", "FILLED", "3000", 1710001800000, 1710001800000),
        ],
    )
    conn.commit()
    conn.close()

    with module.app.app_context():
        payload = module.api_trades().get_json()

    assert payload["trades"][0]["symbol"] == "BTC-USDT"
    assert payload["trades"][0]["time"] == "2024-03-10 01:00:00"


def test_api_trades_uses_active_runtime_paths(monkeypatch, tmp_path):
    module = load_web_dashboard_module()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    root_run = reports_dir / "runs" / "20260408_02"
    runtime_run = runtime_dir / "runs" / "20260408_01"
    root_run.mkdir(parents=True, exist_ok=True)
    runtime_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)

    root_orders_db = reports_dir / "orders.sqlite"
    conn = sqlite3.connect(str(root_orders_db))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            inst_id TEXT,
            side TEXT,
            notional_usdt REAL,
            fee TEXT,
            state TEXT,
            avg_px TEXT,
            created_ts INTEGER
        )
        """
    )
    cur.execute(
        """
        INSERT INTO orders(inst_id, side, notional_usdt, fee, state, avg_px, created_ts)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("BTC-USDT", "buy", 100.0, "0.5", "FILLED", "50000", 1_710_000_000_000),
    )
    conn.commit()
    conn.close()

    (root_run / "trades.csv").write_text(
        "symbol,side,notional_usdt,fee_usdt,ts\n"
        "BTC/USDT,buy,100,0.5,2024-04-08 10:00:00\n",
        encoding="utf-8",
    )
    (runtime_run / "trades.csv").write_text(
        "symbol,side,notional_usdt,fee_usdt,ts\n"
        "ETH/USDT,sell,200,1.5,2024-04-08 11:00:00\n",
        encoding="utf-8",
    )
    os.utime(root_run, (20, 20))
    os.utime(runtime_run, (10, 10))

    with module.app.app_context():
        payload = module.api_trades().get_json()

    assert len(payload["trades"]) == 1
    assert payload["trades"][0]["symbol"] == "ETH-USDT"
    assert payload["trades"][0]["price"] == pytest.approx(0.0)
    assert payload["trades"][0]["qty"] == pytest.approx(0.0)
    assert payload["trades"][0]["amount"] == pytest.approx(200.0)
    assert payload["trades"][0]["fee"] == pytest.approx(1.5)
    assert payload["trades"][0]["time"] == "2024-04-08 11:00:00"


def test_api_trades_fallback_prefers_trade_file_mtime_over_run_dir_mtime(monkeypatch, tmp_path):
    module = load_web_dashboard_module()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    stale_run = runtime_dir / "runs" / "20260408_00"
    fresh_run = runtime_dir / "runs" / "20260408_01"
    stale_run.mkdir(parents=True, exist_ok=True)
    fresh_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)

    stale_trades = stale_run / "trades.csv"
    stale_trades.write_text(
        "symbol,side,notional_usdt,fee_usdt,ts\n"
        + "".join(
            f"BTC/USDT,buy,{100 + idx},0.5,2024-04-08 10:{idx:02d}:00\n"
            for idx in range(100)
        ),
        encoding="utf-8",
    )
    fresh_trades = fresh_run / "trades.csv"
    fresh_trades.write_text(
        "symbol,side,notional_usdt,fee_usdt,ts\n"
        "ETH/USDT,sell,200,1.5,2024-04-08 11:00:00\n",
        encoding="utf-8",
    )

    os.utime(stale_trades, (100, 100))
    os.utime(fresh_trades, (200, 200))
    os.utime(stale_run, (300, 300))
    os.utime(fresh_run, (50, 50))

    with module.app.app_context():
        payload = module.api_trades().get_json()

    assert len(payload["trades"]) == 100
    assert payload["trades"][0]["symbol"] == "ETH-USDT"
    assert payload["trades"][0]["amount"] == pytest.approx(200.0)
    assert payload["trades"][0]["fee"] == pytest.approx(1.5)
    assert payload["trades"][0]["time"] == "2024-04-08 11:00:00"


def test_api_trades_fallback_prefers_run_id_epoch_when_trade_file_mtime_is_misleading(monkeypatch, tmp_path):
    module = load_web_dashboard_module()

    workspace = tmp_path / "ws"
    reports_dir = workspace / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    older_run = runtime_dir / "runs" / "20260408_00"
    newer_run = runtime_dir / "runs" / "20260408_01"
    older_run.mkdir(parents=True, exist_ok=True)
    newer_run.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "WORKSPACE", workspace)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "load_config",
        lambda: {"execution": {"order_store_path": "reports/shadow_runtime/orders.sqlite"}},
    )
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)

    older_trades = older_run / "trades.csv"
    older_trades.write_text(
        "symbol,side,notional_usdt,fee_usdt,ts\n"
        "BTC/USDT,buy,100,0.5,2024-04-08 10:00:00\n",
        encoding="utf-8",
    )
    newer_trades = newer_run / "trades.csv"
    newer_trades.write_text(
        "symbol,side,notional_usdt,fee_usdt,ts\n"
        "ETH/USDT,sell,200,1.5,2024-04-08 11:00:00\n",
        encoding="utf-8",
    )

    os.utime(older_trades, (200, 200))
    os.utime(newer_trades, (100, 100))

    with module.app.app_context():
        payload = module.api_trades().get_json()

    assert len(payload["trades"]) == 2
    assert payload["trades"][0]["symbol"] == "ETH-USDT"
    assert payload["trades"][0]["amount"] == pytest.approx(200.0)
