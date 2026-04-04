import os
import time
import json

from event_driven_check import (
    _load_decision_audit_signal_states,
    _load_fused_signal_states,
    _load_positions_snapshot,
    find_latest_decision_audit_file,
    load_current_state,
    run_event_param_scan,
    should_bypass_live_trigger_throttle,
)
from src.execution.event_driven_integration import create_event_driven_trader
from src.execution.position_store import PositionStore


def test_find_latest_decision_audit_file_respects_freshness(tmp_path):
    current = tmp_path / "20260316_15"
    stale = tmp_path / "20260308_16"
    current.mkdir()
    stale.mkdir()

    stale_file = stale / "decision_audit.json"
    current_file = current / "decision_audit.json"
    stale_file.write_text("{}", encoding="utf-8")
    current_file.write_text("{}", encoding="utf-8")
    now = time.time()
    os.utime(stale_file, (now - 60, now - 60))
    os.utime(current_file, (now, now))

    path, meta = find_latest_decision_audit_file(tmp_path, max_age_minutes=90)

    assert path == current_file
    assert meta is not None
    assert meta["fresh"] is True


def test_load_fused_signal_states_filters_tradeable_symbols():
    payload = {
        "fused": {
            "BTC/USDT": {"direction": "buy", "score": 0.8, "rank": 1},
            "ETH/USDT": {"direction": "sell", "score": 0.4, "rank": 2},
        }
    }

    signals = _load_fused_signal_states(payload, {"BTC/USDT"})

    assert set(signals.keys()) == {"BTC/USDT"}
    assert signals["BTC/USDT"].direction == "buy"
    assert signals["BTC/USDT"].rank == 1


def test_load_decision_audit_signal_states_uses_top_scores():
    payload = {
        "top_scores": [
            {"symbol": "BNB/USDT", "score": 0.88, "rank": 1},
            {"symbol": "XRP/USDT", "score": -0.22, "rank": 2},
        ]
    }

    signals = _load_decision_audit_signal_states(payload, {"BNB/USDT", "XRP/USDT"})

    assert set(signals.keys()) == {"BNB/USDT", "XRP/USDT"}
    assert signals["BNB/USDT"].direction == "buy"
    assert signals["BNB/USDT"].score == 0.88
    assert signals["XRP/USDT"].direction == "sell"
    assert signals["XRP/USDT"].rank == 2


def test_load_positions_snapshot_prefers_sqlite_store(tmp_path):
    db_path = tmp_path / "positions.sqlite"
    store = PositionStore(str(db_path))
    store.upsert_buy("ADA/USDT", qty=12.5, px=0.42, now_ts="2026-03-25T10:00:00Z")

    legacy_path = tmp_path / "portfolio.json"
    legacy_path.write_text(
        json.dumps(
            {
                "positions": {
                    "BTC/USDT": {"avg_price": 80000, "quantity": 0.1},
                }
            }
        ),
        encoding="utf-8",
    )

    positions, symbols, source = _load_positions_snapshot(
        positions_db_path=db_path,
        portfolio_path=legacy_path,
    )

    assert source == "position_store"
    assert positions["ADA/USDT"]["quantity"] == 12.5
    assert symbols == {"ADA/USDT"}


def test_load_current_state_keeps_held_symbols_in_event_scope(tmp_path, monkeypatch):
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    (reports_dir / "regime.json").write_text(json.dumps({"regime": "TRENDING"}), encoding="utf-8")
    (reports_dir / "universe_cache.json").write_text(
        json.dumps({"symbols": ["BTC/USDT"]}),
        encoding="utf-8",
    )

    db_path = reports_dir / "positions.sqlite"
    store = PositionStore(str(db_path))
    store.upsert_buy("ADA/USDT", qty=5.0, px=0.5, now_ts="2026-03-25T10:00:00Z")

    import event_driven_check as mod
    import src.execution.price_fetcher as price_fetcher

    monkeypatch.setattr(mod, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(mod, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        price_fetcher,
        "fetch_prices",
        lambda: {"BTC/USDT": 85000.0, "ADA/USDT": 0.55},
    )

    state = load_current_state(
        cfg={
            "symbols": ["BTC/USDT"],
            "universe": {
                "enabled": True,
                "use_universe_symbols": True,
                "cache_path": "reports/universe_cache.json",
            },
        }
    )

    assert state is not None
    assert "ADA/USDT" in state["positions"]
    assert "ADA/USDT" in state["prices"]


def test_load_current_state_sorts_selected_symbols_by_signal_rank(tmp_path, monkeypatch):
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    (reports_dir / "regime.json").write_text(json.dumps({"regime": "TRENDING"}), encoding="utf-8")
    (reports_dir / "alpha_snapshot.json").write_text(
        json.dumps(
            {
                "scores": {
                    "BTC/USDT": -0.1,
                    "MON/USDT": 0.92,
                    "ETH/USDT": 0.35,
                    "SOL/USDT": 0.11,
                }
            }
        ),
        encoding="utf-8",
    )

    import event_driven_check as mod
    import src.execution.price_fetcher as price_fetcher

    monkeypatch.setattr(mod, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(mod, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        price_fetcher,
        "fetch_prices",
        lambda: {
            "BTC/USDT": 85000.0,
            "MON/USDT": 0.027,
            "ETH/USDT": 2500.0,
            "SOL/USDT": 150.0,
        },
    )

    state = load_current_state(cfg={"symbols": ["BTC/USDT", "MON/USDT", "ETH/USDT", "SOL/USDT"]})

    assert state is not None
    assert state["selected_symbols"][:4] == ["MON/USDT", "ETH/USDT", "SOL/USDT", "BTC/USDT"]


def test_event_driven_trader_build_market_state_reorders_stale_selected_symbols():
    trader = create_event_driven_trader({"enabled": True})
    market_state = trader._build_market_state(
        {
            "timestamp_ms": 1,
            "regime": "TRENDING",
            "prices": {},
            "positions": {},
            "signals": {
                "BTC/USDT": {"symbol": "BTC/USDT", "direction": "sell", "score": 0.1, "rank": 4, "timestamp_ms": 1},
                "MON/USDT": {"symbol": "MON/USDT", "direction": "buy", "score": 0.9, "rank": 1, "timestamp_ms": 1},
                "ETH/USDT": {"symbol": "ETH/USDT", "direction": "buy", "score": 0.4, "rank": 2, "timestamp_ms": 1},
            },
            "selected_symbols": ["BTC/USDT"],
        }
    )

    assert market_state.selected_symbols == ["MON/USDT", "ETH/USDT", "BTC/USDT"]


def test_event_driven_trader_uses_custom_state_paths(tmp_path):
    monitor_state = tmp_path / "scan_monitor_state.json"
    cooldown_state = tmp_path / "scan_cooldown_state.json"

    trader = create_event_driven_trader(
        {
            "enabled": True,
            "monitor_state_path": str(monitor_state),
            "cooldown_state_path": str(cooldown_state),
        }
    )

    assert trader.monitor.config.state_path == str(monitor_state)
    assert trader.cooldown.config.state_path == str(cooldown_state)


def test_run_event_param_scan_does_not_touch_live_state_files(tmp_path, monkeypatch):
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    monitor_state = reports_dir / "event_monitor_state.json"
    cooldown_state = reports_dir / "cooldown_state.json"
    monitor_payload = '{"sentinel":"monitor"}'
    cooldown_payload = '{"sentinel":"cooldown"}'
    monitor_state.write_text(monitor_payload, encoding="utf-8")
    cooldown_state.write_text(cooldown_payload, encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    state = {
        "timestamp_ms": 2_000,
        "regime": "TRENDING_UP",
        "prices": {"BTC/USDT": 105.0},
        "positions": {},
        "signals": {
            "BTC/USDT": {
                "symbol": "BTC/USDT",
                "direction": "buy",
                "score": 0.9,
                "rank": 1,
                "timestamp_ms": 2_000,
            }
        },
        "selected_symbols": ["BTC/USDT"],
    }
    last_state = {
        "timestamp_ms": 1_000,
        "regime": "SIDEWAYS",
        "prices": {"BTC/USDT": 100.0},
        "positions": {},
        "signals": {
            "BTC/USDT": {
                "symbol": "BTC/USDT",
                "direction": "sell",
                "score": 0.2,
                "rank": 4,
                "timestamp_ms": 1_000,
            }
        },
        "selected_symbols": [],
    }

    result = run_event_param_scan(state, last_state, {})

    assert result["count"] == 72
    assert monitor_state.read_text(encoding="utf-8") == monitor_payload
    assert cooldown_state.read_text(encoding="utf-8") == cooldown_payload


def test_risk_close_actions_bypass_live_trigger_throttle():
    assert should_bypass_live_trigger_throttle(
        [{"symbol": "MON/USDT", "action": "close", "reason": "take_profit_5%", "priority": 0}]
    ) is True
    assert should_bypass_live_trigger_throttle(
        [{"symbol": "BTC/USDT", "action": "open", "reason": "signal_rank_jump", "priority": 2}]
    ) is False
