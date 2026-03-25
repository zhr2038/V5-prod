import os
import time
import json

from event_driven_check import (
    _load_decision_audit_signal_states,
    _load_fused_signal_states,
    _load_positions_snapshot,
    find_latest_decision_audit_file,
    load_current_state,
)
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
