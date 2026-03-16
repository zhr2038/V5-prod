from __future__ import annotations

from src.research.latest_signal_monitor import build_latest_signal_summary


def test_build_latest_signal_summary_flags_selection_and_order_changes():
    baseline = {
        "name": "core6_cost018",
        "signal_ts": 1000,
        "signal_dt": "2026-03-14T00:00:00Z",
        "regime": {"state": "Sideways", "multiplier": 0.35},
        "selected": ["BTC/USDT", "ETH/USDT"],
        "entry_candidates": ["BTC/USDT"],
        "target_weights": {"BTC/USDT": 0.5, "ETH/USDT": 0.5},
        "orders": [{"symbol": "BTC/USDT", "side": "buy", "intent": "OPEN_LONG"}],
    }
    champion = {
        "name": "avax_015",
        "signal_ts": 1000,
        "signal_dt": "2026-03-14T00:00:00Z",
        "regime": {"state": "Sideways", "multiplier": 0.35},
        "selected": ["BTC/USDT", "AVAX/USDT"],
        "entry_candidates": ["AVAX/USDT"],
        "target_weights": {"BTC/USDT": 0.3, "AVAX/USDT": 0.7},
        "orders": [{"symbol": "AVAX/USDT", "side": "buy", "intent": "OPEN_LONG"}],
    }

    summary = build_latest_signal_summary(
        generated_at="2026-03-14T00:00:05Z",
        baseline=baseline,
        champion=champion,
        baseline_name="core6_cost018",
        champion_name="avax_015",
    )

    compare = summary["compare"]
    assert compare["same_signal_ts"] is True
    assert compare["same_regime"] is True
    assert compare["selection_changed"] is True
    assert compare["orders_changed"] is True
    assert compare["baseline_selected_only"] == ["ETH/USDT"]
    assert compare["champion_selected_only"] == ["AVAX/USDT"]
    assert compare["needs_review"] is True
