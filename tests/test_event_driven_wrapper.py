import os
import time

from event_driven_check import (
    _load_decision_audit_signal_states,
    _load_fused_signal_states,
    find_latest_decision_audit_file,
)


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
