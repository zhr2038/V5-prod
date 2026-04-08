import json
from pathlib import Path

from scripts.compare_runs import compare


def test_compare_md_contains_keys(tmp_path):
    v4 = {"run_id": "v4", "num_trades": 1}
    v5 = {"run_id": "v5", "num_trades": 2}
    md = compare(v4, v5, window="[a,b]")
    assert "num_trades" in md
    assert "window" in md
    assert "v4" in md and "v5" in md


def test_compare_md_treats_string_false_budget_action_enabled_as_false():
    v4 = {"run_id": "v4", "num_trades": 1}
    v5 = {"run_id": "v5", "num_trades": 2}
    v5_audit = {
        "budget_action": {
            "enabled": "false",
            "deadband_effective": 0.12,
            "min_trade_notional_effective": 25.0,
        }
    }
    md = compare(v4, v5, window="[a,b]", v5_audit=v5_audit)
    assert "deadband_effective" not in md
    assert "min_trade_notional_effective" not in md
