from __future__ import annotations

import json
from pathlib import Path

from src.backtest.cost_calibration import CalibratedCostModel, FixedCostModel


def _stats(tmp_path: Path, day: str, fills: int, buckets: dict):
    p = tmp_path / f"daily_cost_stats_{day}.json"
    p.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "day": day,
                "coverage": {"events_total": fills, "fills": fills, "missing_bidask": 0},
                "buckets": buckets,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return json.loads(p.read_text(encoding="utf-8"))


def test_calibrated_global_ok(tmp_path: Path):
    # global bucket only
    b = {
        "ALL|ALL|ALL|ALL": {
            "count": 50,
            "fee_bps": {"p75": 6.5},
            "slippage_bps": {"p90": 12.0},
        }
    }
    stats = _stats(tmp_path, "20260101", 50, b)
    m = CalibratedCostModel(stats=stats, min_fills_global=30, min_fills_bucket=10)
    fee, slp, meta = m.resolve("SOL/USDT", "Sideways", "fill", 60)
    assert fee == 6.5
    assert slp == 12.0
    assert meta["mode"] == "calibrated"


def test_calibrated_bucket_fallback(tmp_path: Path):
    # exact bucket insufficient, fallback to global
    b = {
        "SOL/USDT|Sideways|fill|50_100": {
            "count": 5,
            "fee_bps": {"p75": 1.0},
            "slippage_bps": {"p90": 1.0},
        },
        "ALL|ALL|ALL|ALL": {
            "count": 50,
            "fee_bps": {"p75": 6.0},
            "slippage_bps": {"p90": 10.0},
        },
    }
    stats = _stats(tmp_path, "20260101", 50, b)
    m = CalibratedCostModel(stats=stats, min_fills_global=30, min_fills_bucket=10)
    fee, slp, meta = m.resolve("SOL/USDT", "Sideways", "fill", 60)
    assert fee == 6.0
    assert slp == 10.0
    assert meta["mode"] == "calibrated"


def test_calibrated_global_insufficient_fills(tmp_path: Path):
    b = {
        "ALL|ALL|ALL|ALL": {
            "count": 20,
            "fee_bps": {"p75": 6.0},
            "slippage_bps": {"p90": 10.0},
        }
    }
    stats = _stats(tmp_path, "20260101", 20, b)
    m = CalibratedCostModel(stats=stats, min_fills_global=30, default_fee_bps=6.0, default_slippage_bps=5.0)
    fee, slp, meta = m.resolve("SOL/USDT", "Sideways", "fill", 60)
    assert fee == 6.0
    assert slp == 5.0
    assert meta["mode"] == "default"
    assert meta["reason"] == "min_fills_global"
