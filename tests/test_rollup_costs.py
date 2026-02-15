from __future__ import annotations

import json
from pathlib import Path

from scripts.rollup_costs import rollup_day


def test_rollup_costs_basic(tmp_path: Path):
    base = tmp_path / "cost_events"
    out = tmp_path / "cost_stats"
    base.mkdir(parents=True, exist_ok=True)

    day = "20260101"
    p = base / f"{day}.jsonl"
    p.write_text(
        "\n".join(
            [
                json.dumps({
                    "schema_version": 1,
                    "event_type": "fill",
                    "ts": 1700000000,
                    "run_id": "r",
                    "window_start_ts": 1700000000,
                    "window_end_ts": 1700003600,
                    "symbol": "SOL/USDT",
                    "side": "buy",
                    "intent": "OPEN_LONG",
                    "regime": "Sideways",
                    "router_action": "fill",
                    "notional_usdt": 60,
                    "spread_bps": None,
                    "slippage_bps": 5,
                    "fee_bps": 6,
                    "cost_bps_total": 11,
                }, ensure_ascii=False, separators=(",", ":")),
                json.dumps({
                    "schema_version": 1,
                    "event_type": "fill",
                    "ts": 1700000001,
                    "run_id": "r",
                    "window_start_ts": 1700000000,
                    "window_end_ts": 1700003600,
                    "symbol": "SOL/USDT",
                    "side": "buy",
                    "intent": "OPEN_LONG",
                    "regime": "Sideways",
                    "router_action": "fill",
                    "notional_usdt": 60,
                    "slippage_bps": 7,
                    "fee_bps": 6,
                    "cost_bps_total": 13,
                }, ensure_ascii=False, separators=(",", ":")),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out_path = rollup_day(day, base_dir=str(base), out_dir=str(out))
    d = json.loads(out_path.read_text(encoding="utf-8"))
    assert d["schema_version"] == 1
    assert d["coverage"]["fills"] == 2
    assert d["buckets"]
