from __future__ import annotations

import json
import sys
from pathlib import Path

import scripts.rollup_costs as rollup_costs
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


def test_rollup_costs_main_defaults_to_repo_root(monkeypatch, tmp_path: Path) -> None:
    fake_root = tmp_path / "repo"
    events_dir = fake_root / "reports" / "cost_events"
    events_dir.mkdir(parents=True, exist_ok=True)
    day = "20260101"
    (events_dir / f"{day}.jsonl").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "event_type": "fill",
                "ts": 1700000000,
                "run_id": "run-1",
                "window_start_ts": 1700000000,
                "window_end_ts": 1700003600,
                "symbol": "BTC/USDT",
                "side": "buy",
                "intent": "OPEN_LONG",
                "regime": "Trending",
                "router_action": "fill",
                "notional_usdt": 120.0,
                "spread_bps": 4.0,
                "slippage_bps": 5.0,
                "fee_bps": 6.0,
                "cost_bps_total": 11.0,
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(rollup_costs, "PROJECT_ROOT", fake_root)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["rollup_costs.py", "--day", day])

    rollup_costs.main()

    out_path = fake_root / "reports" / "cost_stats" / f"daily_cost_stats_{day}.json"
    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["coverage"]["fills"] == 1
    assert out_path.exists()
    assert not (tmp_path / "reports" / "cost_stats" / f"daily_cost_stats_{day}.json").exists()
