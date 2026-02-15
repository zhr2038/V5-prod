from __future__ import annotations

import json
from pathlib import Path

from src.reporting.cost_events import append_cost_event


def test_cost_event_ndjson_one_line(tmp_path: Path):
    base = tmp_path / "cost_events"
    e = {
        "schema_version": 1,
        "event_type": "fill",
        "ts": 1700000000,
        "run_id": "r",
        "window_start_ts": 1700000000,
        "window_end_ts": 1700003600,
        "symbol": "SOL/USDT",
        "notional_usdt": 10,
    }

    p = append_cost_event(e, base_dir=str(base))
    lines = p.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    obj = json.loads(lines[0])
    assert obj["schema_version"] == 1
    assert obj["window_start_ts"] == 1700000000
