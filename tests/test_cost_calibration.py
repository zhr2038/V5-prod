from __future__ import annotations

import json
from pathlib import Path

from src.backtest.cost_calibration import load_latest_cost_stats


def test_load_latest_cost_stats_ignores_non_dated_files(tmp_path: Path) -> None:
    stats_dir = tmp_path / "reports" / "cost_stats"
    stats_dir.mkdir(parents=True, exist_ok=True)

    valid_file = stats_dir / "daily_cost_stats_20260418.json"
    invalid_file = stats_dir / "daily_cost_stats_latest.json"
    valid_file.write_text(json.dumps({"day": "valid"}), encoding="utf-8")
    invalid_file.write_text(json.dumps({"day": "invalid"}), encoding="utf-8")

    stats, stats_path = load_latest_cost_stats(str(stats_dir), max_age_days=7)

    assert stats == {"day": "valid"}
    assert stats_path == str(valid_file)
