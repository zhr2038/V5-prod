from __future__ import annotations

import json
from pathlib import Path

from src.alpha.ic_monitor import AlphaICMonitor, AlphaICMonitorConfig


class _Snapshot:
    def __init__(self, scores, closes):
        self.scores = scores
        self.telemetry_scores = {}
        self.z_factors = {}
        self._closes = closes


def test_alpha_ic_monitor_update_prefers_latest_history_ts_when_history_is_unsorted(tmp_path: Path) -> None:
    history_path = tmp_path / "alpha_ic_history.jsonl"
    timeseries_path = tmp_path / "alpha_ic_timeseries.jsonl"
    summary_path = tmp_path / "alpha_ic_monitor.json"

    rows = [
        {
            "ts_ms": 2_000,
            "ts_iso": "1970-01-01T00:00:02Z",
            "scores": {"A": 1.0, "B": -1.0, "C": 0.5, "D": -0.5, "E": 0.2, "F": -0.2},
            "score_source": "scores",
            "z_factors": {},
            "closes": {"A": 11, "B": 9, "C": 10.5, "D": 9.5, "E": 10.2, "F": 9.8},
        },
        {
            "ts_ms": 1_000,
            "ts_iso": "1970-01-01T00:00:01Z",
            "scores": {"A": -1.0, "B": 1.0, "C": -0.5, "D": 0.5, "E": -0.2, "F": 0.2},
            "score_source": "scores",
            "z_factors": {},
            "closes": {"A": 10, "B": 10, "C": 10, "D": 10, "E": 10, "F": 10},
        },
    ]
    history_path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n", encoding="utf-8")

    monitor = AlphaICMonitor(
        AlphaICMonitorConfig(
            history_path=str(history_path),
            timeseries_path=str(timeseries_path),
            summary_path=str(summary_path),
            min_cross_section=2,
        )
    )

    summary = monitor.update(
        now_ts_ms=3_000,
        alpha_snapshot=_Snapshot(
            scores={"A": 1.0, "B": -1.0, "C": 0.5, "D": -0.5, "E": 0.2, "F": -0.2},
            closes={"A": 12, "B": 8, "C": 11, "D": 9, "E": 10.4, "F": 9.6},
        ),
        closes={"A": 12, "B": 8, "C": 11, "D": 9, "E": 10.4, "F": 9.6},
    )

    assert summary is not None
    lines = timeseries_path.read_text(encoding="utf-8").splitlines()
    latest = json.loads(lines[-1])
    assert latest["from_ts_ms"] == 2_000
    assert latest["to_ts_ms"] == 3_000
