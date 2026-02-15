from __future__ import annotations

from src.reporting.spread_snapshots import compute_spread_stats


def test_compute_spread_stats_quantiles():
    rows = [{"spread_bps": x} for x in [1, 2, 3, 4, 5, 100]]
    st = compute_spread_stats(rows)
    assert st["count"] == 6
    assert st["p50"] >= 1
    assert st["max"] == 100.0
