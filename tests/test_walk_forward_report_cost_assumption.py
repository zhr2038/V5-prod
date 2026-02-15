from __future__ import annotations

from configs.schema import AppConfig
from src.backtest.walk_forward import run_walk_forward, build_walk_forward_report
from src.core.models import MarketSeries


def test_walk_forward_report_contains_cost_assumption():
    # minimal market data
    n = 120
    closes = [100.0 + i for i in range(n)]
    md = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=list(range(n)),
            open=closes,
            high=closes,
            low=closes,
            close=closes,
            volume=[1e7] * n,
        )
    }

    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.backtest.cost_model = "default"

    folds = run_walk_forward(md, folds=2, cfg=cfg)
    report = build_walk_forward_report(folds, cost_meta={"mode": "default"})

    assert report.get("schema_version") == 2
    assert "cost_assumption_meta" in report
    assert "cost_assumption_aggregate" in report
    assert "folds" in report

    # for small n, folds may be empty; report should still be well-formed
    if report["folds"]:
        assert "cost_assumption" in report["folds"][0]

    # aggregate is JSON-serializable dict
    assert isinstance(report["cost_assumption_aggregate"]["fallback_level_counts"], dict)
