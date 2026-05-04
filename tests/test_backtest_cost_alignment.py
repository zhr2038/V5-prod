from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from configs.loader import load_config
from configs.schema import AppConfig
from src.backtest.cost_factory import make_cost_model_from_cfg


def test_load_config_warns_when_backtest_costs_are_below_live_costs(tmp_path, caplog) -> None:
    cfg_path = tmp_path / "cost_mismatch.yaml"
    cfg_path.write_text(
        """
symbols:
  - BTC/USDT
execution:
  fee_bps: 10
  slippage_bps: 5
backtest:
  fee_bps: 6
  slippage_bps: 4
""",
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING, logger="configs.loader"):
        cfg = load_config(str(cfg_path), env_path=None)

    assert cfg.backtest.fee_bps == 6
    assert any(
        "Backtest fee_bps below live execution fee_bps" in record.message
        for record in caplog.records
    )
    assert any(
        "Backtest slippage_bps below live execution slippage_bps" in record.message
        for record in caplog.records
    )


def test_calibrated_cost_falls_back_to_live_cost_when_stats_insufficient(tmp_path) -> None:
    stats_dir = tmp_path / "cost_stats"
    stats_dir.mkdir()
    day = datetime.now(timezone.utc).strftime("%Y%m%d")
    (stats_dir / f"daily_cost_stats_{day}.json").write_text(
        json.dumps(
            {
                "day": day,
                "coverage": {"fills": 2},
                "buckets": {
                    "ALL|ALL|ALL|ALL": {
                        "count": 2,
                        "fee_bps": {"p75": 1.0},
                        "slippage_bps": {"p90": 1.0},
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.fee_bps = 10.0
    cfg.execution.slippage_bps = 5.0
    cfg.backtest.fee_bps = 6.0
    cfg.backtest.slippage_bps = 1.0
    cfg.backtest.cost_model = "calibrated"
    cfg.backtest.cost_stats_dir = str(stats_dir)
    cfg.backtest.min_fills_global = 30
    cfg.backtest.min_fills_bucket = 10

    model, meta = make_cost_model_from_cfg(cfg)
    fee_bps, slippage_bps, detail = model.resolve(
        "BTC/USDT",
        regime="PROTECT",
        router_action="OPEN_LONG",
        notional_usdt=16.0,
    )

    assert meta.mode == "default"
    assert meta.reason == "global_fills_insufficient"
    assert detail["reason"] == "min_fills_global"
    assert fee_bps == 10.0
    assert slippage_bps == 5.0
