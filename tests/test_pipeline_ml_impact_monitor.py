from __future__ import annotations

import json
from pathlib import Path

from configs.schema import AppConfig
from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import MarketSeries
from src.core.pipeline import V5Pipeline


def test_ml_impact_monitor_uses_latest_close_when_market_series_is_unsorted(tmp_path: Path) -> None:
    pipe = V5Pipeline(AppConfig(symbols=["BTC/USDT"]))

    state_path = tmp_path / "ml_overlay_impact_state.json"
    history_path = tmp_path / "ml_overlay_impact_history.jsonl"
    summary_path = tmp_path / "ml_overlay_impact.json"

    def _resolve(attr_name: str, default_name: str) -> Path:
        mapping = {
            "impact_state_path": state_path,
            "impact_history_path": history_path,
            "impact_summary_path": summary_path,
        }
        return mapping[attr_name]

    pipe._resolve_ml_impact_path = _resolve  # type: ignore[method-assign]

    alpha = AlphaSnapshot(
        raw_factors={},
        z_factors={},
        scores={"BTC/USDT": 1.0},
        base_scores={"BTC/USDT": 0.8},
        ml_attribution_scores={"BTC/USDT": 1.0},
        ml_overlay_scores={"BTC/USDT": 0.2},
        ml_overlay_raw_scores={"BTC/USDT": 1.3},
    )
    market_data = {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=[1_710_216_000_000, 1_710_212_400_000],
            open=[119.0, 80.0],
            high=[130.0, 81.0],
            low=[118.0, 79.0],
            close=[120.0, 80.0],
            volume=[1.0, 1.0],
        )
    }

    pipe._update_ml_impact_monitor(alpha, market_data, snapshot_ts_ms=1_710_216_000_000)

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert payload["closes"]["BTC/USDT"] == 120.0
