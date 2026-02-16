from __future__ import annotations

import json
import sys
from pathlib import Path

# allow running as a script from repo root
sys.path.append(str(Path(__file__).resolve().parents[1]))

from configs.loader import load_config
from src.data.mock_provider import MockProvider
from src.data.okx_ccxt_provider import OKXCCXTProvider
from src.backtest.walk_forward import run_walk_forward, build_walk_forward_report


def main() -> None:
    import os

    cfg_path = os.getenv("V5_CONFIG") or "configs/config.yaml"
    cfg = load_config(cfg_path, env_path=".env")

    which = (os.getenv("V5_DATA_PROVIDER") or "mock").lower()
    provider = OKXCCXTProvider() if which == "okx" else MockProvider(seed=7)

    md = provider.fetch_ohlcv(cfg.symbols, timeframe=cfg.timeframe_main, limit=24 * 120)
    folds = run_walk_forward(md, folds=int(cfg.backtest.walk_forward_folds), cfg=cfg)

    report = build_walk_forward_report(folds, cost_meta={
        # keep meta minimal here; detailed per-fill fallback is inside each fold.result.cost_assumption
        "mode": str(cfg.backtest.cost_model),
        "fee_quantile": str(cfg.backtest.fee_quantile),
        "slippage_quantile": str(cfg.backtest.slippage_quantile),
        "min_fills_global": int(cfg.backtest.min_fills_global),
        "min_fills_bucket": int(cfg.backtest.min_fills_bucket),
        "max_stats_age_days": int(cfg.backtest.max_stats_age_days),
        "cost_stats_dir": str(cfg.backtest.cost_stats_dir),
    })

    Path("reports").mkdir(exist_ok=True)
    Path("reports/walk_forward.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote reports/walk_forward.json folds={len(report.get('folds') or [])}")


if __name__ == "__main__":
    main()
