from __future__ import annotations

import json
from pathlib import Path

from configs.loader import load_config
from src.data.mock_provider import MockProvider
from src.data.okx_ccxt_provider import OKXCCXTProvider
from src.backtest.walk_forward import run_walk_forward


def main() -> None:
    cfg = load_config("configs/config.yaml", env_path=".env")

    import os
    which = (os.getenv("V5_DATA_PROVIDER") or "mock").lower()
    provider = OKXCCXTProvider() if which == "okx" else MockProvider(seed=7)

    md = provider.fetch_ohlcv(cfg.symbols, timeframe=cfg.timeframe_main, limit=24 * 120)
    folds = run_walk_forward(md, folds=int(cfg.backtest.walk_forward_folds), cfg=cfg)

    out = [
        {
            "train_range": f.train_range,
            "test_range": f.test_range,
            "result": f.result.__dict__,
        }
        for f in folds
    ]

    Path("reports").mkdir(exist_ok=True)
    Path("reports/walk_forward.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote reports/walk_forward.json folds={len(out)}")


if __name__ == "__main__":
    main()
