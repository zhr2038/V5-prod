#!/usr/bin/env python3
"""Run IC research (30d + ~100 symbols) without affecting live.

This script:
1) Builds/refreshes a research universe cache
2) Runs dynamic alpha weights training using existing compute_dynamic_alpha_weights.py

It does NOT place any live orders.

Usage:
  cd /home/admin/clawd/v5-trading-bot
  python3 scripts/run_ic_research.py --config configs/ic_research_30d_100sym.yaml

Notes:
- Universe selection uses OKX public endpoints.
- Training relies on your existing market-data/backfill tooling.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

# allow running from repo root
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

import yaml

from src.data.universe.okx_universe import OKXUniverseProvider


def rebuild_universe(cfg_path: str) -> list[str]:
    cfg = yaml.safe_load(Path(cfg_path).read_text())
    ucfg = cfg.get("universe") or {}
    cache_path = Path(ucfg.get("cache_path") or "reports/universe_cache_ic_research.json")
    if cache_path.exists():
        cache_path.unlink()

    p = OKXUniverseProvider(
        cache_path=str(cache_path),
        cache_ttl_sec=int(ucfg.get("cache_ttl_sec", 0)),
        top_n=int(ucfg.get("top_n_market_cap", 100)),
        min_24h_quote_volume_usdt=float(ucfg.get("min_24h_quote_volume_usdt", 10_000_000)),
        blacklist_path=str(ucfg.get("blacklist_path", "configs/blacklist.json")),
        exclude_stablecoins=bool(ucfg.get("exclude_stablecoins", True)),
        max_spread_bps=ucfg.get("max_spread_bps"),
        refine_with_single_ticker=bool(ucfg.get("refine_with_single_ticker", True)),
        refine_single_ticker_max_candidates=int(ucfg.get("refine_single_ticker_max_candidates", 300)),
        refine_single_ticker_sleep_sec=float(ucfg.get("refine_single_ticker_sleep_sec", 0.02)),
    )
    syms = p.get_universe()

    inc = list(ucfg.get("include_symbols") or [])
    out = []
    seen = set()
    for s in inc + syms:
        if s not in seen:
            out.append(s)
            seen.add(s)

    cache_path.write_text(json.dumps({"ts": time.time(), "ttl_sec": int(ucfg.get("cache_ttl_sec", 0)), "symbols": out}, ensure_ascii=False, indent=2))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/ic_research_30d_100sym.yaml")
    args = ap.parse_args()

    syms = rebuild_universe(args.config)
    print(f"IC_RESEARCH universe_count={len(syms)}")
    print("sample:", syms[:25])

    cfg = yaml.safe_load(Path(args.config).read_text())
    iccfg = cfg.get("ic_research") or {}
    lookback = int(iccfg.get("lookback_days", 30))
    horizon = str(iccfg.get("horizon", "1h"))

    # Delegate to existing trainer script if present.
    trainer = Path("scripts/compute_dynamic_alpha_weights.py")
    if not trainer.exists():
        print("missing scripts/compute_dynamic_alpha_weights.py")
        return 1

    # We don't hard-wire all args (trainer may evolve). We pass the essential knobs.
    import subprocess

    cmd = [
        "python3",
        str(trainer),
        "--lookback-days",
        str(lookback),
        "--horizon",
        str(horizon),
    ]
    print("RUN:", " ".join(cmd))
    subprocess.check_call(cmd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
