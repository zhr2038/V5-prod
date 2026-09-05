#!/usr/bin/env python3
"""Compare the three frozen participation stop policies, using offline sources."""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.research.participation_replay import prepare_inputs, replay
from src.strategy.participation_policy import policy_hash, validate_policy


def load(path: Path) -> dict:
    raw = path.read_bytes()
    return json.loads(gzip.decompress(raw) if path.suffix == ".gz" else raw)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--candles", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=ROOT / "configs/research/participation_policy_v1.json")
    parser.add_argument("--start", default="2026-07-05T13:00:00Z")
    parser.add_argument("--end", default="2026-09-03T13:00:00Z")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = load(args.config)
    validate_policy(config)
    start, end = (int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()) for s in (args.start, args.end))
    records, features, quality = prepare_inputs(load(args.history), load(args.candles))
    results = []
    for stop in config["comparison_stop_modes"]:
        for cost in config["roundtrip_cost_scenarios_bps"]:
            slippage = cost / 2 - config["fee_bps"]
            if slippage < 0:
                raise ValueError("cost stress reserve cannot be less than the fixed roundtrip fee")
            scenario = {**config, "stop_mode": stop, "slippage_bps": slippage}
            results.append(replay(records, features, scenario, start, end))
    sources = [args.history, args.candles, args.config, Path(__file__),
               ROOT / "src/strategy/participation_policy.py", ROOT / "src/research/participation_replay.py",
               ROOT / "src/research/factor_ablation.py", ROOT / "src/research/rally_reentry_validation.py"]
    report = {"schema_version": "v5.participation_comparison.v1", "generated_at_utc": datetime.now(timezone.utc).isoformat(),
              "policy_hash": policy_hash(config), "config": config, "data_quality": quality,
              "sources": [{"path": str(p.resolve()), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in sources],
              "limitations": [
                  "Exploratory historical comparison of three predeclared stop definitions; no optimization or automatic live promotion.",
                  "Hourly observed quotes may miss intrabar stops. Stops and 3 percent drawdown limits are action triggers, not guaranteed realized loss ceilings.",
                  "Both entries and exits execute at a subsequent recorded directional quote, never the same decision quote.",
                  "30/60/120bps are roundtrip fee/slippage reserves: fixed configured fee on each leg, remaining reserve applied to directional entry/exit prices as slippage. Observed bid/ask spread is embedded once in prices.",
                  "The 30bps price-stop primary scenario uses exactly the same fee10/slippage5 configuration as forward; cost stress changes slippage rather than relabeling all costs as fees.",
                  "The September 3 replay used intrabar OHLC stops and different execution semantics; its returns must not be mixed with this comparison.",
                  "All strategies share one cash account and one position. End-of-window positions remain open and are marked conservatively.",
                  "Recorded historical rank weights, operational blocks and regimes are preserved; this is not a full replay of historical production code.",
                  "Trailing momentum is an entry feature, not calibrated expected future profit. Future profitability is unproven.",
              ], "live_promotion_allowed": False, "results": results}
    args.output.mkdir(parents=True, exist_ok=False)
    (args.output / "results.json").write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    metrics = [r["metrics"] for r in results]
    with (args.output / "comparison.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(metrics[0]))
        writer.writeheader()
        writer.writerows({k: json.dumps(v, sort_keys=True) if isinstance(v, dict) else v for k, v in row.items()} for row in metrics)
    snapshots = args.output / "source_snapshots"
    snapshots.mkdir()
    for source in report["sources"]:
        p = Path(source["path"])
        if p.suffix in (".py", ".json"):
            (snapshots / (source["sha256"][:12] + "_" + p.name)).write_bytes(p.read_bytes())
    print(json.dumps({"output": str(args.output.resolve()), "policy_hash": report["policy_hash"],
                      "results": [{k: r[k] for k in ("stop_mode", "roundtrip_cost_reserve_bps", "closed_trades", "net_liquidation_pnl_usdt", "max_drawdown_pct", "mean_gross_weight_pct", "trial_halted")} for r in metrics]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
