#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.execution.ml_data_collector import MLDataCollector


def _parse_timestamp_ms(raw: str) -> int:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("timestamp is empty")
    if text.isdigit():
        value = int(text)
        return value if value >= 1_000_000_000_000 else value * 1000
    normalized = text.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _load_symbols(args: argparse.Namespace) -> list[str]:
    if args.symbols:
        return [str(sym).strip() for sym in str(args.symbols).split(",") if str(sym).strip()]

    universe_path = Path(args.universe_path)
    if not universe_path.is_absolute():
        universe_path = (PROJECT_ROOT / universe_path).resolve()
    if not universe_path.exists():
        raise FileNotFoundError(f"universe file not found: {universe_path}")

    payload = json.loads(universe_path.read_text(encoding="utf-8"))
    symbols = payload.get("symbols", []) if isinstance(payload, dict) else []
    out = [str(sym).strip() for sym in symbols if str(sym).strip()]
    if not out:
        raise ValueError(f"universe file has no symbols: {universe_path}")
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill ML feature snapshots from cache for a stable research universe")
    parser.add_argument("--db-path", default="reports/ml_training_data.db", help="SQLite DB path")
    parser.add_argument("--csv-path", default="reports/ml_training_data.csv", help="Exported training CSV path")
    parser.add_argument("--universe-path", default="reports/universe_cache.json", help="JSON file containing {symbols:[...]}")
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbol override")
    parser.add_argument("--start", required=True, help="Backfill start timestamp (ISO8601, seconds, or ms)")
    parser.add_argument("--end", default="", help="Backfill end timestamp (default: current UTC hour)")
    parser.add_argument("--lookback-bars", type=int, default=600, help="Bars used for feature calculation")
    parser.add_argument("--overwrite-existing", action="store_true", help="Overwrite existing snapshots in the backfill window")
    parser.add_argument("--regime", default="UNKNOWN", help="Regime value stored for backfilled snapshots")
    parser.add_argument("--min-samples", type=int, default=100, help="Minimum samples required to export CSV")
    parser.add_argument("--max-label-batches", type=int, default=100, help="Maximum repeated label-fill batches to run")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    db_path = Path(args.db_path)
    if not db_path.is_absolute():
        db_path = (PROJECT_ROOT / db_path).resolve()
    csv_path = Path(args.csv_path)
    if not csv_path.is_absolute():
        csv_path = (PROJECT_ROOT / csv_path).resolve()

    start_ms = _parse_timestamp_ms(args.start)
    if args.end:
        end_ms = _parse_timestamp_ms(args.end)
    else:
        now = datetime.now(timezone.utc)
        end_ms = int(now.replace(minute=0, second=0, microsecond=0).timestamp() * 1000)
    if end_ms < start_ms:
        raise ValueError("end must be >= start")

    symbols = _load_symbols(args)

    collector = MLDataCollector(db_path=str(db_path))
    stats = collector.backfill_feature_snapshots_from_cache(
        symbols=symbols,
        start_timestamp=start_ms,
        end_timestamp=end_ms,
        lookback_bars=int(args.lookback_bars),
        overwrite_existing=bool(args.overwrite_existing),
        regime=str(args.regime),
    )
    fill_result = collector.fill_all_labels(end_ms, max_batches=int(args.max_label_batches))
    exported = collector.export_training_data(str(csv_path), min_samples=int(args.min_samples))
    summary = collector.get_statistics()

    print(
        json.dumps(
            {
                "backfill": stats,
                "labels_filled": int(fill_result["filled"]),
                "label_batches": int(fill_result["batches"]),
                "csv_exported": bool(exported),
                "summary": summary,
                "window": {"start_ms": start_ms, "end_ms": end_ms},
                "symbols": symbols,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
