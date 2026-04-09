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

from configs.runtime_config import load_runtime_config, resolve_runtime_path
from src.execution.fill_store import derive_runtime_reports_dir
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


def _resolve_cli_or_default_path(raw_path: str, *, default_path: Path) -> Path:
    text = str(raw_path or "").strip()
    if not text:
        return default_path.resolve()
    path = Path(text)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def _runtime_defaults(raw_config_path: str | None = None) -> tuple[Path, Path, Path]:
    cfg = load_runtime_config(raw_config_path, project_root=PROJECT_ROOT)
    execution_cfg = cfg.get("execution") if isinstance(cfg.get("execution"), dict) else {}
    universe_cfg = cfg.get("universe") if isinstance(cfg.get("universe"), dict) else {}

    order_store_path = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path") if isinstance(execution_cfg, dict) else None,
            default="reports/orders.sqlite",
            project_root=PROJECT_ROOT,
        )
    )
    reports_dir = derive_runtime_reports_dir(order_store_path).resolve()
    default_db_path = reports_dir / "ml_training_data.db"
    default_csv_path = reports_dir / "ml_training_data.csv"

    universe_raw = ""
    if isinstance(execution_cfg, dict):
        universe_raw = str(execution_cfg.get("ml_research_universe_path") or "").strip()
    if not universe_raw and isinstance(universe_cfg, dict):
        universe_raw = str(universe_cfg.get("cache_path") or "").strip()

    if universe_raw:
        universe_path = Path(
            resolve_runtime_path(
                universe_raw,
                default="reports/universe_cache.json",
                project_root=PROJECT_ROOT,
            )
        ).resolve()
    else:
        universe_path = (reports_dir / "universe_cache.json").resolve()

    return default_db_path, default_csv_path, universe_path


def _load_symbols(args: argparse.Namespace, *, universe_path: Path) -> list[str]:
    if args.symbols:
        return [str(sym).strip() for sym in str(args.symbols).split(",") if str(sym).strip()]

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
    parser.add_argument("--config", default="", help="Optional config path used to resolve runtime defaults")
    parser.add_argument("--db-path", default="", help="SQLite DB path (default: runtime ml_training_data.db)")
    parser.add_argument("--csv-path", default="", help="Exported training CSV path (default: runtime ml_training_data.csv)")
    parser.add_argument("--universe-path", default="", help="JSON file containing {symbols:[...]} (default: runtime stable universe)")
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

    default_db_path, default_csv_path, default_universe_path = _runtime_defaults(args.config or None)
    db_path = _resolve_cli_or_default_path(args.db_path, default_path=default_db_path)
    csv_path = _resolve_cli_or_default_path(args.csv_path, default_path=default_csv_path)
    universe_path = _resolve_cli_or_default_path(args.universe_path, default_path=default_universe_path)

    start_ms = _parse_timestamp_ms(args.start)
    if args.end:
        end_ms = _parse_timestamp_ms(args.end)
    else:
        now = datetime.now(timezone.utc)
        end_ms = int(now.replace(minute=0, second=0, microsecond=0).timestamp() * 1000)
    if end_ms < start_ms:
        raise ValueError("end must be >= start")

    symbols = _load_symbols(args, universe_path=universe_path)

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
