#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import load_runtime_config, resolve_runtime_path
from src.execution.fill_store import derive_runtime_named_artifact_path
from src.execution.ml_data_collector import MLDataCollector


TRAINING_COLUMNS = [
    "timestamp",
    "symbol",
    "returns_1h",
    "returns_6h",
    "returns_24h",
    "momentum_5d",
    "momentum_20d",
    "volatility_6h",
    "volatility_24h",
    "volatility_ratio",
    "volume_ratio",
    "obv",
    "rsi",
    "macd",
    "macd_signal",
    "bb_position",
    "price_position",
    "regime",
    "future_return_6h",
    "future_return_12h",
    "future_return_24h",
]


def _load_csv(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    for optional_col in ("future_return_12h", "future_return_24h"):
        if optional_col not in df.columns:
            df[optional_col] = pd.NA
    missing = [col for col in TRAINING_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"training csv missing columns: {missing}")
    df = df[TRAINING_COLUMNS].copy()
    if df.empty:
        raise ValueError("training csv is empty")
    if int(df.duplicated(["timestamp", "symbol"]).sum()) > 0:
        raise ValueError("training csv contains duplicate (timestamp, symbol) rows")
    return df


def _db_stats(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    total = int(cur.execute("SELECT COUNT(*) FROM feature_snapshots").fetchone()[0])
    labeled = int(
        cur.execute(
            """
            SELECT COUNT(*)
            FROM feature_snapshots
            WHERE label_filled = 1
              AND future_return_6h IS NOT NULL
              AND future_return_12h IS NOT NULL
              AND future_return_24h IS NOT NULL
            """
        ).fetchone()[0]
    )
    pending = int(cur.execute("SELECT COUNT(*) FROM feature_snapshots WHERE label_filled = 0").fetchone()[0])
    symbols = int(cur.execute("SELECT COUNT(DISTINCT symbol) FROM feature_snapshots").fetchone()[0])
    return {
        "total": total,
        "labeled": labeled,
        "pending": pending,
        "symbols": symbols,
    }


def _existing_rows(conn: sqlite3.Connection) -> dict[tuple[int, str], tuple[int, int]]:
    cur = conn.cursor()
    rows = cur.execute("SELECT id, timestamp, symbol, label_filled FROM feature_snapshots").fetchall()
    out: dict[tuple[int, str], tuple[int, int]] = {}
    for row_id, ts, symbol, label_filled in rows:
        key = (int(ts), str(symbol))
        current = out.get(key)
        if current is None or int(label_filled) > current[1] or row_id > current[0]:
            out[key] = (int(row_id), int(label_filled))
    return out


def _resolve_runtime_training_paths(raw_config_path: str | None = None) -> tuple[Path, Path]:
    cfg = load_runtime_config(raw_config_path, project_root=PROJECT_ROOT)
    execution_cfg = cfg.get("execution") if isinstance(cfg.get("execution"), dict) else {}
    order_store_path = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path") if isinstance(execution_cfg, dict) else None,
            default="reports/orders.sqlite",
            project_root=PROJECT_ROOT,
        )
    ).resolve()
    return (
        derive_runtime_named_artifact_path(order_store_path, "ml_training_data", ".csv").resolve(),
        derive_runtime_named_artifact_path(order_store_path, "ml_training_data", ".db").resolve(),
    )


def _resolve_cli_path(raw_path: str | None, fallback: Path) -> Path:
    if not raw_path:
        return fallback.resolve()
    return Path(resolve_runtime_path(raw_path, default=str(fallback), project_root=PROJECT_ROOT)).resolve()


def backfill_from_csv(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    *,
    relabel_existing: bool = True,
) -> dict[str, int]:
    existing = _existing_rows(conn)
    insert_rows = []
    update_rows = []

    for row in df.itertuples(index=False):
        key = (int(row.timestamp), str(row.symbol))
        payload = (
            int(row.timestamp),
            str(row.symbol),
            float(row.returns_1h),
            float(row.returns_6h),
            float(row.returns_24h),
            float(row.momentum_5d),
            float(row.momentum_20d),
            float(row.volatility_6h),
            float(row.volatility_24h),
            float(row.volatility_ratio),
            float(row.volume_ratio),
            float(row.obv),
            float(row.rsi),
            float(row.macd),
            float(row.macd_signal),
            float(row.bb_position),
            float(row.price_position),
            str(row.regime),
            float(row.future_return_6h),
            float(row.future_return_12h),
            float(row.future_return_24h),
            int(
                pd.notna(row.future_return_6h)
                and pd.notna(row.future_return_12h)
                and pd.notna(row.future_return_24h)
            ),
        )

        existing_row = existing.get(key)
        if existing_row is None:
            insert_rows.append(payload)
            continue

        row_id, label_filled = existing_row
        if relabel_existing or label_filled != 1:
            update_rows.append(payload + (row_id,))

    cur = conn.cursor()
    if insert_rows:
        cur.executemany(
            """
            INSERT INTO feature_snapshots (
                timestamp, symbol, returns_1h, returns_6h, returns_24h,
                momentum_5d, momentum_20d, volatility_6h, volatility_24h,
                volatility_ratio, volume_ratio, obv, rsi, macd, macd_signal,
                bb_position, price_position, regime,
                future_return_6h, future_return_12h, future_return_24h, label_filled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )

    if update_rows:
        cur.executemany(
            """
            UPDATE feature_snapshots
            SET
                timestamp = ?,
                symbol = ?,
                returns_1h = ?,
                returns_6h = ?,
                returns_24h = ?,
                momentum_5d = ?,
                momentum_20d = ?,
                volatility_6h = ?,
                volatility_24h = ?,
                volatility_ratio = ?,
                volume_ratio = ?,
                obv = ?,
                rsi = ?,
                macd = ?,
                macd_signal = ?,
                bb_position = ?,
                price_position = ?,
                regime = ?,
                future_return_6h = ?,
                future_return_12h = ?,
                future_return_24h = ?,
                label_filled = ?
            WHERE id = ?
            """,
            update_rows,
        )

    conn.commit()
    return {
        "inserted": len(insert_rows),
        "updated": len(update_rows),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill or relabel ml_training_data.db from training CSV")
    parser.add_argument(
        "--config",
        default=None,
        help="Optional config path used to resolve runtime default CSV/DB paths",
    )
    parser.add_argument(
        "--csv-path",
        default=None,
        help="Path to labeled training CSV",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to SQLite training DB",
    )
    parser.add_argument(
        "--no-relabel-existing",
        action="store_true",
        help="Do not overwrite existing matching DB rows",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    default_csv_path, default_db_path = _resolve_runtime_training_paths(args.config)
    csv_path = _resolve_cli_path(args.csv_path, default_csv_path)
    db_path = _resolve_cli_path(args.db_path, default_db_path)
    if not csv_path.exists():
        print(f"csv not found: {csv_path}")
        return 1

    db_path.parent.mkdir(parents=True, exist_ok=True)
    MLDataCollector(db_path=str(db_path))
    df = _load_csv(csv_path)

    conn = sqlite3.connect(str(db_path))
    try:
        before = _db_stats(conn)
        result = backfill_from_csv(
            conn,
            df,
            relabel_existing=not args.no_relabel_existing,
        )
        after = _db_stats(conn)
    finally:
        conn.close()

    print(
        "backfill complete: "
        f"csv_rows={len(df)} inserted={result['inserted']} updated={result['updated']} "
        f"db_total={before['total']}->{after['total']} "
        f"db_labeled={before['labeled']}->{after['labeled']} "
        f"db_pending={before['pending']}->{after['pending']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
