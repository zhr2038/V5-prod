#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import load_runtime_config, resolve_runtime_config_path, resolve_runtime_path
from src.data.okx_ccxt_provider import OKXCCXTProvider
from src.execution.fill_store import derive_runtime_reports_dir
from src.execution.ml_data_collector import MLDataCollector


HORIZONS = (6, 12, 24)
ONE_HOUR_MS = 3600 * 1000


def _cache_symbol_prefix(symbol: str) -> str:
    return str(symbol or "").replace("/", "_").replace("-", "_").strip()


def _parse_timestamp_ms(values: pd.Series) -> pd.Series:
    raw = pd.Series(values).reset_index(drop=True)
    numeric = pd.to_numeric(raw, errors="coerce")
    numeric_ratio = float(numeric.notna().mean()) if len(raw) else 0.0
    if numeric_ratio >= 0.8:
        max_abs = float(numeric.abs().max()) if numeric.notna().any() else 0.0
        unit = "ms" if max_abs >= 1e12 else "s"
        dt = pd.to_datetime(numeric, unit=unit, errors="coerce")
    else:
        dt = pd.to_datetime(raw, errors="coerce")

    out = pd.Series(pd.NA, index=raw.index, dtype="Int64")
    valid = dt.notna()
    if valid.any():
        out.loc[valid] = dt.loc[valid].map(lambda x: int(x.value // 1_000_000)).astype("Int64")
    return out


def _empty_candles() -> pd.DataFrame:
    return pd.DataFrame(columns=["timestamp_ms", "close"])


def _cache_file_epoch(path: Path, *, prefix: str) -> float:
    suffix = path.stem[len(prefix):] if path.stem.startswith(prefix) else path.stem

    hourly_match = re.search(r"(20\d{6}_\d{2})$", suffix)
    if hourly_match:
        try:
            return datetime.strptime(hourly_match.group(1), "%Y%m%d_%H").timestamp()
        except Exception:
            pass

    date_tokens = re.findall(r"(20\d{2}-\d{2}-\d{2}|20\d{6})", suffix)
    if date_tokens:
        token = date_tokens[-1]
        try:
            fmt = "%Y-%m-%d" if "-" in token else "%Y%m%d"
            return datetime.strptime(token, fmt).timestamp()
        except Exception:
            pass

    return path.stat().st_mtime


def load_cache_candles(
    cache_dir: Path,
    symbol: str,
    *,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> pd.DataFrame:
    prefix = _cache_symbol_prefix(symbol)
    files = sorted(cache_dir.glob(f"{prefix}_1H_*.csv"), key=lambda path: _cache_file_epoch(path, prefix=f"{prefix}_1H_"))
    if not files:
        return _empty_candles()

    frames = []
    for path in files:
        try:
            df = pd.read_csv(path, usecols=lambda c: str(c).strip().lower() in {"timestamp", "close"})
        except Exception:
            continue
        if df.empty or "timestamp" not in df.columns or "close" not in df.columns:
            continue
        frame = pd.DataFrame(
            {
                "timestamp_ms": _parse_timestamp_ms(df["timestamp"]),
                "close": pd.to_numeric(df["close"], errors="coerce"),
            }
        )
        frame = frame.dropna(subset=["timestamp_ms", "close"])
        if frame.empty:
            continue
        frame["timestamp_ms"] = frame["timestamp_ms"].astype("int64")
        frames.append(frame)

    if not frames:
        return _empty_candles()

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=["timestamp_ms"], keep="last").sort_values("timestamp_ms").reset_index(drop=True)
    if start_ms is not None:
        merged = merged[merged["timestamp_ms"] >= int(start_ms) - ONE_HOUR_MS]
    if end_ms is not None:
        merged = merged[merged["timestamp_ms"] <= int(end_ms) + ONE_HOUR_MS]
    return merged.reset_index(drop=True)


def fetch_api_candles(
    provider,
    symbol: str,
    *,
    start_ms: int,
    end_ms: int,
) -> pd.DataFrame:
    if provider is None:
        return _empty_candles()

    requested_hours = int(max(32, math.ceil(max(int(end_ms) - int(start_ms), ONE_HOUR_MS) / ONE_HOUR_MS) + 8))
    series_map = provider.fetch_ohlcv(
        symbols=[symbol],
        timeframe="1h",
        limit=requested_hours,
        end_ts_ms=int(end_ms) + ONE_HOUR_MS,
    )
    series = series_map.get(symbol)
    if series is None or not getattr(series, "ts", None):
        return _empty_candles()

    frame = pd.DataFrame(
        {
            "timestamp_ms": pd.Series(series.ts, dtype="int64"),
            "close": pd.Series(series.close, dtype="float64"),
        }
    )
    frame = frame.dropna(subset=["timestamp_ms", "close"])
    frame = frame.drop_duplicates(subset=["timestamp_ms"], keep="last").sort_values("timestamp_ms").reset_index(drop=True)
    frame = frame[
        (frame["timestamp_ms"] >= int(start_ms) - ONE_HOUR_MS)
        & (frame["timestamp_ms"] <= int(end_ms) + ONE_HOUR_MS)
    ]
    return frame.reset_index(drop=True)


def merge_candles(*frames: Iterable[pd.DataFrame]) -> pd.DataFrame:
    usable = [frame for frame in frames if isinstance(frame, pd.DataFrame) and not frame.empty]
    if not usable:
        return _empty_candles()

    merged = pd.concat(usable, ignore_index=True)
    merged = merged.dropna(subset=["timestamp_ms", "close"])
    merged["timestamp_ms"] = merged["timestamp_ms"].astype("int64")
    merged["close"] = merged["close"].astype("float64")
    merged = merged.drop_duplicates(subset=["timestamp_ms"], keep="last").sort_values("timestamp_ms").reset_index(drop=True)
    return merged


def compute_future_returns(rows: pd.DataFrame, candles: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows.assign(future_return_6h=pd.Series(dtype=float), future_return_12h=pd.Series(dtype=float), future_return_24h=pd.Series(dtype=float))

    out = rows[["id", "timestamp"]].copy().reset_index(drop=True)
    for hours in HORIZONS:
        out[f"future_return_{hours}h"] = pd.Series([np.nan] * len(out), dtype="float64")

    if candles.empty:
        return out

    ts = candles["timestamp_ms"].to_numpy(dtype=np.int64)
    close = candles["close"].to_numpy(dtype=np.float64)
    snap_ts = rows["timestamp"].to_numpy(dtype=np.int64)
    start_idx = np.searchsorted(ts, snap_ts, side="right") - 1
    valid_start = start_idx >= 0

    if not valid_start.any():
        return out

    start_px = np.zeros(len(out), dtype=np.float64)
    start_px[valid_start] = close[start_idx[valid_start]]

    last_ts = int(ts[-1])
    for hours in HORIZONS:
        target_ts = snap_ts + int(hours) * ONE_HOUR_MS
        end_idx = np.searchsorted(ts, target_ts, side="right") - 1
        valid = valid_start & (end_idx >= start_idx) & (target_ts <= last_ts)
        if not valid.any():
            continue
        end_px = close[end_idx[valid]]
        out.loc[valid, f"future_return_{hours}h"] = (end_px - start_px[valid]) / start_px[valid]

    return out


def _load_pending_rows(
    conn: sqlite3.Connection,
    *,
    as_of_ms: int,
    symbol: str | None = None,
) -> pd.DataFrame:
    params: list[object] = [int(as_of_ms) - 24 * ONE_HOUR_MS]
    symbol_filter = ""
    if symbol:
        symbol_filter = "AND symbol = ?"
        params.append(str(symbol))

    query = f"""
        SELECT id, timestamp, symbol
        FROM feature_snapshots
        WHERE timestamp <= ?
          AND (
                label_filled != 1
             OR future_return_6h IS NULL
             OR future_return_12h IS NULL
             OR future_return_24h IS NULL
          )
          {symbol_filter}
        ORDER BY symbol, timestamp
    """
    return pd.read_sql_query(query, conn, params=params)


def backfill_multihorizon_labels(
    *,
    db_path: Path,
    cache_dir: Path,
    as_of_ms: int,
    provider=None,
    symbol: str | None = None,
    max_symbols: int | None = None,
    mark_failed: bool = False,
) -> dict:
    MLDataCollector(db_path=str(db_path))

    conn = sqlite3.connect(str(db_path))
    try:
        pending = _load_pending_rows(conn, as_of_ms=as_of_ms, symbol=symbol)
        if pending.empty:
            return {
                "symbols": 0,
                "rows_pending": 0,
                "rows_filled": 0,
                "rows_failed": 0,
                "cache_symbols": 0,
                "api_symbols": 0,
            }

        if max_symbols is not None and int(max_symbols) > 0:
            keep = pending["symbol"].drop_duplicates().head(int(max_symbols)).tolist()
            pending = pending[pending["symbol"].isin(keep)].reset_index(drop=True)

        stats = {
            "symbols": int(pending["symbol"].nunique()),
            "rows_pending": int(len(pending)),
            "rows_filled": 0,
            "rows_failed": 0,
            "cache_symbols": 0,
            "api_symbols": 0,
        }

        updates = []
        failed_ids: list[tuple[int]] = []
        for sym, group in pending.groupby("symbol", sort=True):
            start_ms = int(group["timestamp"].min()) - ONE_HOUR_MS
            end_ms = int(group["timestamp"].max()) + 24 * ONE_HOUR_MS

            cache_frame = load_cache_candles(cache_dir, sym, start_ms=start_ms, end_ms=end_ms)
            if not cache_frame.empty:
                stats["cache_symbols"] += 1

            need_api = True
            if not cache_frame.empty:
                cache_min = int(cache_frame["timestamp_ms"].min())
                cache_max = int(cache_frame["timestamp_ms"].max())
                need_api = cache_min > start_ms or cache_max < end_ms

            api_frame = _empty_candles()
            if need_api and provider is not None:
                api_frame = fetch_api_candles(provider, sym, start_ms=start_ms, end_ms=end_ms)
                if not api_frame.empty:
                    stats["api_symbols"] += 1

            candles = merge_candles(cache_frame, api_frame)
            filled = compute_future_returns(group, candles)
            for row in filled.itertuples(index=False):
                fr6 = row.future_return_6h
                fr12 = row.future_return_12h
                fr24 = row.future_return_24h
                if pd.notna(fr6) and pd.notna(fr12) and pd.notna(fr24):
                    updates.append((float(fr6), float(fr12), float(fr24), int(row.id)))
                elif mark_failed:
                    failed_ids.append((int(row.id),))

        cur = conn.cursor()
        if updates:
            cur.executemany(
                """
                UPDATE feature_snapshots
                SET future_return_6h = ?,
                    future_return_12h = ?,
                    future_return_24h = ?,
                    label_filled = 1
                WHERE id = ?
                """,
                updates,
            )
        if failed_ids:
            cur.executemany(
                """
                UPDATE feature_snapshots
                SET label_filled = -1
                WHERE id = ?
                """,
                failed_ids,
            )
        conn.commit()

        stats["rows_filled"] = int(len(updates))
        stats["rows_failed"] = int(len(failed_ids))
        return stats
    finally:
        conn.close()


def _resolve_cli_or_default_path(raw_path: str, *, default_path: Path) -> Path:
    text = str(raw_path or "").strip()
    if not text:
        return default_path.resolve()
    path = Path(text)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def _runtime_training_db_path(raw_config_path: str | None = None) -> Path:
    config_path = Path(resolve_runtime_config_path(raw_config_path, project_root=PROJECT_ROOT)).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"runtime config not found: {config_path}")
    cfg = load_runtime_config(raw_config_path, project_root=PROJECT_ROOT)
    execution_cfg = cfg.get("execution") if isinstance(cfg.get("execution"), dict) else {}
    order_store_path = resolve_runtime_path(
        execution_cfg.get("order_store_path") if isinstance(execution_cfg, dict) else None,
        default="reports/orders.sqlite",
        project_root=PROJECT_ROOT,
    )
    return (derive_runtime_reports_dir(order_store_path) / "ml_training_data.db").resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bulk backfill 6h/12h/24h ML labels from cache and OKX public candles")
    parser.add_argument("--config", default="", help="Optional config path used to resolve runtime defaults")
    parser.add_argument("--db-path", default="", help="SQLite DB path (default: runtime ml_training_data.db)")
    parser.add_argument("--cache-dir", default=str(PROJECT_ROOT / "data/cache"))
    parser.add_argument("--as-of-ms", type=int, default=int(time.time() * 1000))
    parser.add_argument("--symbol", default=None, help="Only process one symbol, e.g. BTC/USDT")
    parser.add_argument("--max-symbols", type=int, default=None, help="Process only the first N symbols with pending rows")
    parser.add_argument("--skip-api", action="store_true", help="Use cache only, do not fetch missing candles from OKX")
    parser.add_argument("--mark-failed", action="store_true", help="Mark rows that still cannot be filled as label_filled=-1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = _resolve_cli_or_default_path(args.db_path, default_path=_runtime_training_db_path(args.config or None))
    cache_dir = _resolve_cli_or_default_path(args.cache_dir, default_path=(PROJECT_ROOT / "data" / "cache"))
    if not db_path.exists():
        print(f"db not found: {db_path}")
        return 1

    provider = None if args.skip_api else OKXCCXTProvider(rate_limit=True)
    result = backfill_multihorizon_labels(
        db_path=db_path,
        cache_dir=cache_dir,
        as_of_ms=int(args.as_of_ms),
        provider=provider,
        symbol=args.symbol,
        max_symbols=args.max_symbols,
        mark_failed=bool(args.mark_failed),
    )
    print(
        "multihorizon backfill complete: "
        f"symbols={result['symbols']} rows_pending={result['rows_pending']} "
        f"rows_filled={result['rows_filled']} rows_failed={result['rows_failed']} "
        f"cache_symbols={result['cache_symbols']} api_symbols={result['api_symbols']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
