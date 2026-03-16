#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.okx_ccxt_provider import OKXCCXTProvider


ONE_HOUR_MS = 3600 * 1000


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


def _to_cache_frame(series) -> pd.DataFrame:
    if series is None or not getattr(series, "ts", None):
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    frame = pd.DataFrame(
        {
            "timestamp_ms": list(getattr(series, "ts", []) or []),
            "open": list(getattr(series, "open", []) or []),
            "high": list(getattr(series, "high", []) or []),
            "low": list(getattr(series, "low", []) or []),
            "close": list(getattr(series, "close", []) or []),
            "volume": list(getattr(series, "volume", []) or []),
        }
    )
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    frame["timestamp_ms"] = pd.to_numeric(frame["timestamp_ms"], errors="coerce").astype("Int64")
    frame = frame.dropna(subset=["timestamp_ms"]).copy()
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    frame["timestamp_ms"] = frame["timestamp_ms"].astype("int64")
    frame["timestamp"] = pd.to_datetime(frame["timestamp_ms"], unit="ms", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
    return frame[["timestamp", "open", "high", "low", "close", "volume"]].reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh 1H OHLCV cache files for the stable research universe")
    parser.add_argument("--universe-path", default="reports/universe_cache.json", help="JSON file containing {symbols:[...]}")
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbol override")
    parser.add_argument("--cache-dir", default="data/cache", help="Destination cache directory")
    parser.add_argument("--start", required=True, help="Start timestamp (ISO8601, seconds, or ms)")
    parser.add_argument("--end", default="", help="End timestamp (default: current UTC hour)")
    parser.add_argument("--timeframe", default="1h", help="Timeframe to fetch (default: 1h)")
    parser.add_argument("--base-url", default="https://www.okx.com", help="OKX REST base URL")
    parser.add_argument("--timeout-sec", type=float, default=10.0, help="HTTP timeout seconds")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if str(args.timeframe).lower() != "1h":
        raise ValueError("only 1h timeframe is supported for cache refresh")

    start_ms = _parse_timestamp_ms(args.start)
    if args.end:
        end_ms = _parse_timestamp_ms(args.end)
    else:
        now = datetime.now(timezone.utc)
        end_ms = int(now.replace(minute=0, second=0, microsecond=0).timestamp() * 1000)
    if end_ms < start_ms:
        raise ValueError("end must be >= start")

    symbols = _load_symbols(args)
    cache_dir = Path(args.cache_dir)
    if not cache_dir.is_absolute():
        cache_dir = (PROJECT_ROOT / cache_dir).resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)

    requested_hours = int(math.ceil((end_ms - start_ms) / ONE_HOUR_MS)) + 1
    provider = OKXCCXTProvider(base_url=str(args.base_url), timeout_sec=float(args.timeout_sec))
    market_data = provider.fetch_ohlcv(
        symbols=symbols,
        timeframe="1h",
        limit=max(requested_hours, 2),
        end_ts_ms=end_ms + ONE_HOUR_MS,
    )

    summary = {
        "symbols_requested": len(symbols),
        "symbols_fetched": 0,
        "rows_written": 0,
        "files": {},
        "missing_symbols": [],
        "window": {"start_ms": start_ms, "end_ms": end_ms},
    }

    start_label = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    end_label = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

    for symbol in symbols:
        series = market_data.get(symbol)
        frame = _to_cache_frame(series)
        if frame.empty:
            summary["missing_symbols"].append(symbol)
            continue

        parsed_ts = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        ts_ms = parsed_ts.map(lambda x: int(x.value // 1_000_000) if pd.notna(x) else None)
        frame = frame[(ts_ms >= start_ms) & (ts_ms <= end_ms)].reset_index(drop=True)
        if frame.empty:
            summary["missing_symbols"].append(symbol)
            continue

        file_name = f"{symbol.replace('/', '_').replace('-', '_')}_1H_{start_label}_{end_label}.csv"
        out_path = cache_dir / file_name
        frame.to_csv(out_path, index=False)
        summary["symbols_fetched"] += 1
        summary["rows_written"] += int(len(frame))
        summary["files"][symbol] = {"path": str(out_path), "rows": int(len(frame))}

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
