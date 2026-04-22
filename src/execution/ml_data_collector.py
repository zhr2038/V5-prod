"""ML training snapshot collection and multihorizon label backfill."""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class MLDataCollectorError(Exception):
    """Raised when collector persistence or label backfill fails."""


class FeatureCalculationError(MLDataCollectorError):
    """Raised when a feature snapshot cannot be computed safely."""


@dataclass
class FeatureRecord:
    timestamp: int
    symbol: str
    returns_1h: float
    returns_6h: float
    returns_24h: float
    momentum_5d: float
    momentum_20d: float
    volatility_6h: float
    volatility_24h: float
    volatility_ratio: float
    volume_ratio: float
    obv: float
    rsi: float
    macd: float
    macd_signal: float
    bb_position: float
    price_position: float
    regime: str
    future_return_6h: Optional[float] = None
    future_return_12h: Optional[float] = None
    future_return_24h: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class MLDataCollector:
    ONE_HOUR_MS = 3600 * 1000

    def __init__(self, db_path: str = "reports/ml_training_data.db", data_provider=None):
        self.db_path = str(self._resolve_path(db_path))
        self._data_provider = data_provider
        self._conn: sqlite3.Connection | None = None
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_database()

    @staticmethod
    def _resolve_path(path: str | Path) -> Path:
        resolved = Path(path)
        if not resolved.is_absolute():
            resolved = (PROJECT_ROOT / resolved).resolve()
        return resolved

    def _get_connection(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _close_connection(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception as exc:
                logger.warning("[ML] warning closing sqlite connection: %s", exc)
            finally:
                self._conn = None

    def __enter__(self) -> MLDataCollector:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._close_connection()
        return False

    def _init_database(self) -> None:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS feature_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER,
                symbol TEXT,
                returns_1h REAL,
                returns_6h REAL,
                returns_24h REAL,
                momentum_5d REAL,
                momentum_20d REAL,
                volatility_6h REAL,
                volatility_24h REAL,
                volatility_ratio REAL,
                volume_ratio REAL,
                obv REAL,
                rsi REAL,
                macd REAL,
                macd_signal REAL,
                bb_position REAL,
                price_position REAL,
                regime TEXT,
                future_return_6h REAL,
                future_return_12h REAL,
                future_return_24h REAL,
                label_filled INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON feature_snapshots(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON feature_snapshots(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_label_filled ON feature_snapshots(label_filled)")

        existing_cols = {str(row[1]) for row in cursor.execute("PRAGMA table_info(feature_snapshots)").fetchall()}
        if "future_return_12h" not in existing_cols:
            cursor.execute("ALTER TABLE feature_snapshots ADD COLUMN future_return_12h REAL")
        if "future_return_24h" not in existing_cols:
            cursor.execute("ALTER TABLE feature_snapshots ADD COLUMN future_return_24h REAL")

        cursor.execute(
            """
            UPDATE feature_snapshots
            SET label_filled = 0
            WHERE label_filled = 1
              AND (
                    future_return_6h IS NULL
                 OR future_return_12h IS NULL
                 OR future_return_24h IS NULL
              )
            """
        )

        self._dedupe_feature_snapshots(conn)
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_feature_ts_symbol_unique
            ON feature_snapshots(timestamp, symbol)
            """
        )
        conn.commit()
        conn.close()

    @staticmethod
    def _dedupe_feature_snapshots(conn: sqlite3.Connection) -> int:
        cursor = conn.cursor()
        rows = cursor.execute(
            """
            SELECT
                id,
                timestamp,
                symbol,
                label_filled,
                future_return_6h,
                future_return_12h,
                future_return_24h
            FROM feature_snapshots
            ORDER BY id
            """
        ).fetchall()
        if not rows:
            return 0

        keep_by_key: dict[tuple[int, str], tuple[tuple[int, int, int], int]] = {}
        drop_ids: list[int] = []
        for row_id, ts, symbol, label_filled, future_6h, future_12h, future_24h in rows:
            key = (int(ts), str(symbol))
            label_count = sum(1 for value in (future_6h, future_12h, future_24h) if value is not None)
            score = (int(label_filled or 0), label_count, int(row_id))
            previous = keep_by_key.get(key)
            if previous is None or score > previous[0]:
                if previous is not None:
                    drop_ids.append(previous[1])
                keep_by_key[key] = (score, int(row_id))
            else:
                drop_ids.append(int(row_id))

        if drop_ids:
            cursor.executemany("DELETE FROM feature_snapshots WHERE id = ?", [(row_id,) for row_id in drop_ids])
        return len(drop_ids)

    def collect_features(
        self,
        timestamp: int,
        symbol: str,
        market_data: Dict[str, Any],
        regime: str,
    ) -> bool:
        try:
            if not market_data or "close" not in market_data:
                raise FeatureCalculationError(f"invalid market_data for {symbol}: missing close")
            if len(market_data["close"]) < 2:
                raise FeatureCalculationError(f"insufficient bars for {symbol}: need at least 2")

            features = self._calculate_features(market_data)
            for key, value in features.items():
                if pd.isna(value) or np.isinf(value):
                    raise FeatureCalculationError(f"invalid feature {key}={value} for {symbol}")

            record = FeatureRecord(
                timestamp=int(timestamp),
                symbol=str(symbol),
                regime=str(regime),
                future_return_6h=None,
                future_return_12h=None,
                future_return_24h=None,
                **features,
            )
            self._save_record(record)
            return True
        except FeatureCalculationError as exc:
            logger.warning("[ML Warning] feature calculation failed for %s: %s", symbol, exc)
            return False
        except MLDataCollectorError as exc:
            logger.error("[ML Error] database error for %s: %s", symbol, exc)
            return False
        except Exception as exc:
            logger.exception("[ML Critical] unexpected error collecting features for %s: %s", symbol, exc)
            return False

    def _calculate_features(self, data: Dict[str, Any]) -> Dict[str, float]:
        from src.research.feature_registry import build_snapshot_feature_row

        row = build_snapshot_feature_row(
            symbol=str(data.get("symbol", "_tmp")),
            close=list(data["close"]),
            high=list(data.get("high", data["close"])),
            low=list(data.get("low", data["close"])),
            volume=list(data.get("volume", [0] * len(data["close"]))),
            feature_groups=("classic",),
            include_time_features=False,
        )
        return {key: float(value) for key, value in row.items() if key != "symbol"}

    def _save_record(self, record: FeatureRecord) -> None:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO feature_snapshots (
                    timestamp, symbol, returns_1h, returns_6h, returns_24h,
                    momentum_5d, momentum_20d, volatility_6h, volatility_24h,
                    volatility_ratio, volume_ratio, obv, rsi, macd, macd_signal,
                    bb_position, price_position, regime,
                    future_return_6h, future_return_12h, future_return_24h, label_filled
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(timestamp, symbol) DO UPDATE SET
                    returns_1h = excluded.returns_1h,
                    returns_6h = excluded.returns_6h,
                    returns_24h = excluded.returns_24h,
                    momentum_5d = excluded.momentum_5d,
                    momentum_20d = excluded.momentum_20d,
                    volatility_6h = excluded.volatility_6h,
                    volatility_24h = excluded.volatility_24h,
                    volatility_ratio = excluded.volatility_ratio,
                    volume_ratio = excluded.volume_ratio,
                    obv = excluded.obv,
                    rsi = excluded.rsi,
                    macd = excluded.macd,
                    macd_signal = excluded.macd_signal,
                    bb_position = excluded.bb_position,
                    price_position = excluded.price_position,
                    regime = excluded.regime
                """,
                (
                    record.timestamp,
                    record.symbol,
                    record.returns_1h,
                    record.returns_6h,
                    record.returns_24h,
                    record.momentum_5d,
                    record.momentum_20d,
                    record.volatility_6h,
                    record.volatility_24h,
                    record.volatility_ratio,
                    record.volume_ratio,
                    record.obv,
                    record.rsi,
                    record.macd,
                    record.macd_signal,
                    record.bb_position,
                    record.price_position,
                    record.regime,
                    record.future_return_6h,
                    record.future_return_12h,
                    record.future_return_24h,
                ),
            )
            conn.commit()
        except sqlite3.Error as exc:
            conn.rollback()
            raise MLDataCollectorError(f"database error saving record: {exc}") from exc

    @staticmethod
    def _align_export_cycles(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, int]]:
        if df.empty or "timestamp" not in df.columns or "symbol" not in df.columns:
            rows = int(len(df))
            return df, {"rows_before": rows, "rows_after": rows, "duplicates_removed": 0}

        out = df.copy()
        ts = pd.to_numeric(out["timestamp"], errors="coerce")
        hour_ms = 3600 * 1000
        out["timestamp"] = ((ts // hour_ms) * hour_ms).astype("Int64")
        out = out.dropna(subset=["timestamp"]).copy()
        out["timestamp"] = out["timestamp"].astype("int64")
        rows_before = int(len(out))
        out = (
            out.sort_values(["timestamp", "symbol"])
            .drop_duplicates(subset=["timestamp", "symbol"], keep="last")
            .reset_index(drop=True)
        )
        rows_after = int(len(out))
        return out, {
            "rows_before": rows_before,
            "rows_after": rows_after,
            "duplicates_removed": rows_before - rows_after,
        }

    def _fill_labels_batch(self, current_timestamp: int) -> Dict[str, int]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            six_hours_ago = int(current_timestamp) - 6 * self.ONE_HOUR_MS
            twelve_hours_ago = int(current_timestamp) - 12 * self.ONE_HOUR_MS
            twenty_four_hours_ago = int(current_timestamp) - 24 * self.ONE_HOUR_MS
            cursor.execute(
                """
                SELECT
                    id, timestamp, symbol,
                    future_return_6h, future_return_12h, future_return_24h
                FROM feature_snapshots
                WHERE label_filled = 0
                  AND (
                    (timestamp <= ? AND future_return_6h IS NULL)
                    OR (timestamp <= ? AND future_return_12h IS NULL)
                    OR (timestamp <= ? AND future_return_24h IS NULL)
                  )
                ORDER BY timestamp, id
                LIMIT 1000
                """,
                (six_hours_ago, twelve_hours_ago, twenty_four_hours_ago),
            )
            records_to_fill = cursor.fetchall()

            partial_updates: list[tuple[Optional[float], Optional[float], Optional[float], int]] = []
            full_updates: list[tuple[float, float, float, int]] = []
            failed_updates: list[tuple[int]] = []

            for row in records_to_fill:
                record_id = int(row["id"])
                record_ts = int(row["timestamp"])
                symbol = str(row["symbol"])
                age_ms = max(0, int(current_timestamp) - record_ts)
                available_horizons = [hours for hours in (6, 12, 24) if age_ms >= hours * self.ONE_HOUR_MS]
                values = {
                    6: row["future_return_6h"],
                    12: row["future_return_12h"],
                    24: row["future_return_24h"],
                }
                failed = False

                try:
                    for hours in available_horizons:
                        if values.get(hours) is not None:
                            continue
                        future_return = self._calculate_future_return(symbol, record_ts, hours)
                        if future_return is None:
                            failed = True
                            break
                        values[hours] = float(future_return)
                except Exception as exc:
                    logger.error("[ML] error processing record %s: %s", record_id, exc)
                    failed = True

                if failed:
                    if 24 in available_horizons:
                        failed_updates.append((record_id,))
                    continue

                payload = (
                    values.get(6),
                    values.get(12),
                    values.get(24),
                    record_id,
                )
                if values.get(6) is not None and values.get(12) is not None and values.get(24) is not None:
                    full_updates.append((float(values[6]), float(values[12]), float(values[24]), record_id))
                else:
                    partial_updates.append(payload)

            if partial_updates:
                cursor.executemany(
                    """
                    UPDATE feature_snapshots
                    SET future_return_6h = ?,
                        future_return_12h = ?,
                        future_return_24h = ?
                    WHERE id = ?
                    """,
                    partial_updates,
                )

            if full_updates:
                cursor.executemany(
                    """
                    UPDATE feature_snapshots
                    SET future_return_6h = ?,
                        future_return_12h = ?,
                        future_return_24h = ?,
                        label_filled = 1
                    WHERE id = ?
                    """,
                    full_updates,
                )

            if failed_updates:
                cursor.executemany(
                    """
                    UPDATE feature_snapshots
                    SET label_filled = -1
                    WHERE id = ?
                    """,
                    failed_updates,
                )

            conn.commit()
            if full_updates or partial_updates or failed_updates:
                logger.info(
                    "[ML] label fill complete: fully_labeled=%d partial=%d failed=%d",
                    len(full_updates),
                    len(partial_updates),
                    len(failed_updates),
                )
            return {
                "filled": int(len(full_updates)),
                "partial": int(len(partial_updates)),
                "failed": int(len(failed_updates)),
                "processed": int(len(full_updates) + len(partial_updates) + len(failed_updates)),
            }
        except sqlite3.Error as exc:
            conn.rollback()
            raise MLDataCollectorError(f"database error filling labels: {exc}") from exc

    def fill_labels(self, current_timestamp: int) -> int:
        return int(self._fill_labels_batch(current_timestamp)["filled"])

    def fill_all_labels(self, current_timestamp: int, *, max_batches: int = 100) -> dict[str, int]:
        total_filled = 0
        batches_run = 0
        for _ in range(max(int(max_batches), 1)):
            batch_stats = self._fill_labels_batch(current_timestamp)
            processed = int(batch_stats.get("processed", 0))
            if processed <= 0:
                break
            filled = int(batch_stats.get("filled", 0))
            batches_run += 1
            total_filled += filled
        return {"filled": total_filled, "batches": batches_run}

    def _calculate_future_return(self, symbol: str, start_timestamp: int, hours: int) -> Optional[float]:
        try:
            if self._data_provider is not None:
                try:
                    return self._fetch_future_return_from_api(symbol, start_timestamp, hours)
                except Exception as exc:
                    logger.warning("[ML] API future-return fetch failed, fallback to cache: %s", exc)
            return self._fetch_future_return_from_cache(symbol, start_timestamp, hours)
        except Exception as exc:
            logger.error("[ML] error calculating future return for %s: %s", symbol, exc)
            return None

    @staticmethod
    def _parse_cache_timestamp_ms(values: pd.Series) -> pd.Series:
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

    @staticmethod
    def _empty_candle_frame() -> pd.DataFrame:
        return pd.DataFrame(columns=["timestamp_ms", "close"])

    @staticmethod
    def _empty_ohlcv_frame() -> pd.DataFrame:
        return pd.DataFrame(columns=["timestamp_ms", "open", "high", "low", "close", "volume"])

    def _default_cache_dir(self) -> Path:
        return Path(self.db_path).resolve().parent.parent / "data" / "cache"

    @staticmethod
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

    @classmethod
    def _load_cache_ohlcv(
        cls,
        cache_dir: Path,
        symbol: str,
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> pd.DataFrame:
        prefix = str(symbol or "").replace("/", "_").replace("-", "_").strip()
        files = sorted(cache_dir.glob(f"{prefix}_1H_*.csv"), key=lambda path: cls._cache_file_epoch(path, prefix=f"{prefix}_1H_"))
        if not files:
            return cls._empty_ohlcv_frame()

        frames: list[pd.DataFrame] = []
        for path in files:
            try:
                df = pd.read_csv(
                    path,
                    usecols=lambda c: str(c).strip().lower() in {"timestamp", "open", "high", "low", "close", "volume"},
                )
            except Exception:
                continue
            if df.empty or "timestamp" not in df.columns or "close" not in df.columns:
                continue
            close_s = pd.to_numeric(df["close"], errors="coerce")
            frame = pd.DataFrame(
                {
                    "timestamp_ms": cls._parse_cache_timestamp_ms(df["timestamp"]),
                    "open": pd.to_numeric(df["open"], errors="coerce") if "open" in df.columns else close_s,
                    "high": pd.to_numeric(df["high"], errors="coerce") if "high" in df.columns else close_s,
                    "low": pd.to_numeric(df["low"], errors="coerce") if "low" in df.columns else close_s,
                    "close": close_s,
                    "volume": pd.to_numeric(df["volume"], errors="coerce") if "volume" in df.columns else 0.0,
                }
            ).dropna(subset=["timestamp_ms", "close"])
            if frame.empty:
                continue
            frame["timestamp_ms"] = frame["timestamp_ms"].astype("int64")
            for col in ("open", "high", "low", "close", "volume"):
                frame[col] = pd.to_numeric(frame[col], errors="coerce")
            frame["open"] = frame["open"].fillna(frame["close"]).astype("float64")
            frame["high"] = frame["high"].fillna(frame["close"]).astype("float64")
            frame["low"] = frame["low"].fillna(frame["close"]).astype("float64")
            frame["close"] = frame["close"].astype("float64")
            frame["volume"] = frame["volume"].fillna(0.0).astype("float64")
            frames.append(frame)

        if not frames:
            return cls._empty_ohlcv_frame()

        merged = pd.concat(frames, ignore_index=True)
        merged = merged.drop_duplicates(subset=["timestamp_ms"], keep="last").sort_values("timestamp_ms").reset_index(drop=True)
        if start_ms is not None:
            merged = merged[merged["timestamp_ms"] >= int(start_ms) - cls.ONE_HOUR_MS]
        if end_ms is not None:
            merged = merged[merged["timestamp_ms"] <= int(end_ms) + cls.ONE_HOUR_MS]
        return merged.reset_index(drop=True)

    @classmethod
    def _load_cache_candles(
        cls,
        cache_dir: Path,
        symbol: str,
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> pd.DataFrame:
        ohlcv = cls._load_cache_ohlcv(cache_dir, symbol, start_ms=start_ms, end_ms=end_ms)
        if ohlcv.empty:
            return cls._empty_candle_frame()
        return ohlcv[["timestamp_ms", "close"]].copy()

    def load_market_data_for_feature_snapshot(
        self,
        symbol: str,
        *,
        end_timestamp: int,
        lookback_bars: int = 600,
    ) -> Optional[Dict[str, Any]]:
        end_ms = int(end_timestamp)
        lookback = max(int(lookback_bars), 2)
        cache_dir = self._default_cache_dir()
        ohlcv = self._load_cache_ohlcv(cache_dir, symbol, end_ms=end_ms)
        if not ohlcv.empty:
            ohlcv = ohlcv[ohlcv["timestamp_ms"] <= end_ms].tail(lookback).reset_index(drop=True)
            if len(ohlcv) >= 2:
                return {
                    "symbol": str(symbol),
                    "ts": ohlcv["timestamp_ms"].astype("int64").tolist(),
                    "open": ohlcv["open"].astype(float).tolist(),
                    "high": ohlcv["high"].astype(float).tolist(),
                    "low": ohlcv["low"].astype(float).tolist(),
                    "close": ohlcv["close"].astype(float).tolist(),
                    "volume": ohlcv["volume"].astype(float).tolist(),
                }

        if self._data_provider is None:
            return None

        try:
            series_dict = self._data_provider.fetch_ohlcv(
                symbols=[str(symbol)],
                timeframe="1h",
                limit=lookback,
                end_ts_ms=end_ms + self.ONE_HOUR_MS,
            )
        except Exception as exc:
            logger.warning("[ML] failed loading snapshot OHLCV for %s from provider: %s", symbol, exc)
            return None

        series = series_dict.get(str(symbol))
        if series is None or not getattr(series, "close", None):
            return None

        ts = list(getattr(series, "ts", []) or [])
        close = list(getattr(series, "close", []) or [])
        if len(close) < 2:
            return None
        return {
            "symbol": str(symbol),
            "ts": ts,
            "open": list(getattr(series, "open", []) or close),
            "high": list(getattr(series, "high", []) or close),
            "low": list(getattr(series, "low", []) or close),
            "close": close,
            "volume": list(getattr(series, "volume", []) or [0.0] * len(close)),
        }

    def backfill_feature_snapshots_from_cache(
        self,
        *,
        symbols: list[str],
        start_timestamp: int,
        end_timestamp: int,
        lookback_bars: int = 600,
        overwrite_existing: bool = False,
        regime: str = "UNKNOWN",
    ) -> dict[str, Any]:
        start_ms = int(start_timestamp)
        end_ms = int(end_timestamp)
        lookback = max(int(lookback_bars), 2)
        cache_dir = self._default_cache_dir()

        conn = self._get_connection()
        cursor = conn.cursor()
        rows = cursor.execute(
            """
            SELECT timestamp, symbol
            FROM feature_snapshots
            WHERE timestamp BETWEEN ? AND ?
            """,
            (start_ms, end_ms),
        ).fetchall()
        existing_keys: set[tuple[int, str]] = {(int(ts), str(symbol)) for ts, symbol in rows}

        stats = {
            "symbols_requested": len(symbols or []),
            "symbols_loaded": 0,
            "inserted": 0,
            "updated": 0,
            "skipped_existing": 0,
            "failed": 0,
            "missing_cache_symbols": [],
        }

        for raw_symbol in symbols or []:
            symbol = str(raw_symbol).strip()
            if not symbol:
                continue
            ohlcv = self._load_cache_ohlcv(
                cache_dir,
                symbol,
                start_ms=start_ms - (lookback * self.ONE_HOUR_MS),
                end_ms=end_ms,
            )
            if ohlcv.empty:
                stats["missing_cache_symbols"].append(symbol)
                continue

            stats["symbols_loaded"] += 1
            target_rows = ohlcv[(ohlcv["timestamp_ms"] >= start_ms) & (ohlcv["timestamp_ms"] <= end_ms)].reset_index(drop=True)
            if target_rows.empty:
                continue

            for _, row in target_rows.iterrows():
                ts = int(row["timestamp_ms"])
                key = (ts, symbol)
                if key in existing_keys and not overwrite_existing:
                    stats["skipped_existing"] += 1
                    continue

                upto = ohlcv[ohlcv["timestamp_ms"] <= ts].tail(lookback).reset_index(drop=True)
                ok = self.collect_features(
                    timestamp=ts,
                    symbol=symbol,
                    market_data={
                        "symbol": symbol,
                        "ts": upto["timestamp_ms"].astype("int64").tolist(),
                        "open": upto["open"].astype(float).tolist(),
                        "high": upto["high"].astype(float).tolist(),
                        "low": upto["low"].astype(float).tolist(),
                        "close": upto["close"].astype(float).tolist(),
                        "volume": upto["volume"].astype(float).tolist(),
                    },
                    regime=regime,
                )
                if not ok:
                    stats["failed"] += 1
                    continue

                if key in existing_keys:
                    stats["updated"] += 1
                else:
                    stats["inserted"] += 1
                    existing_keys.add(key)

        return stats

    @staticmethod
    def _compute_future_return_from_candles(
        candles: pd.DataFrame,
        *,
        start_timestamp: int,
        hours: int,
    ) -> Optional[float]:
        if candles.empty:
            return None

        end_timestamp = int(start_timestamp) + int(hours) * MLDataCollector.ONE_HOUR_MS
        ts = candles["timestamp_ms"].to_numpy(dtype=np.int64)
        close = candles["close"].to_numpy(dtype=np.float64)
        start_idx = int(np.searchsorted(ts, int(start_timestamp), side="right") - 1)
        if start_idx < 0:
            return None
        end_idx = int(np.searchsorted(ts, end_timestamp, side="right") - 1)
        if end_idx < start_idx or end_timestamp > int(ts[-1]):
            return None

        start_price = float(close[start_idx])
        end_price = float(close[end_idx])
        if not np.isfinite(start_price) or not np.isfinite(end_price) or start_price <= 0.0:
            return None
        return float((end_price - start_price) / start_price)

    def _fetch_future_return_from_api(self, symbol: str, start_timestamp: int, hours: int) -> Optional[float]:
        if self._data_provider is None:
            return None

        end_timestamp = int(start_timestamp) + int(hours) * self.ONE_HOUR_MS
        series_dict = self._data_provider.fetch_ohlcv(
            symbols=[symbol],
            timeframe="1h",
            limit=max(int(hours) + 10, 32),
            end_ts_ms=end_timestamp + self.ONE_HOUR_MS,
        )
        if symbol not in series_dict:
            raise MLDataCollectorError(f"API did not return candles for {symbol}")

        series = series_dict[symbol]
        if not getattr(series, "ts", None):
            raise MLDataCollectorError(f"{symbol} provider returned empty candles")

        candles = pd.DataFrame(
            {
                "timestamp_ms": pd.Series(series.ts, dtype="int64"),
                "close": pd.Series(series.close, dtype="float64"),
            }
        ).dropna(subset=["timestamp_ms", "close"])
        candles = candles.drop_duplicates(subset=["timestamp_ms"], keep="last").sort_values("timestamp_ms").reset_index(drop=True)
        future_return = self._compute_future_return_from_candles(
            candles,
            start_timestamp=start_timestamp,
            hours=hours,
        )
        if future_return is None:
            raise MLDataCollectorError(f"{symbol} provider candles do not fully cover {hours}h horizon")
        logger.debug("[ML] API future return ok %s horizon=%sh return=%.4f", symbol, hours, future_return)
        return future_return

    def _fetch_future_return_from_cache(self, symbol: str, start_timestamp: int, hours: int) -> Optional[float]:
        cache_dir = self._default_cache_dir()
        end_timestamp = int(start_timestamp) + int(hours) * self.ONE_HOUR_MS
        candles = self._load_cache_candles(
            cache_dir,
            symbol,
            start_ms=start_timestamp,
            end_ms=end_timestamp,
        )
        if candles.empty:
            logger.warning("[ML Warning] no local cache candles for %s", symbol)
            return None

        future_return = self._compute_future_return_from_candles(
            candles,
            start_timestamp=start_timestamp,
            hours=hours,
        )
        if future_return is not None:
            return future_return

        end_dt = pd.to_datetime(end_timestamp, unit="ms")
        max_data_time = pd.to_datetime(int(candles["timestamp_ms"].max()), unit="ms")
        if max_data_time < end_dt:
            logger.warning("[ML Warning] 缓存数据不足: 需要%s, 实际%s", end_dt, max_data_time)
        return None

    def export_training_data(self, output_path: str = "reports/ml_training_data.csv", min_samples: int = 100) -> bool:
        conn = self._get_connection()
        try:
            query = """
                SELECT
                    timestamp, symbol,
                    returns_1h, returns_6h, returns_24h,
                    momentum_5d, momentum_20d,
                    volatility_6h, volatility_24h, volatility_ratio,
                    volume_ratio, obv,
                    rsi, macd, macd_signal,
                    bb_position, price_position,
                    regime,
                    future_return_6h,
                    future_return_12h,
                    future_return_24h
                FROM feature_snapshots
                WHERE label_filled = 1
                  AND future_return_6h IS NOT NULL
                  AND future_return_12h IS NOT NULL
                  AND future_return_24h IS NOT NULL
                ORDER BY timestamp
            """
            df = pd.read_sql_query(query, conn)
            df, align_meta = self._align_export_cycles(df)
            if len(df) < int(min_samples):
                logger.warning("Insufficient samples: %d < %d", len(df), int(min_samples))
                return False

            resolved_output_path = self._resolve_path(output_path)
            resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(resolved_output_path, index=False)
            logger.info("Exported %d samples to %s", len(df), resolved_output_path)
            logger.info("Training data symbols=%d range=%s..%s", df["symbol"].nunique(), df["timestamp"].min(), df["timestamp"].max())
            if align_meta["duplicates_removed"] > 0:
                logger.info(
                    "Cycle alignment removed %d duplicate rows (%d -> %d)",
                    align_meta["duplicates_removed"],
                    align_meta["rows_before"],
                    align_meta["rows_after"],
                )
            return True
        except sqlite3.Error as exc:
            raise MLDataCollectorError(f"database error exporting data: {exc}") from exc

    def get_statistics(self) -> Dict[str, Any]:
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            total_records = int(cursor.execute("SELECT COUNT(*) FROM feature_snapshots").fetchone()[0])
            labeled_records = int(
                cursor.execute(
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
            num_symbols = int(cursor.execute("SELECT COUNT(DISTINCT symbol) FROM feature_snapshots").fetchone()[0])
            min_ts, max_ts = cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM feature_snapshots").fetchone()
            return {
                "total_records": total_records,
                "labeled_records": labeled_records,
                "unlabeled_records": total_records - labeled_records,
                "num_symbols": num_symbols,
                "time_range": (min_ts, max_ts) if min_ts is not None else None,
                "ready_for_training": labeled_records >= 100,
            }
        except sqlite3.Error as exc:
            raise MLDataCollectorError(f"database error getting statistics: {exc}") from exc
