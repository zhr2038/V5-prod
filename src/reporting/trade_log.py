from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from src.utils.time import utc_now_iso

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_run_dir(run_dir: str | Path) -> Path:
    resolved = Path(run_dir)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


TRADE_EXPORT_SCHEMA_VERSION = "v5.trade_export.v1"

TRADE_CONTRACT_COLUMNS = [
    "run_id",
    "ts_utc",
    "symbol",
    "normalized_symbol",
    "side",
    "action",
    "qty",
    "price",
    "notional_usdt",
    "fee",
    "fee_ccy",
    "fee_usdt",
    "slippage_usdt",
    "order_id",
    "trade_id",
    "strategy_id",
    "position_id",
]

TRADE_COLUMNS = [
    "ts",
    "intent",
    "realized_pnl_usdt",
    "realized_pnl_pct",
    *TRADE_CONTRACT_COLUMNS,
    "trade_export_schema_version",
]

# Preserve deterministic column order while avoiding duplicates from legacy aliases.
TRADE_COLUMNS = list(dict.fromkeys(TRADE_COLUMNS))


def normalize_symbol(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    if "/" in text:
        return text.replace("/", "-")
    if "-" in text:
        return text
    if text.endswith("USDT") and len(text) > len("USDT"):
        return f"{text[:-4]}-USDT"
    return text


def _csv_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:.12g}"
    return str(value)


@dataclass
class Fill:
    """Fill类"""
    ts: str
    run_id: str
    symbol: str
    intent: str
    side: str
    qty: float
    price: float
    notional_usdt: float
    fee_usdt: Optional[float]
    slippage_usdt: Optional[float]
    realized_pnl_usdt: Optional[float] = None
    realized_pnl_pct: Optional[float] = None
    ts_utc: Optional[str] = None
    normalized_symbol: Optional[str] = None
    action: Optional[str] = None
    fee: Optional[float | str] = None
    fee_ccy: Optional[str] = None
    order_id: Optional[str] = None
    trade_id: Optional[str] = None
    strategy_id: Optional[str] = "v5"
    position_id: Optional[str] = None

    def to_row(self) -> Dict[str, Any]:
        """To row"""
        ts_utc = self.ts_utc or self.ts
        normalized_symbol = self.normalized_symbol or normalize_symbol(self.symbol)
        action = self.action or self.intent or self.side
        return {
            "ts": self.ts,
            "ts_utc": ts_utc,
            "run_id": self.run_id,
            "symbol": self.symbol,
            "normalized_symbol": normalized_symbol,
            "intent": self.intent,
            "side": self.side,
            "action": action,
            "qty": _csv_value(float(self.qty)),
            "price": _csv_value(float(self.price)),
            "notional_usdt": _csv_value(float(self.notional_usdt)),
            "fee": _csv_value(self.fee),
            "fee_ccy": _csv_value(self.fee_ccy),
            "fee_usdt": _csv_value(None if self.fee_usdt is None else float(self.fee_usdt)),
            "slippage_usdt": _csv_value(None if self.slippage_usdt is None else float(self.slippage_usdt)),
            "order_id": _csv_value(self.order_id),
            "trade_id": _csv_value(self.trade_id),
            "strategy_id": _csv_value(self.strategy_id),
            "position_id": _csv_value(self.position_id),
            "realized_pnl_usdt": _csv_value(None if self.realized_pnl_usdt is None else float(self.realized_pnl_usdt)),
            "realized_pnl_pct": _csv_value(None if self.realized_pnl_pct is None else float(self.realized_pnl_pct)),
            "trade_export_schema_version": TRADE_EXPORT_SCHEMA_VERSION,
        }


class TradeLogWriter:
    """TradeLogWriter类"""
    def __init__(self, run_dir: str, filename: str = "trades.csv"):
        self.run_dir = _resolve_run_dir(run_dir)
        self.path = self.run_dir / filename
        self.fieldnames = list(TRADE_COLUMNS)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_header()

    def _ensure_header(self) -> None:
        if not self.path.exists():
            with self.path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=TRADE_COLUMNS)
                w.writeheader()
            self.fieldnames = list(TRADE_COLUMNS)
            return

        try:
            with self.path.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = list(reader.fieldnames or [])
                rows = list(reader)
        except Exception:
            self.fieldnames = list(TRADE_COLUMNS)
            return

        if set(TRADE_CONTRACT_COLUMNS).issubset(set(fieldnames)) and "trade_export_schema_version" in fieldnames:
            self.fieldnames = fieldnames
            return

        merged_fields = list(dict.fromkeys([*fieldnames, *TRADE_COLUMNS]))
        normalized_rows = []
        for row in rows:
            normalized = {field: row.get(field, "") for field in merged_fields}
            if not normalized.get("ts_utc"):
                normalized["ts_utc"] = normalized.get("ts", "")
            if not normalized.get("normalized_symbol"):
                normalized["normalized_symbol"] = normalize_symbol(normalized.get("symbol", ""))
            if not normalized.get("action"):
                normalized["action"] = normalized.get("intent") or normalized.get("side") or "null"
            if not normalized.get("trade_export_schema_version"):
                normalized["trade_export_schema_version"] = TRADE_EXPORT_SCHEMA_VERSION
            normalized_rows.append(normalized)

        with self.path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=merged_fields)
            w.writeheader()
            w.writerows(normalized_rows)
        self.fieldnames = merged_fields

    def append_fill(self, fill: Fill) -> None:
        """Append fill"""
        with self.path.open("a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=self.fieldnames, extrasaction="ignore")
            w.writerow(fill.to_row())


def iso_utc_now() -> str:
    """Iso utc now"""
    return utc_now_iso()
