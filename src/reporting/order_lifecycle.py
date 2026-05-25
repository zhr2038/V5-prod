from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from src.execution.fill_store import derive_fill_store_path
from src.reporting.trade_log import normalize_symbol


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ORDER_LIFECYCLE_SCHEMA_VERSION = "v5.order_lifecycle.v1"

ORDER_LIFECYCLE_FIELDS = (
    "schema_version",
    "lifecycle_id",
    "run_id",
    "ts_utc",
    "symbol",
    "normalized_symbol",
    "side",
    "intent",
    "order_state",
    "decision_ts",
    "signal_price",
    "arrival_bid",
    "arrival_ask",
    "arrival_mid",
    "spread_bps_at_decision",
    "submit_ts",
    "order_type",
    "order_px",
    "cl_ord_id",
    "exchange_order_id",
    "first_fill_ts",
    "last_fill_ts",
    "fill_px",
    "avg_fill_px",
    "filled_qty",
    "fee",
    "fee_ccy",
    "fee_usdt",
    "notional_usdt",
    "requested_notional_usdt",
    "trade_ids",
    "fill_count",
)


def lifecycle_id_for(
    *,
    run_id: str,
    symbol: str,
    side: str,
    intent: str,
    notional_usdt: Any,
    signal_price: Any,
) -> str:
    material = "|".join(
        [
            str(run_id or "").strip(),
            normalize_symbol(symbol),
            str(side or "").strip().lower(),
            str(intent or "").strip().upper(),
            str(notional_usdt or "").strip(),
            str(signal_price or "").strip(),
        ]
    )
    return "olc_" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]


def annotate_orders_with_arrival(
    orders: Iterable[Any],
    *,
    run_id: str,
    decision_ts: str,
    top_of_book: Mapping[str, Any] | None = None,
) -> int:
    """Attach decision-time arrival quotes to order.meta for later OrderStore export."""

    count = 0
    book = dict(top_of_book or {})
    for order in orders or []:
        meta = getattr(order, "meta", None)
        if not isinstance(meta, dict):
            meta = {}
            try:
                order.meta = meta
            except Exception:
                continue

        symbol = str(getattr(order, "symbol", "") or "")
        quote = _lookup_top_of_book(book, symbol)
        bid = _float_or_none(quote.get("bid") if isinstance(quote, Mapping) else None)
        ask = _float_or_none(quote.get("ask") if isinstance(quote, Mapping) else None)
        mid = _float_or_none(quote.get("mid") if isinstance(quote, Mapping) else None)
        if mid is None and bid is not None and ask is not None and bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
        spread_bps = _float_or_none(quote.get("spread_bps") if isinstance(quote, Mapping) else None)
        if spread_bps is None and bid is not None and ask is not None and mid and mid > 0:
            spread_bps = (ask - bid) / mid * 10_000.0

        payload = {
            "lifecycle_id": lifecycle_id_for(
                run_id=run_id,
                symbol=symbol,
                side=getattr(order, "side", ""),
                intent=getattr(order, "intent", ""),
                notional_usdt=getattr(order, "notional_usdt", ""),
                signal_price=getattr(order, "signal_price", ""),
            ),
            "decision_ts": decision_ts,
            "signal_price": _float_or_none(getattr(order, "signal_price", None)),
            "arrival_bid": bid,
            "arrival_ask": ask,
            "arrival_mid": mid,
            "spread_bps_at_decision": spread_bps,
        }
        meta["order_lifecycle"] = {**dict(meta.get("order_lifecycle") or {}), **payload}
        count += 1
    return count


def write_order_lifecycle(
    *,
    run_dir: str | Path,
    reports_dir: str | Path | None = None,
    orders: Iterable[Any] = (),
    order_store_path: str | Path | None = None,
    fill_store_path: str | Path | None = None,
    append_reports: bool = False,
) -> list[dict[str, Any]]:
    """Write per-run order_lifecycle.csv and optionally upsert reports/order_lifecycle.csv."""

    run_path = _resolve_path(run_dir)
    run_id = run_path.name
    rows = _rows_from_order_store(
        run_id=run_id,
        order_store_path=order_store_path,
        fill_store_path=fill_store_path,
    )
    if not rows:
        rows = _rows_from_orders(run_id=run_id, orders=orders)

    _write_csv(run_path / "order_lifecycle.csv", rows)
    if append_reports and reports_dir is not None:
        _upsert_csv(Path(reports_dir) / "order_lifecycle.csv", rows)
    return rows


def _rows_from_orders(*, run_id: str, orders: Iterable[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    now = _utc_now()
    for order in orders or []:
        meta = getattr(order, "meta", {}) if isinstance(getattr(order, "meta", {}), dict) else {}
        lifecycle = dict(meta.get("order_lifecycle") or {})
        symbol = str(getattr(order, "symbol", "") or "")
        rows.append(
            _format_row(
                {
                    "schema_version": ORDER_LIFECYCLE_SCHEMA_VERSION,
                    "lifecycle_id": lifecycle.get("lifecycle_id")
                    or lifecycle_id_for(
                        run_id=run_id,
                        symbol=symbol,
                        side=getattr(order, "side", ""),
                        intent=getattr(order, "intent", ""),
                        notional_usdt=getattr(order, "notional_usdt", ""),
                        signal_price=getattr(order, "signal_price", ""),
                    ),
                    "run_id": run_id,
                    "ts_utc": now,
                    "symbol": symbol,
                    "normalized_symbol": normalize_symbol(symbol),
                    "side": getattr(order, "side", ""),
                    "intent": getattr(order, "intent", ""),
                    "order_state": "DECISION",
                    "decision_ts": lifecycle.get("decision_ts"),
                    "signal_price": lifecycle.get("signal_price", getattr(order, "signal_price", None)),
                    "arrival_bid": lifecycle.get("arrival_bid"),
                    "arrival_ask": lifecycle.get("arrival_ask"),
                    "arrival_mid": lifecycle.get("arrival_mid"),
                    "spread_bps_at_decision": lifecycle.get("spread_bps_at_decision"),
                    "requested_notional_usdt": getattr(order, "notional_usdt", None),
                    "notional_usdt": getattr(order, "notional_usdt", None),
                }
            )
        )
    return rows


def _rows_from_order_store(
    *,
    run_id: str,
    order_store_path: str | Path | None,
    fill_store_path: str | Path | None,
) -> list[dict[str, Any]]:
    if order_store_path is None:
        return []
    order_path = _resolve_path(order_store_path)
    if not order_path.exists():
        return []
    fill_path = _resolve_path(fill_store_path) if fill_store_path is not None else _resolve_path(derive_fill_store_path(order_path))

    rows: list[dict[str, Any]] = []
    for row in _order_rows_for_run(order_path, run_id):
        req = _json_obj(row.get("req_json"))
        meta = req.get("_v5_order_meta") if isinstance(req, dict) else {}
        meta = meta if isinstance(meta, dict) else {}
        lifecycle = dict(meta.get("order_lifecycle") or {})
        submit = req.get("_v5_order_lifecycle_submit") if isinstance(req, dict) else {}
        submit = submit if isinstance(submit, dict) else {}
        symbol = str(row.get("inst_id") or "").replace("-", "/")
        fills = _fills_for_order(
            fill_path=fill_path,
            inst_id=str(row.get("inst_id") or ""),
            cl_ord_id=str(row.get("cl_ord_id") or ""),
            ord_id=str(row.get("ord_id") or ""),
        )
        agg = _aggregate_fills(fills, symbol=symbol)
        avg_fill_px = agg.get("avg_fill_px")
        filled_qty = agg.get("filled_qty")
        fill_notional = None
        if avg_fill_px is not None and filled_qty is not None:
            fill_notional = abs(float(avg_fill_px) * float(filled_qty))
        rows.append(
            _format_row(
                {
                    "schema_version": ORDER_LIFECYCLE_SCHEMA_VERSION,
                    "lifecycle_id": lifecycle.get("lifecycle_id")
                    or lifecycle_id_for(
                        run_id=run_id,
                        symbol=symbol,
                        side=row.get("side"),
                        intent=row.get("intent"),
                        notional_usdt=row.get("notional_usdt"),
                        signal_price=lifecycle.get("signal_price"),
                    ),
                    "run_id": run_id,
                    "ts_utc": _ms_to_iso(row.get("updated_ts")) or _utc_now(),
                    "symbol": symbol,
                    "normalized_symbol": normalize_symbol(symbol),
                    "side": row.get("side"),
                    "intent": row.get("intent"),
                    "order_state": row.get("state"),
                    "decision_ts": lifecycle.get("decision_ts"),
                    "signal_price": lifecycle.get("signal_price"),
                    "arrival_bid": lifecycle.get("arrival_bid"),
                    "arrival_ask": lifecycle.get("arrival_ask"),
                    "arrival_mid": lifecycle.get("arrival_mid"),
                    "spread_bps_at_decision": lifecycle.get("spread_bps_at_decision"),
                    "submit_ts": submit.get("submit_ts") or _ms_to_iso(row.get("created_ts")),
                    "order_type": submit.get("order_type") or row.get("ord_type") or req.get("ordType"),
                    "order_px": submit.get("order_px") or row.get("px") or req.get("px"),
                    "cl_ord_id": row.get("cl_ord_id"),
                    "exchange_order_id": row.get("ord_id"),
                    "first_fill_ts": agg.get("first_fill_ts"),
                    "last_fill_ts": agg.get("last_fill_ts"),
                    "fill_px": agg.get("fill_px"),
                    "avg_fill_px": avg_fill_px or _float_or_none(row.get("avg_px")),
                    "filled_qty": filled_qty or _float_or_none(row.get("acc_fill_sz")),
                    "fee": agg.get("fee") or row.get("fee"),
                    "fee_ccy": agg.get("fee_ccy"),
                    "fee_usdt": agg.get("fee_usdt"),
                    "notional_usdt": fill_notional or row.get("notional_usdt"),
                    "requested_notional_usdt": row.get("notional_usdt"),
                    "trade_ids": agg.get("trade_ids"),
                    "fill_count": agg.get("fill_count") or 0,
                }
            )
        )
    return rows


def _order_rows_for_run(order_path: Path, run_id: str) -> list[dict[str, Any]]:
    with sqlite3.connect(str(order_path)) as con:
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute(
            """
            SELECT cl_ord_id, run_id, inst_id, side, intent, td_mode, ord_type, px, sz,
                   notional_usdt, state, ord_id, req_json, ack_json, last_query_json,
                   last_error_code, last_error_msg, created_ts, updated_ts,
                   acc_fill_sz, avg_px, fee
            FROM orders
            WHERE run_id=?
            ORDER BY created_ts ASC, cl_ord_id ASC
            """,
            (str(run_id),),
        )
        return [dict(row) for row in cur.fetchall()]


def _fills_for_order(
    *,
    fill_path: Path,
    inst_id: str,
    cl_ord_id: str,
    ord_id: str,
) -> list[dict[str, Any]]:
    if not fill_path.exists() or not inst_id or (not cl_ord_id and not ord_id):
        return []
    where = ["inst_id=?"]
    params: list[Any] = [inst_id]
    if cl_ord_id and ord_id:
        where.append("(cl_ord_id=? OR ord_id=?)")
        params.extend([cl_ord_id, ord_id])
    elif cl_ord_id:
        where.append("cl_ord_id=?")
        params.append(cl_ord_id)
    else:
        where.append("ord_id=?")
        params.append(ord_id)
    with sqlite3.connect(str(fill_path)) as con:
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute(
            f"""
            SELECT inst_id, trade_id, ts_ms, ord_id, cl_ord_id, side, fill_px,
                   fill_sz, fill_notional, fee, fee_ccy
            FROM fills
            WHERE {' AND '.join(where)}
            ORDER BY ts_ms ASC, trade_id ASC
            """,
            tuple(params),
        )
        return [dict(row) for row in cur.fetchall()]


def _aggregate_fills(fills: Sequence[Mapping[str, Any]], *, symbol: str) -> dict[str, Any]:
    if not fills:
        return {"fill_count": 0}
    sum_qty = 0.0
    sum_px_qty = 0.0
    fees_by_ccy: dict[str, float] = {}
    trade_ids: list[str] = []
    fill_px_first: float | None = None
    for item in fills:
        px = _float_or_none(item.get("fill_px"))
        qty = _float_or_none(item.get("fill_sz"))
        if px is not None and qty is not None and qty > 0:
            if fill_px_first is None:
                fill_px_first = px
            sum_qty += qty
            sum_px_qty += px * qty
        fee = _float_or_none(item.get("fee"))
        fee_ccy = str(item.get("fee_ccy") or "").upper()
        if fee is not None and fee_ccy:
            fees_by_ccy[fee_ccy] = fees_by_ccy.get(fee_ccy, 0.0) + fee
        trade_id = str(item.get("trade_id") or "").strip()
        if trade_id:
            trade_ids.append(trade_id)
    avg_px = (sum_px_qty / sum_qty) if sum_qty > 0 else None
    fee_ccy = ""
    fee_value: Any = ""
    if len(fees_by_ccy) == 1:
        fee_ccy, fee_value = next(iter(fees_by_ccy.items()))
    elif fees_by_ccy:
        fee_ccy = "mixed"
        fee_value = json.dumps(fees_by_ccy, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    fee_usdt = _fee_cost_usdt(fees_by_ccy, symbol=symbol, fill_px=avg_px)
    return {
        "first_fill_ts": _ms_to_iso(min(int(item.get("ts_ms") or 0) for item in fills)),
        "last_fill_ts": _ms_to_iso(max(int(item.get("ts_ms") or 0) for item in fills)),
        "fill_px": fill_px_first,
        "avg_fill_px": avg_px,
        "filled_qty": sum_qty if sum_qty > 0 else None,
        "fee": fee_value,
        "fee_ccy": fee_ccy,
        "fee_usdt": fee_usdt,
        "trade_ids": ";".join(trade_ids),
        "fill_count": len(fills),
    }


def _fee_cost_usdt(fees_by_ccy: Mapping[str, float], *, symbol: str, fill_px: float | None) -> float | None:
    if not fees_by_ccy:
        return None
    base = normalize_symbol(symbol).split("-", 1)[0]
    total = 0.0
    observed = False
    for ccy, fee in fees_by_ccy.items():
        ccy_u = str(ccy or "").upper()
        if ccy_u in {"USDT", "USD", "USDC"}:
            total += abs(float(fee))
            observed = True
        elif ccy_u == base and fill_px is not None:
            total += abs(float(fee)) * float(fill_px)
            observed = True
    return total if observed else None


def _lookup_top_of_book(book: Mapping[str, Any], symbol: str) -> Mapping[str, Any]:
    candidates = [
        symbol,
        normalize_symbol(symbol),
        normalize_symbol(symbol).replace("-", "/"),
        str(symbol or "").replace("/", "-"),
    ]
    for key in candidates:
        value = book.get(key)
        if isinstance(value, Mapping):
            return value
    return {}


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(ORDER_LIFECYCLE_FIELDS))
        writer.writeheader()
        for row in rows:
            writer.writerow(_format_row(row))


def _upsert_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, dict[str, Any]] = {}
    if path.exists() and path.stat().st_size > 0:
        try:
            with path.open("r", encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    key = str(row.get("lifecycle_id") or "")
                    if key:
                        existing[key] = dict(row)
        except Exception:
            existing = {}
    for row in rows:
        formatted = _format_row(row)
        key = str(formatted.get("lifecycle_id") or "")
        if key:
            existing[key] = formatted
    ordered = sorted(existing.values(), key=lambda item: (str(item.get("run_id") or ""), str(item.get("submit_ts") or item.get("decision_ts") or ""), str(item.get("lifecycle_id") or "")))
    _write_csv(path, ordered)


def _format_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {field: _csv_value(row.get(field)) for field in ORDER_LIFECYCLE_FIELDS}


def _csv_value(value: Any) -> Any:
    if value is None or value == "":
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value:.12g}"
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        loaded = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return dict(loaded) if isinstance(loaded, dict) else {}


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() in {"", "null", "None"}:
            return None
        return float(value)
    except Exception:
        return None


def _ms_to_iso(value: Any) -> str | None:
    try:
        raw = int(float(value))
        if raw <= 0:
            return None
        return datetime.fromtimestamp(raw / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved
