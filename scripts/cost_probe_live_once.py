from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config  # noqa: E402
from src.reporting.cost_probe_plan import CostProbeEngine  # noqa: E402


AUTHORIZATION_SCOPE = "v5_cost_probe_live_once"
REQUIRED_ACKS = {
    "one_time_live_cost_probe",
    "immediate_flat_exit",
    "max_open_seconds_60",
    "kill_switch_on_error",
    "sell_only_on_error",
}


def build_live_probe_preflight(
    cfg: Any,
    *,
    reports_dir: str | Path,
    auth_path: str | Path,
    okx: Any,
    project_root: str | Path = PROJECT_ROOT,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    engine = CostProbeEngine(
        cfg,
        reports_dir=reports_dir,
        generated_at=generated_at,
        project_root=project_root,
    )
    payload = engine.build()
    p3 = dict(payload["p3_preflight"])
    auth = _read_authorization(auth_path)
    auth_blockers = _authorization_blockers(auth, p3, generated_at=engine.generated_at)
    symbol = str(p3.get("manual_probe_symbol") or auth.get("symbol") or "").strip()
    instrument = _instrument_preflight(okx, symbol, max_notional_usdt=float(p3.get("max_notional_usdt") or 0.0))
    blockers = [
        *list(p3.get("blockers") or []),
        *auth_blockers,
        *list(instrument.get("blockers") or []),
    ]
    blockers = sorted(set(blockers))
    return {
        "generated_at": engine.generated_at.isoformat().replace("+00:00", "Z"),
        "state": "READY_FOR_LIVE_EXECUTION" if not blockers else "NOT_READY",
        "approved_live_order_execution": bool(auth.get("approved_live_order_execution")) and not blockers,
        "manual_probe_symbol": symbol,
        "live_order_effect": "none_preflight_only_no_order",
        "blockers": blockers,
        "p3_preflight": p3,
        "authorization": _redacted_authorization(auth),
        "instrument_preflight": instrument,
    }


def run_live_probe_once(
    cfg: Any,
    *,
    reports_dir: str | Path,
    auth_path: str | Path,
    okx: Any,
    project_root: str | Path = PROJECT_ROOT,
    execute_live_order: bool = False,
    operator_confirmed: bool = False,
) -> dict[str, Any]:
    preflight = build_live_probe_preflight(
        cfg,
        reports_dir=reports_dir,
        auth_path=auth_path,
        okx=okx,
        project_root=project_root,
    )
    if preflight["state"] != "READY_FOR_LIVE_EXECUTION":
        return preflight
    if not (execute_live_order and operator_confirmed):
        return {
            **preflight,
            "state": "READY_FOR_OPERATOR_CONFIRMATION",
            "approved_live_order_execution": False,
            "live_order_effect": "none_operator_confirmation_missing_no_order",
        }
    return _execute_live_probe(cfg, preflight=preflight, okx=okx, reports_dir=Path(reports_dir))


def _execute_live_probe(
    cfg: Any,
    *,
    preflight: dict[str, Any],
    okx: Any,
    reports_dir: Path,
) -> dict[str, Any]:
    symbol = str(preflight["manual_probe_symbol"])
    inst_id = symbol.replace("/", "-").upper()
    instrument = preflight["instrument_preflight"]
    plan = instrument["order_plan"]
    max_open_seconds = int(preflight["p3_preflight"].get("max_open_seconds") or 60)
    clid_prefix = f"cp{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
    entry_payload = {
        "instId": inst_id,
        "tdMode": "cash",
        "side": "buy",
        "ordType": "ioc",
        "px": plan["entry_px"],
        "sz": plan["base_qty"],
        "clOrdId": f"{clid_prefix}E",
    }
    reports_dir.mkdir(parents=True, exist_ok=True)
    order_events_path = reports_dir / "cost_probe_order_events.jsonl"
    roundtrip_events_path = reports_dir / "cost_probe_roundtrip_events.jsonl"
    try:
        entry = okx.place_order(entry_payload, exp_time_ms=1500)
        entry_data = _first_okx_item(entry)
        entry_ord_id = str(entry_data.get("ordId") or "")
        _append_jsonl(
            order_events_path,
            _event(
                symbol=symbol,
                leg="entry",
                status="submitted",
                cl_ord_id=entry_payload["clOrdId"],
                ord_id=entry_ord_id,
                payload=entry_payload,
            ),
        )
        entry_state = _poll_order(okx, inst_id=inst_id, cl_ord_id=entry_payload["clOrdId"], max_seconds=max_open_seconds)
        filled_qty = _decimal_text(entry_state.get("accFillSz") or entry_state.get("fillSz") or "0")
        _append_jsonl(
            order_events_path,
            _event(
                symbol=symbol,
                leg="entry",
                status=str(entry_state.get("state") or "unknown"),
                cl_ord_id=entry_payload["clOrdId"],
                ord_id=str(entry_state.get("ordId") or entry_ord_id),
                payload=entry_state,
            ),
        )
        if Decimal(filled_qty) <= Decimal("0"):
            return _finish_roundtrip(
                roundtrip_events_path,
                symbol=symbol,
                status="no_entry_fill",
                entry_order_id=entry_ord_id,
                exit_order_id="",
            )
        exit_payload = {
            "instId": inst_id,
            "tdMode": "cash",
            "side": "sell",
            "ordType": "ioc",
            "px": plan["exit_px"],
            "sz": filled_qty,
            "clOrdId": f"{clid_prefix}X",
        }
        exit_resp = okx.place_order(exit_payload, exp_time_ms=1500)
        exit_data = _first_okx_item(exit_resp)
        exit_ord_id = str(exit_data.get("ordId") or "")
        _append_jsonl(
            order_events_path,
            _event(
                symbol=symbol,
                leg="exit",
                status="submitted",
                cl_ord_id=exit_payload["clOrdId"],
                ord_id=exit_ord_id,
                payload=exit_payload,
            ),
        )
        exit_state = _poll_order(okx, inst_id=inst_id, cl_ord_id=exit_payload["clOrdId"], max_seconds=max_open_seconds)
        _append_jsonl(
            order_events_path,
            _event(
                symbol=symbol,
                leg="exit",
                status=str(exit_state.get("state") or "unknown"),
                cl_ord_id=exit_payload["clOrdId"],
                ord_id=str(exit_state.get("ordId") or exit_ord_id),
                payload=exit_state,
            ),
        )
        return _finish_roundtrip(
            roundtrip_events_path,
            symbol=symbol,
            status="closed",
            entry_order_id=entry_ord_id,
            exit_order_id=exit_ord_id,
        )
    except Exception as exc:
        _write_kill_switch(cfg, reason=f"cost_probe_live_once_error:{exc}")
        return {
            "state": "ABORTED_KILL_SWITCH_ENABLED",
            "live_order_effect": "kill_switch_enabled_after_cost_probe_error",
            "error": str(exc),
        }


def _instrument_preflight(okx: Any, symbol: str, *, max_notional_usdt: float) -> dict[str, Any]:
    inst_id = symbol.replace("/", "-").upper()
    blockers: list[str] = []
    spec = _public_okx_item(okx, "/api/v5/public/instruments", {"instType": "SPOT", "instId": inst_id})
    ticker = _public_okx_item(okx, "/api/v5/market/ticker", {"instId": inst_id})
    min_sz = Decimal(str(spec.get("minSz") or "0"))
    lot_sz = Decimal(str(spec.get("lotSz") or "0"))
    tick_sz = Decimal(str(spec.get("tickSz") or "0"))
    ask = Decimal(str(ticker.get("askPx") or ticker.get("last") or "0"))
    bid = Decimal(str(ticker.get("bidPx") or ticker.get("last") or "0"))
    if min_sz <= 0:
        blockers.append("exchange_min_sz_missing")
    if lot_sz <= 0:
        blockers.append("exchange_lot_sz_missing")
    if tick_sz <= 0:
        blockers.append("exchange_tick_sz_missing")
    if ask <= 0 or bid <= 0:
        blockers.append("ticker_bid_ask_missing")
    order_plan: dict[str, str] = {}
    if not blockers:
        max_notional = Decimal(str(max_notional_usdt))
        entry_px = _round_to_step(ask * Decimal("1.001"), tick_sz, ROUND_UP)
        exit_px = _round_to_step(bid * Decimal("0.999"), tick_sz, ROUND_DOWN)
        qty = _round_to_step(max_notional / entry_px, lot_sz, ROUND_DOWN)
        if qty < min_sz:
            blockers.append("max_notional_below_exchange_min_size")
        if qty * entry_px > max_notional:
            blockers.append("planned_order_exceeds_max_notional")
        order_plan = {
            "base_qty": _decimal_text(qty),
            "entry_px": _decimal_text(entry_px),
            "exit_px": _decimal_text(exit_px),
            "estimated_entry_notional_usdt": _decimal_text(qty * entry_px),
        }
    return {
        "inst_id": inst_id,
        "blockers": sorted(set(blockers)),
        "min_sz": _decimal_text(min_sz),
        "lot_sz": _decimal_text(lot_sz),
        "tick_sz": _decimal_text(tick_sz),
        "ask_px": _decimal_text(ask),
        "bid_px": _decimal_text(bid),
        "order_plan": order_plan,
    }


def _authorization_blockers(
    auth: dict[str, Any],
    p3: dict[str, Any],
    *,
    generated_at: datetime,
) -> list[str]:
    blockers: list[str] = []
    if not auth:
        return ["manual_authorization_file_missing_or_invalid"]
    if str(auth.get("scope") or "") != AUTHORIZATION_SCOPE:
        blockers.append("manual_authorization_scope_invalid")
    if auth.get("approved_live_order_execution") is not True:
        blockers.append("manual_authorization_not_approved")
    if str(auth.get("symbol") or "") != str(p3.get("manual_probe_symbol") or ""):
        blockers.append("manual_authorization_symbol_mismatch")
    if float(auth.get("max_notional_usdt") or 0.0) > float(p3.get("max_notional_usdt") or 0.0):
        blockers.append("manual_authorization_notional_exceeds_preflight")
    expires = _parse_dt(auth.get("expires_at"))
    if expires is None or expires <= generated_at:
        blockers.append("manual_authorization_expired_or_invalid")
    if not str(auth.get("authorization_id") or "").strip():
        blockers.append("manual_authorization_id_missing")
    if not str(auth.get("signed_by") or "").strip():
        blockers.append("manual_authorization_signed_by_missing")
    acks = {str(item) for item in (auth.get("acknowledged_risks") or [])}
    missing_acks = sorted(REQUIRED_ACKS - acks)
    if missing_acks:
        blockers.append("manual_authorization_acknowledgements_missing")
    return blockers


def _read_authorization(path: str | Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _redacted_authorization(auth: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in auth.items()
        if key not in {"signature", "secret", "token", "api_key"}
    }


def _parse_dt(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _public_okx_item(okx: Any, path: str, params: dict[str, Any]) -> dict[str, Any]:
    response = okx.request("GET", path, params=params)
    data = getattr(response, "data", response)
    rows = data.get("data") if isinstance(data, dict) else None
    if isinstance(rows, list) and rows:
        row = rows[0]
        return row if isinstance(row, dict) else {}
    return {}


def _first_okx_item(response: Any) -> dict[str, Any]:
    data = getattr(response, "data", response)
    rows = data.get("data") if isinstance(data, dict) else None
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        return rows[0]
    return {}


def _poll_order(okx: Any, *, inst_id: str, cl_ord_id: str, max_seconds: int) -> dict[str, Any]:
    # The production client performs one signed read per poll; tests inject a fake.
    for _ in range(max(int(max_seconds), 1)):
        response = okx.get_order(inst_id=inst_id, cl_ord_id=cl_ord_id)
        row = _first_okx_item(response)
        state = str(row.get("state") or "").lower()
        if state in {"filled", "canceled", "partially_filled", "partially-filled"}:
            return row
        time.sleep(1.0)
    return {"clOrdId": cl_ord_id, "state": "timeout"}


def _finish_roundtrip(
    path: Path,
    *,
    symbol: str,
    status: str,
    entry_order_id: str,
    exit_order_id: str,
) -> dict[str, Any]:
    row = {
        "event_type": f"roundtrip:{status}",
        "event_ts": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "symbol": symbol,
        "roundtrip_status": status,
        "roundtrip_id": f"{entry_order_id}:{exit_order_id}",
        "entry_order_id": entry_order_id,
        "exit_order_id": exit_order_id,
        "no_order_submitted": False,
        "live_order_effect": "live_cost_probe_roundtrip",
    }
    _append_jsonl(path, row)
    return {"state": "COMPLETED" if status == "closed" else "INCOMPLETE", **row}


def _event(
    *,
    symbol: str,
    leg: str,
    status: str,
    cl_ord_id: str,
    ord_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    ts = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    return {
        "event_type": f"order:{leg}:{status}",
        "event_ts": ts,
        "event_id": f"{cl_ord_id}|order:{leg}:{status}|{ts}",
        "order_key": cl_ord_id or ord_id,
        "symbol": symbol,
        "leg": leg,
        "order_status": status,
        "client_order_id": cl_ord_id,
        "exchange_order_id": ord_id,
        "no_order_submitted": False,
        "live_order_effect": "live_cost_probe_order",
        "raw": payload,
    }


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _round_to_step(value: Decimal, step: Decimal, rounding: str) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=rounding) * step


def _decimal_text(value: Any) -> str:
    dec = Decimal(str(value))
    return format(dec.normalize(), "f")


def _write_kill_switch(cfg: Any, *, reason: str) -> None:
    raw_path = getattr(getattr(cfg, "execution", cfg), "kill_switch_path", "reports/kill_switch.json")
    path = Path(raw_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "enabled": True,
                "reason": reason,
                "ts": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one guarded V5 live cost probe.")
    parser.add_argument("--config", default="configs/live_prod.yaml")
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--auth-file", required=True)
    parser.add_argument("--execute-live-order", action="store_true")
    parser.add_argument("--i-understand-this-submits-live-okx-orders", action="store_true")
    args = parser.parse_args(argv)

    from src.execution.okx_private_client import OKXPrivateClient

    cfg = load_config(args.config)
    client = OKXPrivateClient(exchange=cfg.exchange)
    try:
        result = run_live_probe_once(
            cfg,
            reports_dir=args.reports_dir,
            auth_path=args.auth_file,
            okx=client,
            execute_live_order=args.execute_live_order,
            operator_confirmed=args.i_understand_this_submits_live_okx_orders,
        )
    finally:
        client.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("state") in {"READY_FOR_OPERATOR_CONFIRMATION", "COMPLETED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
