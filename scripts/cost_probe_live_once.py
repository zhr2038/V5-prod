from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config  # noqa: E402
from src.execution.account_store import AccountStore  # noqa: E402
from src.execution.fill_store import derive_position_store_path  # noqa: E402
from src.execution.position_store import PositionStore  # noqa: E402
from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds  # noqa: E402
from src.reporting.cost_probe_plan import CostProbeEngine  # noqa: E402


AUTHORIZATION_SCOPE = "v5_cost_probe_live_once"
AUTHORIZATION_MAX_TTL_SEC = 300
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
    symbol = str(
        p3.get("manual_probe_symbol") or auth.get("symbol") or _single_configured_symbol(cfg) or ""
    ).strip()
    instrument = _instrument_preflight(okx, symbol, max_notional_usdt=float(p3.get("max_notional_usdt") or 0.0))
    p3 = _apply_exchange_min_notional_preflight(
        payload=payload,
        p3=p3,
        instrument=instrument,
        symbol=symbol,
    )
    auth_context = _authorization_context(
        cfg,
        p3=p3,
        project_root=project_root,
    )
    auth_blockers = _authorization_blockers(
        auth,
        p3,
        generated_at=engine.generated_at,
        required_context=auth_context,
    )
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
        "required_authorization": auth_context,
        "instrument_preflight": instrument,
        "runtime_paths": _runtime_paths(payload),
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
    return _execute_live_probe(
        cfg,
        preflight=preflight,
        okx=okx,
        reports_dir=Path(reports_dir),
        auth_path=auth_path,
    )


def _execute_live_probe(
    cfg: Any,
    *,
    preflight: dict[str, Any],
    okx: Any,
    reports_dir: Path,
    auth_path: str | Path,
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
        consumed_auth = _consume_authorization_file(auth_path, preflight=preflight)
    except Exception as exc:
        blockers = sorted(set([*list(preflight.get("blockers") or []), str(exc)]))
        return {
            **preflight,
            "state": "NOT_READY",
            "approved_live_order_execution": False,
            "live_order_effect": "none_authorization_consume_failed_no_order",
            "blockers": blockers,
            "authorization_consumed": False,
        }
    entry_state: dict[str, Any] = {}
    exit_state: dict[str, Any] = {}
    entry_ord_id = ""
    exit_ord_id = ""
    entry_cl_ord_id = entry_payload["clOrdId"]
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
                authorization=consumed_auth,
                instrument=instrument,
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
                authorization=consumed_auth,
                instrument=instrument,
            ),
        )
        if Decimal(filled_qty) <= Decimal("0"):
            return _finish_roundtrip(
                roundtrip_events_path,
                symbol=symbol,
                status="no_entry_fill",
                entry_order_id=entry_ord_id,
                exit_order_id="",
                entry_state=entry_state,
                exit_state={},
                instrument=instrument,
                flat_verification=_verify_roundtrip_flat(
                    cfg,
                    preflight=preflight,
                    okx=okx,
                    inst_id=inst_id,
                    symbol=symbol,
                    instrument=instrument,
                    entry_state=entry_state,
                    exit_state={},
                ),
                completed=False,
                authorization=consumed_auth,
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
                authorization=consumed_auth,
                instrument=instrument,
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
                authorization=consumed_auth,
                instrument=instrument,
            ),
        )
        flat = _verify_roundtrip_flat(
            cfg,
            preflight=preflight,
            okx=okx,
            inst_id=inst_id,
            symbol=symbol,
            instrument=instrument,
            entry_state=entry_state,
            exit_state=exit_state,
        )
        if not flat["flat_verified"]:
            emergency = _emergency_flatten_cost_probe(
                okx,
                symbol=symbol,
                inst_id=inst_id,
                instrument=instrument,
                order_events_path=order_events_path,
                fallback_qty=_filled_qty(entry_state) - _filled_qty(exit_state),
                reason="incomplete_exit",
            )
            flat = _verify_roundtrip_flat(
                cfg,
                preflight=preflight,
                okx=okx,
                inst_id=inst_id,
                symbol=symbol,
                instrument=instrument,
                entry_state=entry_state,
                exit_state=exit_state,
                emergency=emergency,
            )
            _write_kill_switch(cfg, reason="cost_probe_incomplete_exit")
            return _finish_roundtrip(
                roundtrip_events_path,
                symbol=symbol,
                status="incomplete_exit",
                entry_order_id=entry_ord_id,
                exit_order_id=exit_ord_id,
                entry_state=entry_state,
                exit_state=exit_state,
                instrument=instrument,
                flat_verification=flat,
                emergency=emergency,
                completed=False,
                authorization=consumed_auth,
            )
        return _finish_roundtrip(
            roundtrip_events_path,
            symbol=symbol,
            status="closed",
            entry_order_id=entry_ord_id,
            exit_order_id=exit_ord_id,
            entry_state=entry_state,
            exit_state=exit_state,
            instrument=instrument,
            flat_verification=flat,
            completed=True,
            authorization=consumed_auth,
        )
    except Exception as exc:
        if entry_cl_ord_id and not entry_state:
            try:
                entry_state = _poll_order(okx, inst_id=inst_id, cl_ord_id=entry_cl_ord_id, max_seconds=1)
            except Exception:
                entry_state = {}
        emergency: dict[str, Any] = {}
        if _filled_qty(entry_state) > Decimal("0"):
            emergency = _emergency_flatten_cost_probe(
                okx,
                symbol=symbol,
                inst_id=inst_id,
                instrument=instrument,
                order_events_path=order_events_path,
                fallback_qty=_filled_qty(entry_state),
                reason="exception_after_entry_fill",
            )
            flat = _verify_roundtrip_flat(
                cfg,
                preflight=preflight,
                okx=okx,
                inst_id=inst_id,
                symbol=symbol,
                instrument=instrument,
                entry_state=entry_state,
                exit_state=exit_state,
                emergency=emergency,
            )
            _finish_roundtrip(
                roundtrip_events_path,
                symbol=symbol,
                status="exception_after_entry_fill",
                entry_order_id=entry_ord_id,
                exit_order_id=exit_ord_id,
                entry_state=entry_state,
                exit_state=exit_state,
                instrument=instrument,
                flat_verification=flat,
                emergency=emergency,
                completed=False,
                authorization=consumed_auth,
            )
        _write_kill_switch(cfg, reason=f"cost_probe_live_once_error:{exc}")
        return {
            "state": "ABORTED_KILL_SWITCH_ENABLED",
            "live_order_effect": "kill_switch_enabled_after_cost_probe_error",
            "error": str(exc),
            "emergency_flatten": emergency,
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
        "instrument_preflight_passed": not blockers,
        "normalized_qty": order_plan.get("base_qty", ""),
    }


def _apply_exchange_min_notional_preflight(
    *,
    payload: dict[str, Any],
    p3: dict[str, Any],
    instrument: dict[str, Any],
    symbol: str,
) -> dict[str, Any]:
    if instrument.get("blockers") or not symbol:
        return p3
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    rows = payload.get("plan_rows") if isinstance(payload.get("plan_rows"), list) else []
    configured_symbols = [str(row.get("symbol") or "") for row in rows]
    symbol_rows = [row for row in rows if str(row.get("symbol") or "") == symbol]
    if len(configured_symbols) != 1 or not symbol_rows:
        return p3
    blocked_reasons = {
        reason
        for row in symbol_rows
        for reason in str(row.get("blocked_reasons") or "").split(";")
        if reason
    }
    if blocked_reasons != {"exchange_min_notional_check_pending"}:
        return p3
    if summary.get("runtime_blockers") or summary.get("symbol_runtime_blockers"):
        return p3
    blockers = [
        blocker
        for blocker in list(p3.get("blockers") or [])
        if blocker
        not in {
            "dry_run_plan_not_ready",
            "single_symbol_plan_required",
            "exchange_min_notional_check_pending",
        }
    ]
    out = dict(p3)
    out["blockers"] = sorted(set(blockers))
    out["state"] = "READY_FOR_MANUAL_AUTHORIZATION" if not blockers else "NOT_READY"
    out["ready_to_request_manual_live_probe"] = not blockers
    out["manual_probe_symbol"] = symbol
    out["planned_symbols"] = [symbol]
    out["instrument_preflight_passed"] = True
    out["exchange_min_notional_verified"] = True
    out["exchange_min_notional_source"] = "okx_public_instruments_and_ticker"
    out["min_sz"] = instrument.get("min_sz", "")
    out["lot_sz"] = instrument.get("lot_sz", "")
    out["tick_sz"] = instrument.get("tick_sz", "")
    out["normalized_qty"] = instrument.get("normalized_qty", "")
    out["dry_run_plan_state"] = "DRY_RUN_PLAN_READY_AFTER_EXCHANGE_PREFLIGHT"
    return out


def _authorization_blockers(
    auth: dict[str, Any],
    p3: dict[str, Any],
    *,
    generated_at: datetime,
    required_context: dict[str, Any],
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
    auth_notional = _decimal_or_none(auth.get("max_notional_usdt"))
    if auth_notional is None:
        blockers.append("manual_authorization_notional_missing")
    elif auth_notional > _decimal(required_context.get("max_notional_usdt")):
        blockers.append("manual_authorization_notional_exceeds_preflight")
    issued = _parse_dt(auth.get("issued_at"))
    expires = _parse_dt(auth.get("expires_at"))
    if issued is None:
        blockers.append("manual_authorization_issued_at_missing_or_invalid")
    if expires is None or expires <= generated_at:
        blockers.append("manual_authorization_expired_or_invalid")
    elif issued is not None and expires > issued + timedelta(seconds=AUTHORIZATION_MAX_TTL_SEC):
        blockers.append("manual_authorization_ttl_exceeds_5_minutes")
    if not str(auth.get("authorization_id") or "").strip():
        blockers.append("manual_authorization_id_missing")
    if not str(auth.get("nonce") or "").strip():
        blockers.append("manual_authorization_nonce_missing")
    if not str(auth.get("signed_by") or "").strip():
        blockers.append("manual_authorization_signed_by_missing")
    if not str(auth.get("signature") or "").strip():
        blockers.append("manual_authorization_signature_missing")
    if str(auth.get("code_sha") or "") != str(required_context.get("code_sha") or ""):
        blockers.append("manual_authorization_code_sha_mismatch")
    if str(auth.get("config_sha256") or "") != str(required_context.get("config_sha256") or ""):
        blockers.append("manual_authorization_config_sha_mismatch")
    if str(auth.get("consumed_at") or "").strip():
        blockers.append("manual_authorization_already_consumed")
    acks = {str(item) for item in (auth.get("acknowledged_risks") or [])}
    missing_acks = sorted(REQUIRED_ACKS - acks)
    if missing_acks:
        blockers.append("manual_authorization_acknowledgements_missing")
    return blockers


def _authorization_context(
    cfg: Any,
    *,
    p3: dict[str, Any],
    project_root: str | Path,
) -> dict[str, Any]:
    return {
        "scope": AUTHORIZATION_SCOPE,
        "symbol": str(p3.get("manual_probe_symbol") or ""),
        "max_notional_usdt": _decimal_text(p3.get("max_notional_usdt") or 0),
        "code_sha": _current_code_sha(Path(project_root)),
        "config_sha256": _cost_probe_config_sha(cfg),
        "authorization_max_ttl_sec": AUTHORIZATION_MAX_TTL_SEC,
        "required_pending_file_suffix": ".pending.json",
    }


def _consume_authorization_file(path: str | Path, *, preflight: dict[str, Any]) -> dict[str, Any]:
    source = Path(path)
    if not source.name.endswith(".pending.json"):
        raise RuntimeError("manual_authorization_pending_file_required")
    lock_path = Path(str(source) + ".lock")
    fd: int | None = None
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
        if not source.is_file():
            raise RuntimeError("manual_authorization_pending_file_missing")
        fresh = _read_authorization(source)
        if not fresh:
            raise RuntimeError("manual_authorization_file_missing_or_invalid")
        expected = preflight.get("authorization") or {}
        if str(fresh.get("authorization_id") or "") != str(expected.get("authorization_id") or ""):
            raise RuntimeError("manual_authorization_changed_after_preflight")
        consumed = source.with_name(source.name[: -len(".pending.json")] + ".consumed.json")
        if consumed.exists():
            raise RuntimeError("manual_authorization_consumed_file_exists")
        source.replace(consumed)
        fresh["consumed_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        consumed.write_text(json.dumps(fresh, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return fresh
    finally:
        if fd is not None:
            os.close(fd)
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


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


def _runtime_paths(payload: dict[str, Any]) -> dict[str, str]:
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    return {
        "order_store_path": str(summary.get("order_store_path") or ""),
        "position_store_path": str(summary.get("position_store_path") or ""),
        "fill_store_path": str(summary.get("fill_store_path") or ""),
        "reconcile_status_path": str(summary.get("reconcile_status_path") or ""),
    }


def _single_configured_symbol(cfg: Any) -> str:
    execution = getattr(cfg, "execution", cfg)
    symbols = []
    for raw in getattr(execution, "cost_probe_symbols", None) or []:
        symbol = str(raw or "").strip().upper().replace("-", "/")
        if symbol and "/" not in symbol and symbol.endswith("USDT"):
            symbol = f"{symbol[:-4]}/USDT"
        if symbol:
            symbols.append(symbol)
    return symbols[0] if len(symbols) == 1 else ""


def _current_code_sha(project_root: Path) -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(project_root),
            text=True,
            capture_output=True,
            timeout=5,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            sha = proc.stdout.strip()
            dirty = subprocess.run(
                ["git", "diff", "--quiet"],
                cwd=str(project_root),
                text=True,
                capture_output=True,
                timeout=5,
            )
            return f"{sha}+dirty" if dirty.returncode else sha
    except Exception:
        pass
    data = Path(__file__).read_bytes()
    return "file-sha256:" + hashlib.sha256(data).hexdigest()


def _cost_probe_config_sha(cfg: Any) -> str:
    execution = getattr(cfg, "execution", cfg)
    keys = (
        "cost_bootstrap_enabled",
        "cost_probe_enabled",
        "cost_probe_dry_run",
        "cost_probe_live_enabled",
        "cost_probe_symbols",
        "cost_probe_max_notional_usdt",
        "cost_probe_use_exchange_min_notional",
        "cost_probe_max_orders_per_day",
        "cost_probe_max_roundtrips_per_symbol_per_day",
        "cost_probe_cooldown_minutes",
        "cost_probe_max_daily_loss_usdt",
        "cost_probe_order_style",
        "cost_probe_exit_policy",
        "cost_probe_max_open_seconds",
        "cost_probe_require_reconcile_clean",
        "cost_probe_require_no_existing_position",
        "cost_probe_respect_kill_switch",
        "cost_probe_disable_if_order_store_dirty",
        "cost_probe_disable_if_position_state_dirty",
        "order_store_path",
        "kill_switch_path",
        "reconcile_status_path",
    )
    payload = {key: getattr(execution, key, None) for key in keys}
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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
    entry_state: dict[str, Any],
    exit_state: dict[str, Any],
    instrument: dict[str, Any],
    flat_verification: dict[str, Any],
    completed: bool,
    authorization: dict[str, Any],
    emergency: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cost = _roundtrip_cost_fields(entry_state, exit_state)
    row = {
        "event_type": f"roundtrip:{status}",
        "event_ts": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "symbol": symbol,
        "roundtrip_status": status,
        "completed": bool(completed),
        "roundtrip_id": f"{entry_order_id}:{exit_order_id}",
        "entry_order_id": entry_order_id,
        "exit_order_id": exit_order_id,
        "no_order_submitted": False,
        "live_order_effect": "live_cost_probe_roundtrip" if completed else "live_cost_probe_roundtrip_incomplete",
        "authorization_id": str(authorization.get("authorization_id") or ""),
        "nonce": str(authorization.get("nonce") or ""),
        "entry_state": entry_state,
        "exit_state": exit_state,
        "entry_filled_qty": cost["entry_filled_qty"],
        "exit_filled_qty": cost["exit_filled_qty"],
        "entry_avg_px": cost["entry_avg_px"],
        "exit_avg_px": cost["exit_avg_px"],
        "entry_fee": cost["entry_fee"],
        "entry_fee_ccy": cost["entry_fee_ccy"],
        "exit_fee": cost["exit_fee"],
        "exit_fee_ccy": cost["exit_fee_ccy"],
        "gross_pnl_usdt": cost["gross_pnl_usdt"],
        "net_pnl_usdt": cost["net_pnl_usdt"],
        "roundtrip_cost_bps": cost["roundtrip_cost_bps"],
        "arrival_bid_px": instrument.get("bid_px", ""),
        "arrival_ask_px": instrument.get("ask_px", ""),
        "arrival_mid_px": _arrival_mid_px(instrument),
        "flat_verification": flat_verification,
        "local_flat_verified": flat_verification.get("local_flat_verified", False),
        "exchange_flat_verified": flat_verification.get("exchange_flat_verified", False),
        "reconcile_ok": flat_verification.get("reconcile_ok", False),
        "open_order_count": flat_verification.get("open_order_count", -1),
        "emergency_flatten": emergency or {},
    }
    _append_jsonl(path, row)
    return {"state": "COMPLETED" if completed else "INCOMPLETE", **row}


def _event(
    *,
    symbol: str,
    leg: str,
    status: str,
    cl_ord_id: str,
    ord_id: str,
    payload: dict[str, Any],
    authorization: dict[str, Any] | None = None,
    instrument: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ts = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    cost = _order_cost_fields(payload)
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
        "authorization_id": str((authorization or {}).get("authorization_id") or ""),
        "filled_qty": cost["filled_qty"],
        "avg_px": cost["avg_px"],
        "fee": cost["fee"],
        "fee_ccy": cost["fee_ccy"],
        "arrival_bid_px": (instrument or {}).get("bid_px", ""),
        "arrival_ask_px": (instrument or {}).get("ask_px", ""),
        "arrival_mid_px": _arrival_mid_px(instrument or {}),
        "raw": payload,
    }


def _verify_roundtrip_flat(
    cfg: Any,
    *,
    preflight: dict[str, Any],
    okx: Any,
    inst_id: str,
    symbol: str,
    instrument: dict[str, Any],
    entry_state: dict[str, Any],
    exit_state: dict[str, Any],
    emergency: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lot_sz = _decimal(instrument.get("lot_sz"))
    tolerance = max(lot_sz, Decimal("0.00000001"))
    entry_qty = _filled_qty(entry_state)
    exit_qty = _filled_qty(exit_state)
    emergency_qty = _decimal((emergency or {}).get("filled_qty"))
    total_exit_qty = exit_qty + max(emergency_qty, Decimal("0"))
    exchange_balance = _query_base_balance(okx, symbol)
    open_order_count = _open_order_count(okx, inst_id=inst_id)
    local_qty = _local_position_qty(cfg, preflight=preflight, symbol=symbol)
    reconcile_refresh = _refresh_reconcile_status(cfg, preflight=preflight, okx=okx, symbol=symbol)
    reconcile = _read_reconcile_status(preflight)
    exit_fully_filled = total_exit_qty + tolerance >= entry_qty
    exchange_flat = exchange_balance is not None and exchange_balance <= tolerance
    local_flat = local_qty is not None and local_qty <= tolerance
    open_orders_clear = open_order_count == 0
    reconcile_ok = bool(reconcile.get("ok"))
    return {
        "flat_verified": bool(exit_fully_filled and exchange_flat and local_flat and open_orders_clear and reconcile_ok),
        "exit_fully_filled": bool(exit_fully_filled),
        "entry_filled_qty": _decimal_text(entry_qty),
        "exit_filled_qty": _decimal_text(exit_qty),
        "emergency_exit_filled_qty": _decimal_text(emergency_qty),
        "lot_sz_tolerance": _decimal_text(tolerance),
        "open_order_count": open_order_count,
        "open_orders_clear": open_orders_clear,
        "exchange_base_balance": _decimal_text(exchange_balance) if exchange_balance is not None else "unverified",
        "exchange_flat_verified": bool(exchange_flat),
        "local_position_qty": _decimal_text(local_qty) if local_qty is not None else "unverified",
        "local_flat_verified": bool(local_flat),
        "reconcile_ok": reconcile_ok,
        "reconcile_refreshed": reconcile_refresh.get("refreshed", False),
        "reconcile_refresh": reconcile_refresh,
        "reconcile_status": reconcile,
    }


def _emergency_flatten_cost_probe(
    okx: Any,
    *,
    symbol: str,
    inst_id: str,
    instrument: dict[str, Any],
    order_events_path: Path,
    fallback_qty: Decimal,
    reason: str,
) -> dict[str, Any]:
    lot_sz = _decimal(instrument.get("lot_sz"))
    balance = _query_base_balance(okx, symbol)
    target_qty = balance if balance is not None and balance > 0 else max(fallback_qty, Decimal("0"))
    sell_qty = _round_to_step(target_qty, lot_sz, ROUND_DOWN) if lot_sz > 0 else target_qty
    result: dict[str, Any] = {
        "attempted": False,
        "reason": reason,
        "target_qty": _decimal_text(target_qty),
        "sell_qty": _decimal_text(sell_qty),
        "balance_before": _decimal_text(balance) if balance is not None else "unverified",
    }
    if sell_qty <= 0:
        result["status"] = "no_sellable_qty"
        return result
    plan = instrument.get("order_plan") if isinstance(instrument.get("order_plan"), dict) else {}
    payload = {
        "instId": inst_id,
        "tdMode": "cash",
        "side": "sell",
        "ordType": "ioc",
        "px": plan.get("exit_px") or instrument.get("bid_px") or "0",
        "sz": _decimal_text(sell_qty),
        "clOrdId": f"cp{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}F",
    }
    response = okx.place_order(payload, exp_time_ms=1500)
    data = _first_okx_item(response)
    ord_id = str(data.get("ordId") or "")
    result.update({"attempted": True, "order_id": ord_id, "client_order_id": payload["clOrdId"]})
    _append_jsonl(
        order_events_path,
        _event(
            symbol=symbol,
            leg="emergency_exit",
            status="submitted",
            cl_ord_id=payload["clOrdId"],
            ord_id=ord_id,
            payload=payload,
            instrument=instrument,
        ),
    )
    state = _poll_order(okx, inst_id=inst_id, cl_ord_id=payload["clOrdId"], max_seconds=5)
    result.update(
        {
            "status": str(state.get("state") or "unknown"),
            "filled_qty": _decimal_text(_filled_qty(state)),
            "balance_after": _decimal_text(_query_base_balance(okx, symbol) or Decimal("0")),
        }
    )
    _append_jsonl(
        order_events_path,
        _event(
            symbol=symbol,
            leg="emergency_exit",
            status=str(state.get("state") or "unknown"),
            cl_ord_id=payload["clOrdId"],
            ord_id=str(state.get("ordId") or ord_id),
            payload=state,
            instrument=instrument,
        ),
    )
    return result


def _roundtrip_cost_fields(entry_state: dict[str, Any], exit_state: dict[str, Any]) -> dict[str, str]:
    entry_qty = _filled_qty(entry_state)
    exit_qty = _filled_qty(exit_state)
    entry_px = _avg_px(entry_state)
    exit_px = _avg_px(exit_state)
    entry_fee = _fee(entry_state)
    exit_fee = _fee(exit_state)
    entry_notional = entry_qty * entry_px
    exit_notional = exit_qty * exit_px
    gross_pnl = exit_notional - entry_notional
    net_pnl = gross_pnl + entry_fee + exit_fee
    cost_bps = Decimal("0")
    if entry_notional > 0:
        cost_bps = (Decimal("0") - net_pnl) / entry_notional * Decimal("10000")
    return {
        "entry_filled_qty": _decimal_text(entry_qty),
        "exit_filled_qty": _decimal_text(exit_qty),
        "entry_avg_px": _decimal_text(entry_px),
        "exit_avg_px": _decimal_text(exit_px),
        "entry_fee": _decimal_text(entry_fee),
        "entry_fee_ccy": str(entry_state.get("feeCcy") or ""),
        "exit_fee": _decimal_text(exit_fee),
        "exit_fee_ccy": str(exit_state.get("feeCcy") or ""),
        "gross_pnl_usdt": _decimal_text(gross_pnl),
        "net_pnl_usdt": _decimal_text(net_pnl),
        "roundtrip_cost_bps": _decimal_text(cost_bps),
    }


def _order_cost_fields(row: dict[str, Any]) -> dict[str, str]:
    return {
        "filled_qty": _decimal_text(_filled_qty(row)),
        "avg_px": _decimal_text(_avg_px(row)),
        "fee": _decimal_text(_fee(row)),
        "fee_ccy": str(row.get("feeCcy") or ""),
    }


def _filled_qty(row: dict[str, Any]) -> Decimal:
    return _decimal(row.get("accFillSz") or row.get("fillSz") or row.get("sz"))


def _avg_px(row: dict[str, Any]) -> Decimal:
    return _decimal(row.get("avgPx") or row.get("fillPx") or row.get("px"))


def _fee(row: dict[str, Any]) -> Decimal:
    return _decimal(row.get("fee"))


def _query_base_balance(okx: Any, symbol: str) -> Decimal | None:
    base_ccy = symbol.split("/")[0].upper()
    try:
        response = okx.get_balance(ccy=base_ccy) if hasattr(okx, "get_balance") else okx.request(
            "GET",
            "/api/v5/account/balance",
            params={"ccy": base_ccy},
        )
    except Exception:
        return None
    data = getattr(response, "data", response)
    rows = data.get("data") if isinstance(data, dict) else None
    details = ((rows[0] if isinstance(rows, list) and rows else {}) or {}).get("details")
    if not isinstance(details, list):
        return None
    for item in details:
        if isinstance(item, dict) and str(item.get("ccy") or "").upper() == base_ccy:
            return _decimal(item.get("availBal") or item.get("cashBal") or item.get("eq") or item.get("bal"))
    return Decimal("0")


def _open_order_count(okx: Any, *, inst_id: str) -> int:
    try:
        if hasattr(okx, "get_open_orders"):
            response = okx.get_open_orders(inst_id=inst_id)
        else:
            response = okx.request("GET", "/api/v5/trade/orders-pending", params={"instId": inst_id})
    except Exception:
        return -1
    data = getattr(response, "data", response)
    rows = data.get("data") if isinstance(data, dict) else None
    return len(rows) if isinstance(rows, list) else -1


def _local_position_qty(cfg: Any, *, preflight: dict[str, Any], symbol: str) -> Decimal | None:
    path_text = str((preflight.get("runtime_paths") or {}).get("position_store_path") or "")
    path = Path(path_text) if path_text else derive_position_store_path(
        Path(getattr(getattr(cfg, "execution", cfg), "order_store_path", "reports/orders.sqlite"))
    )
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        return None
    try:
        with sqlite3.connect(str(path), timeout=2.0) as con:
            cur = con.cursor()
            cur.execute("SELECT qty FROM positions WHERE symbol = ?", (symbol,))
            rows = cur.fetchall()
    except Exception:
        return None
    total = Decimal("0")
    for (qty,) in rows:
        total += _decimal(qty)
    return total


def _read_reconcile_status(preflight: dict[str, Any]) -> dict[str, Any]:
    path_text = str((preflight.get("runtime_paths") or {}).get("reconcile_status_path") or "")
    if not path_text:
        return {"ok": False, "reason": "reconcile_status_path_missing"}
    path = Path(path_text)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"ok": False, "reason": f"reconcile_status_unreadable:{exc}"}
    return payload if isinstance(payload, dict) else {"ok": False, "reason": "reconcile_status_invalid"}


def _refresh_reconcile_status(
    cfg: Any,
    *,
    preflight: dict[str, Any],
    okx: Any,
    symbol: str,
) -> dict[str, Any]:
    runtime_paths = preflight.get("runtime_paths") or {}
    position_path = Path(str(runtime_paths.get("position_store_path") or ""))
    reconcile_path = Path(str(runtime_paths.get("reconcile_status_path") or ""))
    if not str(position_path) or not str(reconcile_path):
        return {"refreshed": False, "reason": "runtime_paths_missing"}
    try:
        execution = getattr(cfg, "execution", cfg)
        thresholds = ReconcileThresholds(
            abs_usdt_tol=float(getattr(execution, "reconcile_abs_usdt_tol", 50.0) or 50.0),
            abs_base_tol=float(getattr(execution, "reconcile_abs_base_tol", 1e-8) or 1e-8),
            dust_usdt_ignore=float(getattr(execution, "reconcile_dust_usdt_ignore", 1.0) or 1.0),
        )
        status = ReconcileEngine(
            okx=okx,
            position_store=PositionStore(path=str(position_path)),
            account_store=AccountStore(path=str(position_path)),
            thresholds=thresholds,
        ).reconcile(
            out_path=str(reconcile_path),
            universe_bases=[symbol.split("/")[0].upper()],
            ccy_mode=str(getattr(execution, "reconcile_ccy_mode", "universe_only") or "universe_only"),
        )
        return {
            "refreshed": True,
            "ok": bool(status.get("ok")),
            "reason": str(status.get("reason") or ""),
            "path": str(reconcile_path),
        }
    except Exception as exc:
        failure = {
            "schema_version": 1,
            "generated_ts_ms": int(datetime.now(UTC).timestamp() * 1000),
            "ok": False,
            "reason": f"cost_probe_post_exit_reconcile_failed:{type(exc).__name__}",
            "error": str(exc),
        }
        try:
            reconcile_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = reconcile_path.with_suffix(reconcile_path.suffix + ".tmp")
            tmp_path.write_text(json.dumps(failure, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            tmp_path.replace(reconcile_path)
        except Exception:
            pass
        return {"refreshed": False, "reason": failure["reason"], "error": str(exc), "path": str(reconcile_path)}


def _arrival_mid_px(instrument: dict[str, Any]) -> str:
    bid = _decimal(instrument.get("bid_px"))
    ask = _decimal(instrument.get("ask_px"))
    if bid <= 0 or ask <= 0:
        return ""
    return _decimal_text((bid + ask) / Decimal("2"))


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _round_to_step(value: Decimal, step: Decimal, rounding: str) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=rounding) * step


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _decimal_text(value: Any) -> str:
    dec = _decimal(value)
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
                "sell_only_on_error": True,
                "manual_clear_required": True,
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
