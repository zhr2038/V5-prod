from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[2]

PROFIT_LOCK_REASONS = {"protect_profit_lock_trailing"}
PROBE_STOP_REASONS = {"probe_stop_loss", "probe_time_stop", "market_impulse_probe_time_stop"}
PROBE_TAKE_PROFIT_REASONS = {"probe_take_profit", "probe_trailing_stop"}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _resolve_path(path: str | Path) -> Path:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


def _load_state(path: str | Path) -> Dict[str, Any]:
    p = _resolve_path(path)
    if not p.exists():
        return {"version": 1, "symbols": {}}
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            return {"version": 1, "symbols": {}}
        if "symbols" in obj and isinstance(obj.get("symbols"), dict):
            obj.setdefault("version", 1)
            return obj
        return {"version": 1, "symbols": obj}
    except Exception:
        return {"version": 1, "symbols": {}}


def _save_state(path: str | Path, state: Dict[str, Any]) -> None:
    p = _resolve_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(state or {"version": 1, "symbols": {}}, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def record_same_symbol_exit_memory(
    *,
    path: str | Path,
    symbol: str,
    exit_ts_ms: Optional[int] = None,
    exit_px: Optional[float] = None,
    exit_reason: str = "",
    highest_px_before_exit: Optional[float] = None,
    net_bps: Optional[float] = None,
) -> None:
    sym = str(symbol or "").strip()
    if not sym:
        return
    reason = str(exit_reason or "").strip()
    event_ts_ms = int(exit_ts_ms or _now_ms())
    if event_ts_ms <= 0:
        event_ts_ms = _now_ms()

    state = _load_state(path)
    symbols = state.setdefault("symbols", {})
    prev = symbols.get(sym) if isinstance(symbols.get(sym), dict) else {}
    prev_ts = int((prev or {}).get("exit_ts_ms") or 0)
    if prev_ts > event_ts_ms:
        return

    def _float_or_none(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            parsed = float(value)
            return parsed if parsed > 0 or value == 0 else parsed
        except Exception:
            return None

    exit_px_f = _float_or_none(exit_px)
    high_f = _float_or_none(highest_px_before_exit)
    if high_f is None and exit_px_f is not None:
        high_f = exit_px_f
    elif high_f is not None and exit_px_f is not None:
        high_f = max(float(high_f), float(exit_px_f))

    payload: Dict[str, Any] = {
        "symbol": sym,
        "exit_ts_ms": int(event_ts_ms),
        "exit_px": exit_px_f,
        "exit_reason": reason,
        "highest_px_before_exit": high_f,
        "net_bps": _float_or_none(net_bps),
    }
    symbols[sym] = payload
    _save_state(path, state)


def _cooldown_hours_for_reason(config: Any, reason: str) -> float:
    execution = getattr(config, "execution", config)
    reason_s = str(reason or "").strip()
    if reason_s in PROFIT_LOCK_REASONS:
        return float(getattr(execution, "same_symbol_reentry_cooldown_hours_after_profit_lock", 6) or 0)
    if reason_s in PROBE_STOP_REASONS:
        return float(getattr(execution, "same_symbol_reentry_cooldown_hours_after_probe_stop", 8) or 0)
    if reason_s in PROBE_TAKE_PROFIT_REASONS:
        return float(getattr(execution, "same_symbol_reentry_cooldown_hours_after_probe_take_profit", 4) or 0)
    return 0.0


def evaluate_same_symbol_reentry_guard(
    *,
    path: str | Path,
    symbol: str,
    latest_px: float,
    config: Any,
    entry_kind: str,
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    execution = getattr(config, "execution", config)
    if not bool(getattr(execution, "same_symbol_reentry_guard_enabled", True)):
        return {"active": False, "blocked": False, "breakout_exception_met": False}

    kind = str(entry_kind or "").strip()
    if kind == "market_impulse_probe" and not bool(
        getattr(execution, "same_symbol_reentry_apply_to_market_impulse_probe", True)
    ):
        return {"active": False, "blocked": False, "breakout_exception_met": False}
    if kind == "btc_leadership_probe" and not bool(
        getattr(execution, "same_symbol_reentry_apply_to_btc_leadership_probe", True)
    ):
        return {"active": False, "blocked": False, "breakout_exception_met": False}
    if kind == "normal_entry" and not bool(
        getattr(execution, "same_symbol_reentry_apply_to_normal_entry", True)
    ):
        return {"active": False, "blocked": False, "breakout_exception_met": False}

    sym = str(symbol or "").strip()
    if not sym:
        return {"active": False, "blocked": False, "breakout_exception_met": False}
    state = _load_state(path)
    rec = (state.get("symbols") or {}).get(sym)
    if not isinstance(rec, dict):
        return {"active": False, "blocked": False, "breakout_exception_met": False}

    exit_reason = str(rec.get("exit_reason") or "").strip()
    cooldown_hours = _cooldown_hours_for_reason(execution, exit_reason)
    if cooldown_hours <= 0:
        return {"active": False, "blocked": False, "breakout_exception_met": False, **dict(rec)}

    exit_ts_ms = int(rec.get("exit_ts_ms") or 0)
    current_ms = int(now_ms or _now_ms())
    if exit_ts_ms <= 0 or current_ms <= 0:
        return {"active": False, "blocked": False, "breakout_exception_met": False, **dict(rec)}

    elapsed_hours = max(0.0, (current_ms - exit_ts_ms) / 3_600_000.0)
    cooldown_active = elapsed_hours < float(cooldown_hours)
    latest = float(latest_px or 0.0)
    exit_px = float(rec.get("exit_px") or 0.0)
    highest = float(rec.get("highest_px_before_exit") or 0.0)
    breakout_last_high_bps = float(
        getattr(execution, "same_symbol_reentry_breakout_above_last_high_bps", 20) or 0.0
    )
    breakout_exit_bps = float(
        getattr(execution, "same_symbol_reentry_breakout_above_exit_bps", 50) or 0.0
    )
    breakout_above_last_high = (
        latest > 0.0
        and highest > 0.0
        and latest >= highest * (1.0 + breakout_last_high_bps / 10000.0)
    )
    breakout_above_exit = (
        latest > 0.0
        and exit_px > 0.0
        and latest >= exit_px * (1.0 + breakout_exit_bps / 10000.0)
    )
    breakout_exception_met = bool(
        getattr(execution, "same_symbol_reentry_allow_breakout", True)
        and (breakout_above_last_high or breakout_above_exit)
    )

    payload = {
        "active": bool(cooldown_active),
        "blocked": bool(cooldown_active and not breakout_exception_met),
        "breakout_exception_met": bool(cooldown_active and breakout_exception_met),
        "symbol": sym,
        "last_exit_reason": exit_reason,
        "last_exit_px": exit_px if exit_px > 0 else None,
        "highest_px_before_exit": highest if highest > 0 else None,
        "elapsed_hours": float(elapsed_hours),
        "required_cooldown_hours": float(cooldown_hours),
        "latest_px": latest if latest > 0 else None,
        "breakout_above_last_high_bps": float(breakout_last_high_bps),
        "breakout_above_exit_bps": float(breakout_exit_bps),
        "breakout_above_last_high": bool(breakout_above_last_high),
        "breakout_above_exit": bool(breakout_above_exit),
        "exit_ts_ms": int(exit_ts_ms),
        "net_bps": rec.get("net_bps"),
    }
    return payload
