from __future__ import print_function

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)

from src.execution.reconcile_reason import FailureContext, classify_reconcile_failure


def _now_ms() -> int:
    return int(time.time() * 1000)


def _atomic_write_json(path: str, obj: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


@dataclass
class GuardConfig:
    """GuardConfig类"""
    reconcile_status_path: str = "reports/reconcile_status.json"
    failure_state_path: str = "reports/reconcile_failure_state.json"
    kill_switch_path: str = "reports/kill_switch.json"

    hard_fail_threshold: int = 5
    auth_fail_threshold: int = 1

    stale_threshold_sec: int = 900  # 15 min

    # G1.2.4: only stale_status triggers a SOFT-kill threshold
    stale_soft_threshold: int = 3
    
    # Auto-clear kill switch when conditions improve
    auto_clear_enabled: bool = True
    auto_clear_after_ok_count: int = 1  # Clear after 1 consecutive OK reconcile


def classify_reason(reason: Optional[str], *, okx_code: Optional[str] = None) -> Tuple[str, str]:
    """Return (category, normalized_reason).

    category: HARD|AUTH|SOFT|OK

    Minimal rules:
    - okx_code startswith 501 => AUTH
    - okx_code == 50041 => AUTH (IP/whitelist-style access control)
    - usdt_mismatch/base_mismatch => HARD
    - stale_status/timeout/network_error/parse_error/rate_limited/api_system_error => SOFT
    """

    if okx_code is not None:
        c = str(okx_code)
        if c.startswith("501"):
            return "AUTH", "auth_error"
        if c == "50041":
            return "AUTH", "auth_error"

    r = (reason or "").strip()
    if not r:
        return "SOFT", "unknown"

    if r in {"usdt_mismatch", "base_mismatch"}:
        return "HARD", r

    if r in {
        "auth_error",
        "rate_limited",
        "api_system_error",
        "network_error",
        "timeout",
        "parse_error",
        "stale_status",
        "unknown",
    }:
        return "SOFT", r

    # default to SOFT for forward-compat
    return "SOFT", r


class KillSwitchGuard:
    """Consumes reconcile_status.json and maintains failure-state + kill-switch.

    Key properties:
    - Idempotent: the same reconcile_status (ts_ms) won't increment counters twice.
    - Conservative: never auto-disables kill-switch.
    """

    def __init__(self, cfg: Optional[GuardConfig] = None):
        self.cfg = cfg or GuardConfig()

    def _load_failure_state(self) -> Dict[str, Any]:
        st = _read_json(self.cfg.failure_state_path) or {}
        return {
            "schema_version": 1,
            "updated_ts_ms": int(st.get("updated_ts_ms") or 0),
            "consecutive_hard": int(st.get("consecutive_hard") or 0),
            "consecutive_soft": int(st.get("consecutive_soft") or 0),
            "consecutive_stale": int(st.get("consecutive_stale") or 0),
            "last_stale_ts_ms": int(st.get("last_stale_ts_ms") or 0),
            "last_reason": st.get("last_reason"),
            "last_ok_ts_ms": int(st.get("last_ok_ts_ms") or 0),
            "last_reconcile_ts_ms": int(st.get("last_reconcile_ts_ms") or 0),
        }

    def _load_kill_switch(self) -> Dict[str, Any]:
        ks = _read_json(self.cfg.kill_switch_path) or {}
        if "enabled" not in ks:
            ks["enabled"] = False
        return ks

    def apply(self) -> Dict[str, Any]:
        """Apply"""
        now = _now_ms()
        status = _read_json(self.cfg.reconcile_status_path) or {}

        # Determine freshness
        gen_ts = status.get("generated_ts_ms")
        if gen_ts is None:
            gen_ts = status.get("ts_ms")
        try:
            gen_ts_ms = int(gen_ts or 0)
        except Exception:
            gen_ts_ms = 0

        age_ms = max(0, now - gen_ts_ms) if gen_ts_ms > 0 else None
        ok0 = bool(status.get("ok")) if status else False
        reason0 = status.get("reason")

        # Capture OKX error code/msg if present
        err = status.get("error") or {}

        # Normalize reason/category (stable, greppable)
        norm_reason, category = classify_reconcile_failure(
            FailureContext(
                ok=ok0,
                reason=reason0,
                error=err if isinstance(err, dict) else None,
                exc=None,
                status_age_ms=age_ms,
                stale_threshold_ms=int(self.cfg.stale_threshold_sec) * 1000,
            )
        )
        ok = bool(ok0) and norm_reason == "ok"

        # Idempotency guard: only count once per reconcile ts
        st = self._load_failure_state()
        if gen_ts_ms and int(st.get("last_reconcile_ts_ms") or 0) == int(gen_ts_ms):
            return {"ok": ok, "reason": norm_reason, "category": category, "skipped": True, "failure_state": st, "kill_switch": self._load_kill_switch()}

        # stale counter is independent; it resets unless the current normalized_reason is exactly stale_status.
        if norm_reason == "stale_status":
            st["consecutive_stale"] = int(st.get("consecutive_stale") or 0) + 1
            st["last_stale_ts_ms"] = int(gen_ts_ms or now)
        else:
            st["consecutive_stale"] = 0

        if ok:
            st["consecutive_hard"] = 0
            st["consecutive_soft"] = 0
            st["last_ok_ts_ms"] = int(gen_ts_ms or now)
            # Track consecutive OKs for auto-clear
            st["consecutive_ok"] = int(st.get("consecutive_ok") or 0) + 1
        else:
            st["consecutive_ok"] = 0
            if category == "HARD":
                st["consecutive_hard"] = int(st.get("consecutive_hard") or 0) + 1
                st["consecutive_soft"] = 0
            elif category == "AUTH":
                # treat AUTH as its own lane; keep soft counter for visibility
                st["consecutive_soft"] = int(st.get("consecutive_soft") or 0) + 1
            else:
                st["consecutive_soft"] = int(st.get("consecutive_soft") or 0) + 1
                # do not reset hard counter on soft by default

        st["last_reason"] = norm_reason
        st["last_reconcile_ts_ms"] = int(gen_ts_ms or now)
        st["updated_ts_ms"] = int(now)
        _atomic_write_json(self.cfg.failure_state_path, st)

        ks = self._load_kill_switch()

        # Auto-clear kill switch if enabled and conditions improve
        if (bool(ks.get("enabled")) and 
            self.cfg.auto_clear_enabled and 
            int(st.get("consecutive_ok") or 0) >= self.cfg.auto_clear_after_ok_count):
            
            ks_cleared = {
                "enabled": False,
                "ts_ms": int(now),
                "auto_cleared": True,
                "auto_cleared_reason": f"consecutive_ok_{st.get('consecutive_ok')}",
                "previous_trigger": ks.get("trigger"),
                "previous_reason": ks.get("reason"),
            }
            _atomic_write_json(self.cfg.kill_switch_path, ks_cleared)
            ks = ks_cleared
            log.warning(f"Kill switch AUTO-CLEARED after {st.get('consecutive_ok')} consecutive OK reconciles")
            return {"ok": ok, "reason": norm_reason, "category": category, "failure_state": st, "kill_switch": ks, "auto_cleared": True}

        # Never auto-disable kill-switch. If it's enabled, operator must clear it.
        if bool(ks.get("enabled")):
            return {"ok": ok, "reason": norm_reason, "category": category, "failure_state": st, "kill_switch": ks}

        trigger = None
        if (not ok) and category == "AUTH" and int(st.get("consecutive_soft") or 0) >= int(self.cfg.auth_fail_threshold):
            trigger = "reconcile_auth_fail"
        elif (not ok) and category == "HARD" and int(st.get("consecutive_hard") or 0) >= int(self.cfg.hard_fail_threshold):
            trigger = "reconcile_hard_fail"
        elif (not ok) and norm_reason == "stale_status" and int(st.get("consecutive_stale") or 0) >= int(self.cfg.stale_soft_threshold):
            trigger = "reconcile_stale_fail"

        if trigger:
            # details snapshot for post-mortem
            stats = status.get("stats") or {}
            max_abs_usdt_delta = stats.get("max_abs_usdt_delta")
            max_abs_base_delta = stats.get("max_abs_base_delta")

            # if stats missing, try compute from diffs
            try:
                if max_abs_base_delta is None:
                    xs = []
                    for d in (status.get("diffs") or []):
                        if str(d.get("ccy") or "").upper() == "USDT":
                            continue
                        xs.append(abs(float(d.get("delta") or 0.0)))
                    max_abs_base_delta = max(xs) if xs else 0.0
            except Exception:
                pass

            details = {
                "max_abs_usdt_delta": max_abs_usdt_delta,
                "max_abs_base_delta": max_abs_base_delta,
                "reconcile_status_path": self.cfg.reconcile_status_path,
                "okx_code": err.get("okx_code"),
                "okx_msg": err.get("okx_msg"),
                "http_status": err.get("http_status"),
                "consecutive_stale": int(st.get("consecutive_stale") or 0),
                "stale_age_ms": int(age_ms) if age_ms is not None else None,
            }
            ks_new = {
                "enabled": True,
                "ts_ms": int(now),
                "trigger": trigger,
                "reason": norm_reason,
                "consecutive_hard": int(st.get("consecutive_hard") or 0),
                "consecutive_soft": int(st.get("consecutive_soft") or 0),
                "last_reconcile_ts_ms": int(gen_ts_ms or 0),
                "details": details,
            }
            _atomic_write_json(self.cfg.kill_switch_path, ks_new)
            ks = ks_new

        return {"ok": ok, "reason": norm_reason, "category": category, "failure_state": st, "kill_switch": ks}
