from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


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
    reconcile_status_path: str = "reports/reconcile_status.json"
    failure_state_path: str = "reports/reconcile_failure_state.json"
    kill_switch_path: str = "reports/kill_switch.json"

    hard_fail_threshold: int = 3
    auth_fail_threshold: int = 1

    stale_threshold_sec: int = 900  # 15 min


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
        ok = bool(status.get("ok")) if status else False
        reason = status.get("reason")

        # Capture OKX error code/msg if present
        err = status.get("error") or {}
        okx_code = err.get("okx_code")

        # stale status becomes SOFT failure even if ok=true
        if age_ms is not None and gen_ts_ms > 0 and age_ms > int(self.cfg.stale_threshold_sec) * 1000:
            ok = False
            reason = "stale_status"

        # Idempotency guard: only count once per reconcile ts
        st = self._load_failure_state()
        if gen_ts_ms and int(st.get("last_reconcile_ts_ms") or 0) == int(gen_ts_ms):
            return {"ok": ok, "reason": reason, "skipped": True, "failure_state": st, "kill_switch": self._load_kill_switch()}

        category, norm_reason = ("OK", "ok") if ok else classify_reason(reason, okx_code=str(okx_code) if okx_code is not None else None)

        if ok:
            st["consecutive_hard"] = 0
            st["consecutive_soft"] = 0
            st["last_ok_ts_ms"] = int(gen_ts_ms or now)
        else:
            if category == "HARD":
                st["consecutive_hard"] = int(st.get("consecutive_hard") or 0) + 1
                st["consecutive_soft"] = 0
            else:
                st["consecutive_soft"] = int(st.get("consecutive_soft") or 0) + 1
                # do not reset hard counter on soft by default

        st["last_reason"] = norm_reason
        st["last_reconcile_ts_ms"] = int(gen_ts_ms or now)
        st["updated_ts_ms"] = int(now)
        _atomic_write_json(self.cfg.failure_state_path, st)

        ks = self._load_kill_switch()
        if bool(ks.get("enabled")):
            return {"ok": ok, "reason": norm_reason, "category": category, "failure_state": st, "kill_switch": ks}

        trigger = None
        if (not ok) and category == "AUTH" and int(st.get("consecutive_soft") or 0) >= int(self.cfg.auth_fail_threshold):
            trigger = "reconcile_auth_fail"
        elif (not ok) and category == "HARD" and int(st.get("consecutive_hard") or 0) >= int(self.cfg.hard_fail_threshold):
            trigger = "reconcile_hard_fail"

        if trigger:
            details = {
                "max_abs_usdt_delta": ((status.get("stats") or {}).get("max_abs_usdt_delta")),
                "reconcile_status_path": self.cfg.reconcile_status_path,
                "okx_code": err.get("okx_code"),
                "okx_msg": err.get("okx_msg"),
                "http_status": err.get("http_status"),
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
