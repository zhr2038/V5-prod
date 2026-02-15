from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from configs.schema import ExecutionConfig
from scripts.bills_sync import sync_once as bills_sync_once
from src.execution.bills_store import BillsStore
from src.execution.kill_switch_guard import GuardConfig, KillSwitchGuard
from src.execution.ledger_engine import LedgerEngine
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds
from src.execution.bootstrap_patch import controlled_patch_from_okx_balance


def _now_ms() -> int:
    return int(time.time() * 1000)


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    try:
        p = Path(path)
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


@dataclass
class LivePreflightResult:
    decision: str  # ALLOW|SELL_ONLY|ABORT
    reconcile_ok: bool
    ledger_ok: bool
    kill_switch_enabled: bool
    reason: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class LivePreflight:
    """Live preflight catch-up.

    Goal: before any live execution, bring bills/ledger/reconcile to *fresh* states,
    and produce an explicit decision: ALLOW / SELL_ONLY / ABORT.

    This does not change strategy logic; it only refreshes safety files and supplies
    a gating decision for main().
    """

    def __init__(
        self,
        cfg: ExecutionConfig,
        *,
        okx: OKXPrivateClient,
        position_store: Any,
        account_store: Any,
        bills_db_path: str = "reports/bills.sqlite",
        ledger_state_path: str = "reports/ledger_state.json",
        ledger_status_path: str = "reports/ledger_status.json",
        reconcile_status_path: Optional[str] = None,
    ):
        self.cfg = cfg
        self.okx = okx
        self.position_store = position_store
        self.account_store = account_store
        self.bills_db_path = str(bills_db_path)
        self.ledger_state_path = str(ledger_state_path)
        self.ledger_status_path = str(ledger_status_path)
        self.reconcile_status_path = str(reconcile_status_path or getattr(cfg, "reconcile_status_path", "reports/reconcile_status.json"))

    def _status_is_fresh(self, obj: Optional[Dict[str, Any]], *, max_age_sec: int) -> bool:
        if not obj:
            return False
        ts_ms = obj.get("generated_ts_ms")
        if ts_ms is None:
            ts_ms = obj.get("ts_ms")
        try:
            ts_ms_i = int(ts_ms or 0)
        except Exception:
            ts_ms_i = 0
        if ts_ms_i <= 0:
            return False
        age_ms = max(0, _now_ms() - ts_ms_i)
        return age_ms <= int(max_age_sec) * 1000

    def run(self, *, max_pages: int = 5, max_status_age_sec: int = 180) -> LivePreflightResult:
        details: Dict[str, Any] = {"ts_ms": _now_ms()}

        # 1) Catch-up bills (source of truth)
        store = BillsStore(path=self.bills_db_path)
        new_bills = bills_sync_once(store=store, client=self.okx, limit=100, max_pages=int(max_pages))
        details["bills"] = {"new": int(new_bills), "total": int(store.count())}

        # 2) Ledger once (bills -> expected -> balance)
        led = LedgerEngine(okx=self.okx, bills_store=store, state_path=self.ledger_state_path)
        ledger_obj = led.run(out_path=self.ledger_status_path)
        ledger_ok = bool(ledger_obj.get("ok"))
        details["ledger"] = {"ok": ledger_ok, "reason": ledger_obj.get("reason"), "bill_count": (ledger_obj.get("bills_aggregate") or {}).get("count")}

        # 3) Reconcile once + kill-switch guard
        # (Write reconcile_status.json; then guard will write failure_state/kill_switch)
        th = ReconcileThresholds(
            abs_usdt_tol=1.0,
            abs_base_tol=1e-8,
            dust_usdt_ignore=float(getattr(self.cfg, "reconcile_dust_usdt_ignore", 0.0) or 0.0),
        )
        eng = ReconcileEngine(
            okx=self.okx,
            position_store=self.position_store,
            account_store=self.account_store,
            thresholds=th,
        )

        status = eng.reconcile(out_path=self.reconcile_status_path)
        status["generated_ts_ms"] = int(status.get("ts_ms") or _now_ms())
        # atomic rewrite (ensure generated_ts_ms persisted)
        p = Path(self.reconcile_status_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(p)

        out = KillSwitchGuard(GuardConfig(reconcile_status_path=self.reconcile_status_path)).apply()
        reconcile_ok = bool(out.get("ok"))
        reconcile_reason = out.get("reason")
        kill_switch_enabled = bool((out.get("kill_switch") or {}).get("enabled"))
        details["reconcile"] = {"ok": reconcile_ok, "reason": reconcile_reason, "category": out.get("category")}
        details["kill_switch"] = {"enabled": kill_switch_enabled, "trigger": (out.get("kill_switch") or {}).get("trigger")}

        # 3b) Optional controlled patch (only for state alignment)
        if (
            (not reconcile_ok)
            and bool(getattr(self.cfg, "preflight_bootstrap_patch_enabled", False))
            and str(reconcile_reason) in {"base_mismatch", "usdt_mismatch"}
            and bool(ledger_ok)
        ):
            pr = controlled_patch_from_okx_balance(
                okx=self.okx,
                position_store=self.position_store,
                account_store=self.account_store,
                max_total_drift_usdt=float(getattr(self.cfg, "preflight_bootstrap_patch_max_total_usdt", 50.0) or 50.0),
                min_interval_sec=int(getattr(self.cfg, "preflight_bootstrap_patch_min_interval_sec", 300) or 300),
            )
            details["bootstrap_patch"] = {
                "applied": bool(pr.applied),
                "reason": pr.reason,
                "est_total_drift_usdt": pr.est_total_drift_usdt,
                "updated_cash": pr.updated_cash,
                "updated_positions": pr.updated_positions,
            }

            # rerun reconcile/guard after patch
            status = eng.reconcile(out_path=self.reconcile_status_path)
            status["generated_ts_ms"] = int(status.get("ts_ms") or _now_ms())
            p = Path(self.reconcile_status_path)
            tmp = p.with_suffix(p.suffix + ".tmp")
            tmp.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(p)

            out = KillSwitchGuard(GuardConfig(reconcile_status_path=self.reconcile_status_path)).apply()
            reconcile_ok = bool(out.get("ok"))
            reconcile_reason = out.get("reason")
            kill_switch_enabled = bool((out.get("kill_switch") or {}).get("enabled"))
            details["reconcile_after_patch"] = {"ok": reconcile_ok, "reason": reconcile_reason, "category": out.get("category")}
            details["kill_switch_after_patch"] = {"enabled": kill_switch_enabled, "trigger": (out.get("kill_switch") or {}).get("trigger")}

        # 4) Freshness check (avoid stale ok=true)
        rec_obj = _read_json(self.reconcile_status_path)
        led_obj = _read_json(self.ledger_status_path)
        fresh_reconcile = self._status_is_fresh(rec_obj, max_age_sec=int(max_status_age_sec))
        fresh_ledger = self._status_is_fresh(led_obj, max_age_sec=int(max_status_age_sec))
        details["fresh"] = {"reconcile": fresh_reconcile, "ledger": fresh_ledger, "max_age_sec": int(max_status_age_sec)}

        if not fresh_reconcile or not fresh_ledger:
            decision = "SELL_ONLY"
            reason = "status_stale"
            return LivePreflightResult(
                decision=decision,
                reconcile_ok=reconcile_ok,
                ledger_ok=ledger_ok,
                kill_switch_enabled=kill_switch_enabled,
                reason=reason,
                details=details,
            )

        # 5) Decision
        if kill_switch_enabled:
            return LivePreflightResult(
                decision="ABORT",
                reconcile_ok=reconcile_ok,
                ledger_ok=ledger_ok,
                kill_switch_enabled=True,
                reason="kill_switch",
                details=details,
            )

        if reconcile_ok and ledger_ok:
            return LivePreflightResult(
                decision="ALLOW",
                reconcile_ok=True,
                ledger_ok=True,
                kill_switch_enabled=False,
                reason="ok",
                details=details,
            )

        return LivePreflightResult(
            decision="SELL_ONLY",
            reconcile_ok=reconcile_ok,
            ledger_ok=ledger_ok,
            kill_switch_enabled=False,
            reason="reconcile_or_ledger_not_ok",
            details=details,
        )
