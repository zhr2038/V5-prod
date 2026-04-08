from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from configs.schema import ExecutionConfig
from scripts.bills_sync import sync_once as bills_sync_once
from src.execution.bills_store import BillsStore
from src.execution.bootstrap_patch import controlled_patch_from_okx_balance
from src.execution.borrow_guard import check_okx_borrows
from src.execution.kill_switch_guard import GuardConfig, KillSwitchGuard
from src.execution.ledger_engine import LedgerEngine
from src.execution.okx_private_client import OKXPrivateClient
from src.execution.reconcile_engine import ReconcileEngine, ReconcileThresholds
from src.utils.auto_blacklist import add_symbol as auto_blacklist_add


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


def _write_json(path: str, obj: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "on"}


def _coalesce(value: Any, default: Any) -> Any:
    return default if value is None else value


def _normalize_kill_switch(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict):
        if "enabled" in data or "active" in data:
            normalized = dict(data)
            if "enabled" not in normalized:
                normalized["enabled"] = _to_bool(normalized.get("active"))
            return normalized

        nested = data.get("kill_switch")
        if isinstance(nested, dict):
            normalized = dict(nested)
            if "enabled" not in normalized:
                normalized["enabled"] = _to_bool(normalized.get("active"))
            return normalized

        normalized = dict(data)
        normalized["enabled"] = _to_bool(nested)
        return normalized

    if data is None:
        return {"enabled": False}

    return {"enabled": _to_bool(data)}


def _is_manual_kill_switch(data: Any) -> bool:
    normalized = _normalize_kill_switch(data)
    return _to_bool(normalized.get("manual")) or str(normalized.get("trigger") or "").strip().lower() == "manual"


def _clear_kill_switch_payload(data: Any, *, now_ms: int, reason: str) -> Dict[str, Any]:
    if isinstance(data, dict):
        payload = dict(data)
    else:
        payload = {}
    nested = payload.get("kill_switch")
    if isinstance(nested, dict):
        nested_payload = dict(nested)
        nested_payload["enabled"] = False
        nested_payload["auto_cleared_ts_ms"] = now_ms
        nested_payload["auto_cleared_reason"] = reason
        payload["kill_switch"] = nested_payload
    payload["enabled"] = False
    payload["auto_cleared_ts_ms"] = now_ms
    payload["auto_cleared_reason"] = reason
    return payload


def _borrow_symbol_for_ccy(ccy: Any) -> Optional[str]:
    base = str(ccy or "").strip().upper()
    if not base or base in {"USDT", "USDC", "USD"}:
        return None
    return f"{base}/USDT"


@dataclass
class LivePreflightResult:
    decision: str  # ALLOW|SELL_ONLY|ABORT
    reconcile_ok: bool
    ledger_ok: bool
    kill_switch_enabled: bool
    reason: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class LivePreflight:
    """Refresh live safety state before any live execution.

    The preflight produces a gating decision only. It does not change strategy logic.
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
        self.reconcile_status_path = str(
            reconcile_status_path
            or getattr(cfg, "reconcile_status_path", "reports/reconcile_status.json")
        )

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

    def _build_guard_config(self) -> GuardConfig:
        return GuardConfig(
            reconcile_status_path=self.reconcile_status_path,
            failure_state_path=str(
                getattr(self.cfg, "reconcile_failure_state_path", "reports/reconcile_failure_state.json")
            ),
            kill_switch_path=str(getattr(self.cfg, "kill_switch_path", "reports/kill_switch.json")),
        )

    def _run_reconcile_guard(
        self,
        *,
        eng: ReconcileEngine,
        guard_cfg: GuardConfig,
    ) -> Dict[str, Any]:
        status = eng.reconcile(
            out_path=self.reconcile_status_path,
            ccy_mode=str(getattr(self.cfg, "reconcile_ccy_mode", "universe_only") or "universe_only"),
        )
        status["generated_ts_ms"] = int(status.get("ts_ms") or _now_ms())
        _write_json(self.reconcile_status_path, status)
        return KillSwitchGuard(guard_cfg).apply()

    def run(self, *, max_pages: int = 5, max_status_age_sec: int = 180) -> LivePreflightResult:
        details: Dict[str, Any] = {"ts_ms": _now_ms()}

        store = BillsStore(path=self.bills_db_path)
        new_bills = bills_sync_once(store=store, client=self.okx, limit=100, max_pages=int(max_pages))
        details["bills"] = {"new": int(new_bills), "total": int(store.count())}

        led = LedgerEngine(okx=self.okx, bills_store=store, state_path=self.ledger_state_path)
        ledger_obj = led.run(out_path=self.ledger_status_path)
        ledger_ok = _to_bool(ledger_obj.get("ok"))
        if not ledger_ok and ledger_obj.get("reason") == "dust_baseline_reset":
            ledger_ok = True
        details["ledger"] = {
            "ok": ledger_ok,
            "reason": ledger_obj.get("reason"),
            "bill_count": (ledger_obj.get("bills_aggregate") or {}).get("count"),
        }

        th = ReconcileThresholds(
            abs_usdt_tol=float(getattr(self.cfg, "reconcile_abs_usdt_tol", 50.0)),
            abs_base_tol=1e-6,
            dust_usdt_ignore=float(_coalesce(getattr(self.cfg, "reconcile_dust_usdt_ignore", None), 5.0)),
        )
        eng = ReconcileEngine(
            okx=self.okx,
            position_store=self.position_store,
            account_store=self.account_store,
            thresholds=th,
        )
        guard_cfg = self._build_guard_config()

        out = self._run_reconcile_guard(eng=eng, guard_cfg=guard_cfg)
        reconcile_ok = _to_bool(out.get("ok"))
        reconcile_reason = out.get("reason")
        kill_switch_state = _normalize_kill_switch(out.get("kill_switch"))
        kill_switch_enabled = _to_bool(kill_switch_state.get("enabled"))
        details["reconcile"] = {
            "ok": reconcile_ok,
            "reason": reconcile_reason,
            "category": out.get("category"),
        }
        details["kill_switch"] = {
            "enabled": kill_switch_enabled,
            "trigger": kill_switch_state.get("trigger"),
        }

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
                max_total_drift_usdt=float(
                    _coalesce(getattr(self.cfg, "preflight_bootstrap_patch_max_total_usdt", None), 50.0)
                ),
                min_interval_sec=int(
                    _coalesce(getattr(self.cfg, "preflight_bootstrap_patch_min_interval_sec", None), 300)
                ),
            )
            details["bootstrap_patch"] = {
                "applied": bool(pr.applied),
                "reason": pr.reason,
                "est_total_drift_usdt": pr.est_total_drift_usdt,
                "updated_cash": pr.updated_cash,
                "updated_positions": pr.updated_positions,
            }

            out = self._run_reconcile_guard(eng=eng, guard_cfg=guard_cfg)
            reconcile_ok = _to_bool(out.get("ok"))
            reconcile_reason = out.get("reason")
            kill_switch_state = _normalize_kill_switch(out.get("kill_switch"))
            kill_switch_enabled = _to_bool(kill_switch_state.get("enabled"))
            details["reconcile_after_patch"] = {
                "ok": reconcile_ok,
                "reason": reconcile_reason,
                "category": out.get("category"),
            }
            details["kill_switch_after_patch"] = {
                "enabled": kill_switch_enabled,
                "trigger": kill_switch_state.get("trigger"),
            }

        rec_obj = _read_json(self.reconcile_status_path)
        led_obj = _read_json(self.ledger_status_path)
        fresh_reconcile = self._status_is_fresh(rec_obj, max_age_sec=int(max_status_age_sec))
        fresh_ledger = self._status_is_fresh(led_obj, max_age_sec=int(max_status_age_sec))
        details["fresh"] = {
            "reconcile": fresh_reconcile,
            "ledger": fresh_ledger,
            "max_age_sec": int(max_status_age_sec),
        }

        if not fresh_reconcile or not fresh_ledger:
            return LivePreflightResult(
                decision="SELL_ONLY",
                reconcile_ok=reconcile_ok,
                ledger_ok=ledger_ok,
                kill_switch_enabled=kill_switch_enabled,
                reason="status_stale",
                details=details,
            )

        if kill_switch_enabled:
            try:
                if (
                    bool(getattr(self.cfg, "auto_clear_kill_switch_if_ok", False))
                    and bool(reconcile_ok)
                    and bool(ledger_ok)
                ):
                    ks_path = str(getattr(self.cfg, "kill_switch_path", "reports/kill_switch.json"))
                    ks_obj = _read_json(ks_path)
                    if not _is_manual_kill_switch(ks_obj):
                        ks_obj = _clear_kill_switch_payload(
                            ks_obj,
                            now_ms=_now_ms(),
                            reason="preflight_ok",
                        )
                        _write_json(ks_path, ks_obj)
                        kill_switch_enabled = False
                        details["kill_switch_auto_cleared"] = True
            except Exception:
                pass

            if kill_switch_enabled:
                return LivePreflightResult(
                    decision="ABORT",
                    reconcile_ok=reconcile_ok,
                    ledger_ok=ledger_ok,
                    kill_switch_enabled=True,
                    reason="kill_switch",
                    details=details,
                )

        try:
            bal = self.okx.get_balance()
            borrow_res = check_okx_borrows(
                bal.data,
                liab_eps=float(_coalesce(getattr(self.cfg, "borrow_liab_eps", None), 1e-6)),
                neg_eq_eps=float(_coalesce(getattr(self.cfg, "borrow_neg_eq_eps", None), 1e-6)),
            )
            details["borrow_check"] = {
                "ok": bool(borrow_res.ok),
                "reason": borrow_res.reason,
                "count": len(borrow_res.items),
                "blocked_symbols": [],
                "quote_liability_ccys": [],
                "items": [
                    {
                        "ccy": i.ccy,
                        "eq": i.eq,
                        "liab": i.liab,
                        "cross_liab": i.cross_liab,
                        "borrow_froz": i.borrow_froz,
                    }
                    for i in (borrow_res.items or [])
                ],
            }

            if (not borrow_res.ok) and bool(getattr(self.cfg, "abort_on_borrow", True)):
                blocked_symbols = []
                quote_liability_ccys = []
                try:
                    for it in (borrow_res.items or []):
                        sym = _borrow_symbol_for_ccy(it.ccy)
                        if sym:
                            blocked_symbols.append(sym)
                            auto_blacklist_add(
                                sym,
                                reason="borrow_detected",
                                ttl_sec=30 * 24 * 3600,
                                meta={
                                    "eq": it.eq,
                                    "liab": it.liab,
                                    "cross_liab": it.cross_liab,
                                    "borrow_froz": it.borrow_froz,
                                },
                            )
                        else:
                            quote_liability_ccys.append(str(it.ccy or "").strip().upper())
                except Exception:
                    pass

                blocked_symbols = sorted({str(sym) for sym in blocked_symbols if str(sym)})
                quote_liability_ccys = sorted({str(ccy) for ccy in quote_liability_ccys if str(ccy)})
                details["borrow_check"]["blocked_symbols"] = blocked_symbols
                details["borrow_check"]["quote_liability_ccys"] = quote_liability_ccys

                borrow_block_mode = str(
                    getattr(self.cfg, "borrow_block_mode", "global_abort") or "global_abort"
                ).lower()
                details["borrow_check"]["block_mode"] = borrow_block_mode
                if borrow_block_mode == "symbol_only":
                    if quote_liability_ccys:
                        details["borrow_check"]["action"] = "sell_only_quote_liability"
                        return LivePreflightResult(
                            decision="SELL_ONLY",
                            reconcile_ok=reconcile_ok,
                            ledger_ok=ledger_ok,
                            kill_switch_enabled=False,
                            reason="borrow_detected_quote_liability",
                            details=details,
                        )
                    details["borrow_check"]["action"] = "symbol_blacklist_only"
                else:
                    return LivePreflightResult(
                        decision="ABORT",
                        reconcile_ok=reconcile_ok,
                        ledger_ok=ledger_ok,
                        kill_switch_enabled=False,
                        reason="borrow_detected",
                        details=details,
                    )
        except Exception as e:
            details["borrow_check"] = {"ok": False, "reason": f"error:{e}"}
            return LivePreflightResult(
                decision="SELL_ONLY",
                reconcile_ok=reconcile_ok,
                ledger_ok=ledger_ok,
                kill_switch_enabled=False,
                reason="borrow_check_error",
                details=details,
            )

        try:
            if bool(getattr(self.cfg, "enforce_account_config_check", True)):
                if not hasattr(self.okx, "get_account_config"):
                    details["account_config"] = {
                        "ok": True,
                        "skipped": True,
                        "reason": "client_missing_get_account_config",
                    }
                else:
                    r = self.okx.get_account_config()
                    rows = (r.data or {}).get("data") if isinstance(r.data, dict) else None
                    cfg0 = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}

                    acct_lv = str(cfg0.get("acctLv", "") or "")
                    pos_mode = str(cfg0.get("posMode", "") or "")
                    auto_loan = _to_bool(cfg0.get("autoLoan"))
                    enable_spot_borrow = _to_bool(cfg0.get("enableSpotBorrow"))
                    spot_auto_repay = _to_bool(cfg0.get("spotBorrowAutoRepay"))
                    fee_type = str(cfg0.get("feeType", "") or "")
                    fee_type_auto_fixed = False

                    if (
                        bool(getattr(self.cfg, "auto_fix_fee_type_zero", False))
                        and fee_type not in {"", "0"}
                        and hasattr(self.okx, "set_fee_type")
                    ):
                        try:
                            rr = self.okx.set_fee_type("0")
                            ok_code = str((rr.data or {}).get("code") or "")
                            if ok_code in {"", "0"}:
                                r = self.okx.get_account_config()
                                rows = (r.data or {}).get("data") if isinstance(r.data, dict) else None
                                cfg0 = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else cfg0
                                acct_lv = str(cfg0.get("acctLv", "") or "")
                                pos_mode = str(cfg0.get("posMode", "") or "")
                                auto_loan = _to_bool(cfg0.get("autoLoan"))
                                enable_spot_borrow = _to_bool(cfg0.get("enableSpotBorrow"))
                                spot_auto_repay = _to_bool(cfg0.get("spotBorrowAutoRepay"))
                                fee_type = str(cfg0.get("feeType", "") or "")
                                fee_type_auto_fixed = fee_type == "0"
                        except Exception as e:
                            details["account_config_auto_fix_fee_type_error"] = str(e)

                    violations = []

                    required_acct_lv = str(getattr(self.cfg, "required_acct_lv", "") or "").strip()
                    if required_acct_lv and acct_lv and acct_lv != required_acct_lv:
                        violations.append(f"acctLv_mismatch:{acct_lv}!={required_acct_lv}")

                    required_pos_mode = str(getattr(self.cfg, "required_pos_mode", "") or "").strip()
                    if required_pos_mode and pos_mode and pos_mode != required_pos_mode:
                        violations.append(f"posMode_mismatch:{pos_mode}!={required_pos_mode}")

                    if bool(getattr(self.cfg, "require_auto_loan_false", True)) and auto_loan:
                        violations.append("autoLoan_true")

                    if bool(getattr(self.cfg, "require_spot_borrow_disabled", False)) and enable_spot_borrow:
                        violations.append("enableSpotBorrow_true")

                    if (
                        bool(getattr(self.cfg, "ensure_spot_auto_repay_true", True))
                        and enable_spot_borrow
                        and (not spot_auto_repay)
                    ):
                        violations.append("spotBorrowAutoRepay_false")

                    if bool(getattr(self.cfg, "require_fee_type_zero", False)) and fee_type not in {"", "0"}:
                        violations.append(f"feeType_mismatch:{fee_type}!=0")

                    details["account_config"] = {
                        "ok": len(violations) == 0,
                        "acctLv": acct_lv,
                        "posMode": pos_mode,
                        "autoLoan": auto_loan,
                        "enableSpotBorrow": enable_spot_borrow,
                        "spotBorrowAutoRepay": spot_auto_repay,
                        "feeType": fee_type,
                        "feeTypeAutoFixed": fee_type_auto_fixed,
                        "violations": violations,
                    }

                    if violations:
                        return LivePreflightResult(
                            decision="SELL_ONLY",
                            reconcile_ok=reconcile_ok,
                            ledger_ok=ledger_ok,
                            kill_switch_enabled=False,
                            reason="account_config_block",
                            details=details,
                        )
        except Exception as e:
            details["account_config"] = {"ok": False, "reason": f"error:{e}"}
            return LivePreflightResult(
                decision="SELL_ONLY",
                reconcile_ok=reconcile_ok,
                ledger_ok=ledger_ok,
                kill_switch_enabled=False,
                reason="account_config_check_error",
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

        if ledger_ok and not reconcile_ok:
            force_allow = bool(getattr(self.cfg, "allow_trade_on_small_reconcile_drift", False))
            if force_allow:
                details["reconcile_warn"] = {
                    "original_ok": False,
                    "allowed": True,
                    "reason": "forced_by_config",
                }
                return LivePreflightResult(
                    decision="ALLOW",
                    reconcile_ok=True,
                    ledger_ok=True,
                    kill_switch_enabled=False,
                    reason="ok_with_forced_config",
                    details=details,
                )

        if not reconcile_ok and not ledger_ok:
            force_allow = bool(getattr(self.cfg, "allow_trade_on_small_reconcile_drift", False))
            if force_allow:
                details["reconcile_warn"] = {
                    "original_ok": False,
                    "allowed": True,
                    "reason": "forced_by_config_emergency",
                }
                return LivePreflightResult(
                    decision="ALLOW",
                    reconcile_ok=True,
                    ledger_ok=True,
                    kill_switch_enabled=False,
                    reason="ok_with_forced_config",
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
