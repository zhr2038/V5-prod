from __future__ import annotations

import csv
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.execution.fill_store import (
    derive_position_store_path,
    derive_runtime_named_json_path,
)


COST_PROBE_PLAN_FIELDS = [
    "generated_at",
    "symbol",
    "plan_status",
    "blocked_reasons",
    "dry_run",
    "live_enabled",
    "no_order_submitted",
    "max_notional_usdt",
    "order_style",
    "exit_policy",
    "max_open_seconds",
    "roundtrip_limit",
    "entry_intent",
    "exit_intent",
    "live_order_effect",
]
COST_PROBE_ORDER_FIELDS = [
    "generated_at",
    "symbol",
    "leg",
    "side",
    "intent",
    "order_status",
    "dry_run",
    "live_enabled",
    "no_order_submitted",
    "notional_usdt",
    "order_style",
    "blocked_reasons",
    "live_order_effect",
]
COST_PROBE_ROUNDTRIP_FIELDS = [
    "generated_at",
    "symbol",
    "roundtrip_status",
    "entry_order_status",
    "exit_order_status",
    "max_open_seconds",
    "blocked_reasons",
    "no_order_submitted",
    "live_order_effect",
]
RUNTIME_COST_GUARD_FIELDS = [
    "generated_at",
    "guard_name",
    "status",
    "reason",
    "path",
    "observed_value",
]
COST_DISAGREEMENT_FIELDS = [
    "generated_at",
    "symbol",
    "status",
    "v5_cost_bps",
    "quant_lab_cost_bps",
    "diff_bps",
    "reason",
    "live_order_effect",
]


class CostProbeEngine:
    """Read-only cost-probe dry-run engine.

    The engine evaluates local runtime gates and writes probe artifacts, but it
    never calls exchange or live execution code.
    """

    def __init__(
        self,
        cfg: Any,
        *,
        reports_dir: str | Path = "reports",
        generated_at: datetime | None = None,
        project_root: str | Path | None = None,
    ) -> None:
        self.cfg = cfg
        self.execution = getattr(cfg, "execution", cfg)
        self.reports_dir = Path(reports_dir)
        self.generated_at = (generated_at or datetime.now(UTC)).astimezone(UTC)
        self.project_root = Path(project_root or Path.cwd()).resolve()
        self.order_store_path = _resolve_project_path(
            getattr(self.execution, "order_store_path", "reports/orders.sqlite"),
            project_root=self.project_root,
        )
        self.position_store_path = derive_position_store_path(self.order_store_path)
        self.kill_switch_path = _runtime_json_path(
            getattr(self.execution, "kill_switch_path", None),
            order_store_path=self.order_store_path,
            base_name="kill_switch",
            legacy_default="reports/kill_switch.json",
            project_root=self.project_root,
        )
        self.reconcile_status_path = _runtime_json_path(
            getattr(self.execution, "reconcile_status_path", None),
            order_store_path=self.order_store_path,
            base_name="reconcile_status",
            legacy_default="reports/reconcile_status.json",
            project_root=self.project_root,
        )

    def build(self) -> dict[str, Any]:
        guard_rows, runtime_blockers = self.evaluate_runtime_guards()
        plan_rows, summary = build_cost_probe_dry_run_plan(
            self.cfg,
            generated_at=self.generated_at,
            runtime_blockers=runtime_blockers,
        )
        order_rows = _cost_probe_order_rows(plan_rows)
        roundtrip_rows = _cost_probe_roundtrip_rows(plan_rows)
        disagreement_rows = _cost_disagreement_rows(plan_rows, generated_at=self.generated_at)
        summary = {
            **summary,
            "engine": "CostProbeEngine",
            "runtime_blockers": sorted(set(runtime_blockers)),
            "order_rows": len(order_rows),
            "roundtrip_rows": len(roundtrip_rows),
            "guard_rows": len(guard_rows),
            "disagreement_rows": len(disagreement_rows),
            "order_store_path": str(self.order_store_path),
            "position_store_path": str(self.position_store_path),
            "kill_switch_path": str(self.kill_switch_path),
            "reconcile_status_path": str(self.reconcile_status_path),
        }
        return {
            "plan_rows": plan_rows,
            "summary": summary,
            "order_rows": order_rows,
            "roundtrip_rows": roundtrip_rows,
            "guard_rows": guard_rows,
            "disagreement_rows": disagreement_rows,
        }

    def write(self) -> dict[str, Path]:
        payload = self.build()
        return write_cost_probe_dry_run_outputs(
            payload["plan_rows"],
            payload["summary"],
            order_rows=payload["order_rows"],
            roundtrip_rows=payload["roundtrip_rows"],
            guard_rows=payload["guard_rows"],
            disagreement_rows=payload["disagreement_rows"],
            plan_path=self.reports_dir / "cost_probe_plan.csv",
            summary_path=self.reports_dir / "cost_probe_summary.json",
            orders_path=self.reports_dir / "cost_probe_orders.csv",
            roundtrips_path=self.reports_dir / "cost_probe_roundtrips.csv",
            runtime_guard_path=self.reports_dir / "runtime_cost_guard.csv",
            disagreement_path=self.reports_dir / "cost_disagreement.csv",
        )

    def evaluate_runtime_guards(self) -> tuple[list[dict[str, Any]], list[str]]:
        rows: list[dict[str, Any]] = []
        blockers: list[str] = []
        if _bool_attr(self.execution, "cost_probe_respect_kill_switch", True):
            status, reason, observed = _kill_switch_guard(self.kill_switch_path)
            rows.append(
                self._guard_row(
                    "kill_switch_clean",
                    status=status,
                    reason=reason,
                    path=self.kill_switch_path,
                    observed_value=observed,
                )
            )
            if status != "PASS":
                blockers.append(reason)

        if _bool_attr(self.execution, "cost_probe_require_reconcile_clean", True):
            status, reason, observed = _reconcile_guard(
                self.reconcile_status_path,
                max_age_sec=_int_attr(self.execution, "max_status_age_sec", 180),
                generated_at=self.generated_at,
            )
            rows.append(
                self._guard_row(
                    "reconcile_clean",
                    status=status,
                    reason=reason,
                    path=self.reconcile_status_path,
                    observed_value=observed,
                )
            )
            if status != "PASS":
                blockers.append(reason)

        if _bool_attr(self.execution, "cost_probe_disable_if_order_store_dirty", True):
            status, reason, observed = _order_store_guard(self.order_store_path)
            rows.append(
                self._guard_row(
                    "order_store_clean",
                    status=status,
                    reason=reason,
                    path=self.order_store_path,
                    observed_value=observed,
                )
            )
            if status != "PASS":
                blockers.append(reason)

        if _bool_attr(self.execution, "cost_probe_require_no_existing_position", True):
            status, reason, observed = _position_store_guard(self.position_store_path)
            rows.append(
                self._guard_row(
                    "no_existing_position",
                    status=status,
                    reason=reason,
                    path=self.position_store_path,
                    observed_value=observed,
                )
            )
            if status != "PASS":
                blockers.append(reason)

        if _bool_attr(self.execution, "cost_probe_disable_if_position_state_dirty", True):
            status = "PASS" if not any(row["guard_name"] == "no_existing_position" and row["status"] != "PASS" for row in rows) else "BLOCK"
            reason = "position_state_clean" if status == "PASS" else "position_state_dirty"
            rows.append(
                self._guard_row(
                    "position_state_clean",
                    status=status,
                    reason=reason,
                    path=self.position_store_path,
                    observed_value=reason,
                )
            )
            if status != "PASS":
                blockers.append(reason)

        return rows, sorted(set(blockers))

    def _guard_row(
        self,
        guard_name: str,
        *,
        status: str,
        reason: str,
        path: Path,
        observed_value: str,
    ) -> dict[str, Any]:
        return {
            "generated_at": _iso(self.generated_at),
            "guard_name": guard_name,
            "status": status,
            "reason": reason,
            "path": str(path),
            "observed_value": observed_value,
        }


def build_cost_probe_dry_run_plan(
    cfg: Any,
    *,
    generated_at: datetime | None = None,
    runtime_blockers: list[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build a read-only cost-probe plan without touching exchange state."""

    execution = getattr(cfg, "execution", cfg)
    generated = (generated_at or datetime.now(UTC)).astimezone(UTC)
    symbols = _cost_probe_symbols(execution)
    blockers = [*_cost_probe_blockers(execution), *(runtime_blockers or [])]
    max_orders = max(_int_attr(execution, "cost_probe_max_orders_per_day", 0), 0)
    roundtrip_limit = max(
        _int_attr(execution, "cost_probe_max_roundtrips_per_symbol_per_day", 0),
        0,
    )
    candidate_symbols = symbols[:max_orders] if max_orders > 0 else []
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        symbol_blockers = list(blockers)
        if max_orders <= 0:
            symbol_blockers.append("cost_probe_max_orders_per_day_zero")
        elif symbol not in candidate_symbols:
            symbol_blockers.append("daily_order_limit_exceeded")
        if roundtrip_limit <= 0:
            symbol_blockers.append("roundtrip_limit_zero")
        if _bool_attr(execution, "cost_probe_use_exchange_min_notional", True):
            symbol_blockers.append("exchange_min_notional_check_pending")
        plan_status = "planned" if not symbol_blockers else "blocked"
        rows.append(
            {
                "generated_at": _iso(generated),
                "symbol": symbol,
                "plan_status": plan_status,
                "blocked_reasons": ";".join(sorted(set(symbol_blockers))),
                "dry_run": _bool_attr(execution, "cost_probe_dry_run", True),
                "live_enabled": _bool_attr(execution, "cost_probe_live_enabled", False),
                "no_order_submitted": True,
                "max_notional_usdt": _float_attr(
                    execution,
                    "cost_probe_max_notional_usdt",
                    0.0,
                ),
                "order_style": str(
                    getattr(execution, "cost_probe_order_style", "marketable_limit_ioc")
                    or "marketable_limit_ioc"
                ),
                "exit_policy": str(
                    getattr(execution, "cost_probe_exit_policy", "immediate_flat")
                    or "immediate_flat"
                ),
                "max_open_seconds": _int_attr(execution, "cost_probe_max_open_seconds", 60),
                "roundtrip_limit": roundtrip_limit,
                "entry_intent": "DRY_RUN_ENTRY_ONLY_NO_ORDER",
                "exit_intent": "DRY_RUN_IMMEDIATE_FLAT_NO_ORDER",
                "live_order_effect": "none_read_only_dry_run_plan",
            }
        )

    planned_rows = [row for row in rows if row["plan_status"] == "planned"]
    summary = {
        "generated_at": _iso(generated),
        "state": _summary_state(execution, blockers, planned_rows),
        "dry_run": _bool_attr(execution, "cost_probe_dry_run", True),
        "live_enabled": _bool_attr(execution, "cost_probe_live_enabled", False),
        "no_order_submitted": True,
        "symbols_configured": symbols,
        "planned_symbols": [str(row["symbol"]) for row in planned_rows],
        "blocked_symbols": [
            str(row["symbol"]) for row in rows if row["plan_status"] == "blocked"
        ],
        "plan_rows": len(rows),
        "planned_rows": len(planned_rows),
        "blocked_reasons": sorted(
            {reason for row in rows for reason in str(row["blocked_reasons"]).split(";") if reason}
        ),
        "max_notional_usdt": _float_attr(execution, "cost_probe_max_notional_usdt", 0.0),
        "max_orders_per_day": max_orders,
        "max_roundtrips_per_symbol_per_day": roundtrip_limit,
        "live_order_effect": "none_read_only_dry_run_plan",
    }
    return rows, summary


def write_cost_probe_dry_run_outputs(
    rows: list[dict[str, Any]],
    summary: dict[str, Any],
    *,
    order_rows: list[dict[str, Any]] | None = None,
    roundtrip_rows: list[dict[str, Any]] | None = None,
    guard_rows: list[dict[str, Any]] | None = None,
    disagreement_rows: list[dict[str, Any]] | None = None,
    plan_path: str | Path = "reports/cost_probe_plan.csv",
    summary_path: str | Path = "reports/cost_probe_summary.json",
    orders_path: str | Path = "reports/cost_probe_orders.csv",
    roundtrips_path: str | Path = "reports/cost_probe_roundtrips.csv",
    runtime_guard_path: str | Path = "reports/runtime_cost_guard.csv",
    disagreement_path: str | Path = "reports/cost_disagreement.csv",
) -> dict[str, Path]:
    plan = Path(plan_path)
    summary_file = Path(summary_path)
    orders = Path(orders_path)
    roundtrips = Path(roundtrips_path)
    runtime_guard = Path(runtime_guard_path)
    disagreement = Path(disagreement_path)
    plan.parent.mkdir(parents=True, exist_ok=True)
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    _write_csv(plan, COST_PROBE_PLAN_FIELDS, rows)
    _write_csv(orders, COST_PROBE_ORDER_FIELDS, order_rows or [])
    _write_csv(roundtrips, COST_PROBE_ROUNDTRIP_FIELDS, roundtrip_rows or [])
    _write_csv(runtime_guard, RUNTIME_COST_GUARD_FIELDS, guard_rows or [])
    _write_csv(disagreement, COST_DISAGREEMENT_FIELDS, disagreement_rows or [])
    summary_file.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return {
        "plan_path": plan,
        "summary_path": summary_file,
        "orders_path": orders,
        "roundtrips_path": roundtrips,
        "runtime_guard_path": runtime_guard,
        "disagreement_path": disagreement,
    }


def _cost_probe_blockers(execution: Any) -> list[str]:
    blockers: list[str] = []
    if not _bool_attr(execution, "cost_bootstrap_enabled", False):
        blockers.append("cost_bootstrap_enabled_false")
    if not _bool_attr(execution, "cost_probe_enabled", False):
        blockers.append("cost_probe_enabled_false")
    if not _bool_attr(execution, "cost_probe_dry_run", True):
        blockers.append("cost_probe_dry_run_false")
    if _bool_attr(execution, "cost_probe_live_enabled", False):
        blockers.append("cost_probe_live_enabled_true")
    if _float_attr(execution, "cost_probe_max_notional_usdt", 0.0) <= 0:
        blockers.append("cost_probe_max_notional_usdt_zero")
    return blockers


def _summary_state(
    execution: Any,
    blockers: list[str],
    planned_rows: list[dict[str, Any]],
) -> str:
    if _bool_attr(execution, "cost_probe_live_enabled", False):
        return "LIVE_ENABLED_BLOCKED_BY_DRY_RUN_PLANNER"
    if blockers:
        return "DISABLED"
    if planned_rows:
        return "DRY_RUN_PLAN_READY"
    return "NO_PLAN_ROWS"


def _cost_probe_order_rows(plan_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in plan_rows:
        for leg, side, intent in (
            ("entry", "buy", "DRY_RUN_ENTRY_ONLY_NO_ORDER"),
            ("exit", "sell", "DRY_RUN_IMMEDIATE_FLAT_NO_ORDER"),
        ):
            rows.append(
                {
                    "generated_at": row.get("generated_at"),
                    "symbol": row.get("symbol"),
                    "leg": leg,
                    "side": side,
                    "intent": intent,
                    "order_status": "not_submitted",
                    "dry_run": row.get("dry_run"),
                    "live_enabled": row.get("live_enabled"),
                    "no_order_submitted": True,
                    "notional_usdt": row.get("max_notional_usdt"),
                    "order_style": row.get("order_style"),
                    "blocked_reasons": row.get("blocked_reasons"),
                    "live_order_effect": row.get("live_order_effect"),
                }
            )
    return rows


def _cost_probe_roundtrip_rows(plan_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "generated_at": row.get("generated_at"),
            "symbol": row.get("symbol"),
            "roundtrip_status": (
                "dry_run_ready" if row.get("plan_status") == "planned" else "blocked"
            ),
            "entry_order_status": "not_submitted",
            "exit_order_status": "not_submitted",
            "max_open_seconds": row.get("max_open_seconds"),
            "blocked_reasons": row.get("blocked_reasons"),
            "no_order_submitted": True,
            "live_order_effect": row.get("live_order_effect"),
        }
        for row in plan_rows
    ]


def _cost_disagreement_rows(
    plan_rows: list[dict[str, Any]],
    *,
    generated_at: datetime,
) -> list[dict[str, Any]]:
    symbols = [str(row.get("symbol") or "") for row in plan_rows if row.get("symbol")]
    return [
        {
            "generated_at": _iso(generated_at),
            "symbol": ",".join(symbols),
            "status": "not_evaluated",
            "v5_cost_bps": "",
            "quant_lab_cost_bps": "",
            "diff_bps": "",
            "reason": "no_live_probe_roundtrip_no_cost_disagreement",
            "live_order_effect": "none_read_only_dry_run_plan",
        }
    ]


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _kill_switch_guard(path: Path) -> tuple[str, str, str]:
    payload = _read_json(path)
    if payload is None:
        return "BLOCK", "kill_switch_status_missing", "missing"
    enabled = _json_bool(payload.get("enabled"))
    if "kill_switch" in payload and isinstance(payload.get("kill_switch"), dict):
        enabled = _json_bool(payload["kill_switch"].get("enabled"))
    if enabled:
        return "BLOCK", "kill_switch_enabled", "enabled"
    return "PASS", "kill_switch_clean", "disabled"


def _reconcile_guard(
    path: Path,
    *,
    max_age_sec: int,
    generated_at: datetime,
) -> tuple[str, str, str]:
    payload = _read_json(path)
    if payload is None:
        return "BLOCK", "reconcile_status_missing", "missing"
    ok = _json_bool(payload.get("ok"))
    reason = str(payload.get("reason") or "")
    ts_ms = _int_value(payload.get("generated_ts_ms") or payload.get("ts_ms"))
    if not ok:
        return "BLOCK", f"reconcile_not_ok:{reason or 'unknown'}", json.dumps(payload, ensure_ascii=False)
    if ts_ms > 0:
        age_sec = max(generated_at.timestamp() - ts_ms / 1000.0, 0.0)
        if age_sec > max_age_sec:
            return "BLOCK", "reconcile_status_stale", f"age_sec={age_sec:.1f}"
    return "PASS", "reconcile_clean", "ok"


def _order_store_guard(path: Path) -> tuple[str, str, str]:
    if not path.exists():
        return "PASS", "order_store_missing_assumed_clean", "missing"
    try:
        with sqlite3.connect(str(path), timeout=2.0) as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT COUNT(*) FROM orders
                WHERE state IN ('NEW','SENT','ACK','OPEN','PARTIAL','UNKNOWN')
                """
            )
            open_count = int(cur.fetchone()[0] or 0)
    except Exception as exc:
        return "BLOCK", "order_store_unreadable", str(exc)
    if open_count > 0:
        return "BLOCK", "order_store_open_orders", f"open_orders={open_count}"
    return "PASS", "order_store_clean", "open_orders=0"


def _position_store_guard(path: Path) -> tuple[str, str, str]:
    if not path.exists():
        return "PASS", "position_store_missing_assumed_flat", "missing"
    try:
        with sqlite3.connect(str(path), timeout=2.0) as con:
            cur = con.cursor()
            cur.execute("SELECT symbol, qty FROM positions WHERE qty > 0")
            rows = [(str(symbol), float(qty or 0.0)) for symbol, qty in cur.fetchall()]
    except Exception as exc:
        return "BLOCK", "position_store_unreadable", str(exc)
    if rows:
        return "BLOCK", "existing_position_present", json.dumps(rows, ensure_ascii=False)
    return "PASS", "position_store_flat", "positions=0"


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _runtime_json_path(
    raw_path: str | None,
    *,
    order_store_path: Path,
    base_name: str,
    legacy_default: str,
    project_root: Path,
) -> Path:
    raw = str(raw_path or "").strip()
    if not raw or Path(raw).as_posix() == Path(legacy_default).as_posix():
        return _resolve_project_path(
            derive_runtime_named_json_path(order_store_path, base_name),
            project_root=project_root,
        )
    return _resolve_project_path(raw, project_root=project_root)


def _resolve_project_path(raw_path: str | Path, *, project_root: Path) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = (project_root / path).resolve()
    return path


def _cost_probe_symbols(execution: Any) -> list[str]:
    raw_symbols = getattr(execution, "cost_probe_symbols", None) or []
    symbols: list[str] = []
    for raw in raw_symbols:
        symbol = str(raw or "").strip().upper().replace("-", "/")
        if not symbol:
            continue
        if "/" not in symbol and symbol.endswith("USDT"):
            symbol = f"{symbol[:-4]}/USDT"
        if symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _bool_attr(obj: Any, name: str, default: bool) -> bool:
    value = getattr(obj, name, default)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _json_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _int_value(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _int_attr(obj: Any, name: str, default: int) -> int:
    try:
        return int(float(getattr(obj, name, default) or 0))
    except (TypeError, ValueError):
        return default


def _float_attr(obj: Any, name: str, default: float) -> float:
    try:
        return float(getattr(obj, name, default) or 0.0)
    except (TypeError, ValueError):
        return default


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
