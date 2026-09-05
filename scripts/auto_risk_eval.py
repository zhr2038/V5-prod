#!/usr/bin/env python3
"""
Auto Risk Evaluator.

Runs on a timer, evaluates recent production runs, and writes the
single-source risk snapshot consumed by both the dashboard and trading
logic.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from contextlib import closing
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional


SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import (
    load_runtime_config,
    resolve_runtime_config_path,
    resolve_runtime_env_path,
    resolve_runtime_path,
)
from src.execution.fill_store import (
    derive_fill_store_path,
    derive_position_store_path,
    derive_runtime_auto_risk_eval_path,
    derive_runtime_auto_risk_guard_path,
    derive_runtime_named_json_path,
    derive_runtime_reports_dir,
)
from src.risk.auto_risk_guard import get_auto_risk_guard


REPORTS_DIR = PROJECT_ROOT / "reports"
RUNS_DIR = REPORTS_DIR / "runs"
AUTO_RISK_EVAL_PATH = REPORTS_DIR / "auto_risk_eval.json"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso_now() -> str:
    return _utc_now().isoformat()


def _run_id_epoch(run_id: str) -> float:
    return datetime.strptime(str(run_id), "%Y%m%d_%H").replace(tzinfo=timezone.utc).timestamp()


class AutoRiskEvalPaths:
    def __init__(
        self,
        *,
        reports_dir: Path,
        runs_dir: Path,
        auto_risk_eval_path: Path,
        positions_db: Path,
        auto_risk_guard_path: Path,
        env_path: Path,
        orders_db: Path | None = None,
        fills_db: Path | None = None,
    ) -> None:
        self.reports_dir = reports_dir
        self.runs_dir = runs_dir
        self.auto_risk_eval_path = auto_risk_eval_path
        self.positions_db = positions_db
        self.auto_risk_guard_path = auto_risk_guard_path
        self.env_path = env_path
        self.orders_db = orders_db or positions_db.with_name(positions_db.name.replace("positions", "orders", 1))
        self.fills_db = fills_db or derive_fill_store_path(self.orders_db)


def _resolve_runtime_paths(
    raw_config_path: str | None = None,
    raw_env_path: str | None = None,
) -> AutoRiskEvalPaths:
    config_path = Path(resolve_runtime_config_path(raw_config_path=raw_config_path, project_root=PROJECT_ROOT))
    if not config_path.exists():
        requested = str(raw_config_path).strip() if raw_config_path is not None else str(config_path)
        if requested and requested != str(config_path):
            raise FileNotFoundError(f"runtime config not found: {requested} (resolved: {config_path})")
        raise FileNotFoundError(f"runtime config not found: {config_path}")

    cfg = load_runtime_config(raw_config_path, project_root=PROJECT_ROOT)
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"runtime config is empty or invalid: {config_path}")

    execution_cfg = cfg.get("execution")
    if not isinstance(execution_cfg, dict):
        raise ValueError(f"runtime config missing execution section: {config_path}")

    orders_db = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path"),
            default="reports/orders.sqlite",
            project_root=PROJECT_ROOT,
        )
    )
    reports_dir = derive_runtime_reports_dir(orders_db)
    return AutoRiskEvalPaths(
        reports_dir=reports_dir,
        runs_dir=reports_dir / "runs",
        auto_risk_eval_path=derive_runtime_auto_risk_eval_path(orders_db),
        positions_db=derive_position_store_path(orders_db),
        auto_risk_guard_path=derive_runtime_auto_risk_guard_path(orders_db),
        env_path=Path(resolve_runtime_env_path(raw_env_path, project_root=PROJECT_ROOT)),
        orders_db=orders_db,
        fills_db=derive_fill_store_path(orders_db),
    )


def _sanitize_peak_equity(live_equity: float, peak_equity: float, initial_capital: float = 120.0) -> float:
    # Keep the legacy argument for callers, but never invent capital or erase a
    # genuine large drawdown based on a ratio heuristic. Cash-flow corrections
    # require a reconciled ledger; this evaluator does not mutate the peak.
    live_equity, peak_equity = float(live_equity), float(peak_equity)
    if not math.isfinite(live_equity) or live_equity < 0:
        raise ValueError("live equity is invalid")
    if not math.isfinite(peak_equity) or peak_equity <= 0:
        raise ValueError("historical equity peak is unavailable")
    return max(live_equity, peak_equity)


def _audit_epoch(run: Dict) -> float:
    for key in ("timestamp", "now_ts", "window_start_ts", "_mtime"):
        value = run.get(key)
        if value is None:
            continue
        try:
            epoch = float(value)
            return epoch / 1000 if epoch > 1_000_000_000_000 else epoch
        except (ValueError, TypeError):
            try:
                dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                return dt.replace(tzinfo=dt.tzinfo or timezone.utc).timestamp()
            except ValueError:
                pass
    try:
        return _run_id_epoch(str(run.get("_run_id") or run.get("run_id") or ""))
    except ValueError:
        return 0.0


def _filled_opportunity_metrics(runs: List[Dict], runtime: AutoRiskEvalPaths) -> Dict:
    """Read actual fills without creating stores, modifying journals or syncing."""
    run_ids = {str(run.get("_run_id") or run.get("run_id") or "") for run in runs}
    if "" in run_ids or not run_ids:
        raise ValueError("run identity missing; fills cannot be attributed")
    since_ms = int(min(_audit_epoch(run) for run in runs) * 1000)
    if since_ms <= 0:
        raise ValueError("run timestamps missing; fills window is unknown")
    now_ms = int(_utc_now().timestamp() * 1000)
    with closing(sqlite3.connect(runtime.orders_db.resolve().as_uri() + "?mode=ro", uri=True)) as con:
        con.row_factory = sqlite3.Row
        markers = ",".join("?" for _ in run_ids)
        orders = con.execute(
            f"SELECT cl_ord_id, ord_id, inst_id, run_id, side FROM orders WHERE run_id IN ({markers})",
            sorted(run_ids),
        ).fetchall()
    with closing(sqlite3.connect(runtime.fills_db.resolve().as_uri() + "?mode=ro", uri=True)) as con:
        con.row_factory = sqlite3.Row
        sync = con.execute("SELECT v FROM sync_state WHERE k='last_sync_ts_ms'").fetchone()
        freshness_source = "fills_sync_state"
        if not sync or not 0 <= now_ms - int(sync[0]) <= 2 * 3600 * 1000:
            # The live fills_pre_route importer does not advance the standalone
            # fill_sync cursor. Fresh account reconciliation and ledger checks
            # are an explicit alternative health source, not a claim that the
            # old cursor or last trade timestamp is current.
            for name in ("reconcile_status", "ledger_status"):
                status_path = derive_runtime_named_json_path(runtime.orders_db, name)
                status = json.loads(status_path.read_text(encoding="utf-8"))
                age_ms = now_ms - int(status.get("ts_ms") or 0)
                if status.get("ok") is not True or not 0 <= age_ms <= 30 * 60 * 1000:
                    raise ValueError(f"{name} is not fresh and healthy")
            freshness_source = "reconcile_and_ledger_current"
        fills = con.execute(
            "SELECT trade_id, cl_ord_id, ord_id, inst_id, side, fill_sz, fill_px, fee, fee_ccy, ts_ms "
            "FROM fills WHERE ts_ms<=? ORDER BY ts_ms, trade_id",
            (now_ms,),
        ).fetchall()
    by_clid = {(str(row["cl_ord_id"]), str(row["inst_id"])): row for row in orders if row["cl_ord_id"]}
    by_oid = {(str(row["ord_id"]), str(row["inst_id"])): row for row in orders if row["ord_id"]}
    filled = set()
    sell_fills = 0
    for fill in fills:
        if int(fill["ts_ms"]) < since_ms:
            continue
        size = float(fill["fill_sz"] or 0)
        if not math.isfinite(size) or size <= 0:
            continue
        if str(fill["side"]).lower() == "sell":
            sell_fills += 1
        order = by_clid.get((str(fill["cl_ord_id"]), str(fill["inst_id"])))
        if order is None:
            order = by_oid.get((str(fill["ord_id"]), str(fill["inst_id"])))
        if order is not None and str(order["side"]).lower() == "buy" and str(fill["side"]).lower() == "buy":
            filled.add((str(order["run_id"]), str(order["inst_id"])))
    pnl_values, pnl_warning = _realized_fill_pnl(fills, since_ms=since_ms) if sell_fills else ([], None)
    return {
        "filled_opportunities": len(filled), "sell_fills": sell_fills,
        "submitted_orders": len(orders), "pnl_values": pnl_values, "pnl_warning": pnl_warning,
        "freshness_source": freshness_source,
    }


def _realized_fill_pnl(fills: List, *, since_ms: int) -> tuple[List[float], str | None]:
    """FIFO net PnL per exit order; an unmatched lot or unknown fee fails closed.

    All earlier fills supply inventory, but only exits in the evaluation window
    count as outcomes. Base-denominated fees change inventory, while quote fees
    change cash. This read-only calculation never substitutes for the ledger.
    """
    lots = defaultdict(deque)
    outcomes: Dict = {}
    try:
        for fill in fills:
            symbol = str(fill["inst_id"])
            base, quote = symbol.split("-")
            if quote != "USDT":
                raise ValueError("non-USDT fill cannot be valued")
            side = str(fill["side"]).lower()
            qty, price, fee = float(fill["fill_sz"]), float(fill["fill_px"]), float(fill["fee"] or 0)
            if not all(math.isfinite(v) for v in (qty, price, fee)) or qty <= 0 or price <= 0:
                raise ValueError("invalid fill quantity, price or fee")
            fee_ccy = str(fill["fee_ccy"] or "").upper()
            if fee and fee_ccy not in {base, quote}:
                raise ValueError("fee currency conversion unavailable")
            base_fee = fee if fee_ccy == base else 0.0
            quote_fee = fee if fee_ccy == quote else 0.0
            if side == "buy":
                received = qty + base_fee
                if received <= 0:
                    raise ValueError("invalid net buy quantity")
                lots[symbol].append([received, qty * price - quote_fee])
            elif side == "sell":
                required = qty - base_fee
                remaining = required
                cost = 0.0
                while remaining > 1e-12 and lots[symbol]:
                    lot = lots[symbol][0]
                    used = min(remaining, lot[0])
                    part_cost = lot[1] * used / lot[0]
                    cost += part_cost
                    lot[0] -= used
                    lot[1] -= part_cost
                    remaining -= used
                    if lot[0] <= 1e-12:
                        lots[symbol].popleft()
                if remaining > max(1e-12, required * 1e-8):
                    raise ValueError("sell fill has no matching historical inventory")
                if int(fill["ts_ms"]) >= since_ms:
                    identity = (symbol, str(fill["ord_id"] or fill["cl_ord_id"] or fill["trade_id"]))
                    prior = outcomes.get(identity, (0, 0.0))
                    outcomes[identity] = (int(fill["ts_ms"]), prior[1] + qty * price + quote_fee - cost)
            else:
                raise ValueError("unknown fill side")
    except (ValueError, TypeError, KeyError) as exc:
        return [], str(exc)
    return [value[1] for value in sorted(outcomes.values(), key=lambda value: value[0])], None


def load_recent_runs(hours: int = 24, *, runtime_paths: Optional[AutoRiskEvalPaths] = None) -> List[Dict]:
    runs: List[Dict] = []
    cutoff = _utc_now() - timedelta(hours=hours)
    runs_dir = (runtime_paths or _resolve_runtime_paths()).runs_dir

    if not runs_dir.exists():
        return runs

    def _candidate_sort_epoch(run_dir: Path) -> float:
        try:
            # Use the end of the run hour as a lightweight upper bound for cutoff filtering.
            return _run_id_epoch(run_dir.name) + 3600.0
        except Exception:
            audit_file = run_dir / "decision_audit.json"
            try:
                return audit_file.stat().st_mtime
            except Exception:
                return 0.0

    def _sort_epoch(run_dir: Path, payload: Dict) -> float:
        audit_file = run_dir / "decision_audit.json"
        for key in ("timestamp", "now_ts", "window_start_ts"):
            value = payload.get(key) if isinstance(payload, dict) else None
            if value is None:
                continue
            try:
                return float(value)
            except Exception:
                pass

        run_id = str(payload.get("run_id") or run_dir.name) if isinstance(payload, dict) else run_dir.name
        try:
            return _run_id_epoch(run_id)
        except Exception:
            try:
                return audit_file.stat().st_mtime
            except Exception:
                return 0.0

    run_dirs = sorted(runs_dir.iterdir(), key=_candidate_sort_epoch, reverse=True)
    run_entries: List[tuple[float, Dict]] = []
    for run_dir in run_dirs:
        if not run_dir.is_dir():
            continue
        audit_file = run_dir / "decision_audit.json"
        if not audit_file.exists():
            continue
        if datetime.fromtimestamp(_candidate_sort_epoch(run_dir), timezone.utc) < cutoff:
            continue

        try:
            with open(audit_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        sort_epoch = _sort_epoch(run_dir, data)
        sort_dt = datetime.fromtimestamp(sort_epoch, timezone.utc)
        if sort_dt < cutoff:
            continue
        data["_run_id"] = run_dir.name
        data["_mtime"] = sort_dt.isoformat()
        run_entries.append((sort_epoch, data))

    run_entries.sort(key=lambda item: item[0], reverse=True)
    runs = [data for _, data in run_entries]

    return runs


def calculate_metrics(runs: List[Dict], *, runtime_paths: Optional[AutoRiskEvalPaths] = None) -> Dict:
    warnings: List[str] = []
    runs = sorted(runs, key=_audit_epoch)
    if not runs:
        return {
            "dd_pct": 0.0,
            "conversion_rate": None,
            "proposal_conversion_rate": None,
            "conversion_source": "unavailable",
            "opportunity_status": "no_audit_data",
            "dust_reject_rate": 0.0,
            "pnl_trend": "flat",
            "consecutive_losses": 0,
            "sample_size": 0,
            "total_selected": 0,
            "total_rebalance": 0,
            "recovery_evidence_ok": False,
            "warnings": ["no_recent_audit_data"],
        }

    total_selected = 0
    total_rebalance = 0
    total_rejected = 0
    total_dust = 0
    pnl_values: List[float] = []
    selected_counts_observed = all("selected" in run.get("counts", {}) for run in runs)

    for run in runs:
        counts = run.get("counts", {})
        rejects = run.get("rejects", {}) if isinstance(run, dict) else {}
        total_selected += int(counts.get("selected", 0) or 0)
        total_rebalance += int(counts.get("orders_rebalance", 0) or 0)
        reject_dust = int(rejects.get("min_notional", 0) or 0)
        reject_dust += int(rejects.get("exchange_min_notional", 0) or 0)
        total_rejected += reject_dust

        router_dust = 0
        for rd in run.get("router_decisions", []):
            if rd.get("reason") in {"min_notional", "exchange_min_notional"}:
                router_dust += 1

        total_dust += max(reject_dust, router_dust)

        pnl = run.get("realized_pnl")
        if pnl is not None:
            pnl_values.append(float(pnl))

    proposal_conversion_rate = total_rebalance / total_selected if total_selected > 0 else None
    conversion_rate = None
    conversion_source = "unavailable"
    opportunity_status = "execution_evidence_unavailable"
    fill_metrics: Dict = {}
    try:
        runtime = runtime_paths or _resolve_runtime_paths()
        fill_metrics = _filled_opportunity_metrics(runs, runtime)
        conversion_source = "fills_matched_to_orders"
        if not selected_counts_observed:
            opportunity_status = "candidate_evidence_unavailable"
            warnings.append("selected_candidate_counts_missing; no_opportunities_unproven")
        elif total_selected > 0:
            conversion_rate = min(1.0, fill_metrics["filled_opportunities"] / total_selected)
            opportunity_status = "observed"
        elif fill_metrics["submitted_orders"] == 0:
            opportunity_status = "no_opportunities"
        else:
            warnings.append("orders_present_without_selected_candidates")
    except (OSError, ValueError, TypeError, sqlite3.Error) as exc:
        warnings.append(f"actual_fills_unavailable: {exc}")
    if fill_metrics.get("freshness_source") == "reconcile_and_ledger_current":
        warnings.append("standalone_fills_sync_cursor_stale; using_current_reconcile_and_ledger_health")
    pnl_source = "audit_realized_pnl_fallback" if pnl_values else "unavailable"
    if fill_metrics and fill_metrics.get("pnl_warning") is None:
        pnl_values = fill_metrics["pnl_values"]
        pnl_source = "fills_fifo_exit_orders"
    elif fill_metrics.get("pnl_warning"):
        warnings.append(f"fill_pnl_unavailable: {fill_metrics['pnl_warning']}")
    total_orders = total_selected + total_rejected
    dust_rate = total_dust / total_orders if total_orders > 0 else 0.0

    pnl_trend = "flat"
    if len(pnl_values) >= 6:
        recent = sum(pnl_values[-3:])
        previous = sum(pnl_values[-6:-3])
        tolerance = max(abs(recent), abs(previous)) * 0.05
        if recent > previous + tolerance:
            pnl_trend = "up"
        elif recent < previous - tolerance:
            pnl_trend = "down"

    consecutive_losses = 0
    for pnl in reversed(pnl_values):
        if float(pnl) < 0:
            consecutive_losses += 1
        else:
            break

    if pnl_source == "audit_realized_pnl_fallback":
        warnings.append("pnl_uses_audit_fallback_not_fill_derived_accounting")
    elif pnl_source == "unavailable":
        warnings.append("realized_pnl_unavailable; no_recovery_from_missing_pnl")

    dd_pct = 0.0
    live_dd_computed = False
    try:
        from src.risk.live_equity_fetcher import get_live_equity_from_okx

        runtime = runtime_paths or _resolve_runtime_paths()
        eq_live = get_live_equity_from_okx(
            env_path=str(runtime.env_path),
            project_root=PROJECT_ROOT,
        )
        acc_db = runtime.positions_db
        peak = 0.0
        if acc_db.exists():
            con = sqlite3.connect(acc_db.resolve().as_uri() + "?mode=ro", uri=True)
            cur = con.cursor()
            cur.execute("SELECT equity_peak_usdt FROM account_state WHERE k='default'")
            row = cur.fetchone()
            con.close()
            if row and row[0] is not None:
                peak = float(row[0])

        if eq_live is not None:
            peak = _sanitize_peak_equity(eq_live, peak)
        if eq_live is not None and peak > 0:
            dd_pct = max(0.0, 1.0 - float(eq_live) / float(peak))
            live_dd_computed = True
    except Exception as exc:
        warnings.append(f"live_drawdown_unavailable: {exc}")

    if not live_dd_computed:
        warnings.append("drawdown_uses_audit_fallback; recovery_disabled")
        for run in runs:
            for note in run.get("notes", []):
                if "drawdown" not in str(note).lower():
                    continue
                try:
                    import re

                    match = re.search(r"drawdown[:\s]+([\d.]+)%", str(note), re.IGNORECASE)
                    if match:
                        dd_pct = max(dd_pct, float(match.group(1)) / 100)
                except Exception:
                    pass

    return {
        "dd_pct": dd_pct,
        "conversion_rate": conversion_rate,
        "proposal_conversion_rate": proposal_conversion_rate,
        "conversion_source": conversion_source,
        "opportunity_status": opportunity_status,
        "filled_opportunities": fill_metrics.get("filled_opportunities"),
        "fills_freshness_source": fill_metrics.get("freshness_source", "unavailable"),
        "dust_reject_rate": dust_rate,
        "pnl_trend": pnl_trend,
        "consecutive_losses": consecutive_losses,
        "pnl_source": pnl_source,
        "pnl_observation_count": len(pnl_values),
        "drawdown_source": "live_equity_and_historical_peak" if live_dd_computed else "audit_fallback",
        "recovery_evidence_ok": bool(
            live_dd_computed
            and opportunity_status in {"observed", "no_opportunities"}
            and pnl_source == "fills_fifo_exit_orders"
        ),
        "warnings": warnings,
        "sample_size": len(runs),
        "total_selected": total_selected,
        "total_rebalance": total_rebalance,
    }


def _write_eval_snapshot(guard, metrics: Dict, reason: str, *, runtime_paths: AutoRiskEvalPaths | None = None) -> None:
    eval_path = (runtime_paths or _resolve_runtime_paths()).auto_risk_eval_path
    eval_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": _utc_iso_now(),
        "current_level": guard.current_level,
        "config": guard.get_current_config(),
        "metrics": metrics,
        "reason": reason,
        "history": guard.history[-5:],
    }
    with open(eval_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def evaluate_and_switch(
    *,
    config_path: str | None = None,
    env_path: str | None = None,
) -> None:
    runtime_paths = _resolve_runtime_paths(config_path, env_path)
    try:
        guard = get_auto_risk_guard(str(runtime_paths.auto_risk_guard_path))
    except TypeError:
        guard = get_auto_risk_guard()
    runs = load_recent_runs(hours=12, runtime_paths=runtime_paths)
    metrics = calculate_metrics(runs, runtime_paths=runtime_paths)

    if len(runs) < 3:
        reason = f"样本不足 ({len(runs)}轮)，维持当前档位"
        print(f"[AutoRiskEval] {reason}: {guard.current_level}")
        _write_eval_snapshot(guard, metrics, reason, runtime_paths=runtime_paths)
        return

    for warning in metrics.get("warnings", []):
        print(f"[AutoRiskEval] WARNING: {warning}")
    conversion_text = f"{metrics['conversion_rate']:.1%}" if metrics["conversion_rate"] is not None else metrics["opportunity_status"]
    print(
        f"[AutoRiskEval] 样本: {metrics['sample_size']}轮 | "
        f"实际成交转化率: {conversion_text} | "
        f"回撤: {metrics['dd_pct']:.1%} | "
        f"趋势: {metrics['pnl_trend']}"
    )

    _, _, reason = guard.evaluate(
        dd_pct=metrics["dd_pct"],
        conversion_rate=metrics["conversion_rate"],
        dust_reject_rate=metrics["dust_reject_rate"],
        recent_pnl_trend=metrics["pnl_trend"],
        consecutive_losses=metrics["consecutive_losses"],
        no_trade_opportunities=metrics.get("opportunity_status") == "no_opportunities",
        recovery_evidence_ok=metrics.get("recovery_evidence_ok", False),
    )

    print(f"[AutoRiskEval] 结果: {guard.current_level} | 原因: {reason}")
    _write_eval_snapshot(guard, metrics, reason, runtime_paths=runtime_paths)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--env", default=".env")
    args = parser.parse_args(argv)
    print("=" * 60)
    print("V5 自动风险评估")
    print("=" * 60)
    evaluate_and_switch(config_path=args.config, env_path=args.env)
    print("=" * 60)


if __name__ == "__main__":
    main()
