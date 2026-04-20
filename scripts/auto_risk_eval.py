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
import sys
from datetime import datetime, timedelta
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
    derive_position_store_path,
    derive_runtime_auto_risk_eval_path,
    derive_runtime_auto_risk_guard_path,
    derive_runtime_reports_dir,
)
from src.risk.auto_risk_guard import get_auto_risk_guard


REPORTS_DIR = PROJECT_ROOT / "reports"
RUNS_DIR = REPORTS_DIR / "runs"
AUTO_RISK_EVAL_PATH = REPORTS_DIR / "auto_risk_eval.json"


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
    ) -> None:
        self.reports_dir = reports_dir
        self.runs_dir = runs_dir
        self.auto_risk_eval_path = auto_risk_eval_path
        self.positions_db = positions_db
        self.auto_risk_guard_path = auto_risk_guard_path
        self.env_path = env_path


def _resolve_runtime_paths(
    raw_config_path: str | None = None,
    raw_env_path: str | None = None,
) -> AutoRiskEvalPaths:
    config_path = Path(resolve_runtime_config_path(raw_config_path=raw_config_path, project_root=PROJECT_ROOT))
    if not config_path.exists():
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
    )


def _sanitize_peak_equity(live_equity: float, peak_equity: float, initial_capital: float = 120.0) -> float:
    live_equity = float(live_equity or 0.0)
    peak_equity = float(peak_equity or 0.0)
    initial_capital = float(initial_capital or 0.0)
    sane_floor = max(live_equity, initial_capital)

    if peak_equity <= 0:
        return sane_floor
    if peak_equity < sane_floor:
        return sane_floor
    if live_equity > 0 and peak_equity > live_equity * 2:
        return sane_floor
    return peak_equity


def load_recent_runs(hours: int = 24, *, runtime_paths: Optional[AutoRiskEvalPaths] = None) -> List[Dict]:
    runs: List[Dict] = []
    cutoff = datetime.now() - timedelta(hours=hours)
    runs_dir = (runtime_paths or _resolve_runtime_paths()).runs_dir

    if not runs_dir.exists():
        return runs

    def _sort_epoch(run_dir: Path) -> float:
        audit_file = run_dir / "decision_audit.json"
        try:
            with open(audit_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            payload = {}

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
            return datetime.strptime(run_id, "%Y%m%d_%H").timestamp()
        except Exception:
            try:
                return audit_file.stat().st_mtime
            except Exception:
                return 0.0

    for run_dir in sorted(runs_dir.iterdir(), key=_sort_epoch, reverse=True):
        if not run_dir.is_dir():
            continue
        audit_file = run_dir / "decision_audit.json"
        if not audit_file.exists():
            continue
        sort_dt = datetime.fromtimestamp(_sort_epoch(run_dir))
        if sort_dt < cutoff:
            continue

        try:
            with open(audit_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            data["_run_id"] = run_dir.name
            data["_mtime"] = sort_dt.isoformat()
            runs.append(data)
        except Exception:
            continue

    return runs


def calculate_metrics(runs: List[Dict], *, runtime_paths: Optional[AutoRiskEvalPaths] = None) -> Dict:
    if not runs:
        return {
            "dd_pct": 0.0,
            "conversion_rate": 0.0,
            "dust_reject_rate": 0.0,
            "pnl_trend": "flat",
            "consecutive_losses": 0,
            "sample_size": 0,
            "total_selected": 0,
            "total_rebalance": 0,
        }

    total_selected = 0
    total_rebalance = 0
    total_rejected = 0
    total_dust = 0
    pnl_values: List[float] = []

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

    conversion_rate = total_rebalance / total_selected if total_selected > 0 else 0.0
    total_orders = total_selected + total_rejected
    dust_rate = total_dust / total_orders if total_orders > 0 else 0.0

    pnl_trend = "flat"
    if len(pnl_values) >= 3:
        recent = sum(pnl_values[-3:])
        previous = sum(pnl_values[-6:-3]) if len(pnl_values) >= 6 else recent
        if recent > previous * 1.05:
            pnl_trend = "up"
        elif recent < previous * 0.95:
            pnl_trend = "down"

    consecutive_losses = 0
    for run in reversed(runs):
        pnl = run.get("realized_pnl")
        if pnl is not None and float(pnl) < 0:
            consecutive_losses += 1
        elif pnl is not None:
            break

    dd_pct = 0.0
    live_dd_computed = False
    try:
        import sqlite3

        from src.risk.live_equity_fetcher import get_live_equity_from_okx

        runtime = runtime_paths or _resolve_runtime_paths()
        eq_live = get_live_equity_from_okx(
            env_path=str(runtime.env_path),
            project_root=PROJECT_ROOT,
        )
        acc_db = runtime.positions_db
        peak = 0.0
        if acc_db.exists():
            con = sqlite3.connect(str(acc_db))
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
    except Exception:
        pass

    if not live_dd_computed:
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
        "dust_reject_rate": dust_rate,
        "pnl_trend": pnl_trend,
        "consecutive_losses": consecutive_losses,
        "sample_size": len(runs),
        "total_selected": total_selected,
        "total_rebalance": total_rebalance,
    }


def _write_eval_snapshot(guard, metrics: Dict, reason: str, *, runtime_paths: AutoRiskEvalPaths | None = None) -> None:
    eval_path = (runtime_paths or _resolve_runtime_paths()).auto_risk_eval_path
    eval_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": datetime.now().isoformat(),
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

    print(
        f"[AutoRiskEval] 样本: {metrics['sample_size']}轮 | "
        f"转化率: {metrics['conversion_rate']:.1%} | "
        f"回撤: {metrics['dd_pct']:.1%} | "
        f"趋势: {metrics['pnl_trend']}"
    )

    _, _, reason = guard.evaluate(
        dd_pct=metrics["dd_pct"],
        conversion_rate=metrics["conversion_rate"],
        dust_reject_rate=metrics["dust_reject_rate"],
        recent_pnl_trend=metrics["pnl_trend"],
        consecutive_losses=metrics["consecutive_losses"],
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
