#!/usr/bin/env python3
"""
Auto Risk Evaluator.

Runs on a timer, evaluates recent production runs, and writes the
single-source risk snapshot consumed by both the dashboard and trading
logic.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List


SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.risk.auto_risk_guard import get_auto_risk_guard


REPORTS_DIR = PROJECT_ROOT / "reports"
RUNS_DIR = REPORTS_DIR / "runs"
AUTO_RISK_EVAL_PATH = REPORTS_DIR / "auto_risk_eval.json"


def load_recent_runs(hours: int = 24) -> List[Dict]:
    runs: List[Dict] = []
    cutoff = datetime.now() - timedelta(hours=hours)

    if not RUNS_DIR.exists():
        return runs

    for run_dir in sorted(RUNS_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if not run_dir.is_dir():
            continue
        mtime = datetime.fromtimestamp(run_dir.stat().st_mtime)
        if mtime < cutoff:
            continue

        audit_file = run_dir / "decision_audit.json"
        if not audit_file.exists():
            continue

        try:
            with open(audit_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            data["_run_id"] = run_dir.name
            data["_mtime"] = mtime.isoformat()
            runs.append(data)
        except Exception:
            continue

    return runs


def calculate_metrics(runs: List[Dict]) -> Dict:
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
        total_selected += int(counts.get("selected", 0) or 0)
        total_rebalance += int(counts.get("orders_rebalance", 0) or 0)
        total_rejected += int(counts.get("orders_exit", 0) or 0)

        for rd in run.get("router_decisions", []):
            if rd.get("reason") == "min_notional":
                total_dust += 1

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

        eq_live = get_live_equity_from_okx()
        acc_db = REPORTS_DIR / "positions.sqlite"
        peak = 0.0
        if acc_db.exists():
            con = sqlite3.connect(str(acc_db))
            cur = con.cursor()
            cur.execute("SELECT equity_peak_usdt FROM account_state WHERE k='default'")
            row = cur.fetchone()
            con.close()
            if row and row[0] is not None:
                peak = float(row[0])

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


def _write_eval_snapshot(guard, metrics: Dict, reason: str) -> None:
    AUTO_RISK_EVAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": datetime.now().isoformat(),
        "current_level": guard.current_level,
        "config": guard.get_current_config(),
        "metrics": metrics,
        "reason": reason,
        "history": guard.history[-5:],
    }
    with open(AUTO_RISK_EVAL_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def evaluate_and_switch() -> None:
    guard = get_auto_risk_guard()
    runs = load_recent_runs(hours=12)
    metrics = calculate_metrics(runs)

    if len(runs) < 3:
        reason = f"样本不足 ({len(runs)}轮)，维持当前档位"
        print(f"[AutoRiskEval] {reason}: {guard.current_level}")
        _write_eval_snapshot(guard, metrics, reason)
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
    _write_eval_snapshot(guard, metrics, reason)


def main() -> None:
    print("=" * 60)
    print("V5 自动风险评估")
    print("=" * 60)
    evaluate_and_switch()
    print("=" * 60)


if __name__ == "__main__":
    main()
