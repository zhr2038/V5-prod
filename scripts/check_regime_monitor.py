#!/usr/bin/env python3
"""
Regime monitor quick health check.

Usage:
  python scripts/check_regime_monitor.py [db_path]
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import resolve_runtime_path


@dataclass(frozen=True)
class RegimeMonitorPaths:
    workspace: Path
    db_path: Path


def build_paths(workspace: Path | None = None, db_path: str | None = None) -> RegimeMonitorPaths:
    root = (workspace or PROJECT_ROOT).resolve()
    resolved_db = Path(resolve_runtime_path(db_path, default="reports/regime_history.db", project_root=root))
    return RegimeMonitorPaths(workspace=root, db_path=resolved_db)


def check_regime_monitor(*, db_path: Path) -> int:
    if not db_path.exists():
        print(f"❌ regime history not found: {db_path}")
        return 2

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM regime_history")
    total = int(cur.fetchone()[0])

    cur.execute(
        """
        SELECT ts_ms, final_state, hmm_state, hmm_sideways_prob, alerts_json
        FROM regime_history
        ORDER BY ts_ms DESC
        LIMIT 20
        """
    )
    rows = cur.fetchall()
    conn.close()

    print(f"DB: {db_path}")
    print(f"rows: {total}")
    if not rows:
        print("⚠️ no regime records yet")
        return 1

    ts_ms, final_state, hmm_state, sideways_prob, alerts_json = rows[0]
    print(f"latest: ts={ts_ms} final={final_state} hmm={hmm_state} sideways_prob={sideways_prob}")

    streak = 0
    for row in rows:
        prob = row[3]
        if prob is not None and float(prob) >= 0.8:
            streak += 1
        else:
            break
    print(f"sideways_prob>=0.8 streak(last20): {streak}")

    critical = {"hmm_predict_none", "all_votes_none", "model_type_mismatch", "hmm_sideways_stuck"}
    latest_alerts = []
    try:
        latest_alerts = json.loads(alerts_json) if alerts_json else []
    except Exception:
        latest_alerts = []

    if latest_alerts:
        print(f"latest alerts: {latest_alerts}")

    recent_alerts = []
    for _, _, _, _, alerts_raw in rows:
        if not alerts_raw:
            continue
        try:
            alerts = json.loads(alerts_raw)
            if alerts:
                recent_alerts.extend(alerts)
        except Exception:
            continue
    recent_alerts = sorted(set(recent_alerts))
    if recent_alerts:
        print(f"recent alerts(last20): {recent_alerts}")

    if any(alert in critical for alert in latest_alerts):
        print("❌ critical regime alert detected")
        return 1

    print("✅ regime monitor healthy")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Quick regime monitor health check.")
    parser.add_argument("db_path", nargs="?", default=None, help="Path to regime_history SQLite DB.")
    args = parser.parse_args(argv)

    paths = build_paths(db_path=args.db_path)
    return check_regime_monitor(db_path=paths.db_path)


if __name__ == "__main__":
    raise SystemExit(main())
