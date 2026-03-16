#!/usr/bin/env python3
"""
Regime monitor quick health check.

Usage:
  python scripts/check_regime_monitor.py [db_path]
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path


def main() -> int:
    db_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("reports/regime_history.db")
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

    # consecutive sideways high-prob count
    streak = 0
    for r in rows:
        p = r[3]
        if p is not None and float(p) >= 0.8:
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
    for _, _, _, _, aj in rows:
        if not aj:
            continue
        try:
            arr = json.loads(aj)
            if arr:
                recent_alerts.extend(arr)
        except Exception:
            continue
    recent_alerts = sorted(set(recent_alerts))
    if recent_alerts:
        print(f"recent alerts(last20): {recent_alerts}")

    if any(a in critical for a in latest_alerts):
        print("❌ critical regime alert detected")
        return 1

    print("✅ regime monitor healthy")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
