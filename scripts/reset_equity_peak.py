#!/usr/bin/env python3
"""Reset equity_peak_usdt in AccountStore to a sane value.

Use when equity_peak_usdt was corrupted (e.g. huge spikes) and DD throttle is stuck.

This script reads the most recent `reports/runs/*/equity.jsonl` and uses its last `equity`
(as raw accounting equity) to reset `reports/positions.sqlite:account_state.equity_peak_usdt`.

This script does NOT place orders.

Example:
  python3 scripts/reset_equity_peak.py
"""

from __future__ import annotations

import os
import sys
import yaml


def main() -> int:
    # ensure repo root on sys.path when running as a script
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    # find latest equity.jsonl
    import glob
    from pathlib import Path
    import json

    paths = glob.glob(str(Path("reports/runs").joinpath("*").joinpath("equity.jsonl")))
    if not paths:
        print("no reports/runs/*/equity.jsonl found")
        return 2
    paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    p = paths[0]
    lines = Path(p).read_text(encoding="utf-8").strip().splitlines()
    if not lines:
        print(f"empty equity.jsonl: {p}")
        return 2
    last = json.loads(lines[-1])
    eq = float(last.get("equity") or 0.0)
    if eq <= 0:
        print(f"invalid equity in {p}: {eq}")
        return 2

    from src.execution.account_store import AccountStore, AccountState

    acc_store = AccountStore("reports/positions.sqlite")
    acc = acc_store.get()

    print(f"latest_equity_file={p}")
    print(f"old_peak={acc.equity_peak_usdt} cash={acc.cash_usdt} last_equity={eq}")
    acc_store.set(AccountState(cash_usdt=float(acc.cash_usdt), equity_peak_usdt=float(eq)))
    acc2 = acc_store.get()
    print(f"new_peak={acc2.equity_peak_usdt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
