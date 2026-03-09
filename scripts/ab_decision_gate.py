#!/usr/bin/env python3
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

REPORTS_DIR = Path('/home/admin/clawd/v5-trading-bot/reports')
RUNS_DIR = REPORTS_DIR / 'runs'
OUT = REPORTS_DIR / 'ab_gate_status.json'


@dataclass
class Stat:
    rounds: int = 0
    selected: int = 0
    rebalance: int = 0
    exits: int = 0
    deadband_blocks: int = 0

    @property
    def conversion(self) -> float:
        if self.selected <= 0:
            return 0.0
        return self.rebalance / self.selected


def load_runs(limit: int = 120):
    if not RUNS_DIR.exists():
        return []
    ds = [d for d in RUNS_DIR.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
    ds.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return ds[:limit]


def calc_current(run_dirs):
    s = Stat()
    deadband_drifts = []
    for d in run_dirs:
        try:
            obj = json.loads((d / 'decision_audit.json').read_text(encoding='utf-8'))
            c = obj.get('counts') or {}
            s.rounds += 1
            s.selected += int(c.get('selected', 0) or 0)
            s.rebalance += int(c.get('orders_rebalance', 0) or 0)
            s.exits += int(c.get('orders_exit', 0) or 0)
            for rd in (obj.get('router_decisions') or []):
                if rd.get('reason') == 'deadband':
                    s.deadband_blocks += 1
                    try:
                        deadband_drifts.append(abs(float(rd.get('drift') or 0)))
                    except Exception:
                        pass
        except Exception:
            continue
    return s, deadband_drifts


def simulate_candidate(current: Stat, deadband_drifts, old_deadband=0.04, new_deadband=0.03):
    # 简化模拟：old挡住但new放行的部分，视为潜在新增rebalance
    opened = sum(1 for x in deadband_drifts if new_deadband < x <= old_deadband)
    sim = Stat(
        rounds=current.rounds,
        selected=current.selected,
        rebalance=current.rebalance + opened,
        exits=current.exits,
        deadband_blocks=max(0, current.deadband_blocks - opened),
    )
    return sim, opened


def decide(cur: Stat, sim: Stat):
    cur_conv = cur.conversion
    sim_conv = sim.conversion
    uplift = (sim_conv - cur_conv)
    rel_uplift = (uplift / cur_conv) if cur_conv > 0 else 0.0

    # 守门规则（保守）：
    # 1) 至少30轮样本
    # 2) 转化率相对提升>=8%
    # 3) 绝对提升>=2pct
    ok = (
        cur.rounds >= 30
        and rel_uplift >= 0.08
        and uplift >= 0.02
    )
    return ok, {
        'current_conversion': round(cur_conv, 4),
        'candidate_conversion': round(sim_conv, 4),
        'uplift_abs': round(uplift, 4),
        'uplift_rel': round(rel_uplift, 4),
    }


def main():
    runs = load_runs(limit=120)
    cur, drifts = calc_current(runs)
    sim, opened = simulate_candidate(cur, drifts)
    switch, detail = decide(cur, sim)

    out = {
        'ts': datetime.now().isoformat(),
        'window_runs': cur.rounds,
        'current': {
            'selected': cur.selected,
            'rebalance': cur.rebalance,
            'deadband_blocks': cur.deadband_blocks,
            'conversion': round(cur.conversion, 4),
        },
        'candidate': {
            'rebalance': sim.rebalance,
            'deadband_blocks': sim.deadband_blocks,
            'conversion': round(sim.conversion, 4),
            'estimated_opened_from_deadband': int(opened),
        },
        'decision': {
            'switch_recommended': bool(switch),
            'reason': 'meets_gate' if switch else 'insufficient_evidence',
            **detail,
        },
        'note': 'advisory_only_no_auto_apply',
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(out, ensure_ascii=False))


if __name__ == '__main__':
    main()
