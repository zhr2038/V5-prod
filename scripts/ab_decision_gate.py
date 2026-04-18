#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import resolve_runtime_config_path
from src.execution.fill_store import derive_runtime_named_json_path, derive_runtime_runs_dir


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


def _load_active_config(*, project_root: Path) -> dict:
    config_path = Path(resolve_runtime_config_path(project_root=project_root))
    try:
        import yaml

        if config_path.exists():
            return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception:
        pass
    return {}


def _resolve_runtime_path(raw_path: object, *, default: str, project_root: Path) -> Path:
    value = str(raw_path or default).strip()
    path = Path(value)
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def _resolve_reports_dir(raw_reports_dir: str | None = None) -> Path:
    if raw_reports_dir is None or not str(raw_reports_dir).strip():
        cfg = _load_active_config(project_root=PROJECT_ROOT)
        execution_cfg = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
        orders_db = _resolve_runtime_path(
            execution_cfg.get("order_store_path"),
            default="reports/orders.sqlite",
            project_root=PROJECT_ROOT,
        )
        return derive_runtime_runs_dir(orders_db).parent.resolve()
    path = Path(str(raw_reports_dir).strip())
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return path


def _resolve_deadband_params(*, project_root: Path) -> tuple[float, float]:
    current_deadband = 0.04
    try:
        cfg = _load_active_config(project_root=project_root)
        rebalance_cfg = cfg.get("rebalance", {}) if isinstance(cfg, dict) else {}
        current_deadband = float(rebalance_cfg.get("deadband_sideways", 0.04) or 0.04)
    except Exception:
        current_deadband = 0.04
    proposed_deadband = max(0.0, round(current_deadband - 0.01, 4))
    return current_deadband, proposed_deadband


def _resolve_ab_gate_output_path(reports_dir: Path) -> Path:
    reports_dir = Path(reports_dir).resolve()
    default_orders = (reports_dir / "orders.sqlite").resolve()
    if default_orders.exists():
        return derive_runtime_named_json_path(default_orders, "ab_gate_status").resolve()

    candidates = sorted(
        {
            candidate.resolve()
            for candidate in reports_dir.glob("*orders*.sqlite")
            if candidate.is_file()
        }
    )
    if len(candidates) == 1:
        return derive_runtime_named_json_path(candidates[0], "ab_gate_status").resolve()
    return (reports_dir / "ab_gate_status.json").resolve()


def load_runs(runs_dir: Path, limit: int = 120):
    if not runs_dir.exists():
        return []
    ds = [d for d in runs_dir.iterdir() if d.is_dir() and (d / "decision_audit.json").exists()]
    ds.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return ds[:limit]


def calc_current(run_dirs):
    s = Stat()
    deadband_drifts = []
    for d in run_dirs:
        try:
            obj = json.loads((d / "decision_audit.json").read_text(encoding="utf-8"))
            c = obj.get("counts") or {}
            s.rounds += 1
            s.selected += int(c.get("selected", 0) or 0)
            s.rebalance += int(c.get("orders_rebalance", 0) or 0)
            s.exits += int(c.get("orders_exit", 0) or 0)
            for rd in (obj.get("router_decisions") or []):
                if rd.get("reason") == "deadband":
                    s.deadband_blocks += 1
                    try:
                        deadband_drifts.append(abs(float(rd.get("drift") or 0)))
                    except Exception:
                        pass
        except Exception:
            continue
    return s, deadband_drifts


def simulate_candidate(current: Stat, deadband_drifts, old_deadband=0.04, new_deadband=0.03):
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
    uplift = sim_conv - cur_conv
    rel_uplift = (uplift / cur_conv) if cur_conv > 0 else 0.0

    ok = cur.rounds >= 30 and rel_uplift >= 0.08 and uplift >= 0.02
    return ok, {
        "current_conversion": round(cur_conv, 4),
        "candidate_conversion": round(sim_conv, 4),
        "uplift_abs": round(uplift, 4),
        "uplift_rel": round(rel_uplift, 4),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reports-dir", default=None)
    parser.add_argument("--limit", type=int, default=120)
    args = parser.parse_args()

    reports_dir = _resolve_reports_dir(args.reports_dir)
    runs_dir = reports_dir / "runs"
    out_path = _resolve_ab_gate_output_path(reports_dir)
    current_deadband, proposed_deadband = _resolve_deadband_params(project_root=PROJECT_ROOT)

    runs = load_runs(runs_dir, limit=args.limit)
    cur, drifts = calc_current(runs)
    sim, opened = simulate_candidate(cur, drifts, old_deadband=current_deadband, new_deadband=proposed_deadband)
    switch, detail = decide(cur, sim)

    out = {
        "ts": datetime.now().isoformat(),
        "window_runs": cur.rounds,
        "current": {
            "selected": cur.selected,
            "rebalance": cur.rebalance,
            "deadband_blocks": cur.deadband_blocks,
            "conversion": round(cur.conversion, 4),
        },
        "candidate": {
            "rebalance": sim.rebalance,
            "deadband_blocks": sim.deadband_blocks,
            "conversion": round(sim.conversion, 4),
            "estimated_opened_from_deadband": int(opened),
            "deadband_sideways": proposed_deadband,
        },
        "decision": {
            "switch_recommended": bool(switch),
            "reason": "meets_gate" if switch else "insufficient_evidence",
            **detail,
        },
        "current_params": {"deadband_sideways": current_deadband},
        "proposed_params": {"deadband_sideways": proposed_deadband},
        "note": "advisory_only_no_auto_apply",
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
