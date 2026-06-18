from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.loader import load_config  # noqa: E402
from src.reporting.cost_probe_plan import (  # noqa: E402
    build_cost_probe_dry_run_plan,
    write_cost_probe_dry_run_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write a read-only V5 cost-probe dry-run plan without submitting orders."
    )
    parser.add_argument("--config", default="configs/live_prod.yaml")
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--plan-out", default=None)
    parser.add_argument("--summary-out", default=None)
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    reports_dir = Path(args.reports_dir)
    plan_path = Path(args.plan_out) if args.plan_out else reports_dir / "cost_probe_plan.csv"
    summary_path = (
        Path(args.summary_out) if args.summary_out else reports_dir / "cost_probe_summary.json"
    )
    rows, summary = build_cost_probe_dry_run_plan(cfg)
    plan_written, summary_written = write_cost_probe_dry_run_outputs(
        rows,
        summary,
        plan_path=plan_path,
        summary_path=summary_path,
    )
    payload = {
        **summary,
        "plan_path": str(plan_written),
        "summary_path": str(summary_written),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
