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
    CostProbeEngine,
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
    engine = CostProbeEngine(cfg, reports_dir=reports_dir, project_root=PROJECT_ROOT)
    payload = engine.build()
    written_paths = write_cost_probe_dry_run_outputs(
        payload["plan_rows"],
        payload["summary"],
        order_rows=payload["order_rows"],
        roundtrip_rows=payload["roundtrip_rows"],
        guard_rows=payload["guard_rows"],
        disagreement_rows=payload["disagreement_rows"],
        p3_preflight=payload["p3_preflight"],
        plan_path=plan_path,
        summary_path=summary_path,
        orders_path=reports_dir / "cost_probe_orders.csv",
        roundtrips_path=reports_dir / "cost_probe_roundtrips.csv",
        runtime_guard_path=reports_dir / "runtime_cost_guard.csv",
        disagreement_path=reports_dir / "cost_disagreement.csv",
        p3_preflight_path=reports_dir / "cost_probe_p3_preflight.json",
    )
    output = {
        **payload["summary"],
        "p3_preflight": payload["p3_preflight"],
        "artifact_paths": {key: str(path) for key, path in written_paths.items()},
        "plan_path": str(written_paths["plan_path"]),
        "summary_path": str(written_paths["summary_path"]),
        "p3_preflight_path": str(written_paths["p3_preflight_path"]),
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
