from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

from src.alpha.alpha_engine import AlphaSnapshot
from src.core.models import ExecutionReport
from src.regime.regime_engine import RegimeResult
from src.portfolio.portfolio_engine import PortfolioSnapshot


def write_json(path: str, obj: Any) -> None:
    """Write json"""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def dump_run_artifacts(
    reports_dir: str,
    alpha: AlphaSnapshot,
    regime: RegimeResult,
    portfolio: PortfolioSnapshot,
    execution: ExecutionReport,
) -> None:
    """Dump run artifacts"""
    import os

    # Shadow/research mode: avoid overwriting top-level artifacts (alpha_snapshot.json etc)
    # which can confuse operators watching the live bot.
    if str(os.getenv("V5_DISABLE_TOPLEVEL_ARTIFACTS", "0")).strip() in {"1", "true", "TRUE", "yes", "YES"}:
        return

    Path(reports_dir).mkdir(parents=True, exist_ok=True)
    write_json(f"{reports_dir}/alpha_snapshot.json", {
        "raw_factors": alpha.raw_factors,
        "z_factors": alpha.z_factors,
        "scores": alpha.scores,
        "raw_scores": alpha.raw_scores,
        "telemetry_scores": alpha.telemetry_scores,
        "base_scores": alpha.base_scores,
        "base_raw_scores": alpha.base_raw_scores,
        "ml_attribution_scores": alpha.ml_attribution_scores,
        "ml_overlay_scores": alpha.ml_overlay_scores,
        "ml_overlay_raw_scores": alpha.ml_overlay_raw_scores,
        "ml_runtime": alpha.ml_runtime,
    })
    write_json(f"{reports_dir}/regime.json", asdict(regime))
    write_json(f"{reports_dir}/portfolio.json", asdict(portfolio))
    write_json(f"{reports_dir}/execution_report.json", {
        "timestamp": execution.timestamp,
        "dry_run": execution.dry_run,
        "orders": [asdict(o) for o in execution.orders],
        "notes": execution.notes,
    })
