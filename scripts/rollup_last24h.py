from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from configs.runtime_config import load_runtime_config, resolve_runtime_config_path, resolve_runtime_path
from src.execution.fill_store import derive_runtime_reports_dir, derive_runtime_runs_dir
from src.reporting.summary_writer import write_summary


def _resolve_repo_path(value: str | Path | None, *, default: Path) -> Path:
    path = Path(value) if value is not None else default
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


@dataclass(frozen=True)
class RollupPaths:
    reports_dir: Path
    runs_dir: Path


def _resolve_runtime_rollup_paths() -> RollupPaths:
    cfg = load_runtime_config(project_root=PROJECT_ROOT)
    config_path = (PROJECT_ROOT / "configs" / "live_prod.yaml").resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"runtime config not found: {config_path}")
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
    ).resolve()
    reports_dir = derive_runtime_reports_dir(orders_db).resolve()
    return RollupPaths(
        reports_dir=reports_dir,
        runs_dir=derive_runtime_runs_dir(orders_db).resolve(),
    )


def _utc_hour_floor(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)


def window_ids(end_hour_exclusive: datetime, hours: int = 24) -> List[str]:
    out = []
    for i in range(hours, 0, -1):
        h = end_hour_exclusive - timedelta(hours=i)
        out.append(h.strftime("%Y%m%d_%H"))
    return out


def _read_equity_jsonl(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    rows: List[Dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            pass
    return rows


def _read_trades_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def rollup(end_hour_exclusive: datetime, runs_dir: Path, out_dir: Path, hours: int = 24) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    eq_all: List[Dict] = []
    tr_all: List[Dict[str, str]] = []

    for wid in window_ids(end_hour_exclusive, hours=hours):
        run_dir = runs_dir / wid
        eq_all.extend(_read_equity_jsonl(run_dir / "equity.jsonl"))
        tr_all.extend(_read_trades_csv(run_dir / "trades.csv"))

    if eq_all:
        (out_dir / "equity.jsonl").write_text(
            "\n".join(json.dumps(row, ensure_ascii=False) for row in eq_all) + "\n",
            encoding="utf-8",
        )

    if tr_all:
        cols = list(tr_all[0].keys())
        with (out_dir / "trades.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()
            for row in tr_all:
                writer.writerow(row)

    window_end_ts = int(end_hour_exclusive.timestamp())
    window_start_ts = int((end_hour_exclusive - timedelta(hours=hours)).timestamp())
    write_summary(str(out_dir), window_start_ts=window_start_ts, window_end_ts=window_end_ts)
    return out_dir / "summary.json"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--runs_dir", default=None)
    ap.add_argument("--out_dir", default=None, help="default: reports/rollups/last24h_YYYYMMDD_HH")
    ap.add_argument("--v4_reports_dir", default=None)
    ap.add_argument("--compare_out", default=None, help="default: reports/compare/daily/v4_vs_v5_last24h_YYYYMMDD_HH.md")
    args = ap.parse_args()

    end = _utc_hour_floor(datetime.now(timezone.utc))
    tag = end.strftime("%Y%m%d_%H")

    runtime_paths = _resolve_runtime_rollup_paths()
    runs_dir = _resolve_repo_path(args.runs_dir, default=runtime_paths.runs_dir)
    out_dir = _resolve_repo_path(args.out_dir, default=runtime_paths.reports_dir / "rollups" / f"last24h_{tag}")
    v4_reports_dir = _resolve_repo_path(args.v4_reports_dir, default=PROJECT_ROOT / "v4_export")

    v5_summary = rollup(end, runs_dir=runs_dir, out_dir=out_dir, hours=int(args.hours))

    compare_out = _resolve_repo_path(
        args.compare_out,
        default=runtime_paths.reports_dir / "compare" / "daily" / f"v4_vs_v5_last24h_{tag}.md",
    )
    compare_out.parent.mkdir(parents=True, exist_ok=True)

    subprocess.check_call(
        [
            sys.executable,
            str(Path(__file__).resolve().parents[0] / "compare_runs.py"),
            "--v4_reports_dir",
            str(v4_reports_dir),
            "--v5_summary",
            str(v5_summary),
            "--out",
            str(compare_out),
        ]
    )

    print(f"wrote {v5_summary}")
    print(f"wrote {compare_out}")


if __name__ == "__main__":
    main()
