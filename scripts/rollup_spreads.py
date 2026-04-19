from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

# allow running as a script from repo root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.reporting.spread_snapshots import compute_spread_stats
from configs.runtime_config import load_runtime_config, resolve_runtime_config_path, resolve_runtime_path
from src.execution.fill_store import (
    derive_runtime_spread_snapshots_dir,
    derive_runtime_spread_stats_dir,
)


def _resolve_repo_path(value: str | Path | None, *, default: Path) -> Path:
    path = Path(value) if value is not None else default
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _resolve_runtime_dirs(
    *,
    snapshots_dir: str | None,
    out_dir: str | None,
    config_path: str | None,
) -> tuple[Path, Path]:
    if snapshots_dir is not None and out_dir is not None:
        return (
            _resolve_repo_path(snapshots_dir, default=PROJECT_ROOT / "reports" / "spread_snapshots"),
            _resolve_repo_path(out_dir, default=PROJECT_ROOT / "reports" / "spread_stats"),
        )

    resolved_config_path = Path(resolve_runtime_config_path(config_path, project_root=PROJECT_ROOT)).resolve()
    cfg = load_runtime_config(config_path, project_root=PROJECT_ROOT)
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"runtime config is empty or invalid: {resolved_config_path}")
    execution_cfg = cfg.get("execution")
    if not isinstance(execution_cfg, dict):
        raise ValueError(f"runtime config missing execution section: {resolved_config_path}")
    order_store_path = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path"),
            default="reports/orders.sqlite",
            project_root=PROJECT_ROOT,
        )
    ).resolve()
    default_snapshots_dir = derive_runtime_spread_snapshots_dir(order_store_path)
    default_out_dir = derive_runtime_spread_stats_dir(order_store_path)
    return (
        _resolve_repo_path(snapshots_dir, default=default_snapshots_dir),
        _resolve_repo_path(out_dir, default=default_out_dir),
    )


def _utc_yyyymmdd_from_epoch_sec(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y%m%d")


def rollup_day(day_ymd: str, snapshots_dir: Path, out_dir: Path) -> Path:
    path = snapshots_dir / f"{day_ymd}.jsonl"
    out_dir.mkdir(parents=True, exist_ok=True)

    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except Exception:
                continue
            for row in (evt.get("symbols") or []):
                sym = str(row.get("symbol") or "")
                if not sym:
                    continue
                by_symbol.setdefault(sym, []).append(dict(row))

    stats = {
        "day": day_ymd,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "symbols": {},
    }

    for sym, rows in sorted(by_symbol.items()):
        stats["symbols"][sym] = compute_spread_stats(rows)

    out_path = out_dir / f"daily_spread_stats_{day_ymd}.json"
    out_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default=None, help="UTC day YYYYMMDD (default: today UTC)")
    ap.add_argument("--config", default=None)
    ap.add_argument("--snapshots_dir", default=None)
    ap.add_argument("--out_dir", default=None)
    args = ap.parse_args()

    day = args.day
    if not day:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")

    snapshots_dir, out_dir = _resolve_runtime_dirs(
        snapshots_dir=args.snapshots_dir,
        out_dir=args.out_dir,
        config_path=args.config,
    )
    out = rollup_day(day, snapshots_dir=snapshots_dir, out_dir=out_dir)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
