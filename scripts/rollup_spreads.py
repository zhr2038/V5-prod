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


def _resolve_repo_path(value: str | Path | None, *, default: Path) -> Path:
    path = Path(value) if value is not None else default
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


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
    ap.add_argument("--snapshots_dir", default=None)
    ap.add_argument("--out_dir", default=None)
    args = ap.parse_args()

    day = args.day
    if not day:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")

    snapshots_dir = _resolve_repo_path(args.snapshots_dir, default=PROJECT_ROOT / "reports" / "spread_snapshots")
    out_dir = _resolve_repo_path(args.out_dir, default=PROJECT_ROOT / "reports" / "spread_stats")
    out = rollup_day(day, snapshots_dir=snapshots_dir, out_dir=out_dir)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
