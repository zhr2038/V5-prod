#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.reporting.probe_lifecycle import build_probe_lifecycle_reports
from src.reporting.btc_leadership_label_consistency import update_btc_leadership_label_issues


def _parse_asof(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate FIFO trade roundtrips and probe lifecycle audit for a V5 live bundle."
    )
    parser.add_argument(
        "bundle_root",
        nargs="?",
        default=".",
        help="Bundle root containing raw/recent_runs and summaries directories.",
    )
    parser.add_argument("--hours", type=int, default=72)
    parser.add_argument("--asof", default=None, help="Optional ISO UTC as-of timestamp for the lookback cutoff.")
    args = parser.parse_args()

    result = build_probe_lifecycle_reports(
        Path(args.bundle_root),
        hours=int(args.hours),
        asof=_parse_asof(args.asof),
    )
    result["btc_leadership_label_consistency"] = update_btc_leadership_label_issues(Path(args.bundle_root))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
