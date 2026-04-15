from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.reporting.summary_writer import refresh_summary_metrics


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, help="e.g. reports/runs/20260215_164735")
    args = ap.parse_args()

    refresh_summary_metrics(args.run_dir)


if __name__ == "__main__":
    main()
