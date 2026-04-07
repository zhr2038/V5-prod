#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class GuidePaths:
    workspace: Path
    output_path: Path


def build_paths(
    workspace: str | Path | None = None,
    output_path: str | Path | None = None,
) -> GuidePaths:
    root = Path(workspace).expanduser().resolve() if workspace is not None else PROJECT_ROOT
    if output_path is None:
        out = root / "reports" / "direct_repay_guide.json"
    else:
        candidate = Path(output_path).expanduser()
        out = candidate if candidate.is_absolute() else (root / candidate)
    return GuidePaths(workspace=root, output_path=out.resolve())


def build_guide() -> dict:
    pepe_borrow = 4_357_782.429
    pepe_needed = 4_353
    estimated_cost_usdt = pepe_needed * 0.0000045
    estimated_cost_cny = estimated_cost_usdt * 7.2
    return {
        "timestamp": datetime.now().isoformat(),
        "pepe_borrow": pepe_borrow,
        "pepe_needed": pepe_needed,
        "estimated_cost_usdt": estimated_cost_usdt,
        "estimated_cost_cny": estimated_cost_cny,
        "steps": [
            "Buy 4,353 PEPE from another exchange.",
            "Transfer PEPE to the OKX deposit address.",
            "Wait for the deposit to arrive.",
            "Repay 4,357,782.429 PEPE on OKX.",
            "Verify the liability is cleared.",
        ],
        "notes": [
            "This is the lowest-friction recovery path.",
            "Estimated cash cost is about $0.02.",
            "Write the guide into the active workspace reports directory.",
        ],
    }


def direct_repay_guide(
    workspace: str | Path | None = None,
    output_path: str | Path | None = None,
) -> Path:
    paths = build_paths(workspace=workspace, output_path=output_path)
    guide = build_guide()
    paths.output_path.parent.mkdir(parents=True, exist_ok=True)
    paths.output_path.write_text(
        json.dumps(guide, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print("Direct repay guide")
    print("=" * 60)
    print(f"PEPE liability: {guide['pepe_borrow']:,}")
    print(f"Required PEPE deposit: {guide['pepe_needed']:,}")
    print(f"Estimated cost (USDT): {guide['estimated_cost_usdt']:.6f}")
    print(f"Guide saved to: {paths.output_path}")
    return paths.output_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write a direct repay guide into reports/")
    parser.add_argument(
        "--out",
        default=None,
        help="Optional output path. Relative paths are resolved from the repo root.",
    )
    args = parser.parse_args(argv)
    direct_repay_guide(output_path=args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
