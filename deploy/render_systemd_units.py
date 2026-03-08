#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from deploy.prod_release import render_unit_text


def _parse_mapping(raw: str) -> tuple[str, str]:
    if "=" not in raw:
        raise argparse.ArgumentTypeError(f"invalid mapping: {raw!r}")
    src_name, dest_name = raw.split("=", 1)
    src_name = src_name.strip()
    dest_name = dest_name.strip()
    if not src_name or not dest_name:
        raise argparse.ArgumentTypeError(f"invalid mapping: {raw!r}")
    return src_name, dest_name


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--src-dir", required=True)
    ap.add_argument("--dst-dir", required=True)
    ap.add_argument("--root", required=True)
    ap.add_argument("--copy-all", action="store_true")
    ap.add_argument("--mapping", action="append", default=[], help="source=dest")
    args = ap.parse_args()

    src_dir = Path(args.src_dir).resolve()
    dst_dir = Path(args.dst_dir).resolve()
    dst_dir.mkdir(parents=True, exist_ok=True)

    mappings: list[tuple[str, str]] = []
    if args.copy_all:
        for path in sorted(src_dir.glob("*.service")) + sorted(src_dir.glob("*.timer")):
            mappings.append((path.name, path.name))

    mappings.extend(_parse_mapping(raw) for raw in args.mapping)

    seen_dest: set[str] = set()
    for src_name, dest_name in mappings:
        if dest_name in seen_dest:
            continue
        seen_dest.add(dest_name)

        src_path = src_dir / src_name
        if not src_path.exists():
            raise FileNotFoundError(src_path)
        dest_path = dst_dir / dest_name
        rendered = render_unit_text(src_path.read_text(encoding="utf-8"), args.root)
        dest_path.write_text(rendered, encoding="utf-8")


if __name__ == "__main__":
    main()
