from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


BUNDLE_PREFIX = "v5_live_followup_bundle_"
BUNDLE_SUFFIX = ".tar.gz"
SHA_SUFFIX = ".tar.gz.sha256"
DEFAULT_KEEP_COUNT = 1000
DEFAULT_MAX_AGE_DAYS = 7.0


@dataclass(frozen=True)
class BundleRetentionResult:
    directory: str
    keep_count: int
    max_age_days: float | None
    kept_bundle_count: int
    deleted_bundle_count: int
    deleted_sha_count: int
    dry_run: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "directory": self.directory,
            "keep_count": self.keep_count,
            "max_age_days": self.max_age_days,
            "kept_bundle_count": self.kept_bundle_count,
            "deleted_bundle_count": self.deleted_bundle_count,
            "deleted_sha_count": self.deleted_sha_count,
            "dry_run": self.dry_run,
        }


def _bundle_paths(directory: Path) -> list[Path]:
    return sorted(
        (
            path
            for path in directory.glob(f"{BUNDLE_PREFIX}*{BUNDLE_SUFFIX}")
            if path.is_file() and not path.name.endswith(SHA_SUFFIX)
        ),
        key=lambda path: (path.stat().st_mtime, path.name),
        reverse=True,
    )


def _delete_paths(paths: Iterable[Path], *, dry_run: bool) -> int:
    deleted = 0
    for path in paths:
        if not path.exists():
            continue
        deleted += 1
        if not dry_run:
            path.unlink(missing_ok=True)
    return deleted


def prune_v5_bundle_exports(
    directory: Path | str,
    *,
    keep_count: int = DEFAULT_KEEP_COUNT,
    max_age_days: float | None = DEFAULT_MAX_AGE_DAYS,
    now: float | None = None,
    dry_run: bool = False,
) -> BundleRetentionResult:
    """Prune V5 follow-up bundles without touching the latest retained evidence."""

    directory = Path(directory)
    keep_count = max(1, int(keep_count))
    now = time.time() if now is None else float(now)
    if not directory.exists():
        return BundleRetentionResult(
            directory=str(directory),
            keep_count=keep_count,
            max_age_days=max_age_days,
            kept_bundle_count=0,
            deleted_bundle_count=0,
            deleted_sha_count=0,
            dry_run=dry_run,
        )
    if not os.access(directory, os.R_OK | os.X_OK):
        raise PermissionError(f"bundle export directory is not readable: {directory}")

    bundles = _bundle_paths(directory)
    keep: set[Path] = set(bundles[:keep_count])
    delete_bundles: set[Path] = set(bundles[keep_count:])
    if max_age_days is not None:
        cutoff = now - float(max_age_days) * 86400.0
        delete_bundles.update(path for path in bundles if path.stat().st_mtime < cutoff)
        keep.difference_update(delete_bundles)

    deleted_bundle_count = _delete_paths(sorted(delete_bundles), dry_run=dry_run)

    kept_bundles = set(_bundle_paths(directory)) if not dry_run else set(bundles) - delete_bundles
    expected_sha = {Path(f"{path}.sha256") for path in kept_bundles}
    sha_paths = {
        path
        for path in directory.glob(f"{BUNDLE_PREFIX}*{SHA_SUFFIX}")
        if path.is_file()
    }
    delete_sha = {
        path
        for path in sha_paths
        if path not in expected_sha or not Path(str(path)[: -len(".sha256")]).is_file()
    }
    if dry_run:
        delete_sha.update(Path(f"{path}.sha256") for path in delete_bundles if Path(f"{path}.sha256").exists())
    deleted_sha_count = _delete_paths(sorted(delete_sha), dry_run=dry_run)

    return BundleRetentionResult(
        directory=str(directory),
        keep_count=keep_count,
        max_age_days=max_age_days,
        kept_bundle_count=len(kept_bundles),
        deleted_bundle_count=deleted_bundle_count,
        deleted_sha_count=deleted_sha_count,
        dry_run=dry_run,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prune V5 live follow-up bundle exports")
    parser.add_argument("directory", nargs="?", default="/var/lib/v5/exports/bundles")
    parser.add_argument("--keep-count", type=int, default=DEFAULT_KEEP_COUNT)
    parser.add_argument("--max-age-days", type=float, default=DEFAULT_MAX_AGE_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    result = prune_v5_bundle_exports(
        args.directory,
        keep_count=args.keep_count,
        max_age_days=args.max_age_days,
        dry_run=args.dry_run,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
