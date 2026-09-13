"""Freeze superseded paper ledgers after their producing services have stopped."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import tempfile
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from src.reporting.participation_runtime import _save_report


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source_files(root: Path, *, excluded: set[str]) -> dict[str, dict[str, int | str]]:
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("paper evidence cannot contain symlinks")
        if not path.is_file() or path.name in excluded or path.name.endswith(("-wal", "-shm")):
            continue
        result[path.relative_to(root).as_posix()] = {"sha256": _sha256(path), "size_bytes": path.stat().st_size}
    return result


def _backup_sqlite(source: Path, destination: Path) -> None:
    descriptor, temporary = tempfile.mkstemp(prefix=".retirement-", suffix=".sqlite", dir=destination.parent)
    os.close(descriptor)
    temporary_path = Path(temporary)
    try:
        with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as input_db:
            with closing(sqlite3.connect(temporary_path)) as output_db:
                input_db.backup(output_db)
                if output_db.execute("PRAGMA quick_check").fetchone() != ("ok",):
                    raise ValueError("retirement SQLite backup failed quick_check")
        os.replace(temporary_path, destination)
    finally:
        temporary_path.unlink(missing_ok=True)


def freeze_review(root: Path, *, reason: str, successor: str, frozen_at: str | None = None) -> dict:
    root = root.resolve()
    marker = root / "FROZEN.json"
    backup = root / "retirement-ledger.sqlite"
    if marker.exists():
        value = json.loads(marker.read_text(encoding="utf-8"))
        if value.get("status") != "FROZEN" or value.get("successor") != successor:
            raise ValueError("existing review freeze marker does not match requested retirement")
        if value.get("retirement_backup_sha256") != _sha256(backup):
            raise ValueError("frozen review backup hash mismatch")
        return value
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    latest = json.loads((root / "latest.json").read_text(encoding="utf-8"))
    identity = manifest.get("identity")
    if not identity or latest.get("identity") != identity:
        raise ValueError("review manifest and latest report identity mismatch")
    excluded = {"FROZEN.json", "worker.lock", backup.name}
    before = _source_files(root, excluded=excluded)
    _backup_sqlite(root / "comparison.sqlite", backup)
    with closing(sqlite3.connect(backup.resolve().as_uri() + "?mode=ro", uri=True)) as con:
        meta = dict(con.execute("SELECT key,value FROM meta"))
        if meta.get("identity") != identity or meta.get("processing"):
            raise ValueError("review ledger identity mismatch or interrupted observation")
        event_count, last_observed = con.execute("SELECT count(*),max(observed_at) FROM events").fetchone()
        checkpoint = json.loads(meta["checkpoint"])
        if not event_count or float(checkpoint["last_observed"]) != float(last_observed):
            raise ValueError("review checkpoint does not match the last committed event")
    after = _source_files(root, excluded=excluded)
    if before != after:
        raise ValueError("review evidence changed while it was being frozen")
    value = {
        "schema_version": "v5.paper_experiment_retirement.v1",
        "status": "FROZEN",
        "kind": "superseded_review_comparison",
        "identity": identity,
        "experiment_id": latest.get("experiment_id"),
        "reason": reason,
        "successor": successor,
        "frozen_at": frozen_at or datetime.now(timezone.utc).isoformat(),
        "event_count": event_count,
        "last_observed_at": last_observed,
        "source_files": before,
        "retirement_backup": backup.name,
        "retirement_backup_sha256": _sha256(backup),
        "history_recalculated": False,
        "account_state_migrated": False,
        "live_order_effect": "none",
    }
    _save_report(marker, value)
    return value


def freeze_participation(ledger: Path, *, reason: str, successor: str, frozen_at: str | None = None) -> dict:
    ledger = ledger.resolve()
    root, stem = ledger.parent, ledger.stem
    marker = root / f"{stem}.FROZEN.json"
    backup = root / f"{stem}.retirement.sqlite"
    if marker.exists():
        value = json.loads(marker.read_text(encoding="utf-8"))
        if value.get("status") != "FROZEN" or value.get("successor") != successor:
            raise ValueError("existing participation freeze marker does not match requested retirement")
        if value.get("retirement_backup_sha256") != _sha256(backup):
            raise ValueError("frozen participation backup hash mismatch")
        return value
    candidates = [ledger, root / f"{stem}.latest.json", root / f"{stem}.worker.json"]
    if not all(path.is_file() for path in candidates):
        raise ValueError("participation ledger, latest report and worker status are required")
    before = {path.name: {"sha256": _sha256(path), "size_bytes": path.stat().st_size} for path in candidates}
    latest = json.loads(candidates[1].read_text(encoding="utf-8"))
    worker = json.loads(candidates[2].read_text(encoding="utf-8"))
    _backup_sqlite(ledger, backup)
    with closing(sqlite3.connect(backup.resolve().as_uri() + "?mode=ro", uri=True)) as con:
        identity_row = con.execute("SELECT identity FROM portfolio WHERE id=1").fetchone()
        event_count, last_observed = con.execute("SELECT count(*),max(observed_ts) FROM decisions").fetchone()
    identity = identity_row[0] if identity_row else None
    if not identity or latest.get("identity") != identity or worker.get("identity") != identity or not event_count:
        raise ValueError("participation report, worker and ledger identity mismatch")
    after = {path.name: {"sha256": _sha256(path), "size_bytes": path.stat().st_size} for path in candidates}
    if before != after:
        raise ValueError("participation evidence changed while it was being frozen")
    value = {
        "schema_version": "v5.paper_experiment_retirement.v1",
        "status": "FROZEN",
        "kind": "superseded_participation_forward_paper",
        "identity": identity,
        "reason": reason,
        "successor": successor,
        "frozen_at": frozen_at or datetime.now(timezone.utc).isoformat(),
        "event_count": event_count,
        "last_observed_at": last_observed,
        "source_files": before,
        "retirement_backup": backup.name,
        "retirement_backup_sha256": _sha256(backup),
        "history_recalculated": False,
        "account_state_migrated": False,
        "live_order_effect": "none",
    }
    _save_report(marker, value)
    return value


def main() -> None:
    import fcntl

    parser = argparse.ArgumentParser()
    parser.add_argument("--review-output", required=True)
    parser.add_argument("--participation-ledger", required=True)
    parser.add_argument("--successor", required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    review = Path(args.review_output).resolve()
    participation = Path(args.participation_ledger).resolve()
    if review.parent != (project / "reports/review_comparison").resolve():
        raise ValueError("direct review_comparison experiment directory required")
    if participation.parent != (project / "reports/participation").resolve() or participation.suffix != ".sqlite":
        raise ValueError("participation SQLite ledger under reports/participation required")
    with (review / "worker.lock").open("a", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        results = {
            "review": freeze_review(review, reason=args.reason, successor=args.successor),
            "participation": freeze_participation(
                participation,
                reason=args.reason,
                successor=args.successor,
            ),
        }
    print(json.dumps(results, sort_keys=True))


if __name__ == "__main__":
    main()
