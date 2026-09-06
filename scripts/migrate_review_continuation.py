"""Offline research-only continuation; caller stops the old timer, CLI locks worker.

No account replay, no new cash, no rewritten events, no trading entrypoint.
The old directory stays frozen. Publish current.json only after the first
successful natural observation in the destination.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
from contextlib import closing


def hashes(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("research continuation forbids symlinked account inputs")
        if path.is_file() and path.name != "worker.lock":
            result[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def prepare_continuation(predecessor, destination, identity):
    predecessor, destination = predecessor.resolve(), destination.resolve()
    if destination.parent != predecessor.parent or destination == predecessor or destination.exists():
        raise ValueError("new sibling research directory required")
    manifest = json.loads((predecessor / "manifest.json").read_text())
    if manifest["identity"] == identity["identity"] or manifest["experiment"] != identity["experiment"]:
        raise ValueError("continuation must change source identity but preserve frozen experiment")
    allowed = {"src/research/review_forward.py", "src/research/review_comparison.py",
               "src/research/review_acceptance.py", "src/research/review_integrity.py",
               "configs/research/review_integrity_v1.json"}
    old, new = manifest["source_hashes"], identity["source_hashes"]
    changed = {k for k in set(old) | set(new) if old.get(k) != new.get(k)}
    if not changed or changed - allowed:
        raise ValueError("continuation source scope exceeds collection and read-only evidence repair")
    before = hashes(predecessor)
    with closing(sqlite3.connect((predecessor / "comparison.sqlite").as_uri() + "?mode=ro", uri=True)) as con:
        meta = dict(con.execute("SELECT key,value FROM meta"))
        if meta.get("identity") != manifest["identity"] or meta.get("processing"):
            raise ValueError("source identity mismatch or interrupted processing; audited recovery required")
        checkpoint = json.loads(meta["checkpoint"])
        if "integrity" in checkpoint.get("metrics", {}):
            raise ValueError("this one-time migration only accepts a pre-integrity ledger")
        count, last = con.execute("SELECT count(*),max(observed_at) FROM events").fetchone()
        if not count or last != checkpoint["last_observed"]:
            raise ValueError("source checkpoint does not match final committed event")
        shutil.copytree(predecessor, destination)
        # SQLite backup is authoritative, even if the old database used WAL.
        with closing(sqlite3.connect(destination / "comparison.sqlite")) as output:
            con.backup(output)
    continuation = {"kind": "collection_repair_account_continuation_not_new_capital_or_strategy",
                    "predecessor_directory": predecessor.name, "predecessor_identity": manifest["identity"],
                    "successor_identity": identity["identity"], "boundary_ts": last,
                    "inherited_events": count, "predecessor_database_sha256": before["comparison.sqlite"],
                    "checkpoint_sha256": hashlib.sha256(meta["checkpoint"].encode()).hexdigest(),
                    "changed_source_paths": sorted(changed), "historical_events_replayed": False,
                    "new_integrity_evidence_start": "first_successful_natural_successor_observation",
                    "old_events_source_identity": manifest["identity"], "account_state_reset": False}
    with closing(sqlite3.connect(destination / "comparison.sqlite")) as con:
        con.execute("UPDATE meta SET value=? WHERE key='identity'", (identity["identity"],))
        con.execute("INSERT INTO meta VALUES('continuation',?)", (json.dumps(continuation, sort_keys=True),))
        con.commit()
        assert con.execute("SELECT value FROM meta WHERE key='checkpoint'").fetchone()[0] == meta["checkpoint"]
    (destination / "predecessor-manifest.json").write_bytes((predecessor / "manifest.json").read_bytes())
    (destination / "manifest.json").write_text(json.dumps(identity, sort_keys=True, indent=2), encoding="utf-8")
    (destination / "continuation.json").write_text(json.dumps(continuation, sort_keys=True, indent=2), encoding="utf-8")
    after = hashes(predecessor)
    if before != after:
        raise ValueError("source changed during migration; destination must not be activated")
    copied = hashes(destination)
    if any(copied.get(name) != sha for name, sha in before.items()
           if name != "manifest.json" and not name.startswith("comparison.sqlite")):
        raise ValueError("copied account state or evidence mismatch")
    return {**continuation, "predecessor_files": before, "destination_files": copied}


def main():
    import argparse
    import fcntl
    from src.research.review_forward import source_identity
    parser = argparse.ArgumentParser()
    parser.add_argument("--predecessor", required=True)
    parser.add_argument("--destination", required=True)
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    base = (project / "reports/review_comparison").resolve()
    old, new = Path(args.predecessor).resolve(), Path(args.destination).resolve()
    if old.parent != base or new.parent != base:
        raise ValueError("only isolated review account directories may be migrated")
    experiment = json.loads((project / "configs/research/review_experiment_v2.json").read_text())
    identity = source_identity(project, experiment, project / "configs/live_prod.yaml")
    with (old / "worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print(json.dumps(prepare_continuation(old, new, identity), sort_keys=True))


if __name__ == "__main__":
    main()
