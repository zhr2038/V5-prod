"""Public acquisition and atomic, restart-safe paired paper observations."""
from __future__ import annotations

import ast
import hashlib
import json
import shutil
import sqlite3
import zlib
from contextlib import closing
from datetime import datetime

from src.reporting.participation_runtime import _save_report
from src.research.paired_reference_paper import ACCOUNTS, PairedComparison, summarize, validate_experiment
from src.research.review_comparison import digest, validate_frame
from src.research.review_forward import collect as collect_public
from src.research.reference_contract import reference_use


def references_at(path, now, diagnostics):
    """Newest received four-hour publication wins, including inadmissible versions."""
    diagnostics.update(status="observed" if path.exists() else "missing", live_order_effect="none")
    if not path.exists():
        return {}
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)) as con:
            rows = con.execute("SELECT first_received,payload,receipt FROM advice WHERE first_received<=? AND first_received>=? ORDER BY first_received,advice_id", (now, now - 7200)).fetchall()
        result = {}
        def stamp(value):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("timezone_required")
            return parsed.timestamp()
        for received, raw, receipt_raw in rows:
            advice, receipt = json.loads(raw), json.loads(receipt_raw)
            if advice.get("horizon_hours") != 4:
                continue
            symbol = advice["symbol"].removesuffix("USDT") + "/USDT"
            result[symbol] = {"advice_id": advice["advice_id"], "first_received_ts": received,
                              "published_ts": stamp(receipt["published_at"]), "expires_ts": stamp(advice["expires_at"]),
                              "horizon_hours": 4, "action": advice.get("effective_action", advice["action"]),
                              **advice["eligibility"], "live_order_effect": advice.get("live_order_effect"),
                              "receipt_reason": receipt.get("reason"), "reference_schema": receipt.get("reference_schema"),
                              "experiment_version": receipt.get("experiment_version"), "strategy_version": receipt.get("strategy_version"),
                              "analysis_source_identity": receipt.get("analysis_source_identity"), "cost_version": advice["cost"]["version"]}
        return result
    except (OSError, sqlite3.Error, KeyError, TypeError, ValueError) as exc:
        diagnostics.update(status="reference_read_failed", reason=type(exc).__name__)
        return {}


def collect(*, experiment, root, reference_path):
    frame = collect_public(symbols=experiment["symbols"], root=root, reference_path=reference_path,
                           entry_minimum_notional_usdt=experiment["entry_minimum_notional_usdt"])
    frame["reference_read"] = {}
    frame["references"] = references_at(reference_path, frame["observed_at"], frame["reference_read"])
    return frame


def strategy_dependencies(project):
    """Auditable local import closure; unrelated UI/telemetry code is excluded.

    Include conditional imports and literal dynamic module names conservatively.
    All resulting paths are listed in the manifest rather than hiding a directory
    hash. A changed decision dependency must start a new cohort.
    """
    pending = ["scripts.run_paired_reference_paper"]
    found = set()
    while pending:
        module = pending.pop()
        candidates = [project.joinpath(*module.split(".")).with_suffix(".py"), project.joinpath(*module.split("."), "__init__.py")]
        path = next((p for p in candidates if p.is_file()), None)
        if path is None or path in found:
            continue
        found.add(path)
        package = module.split(".") if path.name == "__init__.py" else module.split(".")[:-1]
        for end in range(1, len(package) + 1):
            pending.append(".".join(package[:end]))
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                prefix = ".".join(package[:len(package) - node.level + 1]) if node.level else ""
                base = ".".join(part for part in (prefix, node.module or "") if part)
                names = [base, *(base + "." + alias.name for alias in node.names)]
            elif isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.startswith(("src.", "configs.")):
                names = [node.value]  # e.g. OriginalV5Adapter's importlib module list.
            pending.extend(name for name in names if name.startswith(("src.", "configs.")))
    return sorted(found)


def source_identity(project, experiment, config_path, cfg):
    """Freeze transitive strategy imports plus effective non-secret configuration."""
    paths = strategy_dependencies(project)
    paths += [config_path, project / "configs/research/paired_reference_paper_v1.json", project / "scripts/run_paired_reference_paper.py"]
    files = {p.relative_to(project).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    effective = cfg.model_dump(mode="json")
    for key in ("api_key", "api_secret", "passphrase"):
        effective.get("exchange", {})[key] = ""
    # Tokens are irrelevant to a network-disabled pipeline and never exported.
    def redact(value):
        if isinstance(value, dict):
            return {k: "" if any(token in k.lower() for token in ("password", "secret", "token", "api_key", "passphrase")) else redact(v) for k, v in value.items()}
        if isinstance(value, list):
            return [redact(v) for v in value]
        return value
    effective = redact(effective)
    value = {"experiment": experiment, "source_hashes": files, "effective_config_hash": digest(effective),
             "dependency_rule": "local_import_closure_plus_literal_dynamic_modules; unrelated_UI_not_included",
             "baseline_scope": experiment["baseline_scope"], "paper_only": True, "live_order_effect": "none"}
    return {**value, "identity": digest(value)}


def _remove_generation(root, generation):
    path = (root / "generations" / generation).resolve()
    parent = (root / "generations").resolve()
    if path.parent != parent or len(generation) != 64 or any(c not in "0123456789abcdef" for c in generation):
        raise ValueError("invalid_paper_generation_path")
    if path.exists():
        shutil.rmtree(path)


def _hydrate(comparison, root, frame):
    for scenario in comparison.scenarios.values():
        for name in ACCOUNTS:
            sandbox = scenario[name]["adapter"].root
            _save_report(sandbox / "reports/okx_spot_instruments.json", frame["instrument_raw"])
            for relative, artifact in frame.get("input_artifacts", {}).items():
                target = (sandbox / relative).resolve()
                if not target.is_relative_to(sandbox) or not relative.startswith(("configs/", "models/", "data/sentiment_cache/")):
                    raise ValueError("unapproved_paper_input_artifact_path")
                if artifact["status"] != "captured":
                    if target.exists():
                        target.unlink()
                    continue
                blob = root / "input-blobs" / artifact["sha256"]
                raw = blob.read_bytes()
                if hashlib.sha256(raw).hexdigest() != artifact["sha256"]:
                    raise ValueError("paper_input_blob_hash_mismatch")
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)


def archive_frame(root, frame):
    """Retain raw inputs without duplicating 600 candles every observed minute."""
    stored = dict(frame)
    stored["frame_storage"] = "content_addressed_market_v1"
    for key in ("market_data", "instrument_raw"):
        raw = json.dumps(frame[key], sort_keys=True, allow_nan=False).encode()
        sha = hashlib.sha256(raw).hexdigest()
        path = root / "frame-blobs" / (sha + ".json.zlib")
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if hashlib.sha256(zlib.decompress(path.read_bytes())).hexdigest() != sha:
                raise ValueError("archived_paper_input_corrupt")
        else:
            with path.open("xb") as stream:
                stream.write(zlib.compress(raw))
        stored[key] = {"sha256": sha, "encoding": "zlib_json", "path": "frame-blobs/" + path.name}
    return json.dumps(stored, allow_nan=False)


def process(*, cfg, experiment, root, frame, identity, comparison_factory=PairedComparison):
    """Commit account state, runtime generation and event together in SQLite.

Strategy writes occur in a new generation. A crash before commit cannot corrupt
the previous generation; no old quote is replayed to produce a new observation.
Failed input bytes remain in acquisitions and are never promoted to evidence.
"""
    validate_experiment(experiment)
    validate_frame(frame)
    if (root / "FROZEN.json").exists():
        raise ValueError("frozen_paper_experiment_is_read_only")
    root.mkdir(parents=True, exist_ok=True)
    input_hash = digest(frame)
    with closing(sqlite3.connect(root / "comparison.sqlite", timeout=10)) as con:
        con.execute("PRAGMA synchronous=FULL")
        con.execute("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        con.execute("CREATE TABLE IF NOT EXISTS acquisitions(input_hash TEXT PRIMARY KEY,observed_at REAL NOT NULL,frame TEXT NOT NULL,status TEXT NOT NULL,error TEXT)")
        con.execute("CREATE TABLE IF NOT EXISTS events(observed_at REAL PRIMARY KEY,input_hash TEXT NOT NULL UNIQUE,event TEXT NOT NULL,report TEXT NOT NULL)")
        saved = dict(con.execute("SELECT key,value FROM meta"))
        if saved.get("identity") not in {None, identity["identity"]}:
            raise ValueError("frozen_strategy_or_config_changed_requires_new_experiment")
        existing = con.execute("SELECT input_hash,report FROM events WHERE observed_at=? OR input_hash=?", (frame["observed_at"], input_hash)).fetchone()
        if existing:
            if existing[0] != input_hash:
                raise ValueError("conflicting_same_time_observation")
            report = json.loads(existing[1])
            _save_report(root / "latest.json", report)
            return report
        if con.execute("SELECT 1 FROM acquisitions WHERE input_hash=?", (input_hash,)).fetchone():
            raise ValueError("uncommitted_old_input_requires_new_natural_observation_not_replay")
        checkpoint = json.loads(saved["checkpoint"]) if saved.get("checkpoint") else None
        if checkpoint and frame["observed_at"] <= checkpoint["last_observed"]:
            raise ValueError("historical_or_reversed_observation_forbidden")
        binding = json.dumps({k: v.get("sha256") for k, v in frame.get("input_artifacts", {}).items() if k.startswith(("models/", "configs/"))}, sort_keys=True)
        if saved.get("input_binding") not in {None, binding}:
            raise ValueError("frozen_strategy_artifact_changed_requires_new_experiment")
        con.execute("INSERT OR IGNORE INTO acquisitions VALUES(?,?,?,'acquired',NULL)", (input_hash, frame["observed_at"], archive_frame(root, frame)))
        con.commit()
        generation = (root / "generations" / input_hash).resolve()
        previous_generation = saved.get("generation")
        if previous_generation:
            if (len(previous_generation) != 64 or any(c not in "0123456789abcdef" for c in previous_generation)
                    or previous_generation == input_hash):
                raise ValueError("invalid_or_conflicting_committed_paper_generation")
        _remove_generation(root, input_hash)  # Only this uncommitted input's staging files.
        if previous_generation:
            previous = root / "generations" / previous_generation
            if not previous.is_dir():
                raise ValueError("committed_paper_runtime_generation_missing")
            shutil.copytree(previous, generation)
        else:
            generation.mkdir(parents=True)
        try:
            comparison = comparison_factory(cfg, experiment, root=generation, checkpoint=checkpoint)
            _hydrate(comparison, root, frame)
            event = comparison.observe(frame)
            report = summarize(comparison, event)
            report.update(identity=identity["identity"], input_hash=input_hash, reference_read=frame.get("reference_read", {}),
                          signal_data=frame.get("signal_data", {"status": "valid"}), storage="independent_paired_paper_sqlite")
            report["latest_reference_observation"] = {
                symbol: reference_use(frame.get("references", {}).get(symbol), frame["observed_at"], experiment["reference_contract"])
                for symbol in experiment["symbols"]}
            con.execute("BEGIN IMMEDIATE")
            con.execute("INSERT INTO events VALUES(?,?,?,?)", (frame["observed_at"], input_hash, json.dumps(event, allow_nan=False), json.dumps(report, allow_nan=False)))
            for key, value in {"identity": identity["identity"], "input_binding": binding, "checkpoint": json.dumps(comparison.checkpoint(), allow_nan=False), "generation": input_hash}.items():
                con.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, value))
            con.execute("UPDATE acquisitions SET status='committed',error=NULL WHERE input_hash=?", (input_hash,))
            con.commit()
        except Exception as exc:
            con.rollback()
            con.execute("UPDATE acquisitions SET status='failed',error=? WHERE input_hash=?", (type(exc).__name__ + ": " + str(exc)[:1000], input_hash))
            con.commit()
            _remove_generation(root, input_hash)
            raise
        _save_report(root / "manifest.json", {**identity, "ledger_start_ts": report["ledger_start_ts"]})
        _save_report(root / "latest.json", report)
        # Runtime snapshots are regenerable state, while all acquired inputs and
        # committed outcomes remain append-only in SQLite. Keep current + prior.
        for path in (root / "generations").iterdir():
            if path.is_dir() and path.name not in {input_hash, previous_generation}:
                _remove_generation(root, path.name)
        return report
