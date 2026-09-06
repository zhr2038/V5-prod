"""Public-data collection and a separate, fail-fast forward research ledger."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from contextlib import closing
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import requests

from src.data.okx_ccxt_provider import OKXCCXTProvider
from src.reporting.participation_runtime import _save_report
from src.research.review_comparison import Comparison, digest, summarize, validate_frame, validate_signal_data
from src.research.reference_contract import reference_use


def capture_artifacts(project, root, now, symbols):
    from src.regime.hmm_model import hmm_model_info_path
    from src.regime.regime_engine import RegimeEngine

    files = [project / name for name in ("configs/blacklist.json", "configs/borrow_prevention_rules.json", "models/hmm_regime.pkl")]
    files.append(hmm_model_info_path(project / "models/hmm_regime.pkl"))
    for symbol in symbols:
        for prefix in ("funding_", "rss_", "deepseek_", ""):
            matches = list((project / "data/sentiment_cache").glob(prefix + symbol.replace("/", "-") + "_*.json"))
            if matches:
                files.append(max(matches, key=RegimeEngine._sentiment_cache_sort_epoch))
    result = {}
    blobs = root / "input-blobs"
    blobs.mkdir(parents=True, exist_ok=True)
    for path in files:
        relative = path.relative_to(project).as_posix()
        if not path.exists():
            result[relative] = {"status": "missing_at_observation"}
            continue
        before = path.stat()
        if before.st_size > 16 * 1024 * 1024 or before.st_mtime > now:
            raise ValueError("input artifact outside size/time bound: " + relative)
        raw = path.read_bytes()
        if path.stat().st_mtime_ns != before.st_mtime_ns:
            raise ValueError("input artifact changed during capture")
        sha = hashlib.sha256(raw).hexdigest()
        blob = blobs / sha
        if not blob.exists():
            blob.write_bytes(raw)
        result[relative] = {"status": "captured", "sha256": sha, "size": len(raw), "observed_at": now}
    return result


def public_get(path, params):
    if path not in {"/api/v5/public/instruments", "/api/v5/market/tickers"}:
        raise ValueError("public endpoint not allowed")
    response = requests.get("https://www.okx.com" + path, params=params, timeout=(3, 10), allow_redirects=False)
    response.raise_for_status()
    value = response.json()
    if value.get("code") != "0" or not isinstance(value.get("data"), list):
        raise ValueError("public input rejected")
    return value["data"]


def references_at(path, now, *, diagnostics=None):
    """Read failure has no veto authority over a candidate or its hard exits."""
    status = diagnostics if diagnostics is not None else {}
    status.update(status="observed" if path.exists() else "missing", live_order_effect="none")
    try:
        return _read_references_at(path, now)
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError) as exc:
        status.update(status="reference_read_failed", reason=type(exc).__name__)
        return {}


def _read_references_at(path, now):
    if not path.exists():
        return {}
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)) as con:
        rows = con.execute("SELECT first_received,payload,receipt FROM advice WHERE first_received<=? AND first_received>=? ORDER BY first_received,advice_id", (now, now - 7200)).fetchall()
    result = {}
    for received, raw, receipt_raw in rows:
        advice, receipt = json.loads(raw), json.loads(receipt_raw)
        if advice.get("horizon_hours") != 24:
            continue
        symbol = advice["symbol"].removesuffix("USDT") + "/USDT"
        def timestamp(value):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            except (AttributeError, TypeError, ValueError):
                return None
        result[symbol] = {"advice_id": advice["advice_id"], "first_received_ts": received,
                          "published_ts": timestamp(receipt.get("published_at")),
                          "expires_ts": timestamp(advice.get("expires_at")),
                          "horizon_hours": 24, "action": advice["action"], **advice["eligibility"],
                          "live_order_effect": advice.get("live_order_effect"),
                          "receipt_reason": receipt.get("reason"),
                          "reference_schema": receipt.get("reference_schema"),
                          "experiment_version": receipt.get("experiment_version"),
                          "strategy_version": receipt.get("strategy_version"),
                          "analysis_source_identity": receipt.get("analysis_source_identity"),
                          "cost_version": advice["cost"]["version"]}
    return result


def _raw_evidence(root, raw):
    """Content-addressed immutable bytes, including malformed/failed acquisitions."""
    sha = hashlib.sha256(raw).hexdigest()
    path = root / "raw-hour-inputs" / (sha + ".json")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(raw)
    except FileExistsError:
        if path.read_bytes() != raw:
            raise ValueError("raw acquisition evidence hash conflict")
    return sha


def _hour_market(symbols, root, bar, now):
    """At most one fetch per invocation and one per 30 seconds, across restarts.

    Cache validity is a data property, never just an hour label. An invalid
    acquisition is archived but cannot replace the last validated cache.
    """
    cache_path = root / "hour-input.json"
    reason, evidence = "no_current_valid_cache", None
    if cache_path.exists():
        raw = cache_path.read_bytes()
        try:
            cache = json.loads(raw)
            if cache["bar_ts"] == bar:
                validate_signal_data(cache["market_data"], symbols, bar)
                if not cache.get("evidence_sha256"):
                    cache["evidence_sha256"] = _raw_evidence(root, raw)
                    _save_report(cache_path, cache)
                return cache["market_data"], {"status": "valid", "source": "validated_cache", "evidence_sha256": cache.get("evidence_sha256")}
        except (ValueError, TypeError, KeyError, AttributeError, IndexError) as exc:
            evidence = _raw_evidence(root, raw)
            reason = str(exc)
    attempt_path = root / "hour-attempt.json"
    try:
        attempt = json.loads(attempt_path.read_text())
    except (OSError, ValueError):
        attempt = {}
    if not isinstance(attempt, dict):
        attempt = {}
    attempted_at = attempt.get("attempted_at")
    if attempt.get("bar_ts") == bar and isinstance(attempted_at, (int, float)) and not isinstance(attempted_at, bool) and 0 <= now - attempted_at < 30:
        return {}, {"status": "unavailable", "reason": attempt.get("reason") or reason,
                    "source": "bounded_retry_wait", "retry_after_ts": attempt["attempted_at"] + 30,
                    "evidence_sha256": attempt.get("evidence_sha256", evidence)}
    # Persist before the remote call so failed/interrupted requests cannot busy-loop.
    attempt = {"bar_ts": bar, "attempted_at": now, "reason": "acquisition_incomplete"}
    _save_report(attempt_path, attempt)
    try:
        provider = OKXCCXTProvider()
        market = provider.fetch_ohlcv(symbols, timeframe="1h", limit=600, end_ts_ms=bar * 1000)
        cache = {"bar_ts": bar, "collected_at": time.time(),
                 "market_data": {symbol: asdict(value) for symbol, value in market.items()}}
        evidence = _raw_evidence(root, json.dumps(cache, sort_keys=True).encode())
        # Retain raw unclosed rows as evidence; only reuse confirmed closed rows.
        for value in cache["market_data"].values():
            indexes = [i for i, stamp in enumerate(value["ts"]) if stamp / 1000 + 3600 <= bar]
            for key in ("ts", "open", "high", "low", "close", "volume"):
                value[key] = [value[key][i] for i in indexes]
        validate_signal_data(cache["market_data"], symbols, bar)
        cache["evidence_sha256"] = evidence
        _save_report(cache_path, cache)
        return cache["market_data"], {"status": "valid", "source": "fresh_acquisition", "evidence_sha256": evidence}
    except Exception as exc:
        # This catch is confined to public signal acquisition, not account processing.
        reason = type(exc).__name__ + ": " + str(exc)[:500]
        if evidence is None:
            evidence = _raw_evidence(root, json.dumps({**attempt, "error": reason}).encode())
        _save_report(attempt_path, {**attempt, "reason": reason, "evidence_sha256": evidence})
        return {}, {"status": "unavailable", "reason": reason, "source": "failed_acquisition",
                    "evidence_sha256": evidence, "retry_after_ts": now + 30}


def collect(*, symbols, root, reference_path, entry_minimum_notional_usdt=10):
    now = time.time()
    bar = int(now // 3600) * 3600
    market, signal = _hour_market(symbols, root, bar, now)
    # Instrument/quote availability is independent of signal history availability.
    spec_path = root / "instrument-input.json"
    try:
        instruments = json.loads(spec_path.read_text())
    except (OSError, ValueError):
        instruments = {}
    if not isinstance(instruments, dict):
        instruments = {}
    if instruments.get("bar_ts") != bar:
        data = public_get("/api/v5/public/instruments", {"instType": "SPOT"})
        instruments = {"bar_ts": bar, "ts": time.time(), "data": [r for r in data if r["instId"].replace("-", "/") in symbols]}
        _save_report(spec_path, instruments)
    tickers = public_get("/api/v5/market/tickers", {"instType": "SPOT"})
    now = time.time()
    observed_bar = int(now // 3600) * 3600
    if observed_bar != bar:
        market, signal = {}, {"status": "unavailable", "reason": "hour_changed_during_acquisition", "evidence_sha256": signal.get("evidence_sha256")}
        bar = observed_bar
    specs = {row["instId"].replace("-", "/"): row for row in instruments["data"]}
    quotes = {row["instId"].replace("-", "/"): row for row in tickers}
    rows = {}
    for symbol in symbols:
        spec, quote = specs[symbol], quotes[symbol]
        rows[symbol] = {"entry_minimum_notional_usdt": entry_minimum_notional_usdt,
                        "quote": {"bid": float(quote["bidPx"]), "ask": float(quote["askPx"]), "ts": float(quote["ts"]) / 1000},
                        "instrument": {"symbol": symbol, "lot_size": spec["lotSz"], "minimum_qty": spec["minSz"],
                                       "minimum_notional_usdt": 0,
                                       "minimum_notional_source": "not_reported_no_additional_notional_limit_assumed",
                                       "buy_fee_currency": spec["baseCcy"], "sell_fee_currency": spec["quoteCcy"],
                                       "fee_currency_source": "explicit_spot_received_currency_model",
                                       "observed_ts": instruments["ts"], "source_hash": digest(spec)}}
    reference_read = {}
    references = references_at(reference_path, now, diagnostics=reference_read)
    frame = {"observed_at": now, "bar_ts": bar, "symbols": rows, "market_data": market, "signal_data": signal,
             "instrument_raw": instruments, "references": references, "reference_read": reference_read,
             "historical_backfill": False, "live_order_effect": "none"}
    frame["input_artifacts"] = capture_artifacts(Path(__file__).resolve().parents[2], root, now, symbols)
    validate_frame(frame)
    return frame


def source_identity(project, experiment, config_path):
    paths = sorted([*project.glob("src/**/*.py"), *project.glob("configs/**/*.py"), *project.glob("configs/research/*.json")])
    paths.extend([config_path, project / "scripts/run_review_forward.py"])
    files = {str(p.relative_to(project)).replace("\\", "/"): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    return {"experiment": experiment, "source_hashes": files, "identity": digest([experiment, files])}


def process(*, cfg, policy, experiment, project, root, frame, identity):
    """A crash leaves an explicit processing marker; no silent partial retry/reset."""
    root.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(root / "comparison.sqlite")) as con:
        con.execute("PRAGMA synchronous=FULL")
        con.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        con.execute("CREATE TABLE IF NOT EXISTS events (observed_at REAL PRIMARY KEY, input_hash TEXT NOT NULL UNIQUE, frame TEXT NOT NULL, event TEXT NOT NULL)")
        saved = dict(con.execute("SELECT key,value FROM meta"))
        if saved.get("identity") not in {None, identity["identity"]}:
            raise ValueError("comparison_version_changed_requires_new_experiment_directory")
        if saved.get("processing"):
            raise ValueError("interrupted_comparison_requires_audited_replay_into_new_directory")
        checkpoint = json.loads(saved["checkpoint"]) if "checkpoint" in saved else None
        model_binding = json.dumps({k: v.get("sha256") for k, v in frame.get("input_artifacts", {}).items() if k.startswith("models/")}, sort_keys=True)
        if saved.get("model_binding") not in {None, model_binding}:
            raise ValueError("frozen_baseline_model_changed_requires_new_experiment")
        if checkpoint and frame["observed_at"] <= checkpoint["last_observed"]:
            raise ValueError("duplicate_or_reversed_comparison_observation")
        validate_frame(frame)
        con.execute("INSERT OR REPLACE INTO meta VALUES('identity',?)", (identity["identity"],))
        con.execute("INSERT OR REPLACE INTO meta VALUES('model_binding',?)", (model_binding,))
        con.execute("INSERT OR REPLACE INTO meta VALUES('processing',?)", (json.dumps(frame, allow_nan=False),))
        con.commit()
        comparison = Comparison(cfg, policy, experiment, root=root / "accounts", checkpoint=checkpoint)
        # Time-bound public constraints and immutable blacklist configuration, never live account state.
        for scenario in comparison.scenarios.values():
            sandbox = scenario["adapter"].root
            _save_report(sandbox / "reports/okx_spot_instruments.json", frame["instrument_raw"])
            for relative, artifact in frame.get("input_artifacts", {}).items():
                target = (sandbox / relative).resolve()
                if not target.is_relative_to(sandbox) or not relative.startswith(("configs/", "models/", "data/sentiment_cache/")):
                    raise ValueError("unapproved research artifact path")
                if artifact["status"] != "captured":
                    continue
                blob = root / "input-blobs" / artifact["sha256"]
                raw = blob.read_bytes()
                if hashlib.sha256(raw).hexdigest() != artifact["sha256"]:
                    raise ValueError("research input blob hash mismatch")
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)
        event = comparison.observe(frame)
        new_checkpoint = comparison.checkpoint()
        con.execute("INSERT INTO events VALUES(?,?,?,?)", (frame["observed_at"], digest(frame), json.dumps(frame, allow_nan=False), json.dumps(event, allow_nan=False)))
        con.execute("INSERT OR REPLACE INTO meta VALUES('checkpoint',?)", (json.dumps(new_checkpoint, allow_nan=False),))
        con.execute("DELETE FROM meta WHERE key='processing'")
        con.commit()
        # Hourly curves bound the report size; all minute observations and fills remain in SQLite.
        events = [json.loads(row[0]) for row in con.execute("SELECT event FROM events WHERE json_extract(event,'$.hourly_decision')=1 ORDER BY observed_at")]
        if not events or events[-1]["observed_at"] != event["observed_at"]:
            events.append(event)
        report = summarize(events, new_checkpoint, experiment,
                           legacy_observation_integrity=json.loads(saved["legacy_observation_integrity"]) if "legacy_observation_integrity" in saved else None)
        first_observation = con.execute("SELECT min(observed_at) FROM events").fetchone()[0]
        report.update(identity=identity["identity"], latest_observed_at=frame["observed_at"],
                      ledger_start_ts=first_observation, latest_decision_clock=event["decision_clock"],
                      reference_read=frame.get("reference_read", {"status": "not_provided"}),
                      latest_reference_observation={s: reference_use(frame.get("references", {}).get(s), frame["observed_at"], experiment.get("reference_contract")) for s in frame["symbols"]},
                      input_hash=digest(frame), storage="independent_comparison_sqlite", report_sampling="common_decisions_plus_latest; raw_minute_events_retained")
        report["signal_data"] = frame.get("signal_data", {"status": "valid"})
        report["continuation"] = json.loads(saved["continuation"]) if "continuation" in saved else None
        report["risk_comparison_limitation"] = "frozen_v2_keeps_drawdown_no_greater_than_control; low_or_zero_exposure_control_is_not_comparable_risk_evidence; any_new_risk_rule_requires_a_separate_predeclared_version"
        _save_report(root / "manifest.json", identity)
        _save_report(root / "latest.json", report)
        return report
