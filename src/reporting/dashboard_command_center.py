"""Bounded local evidence for the trading command center. No exchange calls or writes."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from src.execution.fill_store import derive_runtime_named_json_path
from src.reporting.participation_runtime import runtime_identity

SCHEMA = "v5.command_center.v1"
MAX_JSON_BYTES = 4 * 1024 * 1024
MAX_RUNS = 104


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _epoch(value):
    number = _number(value)
    if number is not None:
        return number / 1000 if number > 10_000_000_000 else number
    if isinstance(value, str):
        try:
            stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return stamp.timestamp() if stamp.tzinfo is not None else None
        except (ValueError, OverflowError):
            pass
    return None


def _iso(value):
    stamp = _epoch(value)
    try:
        return datetime.fromtimestamp(stamp, timezone.utc).isoformat().replace("+00:00", "Z") if stamp is not None else None
    except (ValueError, OverflowError, OSError):
        return None


def _safe(path: Path, root: Path) -> Path:
    path, root = Path(path).resolve(), Path(root).resolve()
    if not path.is_relative_to(root):
        raise ValueError("artifact_outside_runtime_root")
    return path


def _json(path: Path, root: Path):
    path = _safe(path, root)
    try:
        if not path.is_file():
            return None, "missing"
        if path.stat().st_size > MAX_JSON_BYTES:
            return None, "oversized"
        payload = json.loads(path.read_text(encoding="utf-8"))
        return (payload, "observed") if isinstance(payload, dict) else (None, "invalid")
    except (OSError, ValueError):
        return None, "invalid"


def _fresh(stamp, now, maximum_age=5400):
    stamp = _epoch(stamp)
    if stamp is None or _iso(stamp) is None:
        return {"status": "unavailable", "observed_at": None, "age_seconds": None}
    age = now - stamp
    return {"status": "future" if age < 0 else "stale" if maximum_age is not None and age > maximum_age else "observed",
            "observed_at": _iso(stamp), "age_seconds": age}


def _metric(value, unit, status="observed", **extra):
    return {"value": value, "unit": unit, "status": status, **extra}


def _mapping(value):
    return value if isinstance(value, dict) else {}


def _rows(value):
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def _symbol(value):
    return str(value or "").upper().replace("/", "-")


def _audit_time(audit):
    return next((stamp for key in ("now_ts", "timestamp", "ts") if (stamp := _epoch(audit.get(key))) is not None), None)


def _load_audits(paths, warnings):
    root = Path(paths.reports_dir)
    runs = _safe(paths.runs_dir, root)
    if not runs.is_dir():
        return []
    candidates = []
    for directory in runs.iterdir():
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", directory.name) or not directory.is_dir():
            continue
        try:
            artifact = _safe(directory / "decision_audit.json", root)
            if artifact.is_file():
                # Folder identity is a scan hint only, never freshness evidence.
                candidates.append((directory.name, artifact.stat().st_mtime, artifact))
        except (OSError, ValueError):
            warnings.append("audit_path_rejected")
    candidates.sort(key=lambda row: (row[0], row[1]), reverse=True)
    result = []
    for folder, mtime, artifact in candidates[:MAX_RUNS]:
        audit, status = _json(artifact, root)
        if status != "observed":
            warnings.append("decision_audit_" + status)
            result.append({"run_id": folder, "audit": {}, "stamp": None, "scan_stamp": mtime, "status": status, "path": artifact.parent})
            continue
        if audit.get("run_id") != folder:
            warnings.append("decision_audit_run_identity_mismatch")
            result.append({"run_id": folder, "audit": {}, "stamp": None, "scan_stamp": mtime, "status": "identity_mismatch", "path": artifact.parent})
            continue
        stamp = _audit_time(audit)
        result.append({"run_id": folder, "audit": audit, "stamp": stamp, "scan_stamp": stamp if stamp is not None else mtime,
                       "status": "observed" if stamp is not None else "unavailable", "path": artifact.parent})
    return sorted(result, key=lambda row: row["scan_stamp"], reverse=True)


def _candidate_rows(latest, root, now, warnings):
    if latest is None or not latest["audit"]:
        return []
    audit = latest["audit"]
    factors = _mapping(audit.get("alpha_factor_snapshot"))
    scores = {row.get("symbol"): row for row in _rows(audit.get("top_scores"))}
    targets = _mapping(audit.get("targets_post_risk"))
    routes = _rows(audit.get("router_decisions"))
    source_rows = {}
    try:
        path = _safe(latest["path"] / "candidate_snapshot.csv", root)
        if path.is_file() and path.stat().st_size <= MAX_JSON_BYTES:
            with path.open(encoding="utf-8-sig", newline="") as stream:
                for index, row in enumerate(csv.DictReader(stream)):
                    if index >= 128:
                        warnings.append("candidate_snapshot_truncated")
                        break
                    if row.get("run_id") == latest["run_id"]:
                        source_rows[row.get("symbol")] = row
    except (OSError, ValueError, csv.Error):
        warnings.append("candidate_snapshot_unavailable")
    symbols = sorted({str(symbol) for symbol in [*factors, *scores, *targets, *source_rows,
                      *[row.get("symbol") for row in routes if isinstance(row, dict)]] if symbol and symbol != "null"})
    result = []
    selection = _rows(_mapping(audit.get("trade_funnel")).get("selection_decisions"))
    for symbol in symbols[:100]:
        factor = _mapping(factors.get(symbol))
        raw = _mapping(factor.get("raw_factors"))
        source = source_rows.get(symbol, {})
        score = scores.get(symbol, {})
        reasons = list(dict.fromkeys(str(row["reason"]) for row in routes
                       if isinstance(row, dict) and row.get("symbol") == symbol and row.get("action") == "skip" and row.get("reason")))
        selection_reasons = list(dict.fromkeys(str(row["reason"]) for row in selection
                                 if isinstance(row, dict) and row.get("symbol") == symbol and row.get("reason")))
        price_stamp = _epoch(source.get("quote_ts"))
        price = next((number for key in ("arrival_mid", "decision_px")
                      if (number := _number(source.get(key))) is not None and number > 0), None)
        if price_stamp is None or price_stamp > now:
            price = None
        explain = _mapping(_mapping(audit.get("target_execution_explain")).get(symbol))
        direction = next((value for value in (source.get("alpha6_side"), source.get("direction"), explain.get("alpha6_side"))
                          if value in ("buy", "sell", "hold")), None)
        result.append({"symbol": symbol, "alpha_score": _number(factor.get("alpha6_score")),
                       "absolute_alpha_score": _number(raw.get("alpha6_final_score")),
                       "final_score": _number(score.get("final_score", score.get("score"))),
                       "target_weight": _number(targets.get(symbol)), "router_reasons": reasons,
                       "selection_reasons": selection_reasons, "reference_price": price,
                       "price_as_of": _iso(price_stamp) if price is not None else None, "direction": direction,
                       "momentum_4h_bps": _number(source.get("ret4_bps")),
                       "volume_confirm": _number(factor.get("f4_volume_expansion")),
                       "rsi_confirm": _number(factor.get("f5_rsi_trend_confirm")),
                       "source_run_id": latest["run_id"]})
    return result


def _health(paths, config, workspace, now, warnings):
    root = Path(paths.reports_dir)
    output = {}
    ledger_raw = _mapping(config.get("execution")).get("ledger_status_path")
    ledger_path = Path(ledger_raw) if ledger_raw and ledger_raw != "reports/ledger_status.json" else derive_runtime_named_json_path(paths.orders_db, "ledger_status")
    if not ledger_path.is_absolute():
        ledger_path = workspace / ledger_path
    for name, path in (("kill_switch", paths.kill_switch_path), ("reconcile", paths.reconcile_status_path), ("ledger", ledger_path)):
        try:
            payload, status = _json(path, root)
        except ValueError:
            payload, status = None, "path_rejected"
        payload = payload or {}
        value_key = "enabled" if name == "kill_switch" else "ok"
        value = payload.get(value_key)
        value = value if isinstance(value, bool) else None
        fresh = _fresh(payload.get("ts_ms"), now, None if name == "kill_switch" else 900)
        if status != "observed":
            fresh["status"] = status
        elif value is None:
            fresh["status"] = "unavailable"
        elif fresh["status"] == "observed" and (value if name == "kill_switch" else not value):
            fresh["status"] = "blocked" if name == "kill_switch" else "failed"
        output[name] = {**fresh, value_key: value, "reason": payload.get("reason"),
                        "timestamp_semantics": "last_state_change" if name == "kill_switch" else "last_check"}
    snapshots = []
    for source, path, ts_key in (("evaluation", paths.auto_risk_eval_path, "ts"), ("guard", paths.auto_risk_guard_path, "last_update")):
        try:
            payload, status = _json(path, root)
            if payload:
                snapshots.append((source, payload, _epoch(payload.get(ts_key))))
        except ValueError:
            warnings.append("risk_path_rejected")
    snapshots.sort(key=lambda row: row[2] if row[2] is not None else float("-inf"), reverse=True)
    source, payload, stamp = snapshots[0] if snapshots else (None, {}, None)
    level = payload.get("current_level", payload.get("level"))
    metrics = _mapping(payload.get("metrics"))
    output["risk"] = {**_fresh(stamp, now), "level": level if level in ("PROTECT", "DEFENSE", "NEUTRAL", "ATTACK") else None,
                      "dd_pct": _number(metrics.get("dd_pct", metrics.get("last_dd_pct"))),
                      "config": payload.get("config", payload.get("current_config")) or None,
                      "reason": payload.get("reason"), "source": source,
                      "recovery_evidence_ok": metrics.get("recovery_evidence_ok") if isinstance(metrics.get("recovery_evidence_ok"), bool) else None}
    return output


def _fill_metrics(paths, start, now, candidate_pairs, warnings):
    unavailable = {"actual_fill_events": _metric(None, "exchange_fill_events", "unavailable"),
                   "actual_filled_orders": _metric(None, "distinct_exchange_orders", "unavailable"),
                   "attributed_filled_candidates": _metric(None, "run_symbol_pairs", "unavailable")}
    try:
        path = _safe(paths.fills_db, paths.reports_dir)
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1)) as con:
            con.row_factory = sqlite3.Row
            rows = con.execute("SELECT inst_id,trade_id,ord_id,cl_ord_id,side,fill_sz,ts_ms FROM fills WHERE ts_ms>=? AND ts_ms<=? ORDER BY ts_ms LIMIT 5001",
                               (int(start * 1000), int(now * 1000))).fetchall()
        if len(rows) > 5000:
            raise ValueError("fill_window_row_limit")
        valid = [row for row in rows if (_number(row["fill_sz"]) or 0) > 0 and row["inst_id"] and row["trade_id"]
                 and str(row["side"]).lower() in ("buy", "sell")]
        status = "partial" if len(valid) != len(rows) else "observed"
        if status == "partial":
            warnings.append("invalid_exchange_fill_rows_excluded")
        result = {**unavailable, "actual_fill_events": _metric(len(valid), "exchange_fill_events", status, scope="locally_recorded_exchange_events")}
        identified = {(row["inst_id"], row["ord_id"] or row["cl_ord_id"]) for row in valid if row["ord_id"] or row["cl_ord_id"]}
        order_status = status if all(row["ord_id"] or row["cl_ord_id"] for row in valid) else "unavailable"
        result["actual_filled_orders"] = _metric(len(identified) if order_status != "unavailable" else None, "distinct_exchange_orders", order_status)
        path = _safe(paths.orders_db, paths.reports_dir)
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1)) as con:
            con.row_factory = sqlite3.Row
            ids = set(str(row["cl_ord_id"]) for row in valid if row["cl_ord_id"])
            oids = set(str(row["ord_id"]) for row in valid if row["ord_id"])
            if len(ids) + len(oids) > 900:
                raise ValueError("order_attribution_row_limit")
            clauses, params = [], []
            for column, values in (("cl_ord_id", ids), ("ord_id", oids)):
                if values:
                    clauses.append(f"{column} IN ({','.join('?' for _ in values)})")
                    params.extend(sorted(values))
            orders = con.execute("SELECT cl_ord_id,ord_id,inst_id,run_id,side FROM orders WHERE " + (" OR ".join(clauses) or "0"), params).fetchall()
        pairs, unmatched = set(), 0
        for fill in valid:
            if str(fill["side"]).lower() != "buy":
                continue
            matched = {(row["run_id"], row["inst_id"]) for row in orders if row["inst_id"] == fill["inst_id"] and str(row["side"]).lower() == "buy"
                       and ((fill["cl_ord_id"] and row["cl_ord_id"] == fill["cl_ord_id"]) or (fill["ord_id"] and row["ord_id"] == fill["ord_id"]))}
            if len(matched) == 1 and (next(iter(matched))[0], _symbol(next(iter(matched))[1])) in candidate_pairs:
                pairs.update(matched)
            else:
                unmatched += 1
        result["attributed_filled_candidates"] = _metric(None if unmatched else len(pairs), "run_symbol_pairs", "unavailable" if unmatched else status,
                                                         matched_count=len(pairs), unmatched_buy_fill_events=unmatched)
        return result
    except (OSError, ValueError, sqlite3.Error):
        warnings.append("exchange_fill_or_order_attribution_unavailable")
        return locals().get("result", unavailable)


def _participation(paths, config, workspace, now, warnings):
    settings = _mapping(config.get("participation"))
    enabled = settings.get("enabled")
    enabled = enabled if isinstance(enabled, bool) else None
    output = {"enabled": enabled, "mode": "forward_paper", "status": "disabled" if enabled is False else "missing",
              "observed_at": None, "age_seconds": None, "live_order_effect": "none", "live_promotion_allowed": False,
              "entry_count": None, "closed_trade_count": None, "net_realized_pnl_usdt": None,
              "equity_usdt": None, "valuation_status": "unavailable", "curve": [], "events": [],
              "latest_decision": None, "latest_execution": None}
    if enabled is not True:
        if enabled is None:
            output["status"] = "unavailable"
        return output
    try:
        root = _safe(Path(paths.reports_dir) / "participation", paths.reports_dir)
        if settings.get("mode") != "forward_paper":
            raise ValueError("participation_mode_mismatch")
        policy_path = Path(settings.get("policy_path") or "")
        policy_path = _safe(policy_path if policy_path.is_absolute() else workspace / policy_path, workspace)
        policy, policy_status = _json(policy_path, workspace)
        if policy_status != "observed" or policy.get("mode") != "research_shadow" or policy.get("live_promotion_allowed") is not False:
            raise ValueError("participation_policy_unavailable_or_invalid")
        policy_hash = hashlib.sha256(json.dumps(policy, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        identity, _ = runtime_identity(policy, settings, workspace)
        raw_latest = Path(settings.get("latest_path") or root / "latest.json")
        latest_path = _safe(raw_latest if raw_latest.is_absolute() else workspace / raw_latest, root)
        raw_path = Path(settings.get("state_path") or "")
        state_path = _safe(raw_path if raw_path.is_absolute() else workspace / raw_path, root)
        if state_path.suffix != ".sqlite":
            raise ValueError("participation_state_path_invalid")
        quote_enabled = settings.get("quote_execution_enabled") is True
        output["quote_execution_enabled"] = quote_enabled
        if quote_enabled:
            worker, worker_status = _json(state_path.with_suffix(".worker.json"), root)
            worker = worker or {}
            stamp = _epoch(worker.get("observed_ts"))
            status = "observed" if (worker_status == "observed" and worker.get("status") == "observed"
                      and worker.get("identity") == identity and stamp is not None and 0 <= now - stamp <= 30) else "unavailable"
            output["quote_worker"] = {"status": status, "observed_at": _iso(stamp),
                                      "interval_seconds": worker.get("interval_seconds"),
                                      "websocket_connected": worker.get("websocket_connected"),
                                      "last_check_status": worker.get("last_check_status"), "last_error": worker.get("last_error")}
        latest, latest_status = _json(latest_path, root)
        output["status"] = latest_status
        if latest is None:
            return output
        if latest.get("identity") != identity or latest.get("policy_hash") != policy_hash:
            output["status"] = "identity_mismatch"
            return output
        if not _forward_event(latest):
            raise ValueError("participation_forward_provenance_invalid")
        output.update(_fresh(latest.get("observed_ts"), now))
        state = _mapping(latest.get("portfolio"))
        valuation = state.get("valuation_status", "unavailable")
        output.update({"identity": identity, "policy_hash": policy_hash, "source_run_id": latest.get("source_run_id"),
                       "cohort_started_at": _iso(latest.get("cohort_started_at")),
                       "entry_count": _number(state.get("entry_count")), "closed_trade_count": _number(state.get("closed_trade_count")),
                       "net_realized_pnl_usdt": _number(state.get("net_realized_pnl_usdt")),
                       "equity_usdt": _valued_equity(state, now),
                       "valuation_status": valuation, "last_valuation_at": _iso(state.get("last_valuation_quote_ts")),
                       "latest_decision": latest.get("decision"), "latest_execution": latest.get("execution"),
                       "position": state.get("position"), "pending": state.get("pending"), "halted": state.get("halted"),
                       "signal_decision": latest.get("signal_decision", latest.get("decision")),
                       "signal_observed_at": _iso(latest.get("signal_observed_ts", latest.get("observed_ts")))})
        if quote_enabled and output["quote_worker"]["status"] != "observed" and state.get("position"):
            output["equity_usdt"] = None
            output["valuation_status"] = "quote_worker_unavailable"
        if output["status"] in ("future", "unavailable"):
            _clear_participation_values(output)
            return output
        with closing(sqlite3.connect(state_path.as_uri() + "?mode=ro", uri=True, timeout=1)) as con:
            portfolio = con.execute("SELECT identity FROM portfolio WHERE id=1").fetchone()
            if not portfolio or portfolio[0] != identity:
                output["status"] = "identity_mismatch"
                _clear_participation_values(output)
                return output
            # Keep the curve on the latest report's committed observation boundary.
            boundary = _epoch(latest.get("observed_ts"))
            # One valuation per hour keeps the chart bounded when quote events
            # arrive within the same signal hour. Preserve the latest event too.
            rows = con.execute("SELECT observed_ts,event FROM decisions WHERE sequence IN ("
                               "SELECT MAX(sequence) FROM decisions WHERE observed_ts<=? AND observed_ts>=? "
                               "GROUP BY CAST(observed_ts/3600 AS INTEGER) ORDER BY MAX(sequence) DESC LIMIT 168) "
                               "ORDER BY observed_ts DESC,sequence DESC", (boundary, boundary - 168 * 3600)).fetchall()
            event_rows = con.execute("SELECT event FROM decisions WHERE observed_ts<=? AND observed_ts>=? AND "
                                     "(json_extract(event,'$.execution.action') IN ('fill','cancel') OR "
                                     "json_extract(event,'$.decision.action') IN ('entry_intent','exit_intent')) "
                                     "ORDER BY observed_ts DESC,sequence DESC LIMIT 12", (boundary, boundary - 168 * 3600)).fetchall()
        if not rows or _epoch(rows[0][0]) != _epoch(latest.get("observed_ts")):
            raise ValueError("participation_latest_event_missing")
        saved_latest = _mapping(json.loads(rows[0][1]))
        if any(latest.get(key) != saved_latest.get(key) for key in (
            "identity", "policy_hash", "observed_ts", "bar_ts", "cohort_started_at", "source_run_id",
            "portfolio", "decision", "execution", "closed_trade",
        )):
            raise ValueError("participation_latest_event_mismatch")
        for observed, encoded in reversed(rows):
            event = _mapping(json.loads(encoded))
            if event.get("identity") != identity or event.get("policy_hash") != policy_hash:
                output["status"] = "identity_mismatch"
                _clear_participation_values(output)
                return output
            if not _forward_event(event) or _epoch(event.get("observed_ts")) != _epoch(observed):
                raise ValueError("participation_event_provenance_invalid")
            event_state = _mapping(event.get("portfolio"))
            valuation = event_state.get("valuation_status", "unavailable")
            output["curve"].append({"observed_ts": _iso(observed),
                                    "equity_usdt": _valued_equity(event_state, observed),
                                    "net_realized_pnl_usdt": _number(event_state.get("net_realized_pnl_usdt")), "valuation_status": valuation})
            output["events"].append({"observed_ts": _iso(observed), "decision": event.get("decision"), "execution": event.get("execution")})
        if event_rows:
            events = [json.loads(row[0]) for row in reversed(event_rows)]
            if any(not _forward_event(event) or event.get("identity") != identity or event.get("policy_hash") != policy_hash for event in events):
                raise ValueError("participation_execution_event_provenance_invalid")
            output["events"] = [{"observed_ts": _iso(event.get("observed_ts")), "decision": event.get("decision"),
                                 "execution": event.get("execution")} for event in events]
        output["events"] = output["events"][-12:]
    except (OSError, ValueError, TypeError, sqlite3.Error):
        warnings.append("participation_evidence_unavailable")
        output["status"] = "unavailable"
        _clear_participation_values(output)
    return output


def _forward_event(event):
    if not isinstance(event, dict):
        return False
    observed, bar, cohort = (_epoch(event.get(key)) for key in ("observed_ts", "bar_ts", "cohort_started_at"))
    return (observed is not None and bar is not None and cohort is not None and bar <= observed and cohort <= observed
            and event.get("schema_version") == "v5.participation_forward_observation.v1"
            and event.get("mode") == "forward_paper" and event.get("historical_backfill") is False
            and event.get("live_order_effect") == "none" and event.get("live_promotion_allowed") is False)


def _clear_participation_values(output):
    output.update(entry_count=None, closed_trade_count=None, net_realized_pnl_usdt=None, equity_usdt=None,
                  curve=[], events=[], latest_decision=None, latest_execution=None, signal_decision=None, position=None, pending=None)


def _valued_equity(state, observed):
    if state.get("valuation_valid") is not True:
        return None
    status = state.get("valuation_status")
    quote_ts = _epoch(state.get("last_valuation_quote_ts"))
    if status == "flat_cash" and not state.get("position"):
        return _number(state.get("equity_usdt"))
    if status == "observed_quote" and quote_ts is not None and quote_ts <= observed:
        return _number(state.get("equity_usdt"))
    return None


def _finite_json(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(key): _finite_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite_json(item) for item in value]
    return value


def build_command_center(*, config, paths, workspace: Path, now: float):
    """Aggregate a closed 72-hour event window from approved local artifacts."""
    warnings = []
    now = _epoch(now)
    if now is None or _iso(now) is None:
        raise ValueError("command_center_now_invalid")
    start = now - 72 * 3600
    audits = _load_audits(paths, warnings)
    latest = audits[0] if audits else None
    valid = [row for row in audits if row["stamp"] is not None and start <= row["stamp"] <= now and row["status"] == "observed"]
    observed_hours = {int(row["stamp"] // 3600) for row in valid}
    coverage = "complete" if len(observed_hours) >= 72 and not any("decision_audit" in warning for warning in warnings) else "partial" if valid else "unavailable"
    latest_audit = latest["audit"] if latest else {}
    latest_status = _fresh(latest["stamp"] if latest else None, now)
    if latest and latest["status"] != "observed":
        latest_status["status"] = latest["status"]
    latest_decision = {**latest_status, "run_id": latest["run_id"] if latest else None,
                       "decision_ts": latest_status["observed_at"], "window_start_ts": _iso(latest_audit.get("window_start_ts")),
                       "window_end_ts": _iso(latest_audit.get("window_end_ts")), "regime": latest_audit.get("regime")}
    window = {"start_ts": _iso(start), "end_ts": _iso(now), "hours": 72, "observed_runs": len(valid), "expected_runs": 72,
              "coverage_status": coverage}
    for field, key, unit in (("selected_candidates", "selected", "candidate_observations"), ("generated_orders", "orders_rebalance", "generated_rebalance_orders")):
        counts = [_number(_mapping(row["audit"].get("counts")).get(key)) for row in valid]
        observed = [int(value) for value in counts if value is not None and value >= 0 and value == int(value)]
        status = ("observed" if coverage == "complete" else coverage) if len(observed) == len(counts) else "partial"
        window[field] = _metric(sum(observed) if observed else None, unit, status, observed_runs=len(observed))
    # A fill can join a run/symbol candidate only with an explicit local order identity.
    candidate_pairs = {(row["run_id"], _symbol(symbol)) for row in valid
                       for symbol in _mapping(row["audit"].get("alpha_factor_snapshot"))}
    window.update(_fill_metrics(paths, start, now, candidate_pairs, warnings))
    frequencies, symbols = Counter(), defaultdict(set)
    for row in valid:
        for route in _rows(row["audit"].get("router_decisions")):
            if isinstance(route, dict) and route.get("action") == "skip" and route.get("reason"):
                reason = str(route["reason"])
                frequencies[reason] += 1
                if route.get("symbol"):
                    symbols[reason].add(str(route["symbol"]))
    workspace = Path(workspace).resolve()
    health = _health(paths, config, workspace, now, warnings)
    participation = _participation(paths, config, Path(workspace).resolve(), now, warnings)
    candidates = _candidate_rows(latest, paths.reports_dir, now, warnings)
    latest_decision["router_reasons"] = list(dict.fromkeys(str(route["reason"]) for route in _rows(latest_audit.get("router_decisions"))
                                                        if route.get("action") == "skip" and route.get("reason")))
    qlab = _mapping(latest_audit.get("quant_lab"))
    enforced = qlab.get("permission_gate_enforced")
    health_current = all(row["status"] in ("observed", "blocked", "failed") for row in health.values())
    payload = {"schema_version": SCHEMA, "generated_at": _iso(now), "read_only": True,
               "status": "observed" if latest_status["status"] == "observed" and coverage == "complete" and health_current and not warnings else "partial" if latest else "unavailable",
               "latest_decision": latest_decision, "candidates": candidates,
               "window_72h": window, "blockers": [{"reason": reason, "count": count, "unit": "router_block_events", "symbols": sorted(symbols[reason])}
                                                   for reason, count in frequencies.most_common(20)],
               "health": health, "participation": participation,
               "quant_lab": {"mode": "enforced" if enforced is True else "advisory" if enforced is False else "unknown",
                             "permission": qlab.get("raw_permission_decision", qlab.get("quant_lab_permission", qlab.get("permission"))),
                             "effective_permission": qlab.get("final_permission"), "source_mode": qlab.get("mode"),
                             "permission_gate_enforced": enforced if isinstance(enforced, bool) else None},
               "warnings": warnings}
    return _finite_json(payload)
