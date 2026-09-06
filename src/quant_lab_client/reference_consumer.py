"""Independent GET-only reference receipts. This module cannot return trading orders."""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)


def _hash(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _ts(value):
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("reference timestamp requires timezone")
    return stamp.timestamp()


def fetch_reference(endpoint: str) -> dict:
    url = urlparse(endpoint)
    if url.hostname != "qyun2.hrhome.top" or url.path != "/v1/trade-advice/latest" or url.scheme not in {"http", "https"} or url.username or url.password or url.query:
        raise ValueError("reference endpoint must be the declared qyun2 GET advice endpoint")
    with requests.get(endpoint, timeout=(3, 6), stream=True, allow_redirects=False) as response:
        response.raise_for_status()
        data = bytearray()
        for chunk in response.iter_content(16384):
            data.extend(chunk)
            if len(data) > 600000:
                raise ValueError("reference exceeds response budget")
    return json.loads(data)


def _reference_reason(advice, payload, now):
    try:
        if not re.fullmatch(r"advice-[a-f0-9]{64}", advice["advice_id"]):
            return "invalid_advice_identity"
        if payload.get("schema_version") != "qlab.decision.result.v2":
            return "legacy_schema_never_restores_permission"
        if advice.get("live_order_effect") != "none" or advice.get("eligibility", {}).get("live_execution_eligible") is not False:
            return "invalid_research_boundary"
        published = _ts(payload["publication"]["published_at"])
        generated, expiry = _ts(advice["generated_at"]), _ts(advice["expires_at"])
        if generated > published or published > now + 5 or expiry > generated + 3600:
            return "reference_time_invalid"
        if now >= expiry:
            return "reference_expired"
        if advice["horizon_hours"] not in {4, 24} or not advice["cost"]["version"]:
            return "reference_contract_incomplete"
        if advice.get("effective_action", advice["action"]) == "NO_VIEW":
            return "reference_no_view"
        return "record_only_not_adopted"
    except (KeyError, TypeError, ValueError):
        return "reference_contract_invalid"


def read_v5_contexts(runs_dir: Path, orders_path: Path, fills_path: Path) -> list[dict]:
    """Bind to run_id and exchange fill IDs using read-only database connections."""
    contexts = []
    for path in sorted(runs_dir.glob("*/decision_audit.json"), reverse=True)[:48]:
        if path.stat().st_size > 4 * 1024 * 1024:
            continue
        context = None
        try:
            audit = json.loads(path.read_text(encoding="utf-8"))
            run_id = audit["run_id"]
            context = {"run_id": run_id, "decision_ts": audit["now_ts"], "audit_sha256": _hash(audit),
                       "targets_post_risk": audit.get("targets_post_risk", {}),
                       "router_decisions": audit.get("router_decisions", []),
                       "exit_signals": audit.get("exit_signals", []), "orders": [], "fills": [],
                       "execution_status": "unobservable"}
            with closing(sqlite3.connect(orders_path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
                connection.row_factory = sqlite3.Row
                rows = connection.execute("SELECT cl_ord_id,ord_id,inst_id,side,intent,state,acc_fill_sz,avg_px,updated_ts FROM orders WHERE run_id=? ORDER BY cl_ord_id", (run_id,))
                context["orders"] = [dict(row) for row in rows]
            with closing(sqlite3.connect(fills_path.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
                connection.row_factory = sqlite3.Row
                for order in context["orders"]:
                    rows = connection.execute("SELECT inst_id,trade_id,ts_ms,ord_id,cl_ord_id,side,fill_px,fill_sz,fee,fee_ccy FROM fills WHERE cl_ord_id=? OR ord_id=? ORDER BY ts_ms", (order["cl_ord_id"], order["ord_id"]))
                    context["fills"].extend(dict(row) for row in rows)
            context["execution_status"] = "fills_observed" if context["fills"] else "no_recorded_fills"
            contexts.append(context)
        except (OSError, ValueError, KeyError, sqlite3.Error):
            # A reference audit never repairs/mutates live execution stores.
            if context is not None:
                context["execution_status"] = "execution_read_failed"
                contexts.append(context)
    return contexts


def record_cycle(*, payload: dict | None, contexts: list[dict], now: float, output: Path, error: str | None = None) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    if payload is not None and (not isinstance(payload, dict) or not isinstance(payload.get("advice"), list) or len(payload["advice"]) > 8 or any(not isinstance(a, dict) for a in payload["advice"])):
        payload, error = None, "reference_payload_invalid"
    with closing(sqlite3.connect(output / "receipts.sqlite")) as connection, connection:
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("CREATE TABLE IF NOT EXISTS advice (advice_id TEXT PRIMARY KEY, first_received REAL NOT NULL, expires_at REAL, payload TEXT NOT NULL, receipt TEXT NOT NULL)")
        connection.execute("CREATE TABLE IF NOT EXISTS associations (event_hash TEXT PRIMARY KEY, advice_id TEXT NOT NULL, run_id TEXT NOT NULL, observed_at REAL NOT NULL, evidence TEXT NOT NULL)")
        receipts = []
        for advice in (payload or {}).get("advice", []):
            reason = _reference_reason(advice, payload, now)
            if not re.fullmatch(r"advice-[a-f0-9]{64}", str(advice.get("advice_id", ""))):
                continue
            receipt = {"advice_id": advice["advice_id"], "published_at": payload.get("publication", {}).get("published_at"),
                       "reference_schema": payload.get("schema_version"),
                       "analysis_source_identity": payload.get("worker_commit"),
                       "strategy_version": advice.get("strategy_version"),
                       "generated_at": advice.get("generated_at"), "expires_at": advice.get("expires_at"),
                       "horizon_hours": advice.get("horizon_hours"), "cost_version": advice.get("cost", {}).get("version"),
                       "experiment_version": advice.get("experiment_version"), "adoption": "not_adopted",
                       "reason": reason, "live_order_effect": "none", "received_at": now}
            try:
                expiry = _ts(advice["expires_at"])
            except (KeyError, TypeError, ValueError):
                expiry = None
            connection.execute("INSERT OR IGNORE INTO advice VALUES(?,?,?,?,?)", (advice["advice_id"], now, expiry, _json(advice), _json(receipt)))
            receipts.append(receipt)
        # Revisit delayed execution evidence without changing the first receipt or signed advice.
        for advice_id, first_received, expiry, raw, receipt_raw in connection.execute("SELECT * FROM advice WHERE first_received>=?", (now - 172800,)).fetchall():
            advice, receipt = json.loads(raw), json.loads(receipt_raw)
            symbol = str(advice.get("symbol", "")).replace("USDT", "/USDT")
            for context in contexts:
                decision_ts = context["decision_ts"]
                decision_ts = decision_ts / 1000 if decision_ts > 10000000000 else decision_ts
                if not first_received <= decision_ts <= now:
                    continue
                event = {**receipt, "first_received_at": first_received, "run_id": context["run_id"],
                         "v5_original_decision": {"target_weight": context["targets_post_risk"].get(symbol),
                                                  "router": [row for row in context["router_decisions"] if row.get("symbol") == symbol]},
                         "reference_valid_at_v5_decision": bool(expiry and decision_ts < expiry and receipt["reason"] == "record_only_not_adopted"),
                         "final_execution": {"status": context["execution_status"],
                                             "orders": [row for row in context["orders"] if row["inst_id"].replace("-", "/") == symbol],
                                             "fills": [row for row in context["fills"] if row["inst_id"].replace("-", "/") == symbol]},
                         "audit_sha256": context["audit_sha256"], "live_order_effect": "none"}
                connection.execute("INSERT OR IGNORE INTO associations VALUES(?,?,?,?,?)", (_hash(event), advice_id, context["run_id"], now, _json(event)))
        counts = {name: connection.execute(f"SELECT count(*) FROM {name}").fetchone()[0] for name in ("advice", "associations")}
    result = {"schema_version": "v5.reference_receipts.v1", "observed_at": now, "mode": "record_only",
              "live_order_effect": "none", "status": error or (payload or {}).get("effective_status", "MISSING"),
              "receipts": receipts, "counts": counts, "context_runs": len(contexts)}
    descriptor, name = tempfile.mkstemp(dir=output, prefix=".latest-")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(_json(result))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, output / "latest.json")
    finally:
        if os.path.exists(name):
            os.unlink(name)
    return result


def utc_timestamp():
    return datetime.now(timezone.utc).timestamp()
