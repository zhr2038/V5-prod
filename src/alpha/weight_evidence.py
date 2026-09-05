"""Shared admission for dynamic IC and regime weights; failure preserves static weights."""
from __future__ import annotations

import hashlib
import json
import math
import time
from pathlib import Path
from typing import Any

from src.quant_lab_client.return_contract import finite_number

UNIVERSE = ("BNB/USDT", "BTC/USDT", "ETH/USDT", "SOL/USDT")


def factor_version() -> str:
    root = Path(__file__).resolve().parents[2]
    names = ("src/alpha/alpha_engine.py", "src/strategy/multi_strategy_system.py", "configs/schema.py")
    return "alpha6-source-" + hashlib.sha256(b"".join((root / name).read_bytes() for name in names)).hexdigest()


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def independent_count(rows: list[dict]) -> int:
    # Conservative lag-one effective count, in addition to non-overlapping outcome windows.
    values = [float(row["score_rank_ic"]) for row in rows]
    if len(values) < 3:
        return 0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values)
    if variance <= 1e-12:
        return 0
    rho = max(0.0, min(0.999, sum((a - mean) * (b - mean) for a, b in zip(values, values[1:])) / variance))
    return math.floor(len(values) * (1 - rho) / (1 + rho))


def bind_evidence(report: dict, rows: list[dict]) -> dict:
    evidence = {
        "schema": "v5.weight-evidence.v1", "cutoff_ts_ms": max((row["to_ts_ms"] for row in rows), default=0),
        "horizon_hours": 1, "universe": sorted(rows[-1].get("universe", [])) if rows else [],
        "factor_version": factor_version(), "sample_count": len(rows),
        "independent_samples": independent_count(rows), "source_sha256": digest(rows),
        "source_rows": rows, "payload_sha256": digest(report),
    }
    return {**report, "evidence": evidence}


def validate_weight_report(report: Any, cfg: Any, *, factors=(), now_ms: int | None = None) -> str | None:
    get = cfg.get if isinstance(cfg, dict) else lambda key, default: getattr(cfg, key, default)
    try:
        evidence = report["evidence"]
        if evidence["schema"] != "v5.weight-evidence.v1":
            return "evidence_schema_mismatch"
        cutoff = finite_number(evidence["cutoff_ts_ms"], minimum=1, maximum=1e15)
        age = ((time.time() * 1000 if now_ms is None else now_ms) - cutoff) / 1000
        if age < -5 or age > get("max_report_age_hours", 6) * 3600:
            return "report_future_or_expired"
        if evidence["horizon_hours"] != get("horizon_hours", 1):
            return "horizon_mismatch"
        if evidence["universe"] != sorted(get("expected_universe", list(UNIVERSE))):
            return "universe_mismatch"
        if evidence["factor_version"] != factor_version():
            return "factor_version_mismatch"
        rows = evidence["source_rows"]
        if len(rows) != evidence["sample_count"] or len(rows) < get("min_samples", 96):
            return "insufficient_samples"
        previous_end = 0
        for row in rows:
            start = finite_number(row["from_ts_ms"], minimum=1, maximum=1e15)
            end = finite_number(row["to_ts_ms"], minimum=1, maximum=1e15)
            if start < previous_end or end - start != evidence["horizon_hours"] * 3600000:
                return "overlapping_or_wrong_horizon_samples"
            previous_end = end
            if row.get("universe") != evidence["universe"] or row.get("factor_version") != evidence["factor_version"]:
                return "sample_provenance_mismatch"
            if row.get("valid_for_weighting") is not True:
                return "invalid_cross_section"
            finite_number(row["score_rank_ic"], minimum=-1, maximum=1)
        if previous_end != cutoff:
            return "cutoff_mismatch"
        count = independent_count(rows)
        if count != evidence["independent_samples"] or count < get("min_independent_samples", 24):
            return "insufficient_independent_samples"
        if evidence["source_sha256"] != digest(rows) or evidence["payload_sha256"] != digest({key: value for key, value in report.items() if key != "evidence"}):
            return "source_or_payload_hash_mismatch"
        for name in factors:
            record = report["factor_ic"][name]
            for key in ("rank_ic_short", "rank_ic_long"):
                bucket = record[key]
                finite_number(bucket["mean"], minimum=-1, maximum=1)
                sample_count = finite_number(bucket["count"], minimum=24, maximum=len(rows))
                if not sample_count.is_integer():
                    return "invalid_factor_sample_count"
        return None
    except (KeyError, TypeError, ValueError, OverflowError, OSError):
        return "missing_or_invalid_weight_evidence"


def valid_ic_mean(bucket: Any) -> float | None:
    try:
        count = finite_number(bucket["count"], minimum=24, maximum=1000000)
        if count.is_integer():
            return finite_number(bucket["mean"], minimum=-1, maximum=1)
    except (KeyError, TypeError, ValueError):
        pass
    return None
