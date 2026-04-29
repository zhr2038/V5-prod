from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from src.reporting.skipped_candidate_tracker import (
    BTC_LEADERSHIP_PROBE_LABEL_KEY_FIELDS,
    BTC_LEADERSHIP_PROBE_NOT_OBSERVABLE_SKIP_REASONS,
    BTC_LEADERSHIP_PROBE_SKIP_PREFIX,
    btc_leadership_probe_label_key,
    _coerce_epoch_ms,
    _iso_from_ms,
    _parse_timestamp_to_ms,
)

ISSUE_CODE = "btc_leadership_blocked_cases_not_labeled"
ONE_HOUR_MS = 3600 * 1000
SUMMARY_FILE = "btc_leadership_probe_blocked_label_summary.json"
DEFAULT_HORIZONS = (4, 8, 12, 24)


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _is_btc_leadership_skip(row: Mapping[str, Any]) -> bool:
    reason = str(row.get("skip_reason") or row.get("reason") or "")
    action = str(row.get("action") or "").strip().lower()
    return action == "skip" and reason.startswith(BTC_LEADERSHIP_PROBE_SKIP_PREFIX)


def _parse_run_id_ts_ms(run_id: str) -> int:
    text = str(run_id or "").strip()
    for fmt in ("%Y%m%d_%H", "%Y%m%d_%H%M", "%Y%m%dT%H", "%Y%m%dT%H%M"):
        try:
            dt = datetime.strptime(text[: len(datetime.now().strftime(fmt))], fmt)
        except Exception:
            continue
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    return 0


def _has_price_context(decision: Mapping[str, Any]) -> bool:
    return any(decision.get(key) not in (None, "") for key in ("px", "entry_px", "latest_px", "last_px", "signal_price"))


def _entry_px_from_decision(decision: Mapping[str, Any]) -> Any:
    for key in ("entry_px", "latest_px", "last_px", "px", "signal_price"):
        value = decision.get(key)
        if value not in (None, ""):
            return value
    return None


def _extract_ts_utc(decision: Mapping[str, Any], audit_payload: Mapping[str, Any], run_id: str) -> str:
    for key in ("ts_utc", "timestamp_utc", "time_utc"):
        ts_ms = _parse_timestamp_to_ms(decision.get(key))
        if ts_ms:
            return _iso_from_ms(ts_ms)
    for key in ("ts_ms", "timestamp_ms", "entry_ts_ms", "now_ts", "timestamp", "ts"):
        ts_ms = _coerce_epoch_ms(decision.get(key)) or _parse_timestamp_to_ms(decision.get(key))
        if ts_ms:
            return _iso_from_ms(int(ts_ms))
    if _has_price_context(decision):
        window_end_ms = _coerce_epoch_ms(audit_payload.get("window_end_ts"))
        if window_end_ms:
            return _iso_from_ms(int(window_end_ms) - ONE_HOUR_MS)
    for key in ("now_ts", "timestamp", "ts", "window_end_ts"):
        ts_ms = _coerce_epoch_ms(audit_payload.get(key)) or _parse_timestamp_to_ms(audit_payload.get(key))
        if ts_ms:
            return _iso_from_ms(int(ts_ms))
    run_ts_ms = _parse_run_id_ts_ms(run_id)
    return _iso_from_ms(run_ts_ms) if run_ts_ms > 0 else ""


def _expected_records(bundle_root: Path) -> list[dict[str, Any]]:
    recent_runs = bundle_root / "raw" / "recent_runs"
    records: dict[str, dict[str, Any]] = {}
    for audit_path in sorted(recent_runs.glob("*/decision_audit.json")):
        audit_payload = _load_json(audit_path, {})
        if not isinstance(audit_payload, dict):
            continue
        run_id = str(audit_payload.get("run_id") or audit_path.parent.name)
        for decision in audit_payload.get("router_decisions") or []:
            if not isinstance(decision, dict) or not _is_btc_leadership_skip(decision):
                continue
            record = {
                "run_id": run_id,
                "symbol": str(decision.get("symbol") or "BTC/USDT"),
                "skip_reason": str(decision.get("reason") or ""),
                "ts_utc": _extract_ts_utc(decision, audit_payload, run_id),
                "entry_px": _entry_px_from_decision(decision),
            }
            key = btc_leadership_probe_label_key(record)
            if key not in records:
                records[key] = record
    return list(records.values())


def _candidate_label_paths(bundle_root: Path) -> list[Path]:
    paths = {
        bundle_root / "raw" / "reports" / "skipped_candidate_labels.jsonl",
        bundle_root / "reports" / "skipped_candidate_labels.jsonl",
        bundle_root / "skipped_candidate_labels.jsonl",
    }
    paths.update(bundle_root.rglob("skipped_candidate_labels.jsonl"))
    return sorted(path for path in paths if path.exists())


def _candidate_outcome_paths(bundle_root: Path) -> list[Path]:
    paths = set(bundle_root.rglob("skipped_candidate_outcomes.csv"))
    paths.update(bundle_root.rglob("btc_leadership_probe_blocked_outcomes.csv"))
    return sorted(path for path in paths if path.exists())


def _observed_label_keys(bundle_root: Path) -> set[str]:
    return set(_observed_status_by_key(bundle_root).keys())


def _observed_status_by_key(bundle_root: Path) -> dict[str, str]:
    statuses: dict[str, str] = {}

    def add(row: Mapping[str, Any]) -> None:
        if not str(row.get("skip_reason") or "").startswith(BTC_LEADERSHIP_PROBE_SKIP_PREFIX):
            return
        key = btc_leadership_probe_label_key(row)
        if not key:
            return
        current = statuses.get(key)
        incoming = str(row.get("label_status") or "").strip()
        if _status_priority({"label_status": incoming}) > _status_priority({"label_status": current}):
            statuses[key] = incoming
        elif key not in statuses:
            statuses[key] = incoming

    for path in _candidate_label_paths(bundle_root):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if not isinstance(row, dict):
                continue
            add(row)
    for path in _candidate_outcome_paths(bundle_root):
        try:
            with path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    add(row)
        except Exception:
            continue
    return statuses


def _status_priority(row: Mapping[str, Any]) -> int:
    status = str(row.get("label_status") or "").strip()
    if status == "complete":
        return 3
    if status == "pending":
        return 2
    if status == "not_observable":
        return 1
    return 0


def _merge_rows(existing: dict[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    incoming_dict = dict(incoming)
    if _status_priority(incoming_dict) > _status_priority(existing):
        base = incoming_dict
        other = existing
    else:
        base = dict(existing)
        other = incoming_dict
    for key, value in other.items():
        if base.get(key) in (None, "") and value not in (None, ""):
            base[key] = value
    return base


def _dedupe_btc_blocked_outcomes(bundle_root: Path) -> dict[str, int]:
    touched = 0
    removed = 0
    for path in sorted(bundle_root.rglob("btc_leadership_probe_blocked_outcomes.csv")):
        try:
            with path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fieldnames = list(reader.fieldnames or [])
                rows = [dict(row) for row in reader]
        except Exception:
            continue
        merged: dict[str, dict[str, Any]] = {}
        passthrough: list[dict[str, Any]] = []
        for row in rows:
            if str(row.get("skip_reason") or "").startswith(BTC_LEADERSHIP_PROBE_SKIP_PREFIX):
                key = btc_leadership_probe_label_key(row)
                if key in merged:
                    removed += 1
                    merged[key] = _merge_rows(merged[key], row)
                else:
                    merged[key] = row
            else:
                passthrough.append(row)
        deduped = passthrough + sorted(
            merged.values(),
            key=lambda row: (
                str(row.get("ts_utc") or ""),
                str(row.get("run_id") or ""),
                str(row.get("symbol") or ""),
                str(row.get("skip_reason") or ""),
            ),
        )
        if len(deduped) != len(rows):
            touched += 1
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                for row in deduped:
                    writer.writerow(row)
    return {"files_touched": touched, "duplicate_rows_removed": removed}


def _dedupe_skipped_candidate_labels(bundle_root: Path) -> dict[str, int]:
    touched = 0
    removed = 0
    for path in _candidate_label_paths(bundle_root):
        passthrough: list[Any] = []
        merged: dict[str, dict[str, Any]] = {}
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception:
                passthrough.append(line)
                continue
            if not isinstance(row, dict) or not str(row.get("skip_reason") or "").startswith(BTC_LEADERSHIP_PROBE_SKIP_PREFIX):
                passthrough.append(row)
                continue
            key = btc_leadership_probe_label_key(row)
            if key in merged:
                removed += 1
                merged[key] = _merge_rows(merged[key], row)
            else:
                merged[key] = row
        deduped_rows: list[Any] = passthrough + sorted(
            merged.values(),
            key=lambda row: (
                str(row.get("ts_utc") or ""),
                str(row.get("run_id") or ""),
                str(row.get("symbol") or ""),
                str(row.get("skip_reason") or ""),
            ),
        )
        if len(deduped_rows) != len([line for line in lines if line.strip()]):
            touched += 1
            with path.open("w", encoding="utf-8") as handle:
                for row in deduped_rows:
                    if isinstance(row, str):
                        handle.write(row.rstrip() + "\n")
                    else:
                        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return {"label_files_touched": touched, "label_duplicate_rows_removed": removed}


def _default_btc_outcome_fieldnames(existing: list[str]) -> list[str]:
    base = list(existing or [])
    for field in (
        "ts_utc",
        "run_id",
        "symbol",
        "skip_reason",
        "entry_px",
        "label_status",
        "label_not_observable_reason",
        "not_observable_reason",
    ):
        if field not in base:
            base.append(field)
    for horizon in DEFAULT_HORIZONS:
        for field in (
            f"label_{horizon}h_gross_bps",
            f"label_{horizon}h_net_bps",
            f"label_{horizon}h_would_have_won_net",
            f"label_{horizon}h_status",
            f"label_{horizon}h_reason",
        ):
            if field not in base:
                base.append(field)
    return base


def _load_csv_rows(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists():
        return [], []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader], list(reader.fieldnames or [])


def _write_csv_rows(path: Path, rows: list[Mapping[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _not_observable_outcome(record: Mapping[str, Any]) -> dict[str, Any]:
    reason = str(record.get("skip_reason") or "")
    not_observable_reason = f"{reason}_entry_px_not_observable"
    row: dict[str, Any] = {
        "ts_utc": record.get("ts_utc"),
        "run_id": record.get("run_id"),
        "symbol": record.get("symbol") or "BTC/USDT",
        "skip_reason": reason,
        "entry_px": "not_observable",
        "label_status": "not_observable",
        "label_not_observable_reason": not_observable_reason,
        "not_observable_reason": not_observable_reason,
    }
    for horizon in DEFAULT_HORIZONS:
        row[f"label_{horizon}h_gross_bps"] = "not_observable"
        row[f"label_{horizon}h_net_bps"] = "not_observable"
        row[f"label_{horizon}h_would_have_won_net"] = "not_observable"
        row[f"label_{horizon}h_status"] = "not_observable"
        row[f"label_{horizon}h_reason"] = not_observable_reason
    return row


def _ensure_not_observable_outcomes(bundle_root: Path, expected_records: list[dict[str, Any]]) -> int:
    path = bundle_root / "summaries" / "btc_leadership_probe_blocked_outcomes.csv"
    rows, fieldnames = _load_csv_rows(path)
    merged: dict[str, dict[str, Any]] = {}
    passthrough: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("skip_reason") or "").startswith(BTC_LEADERSHIP_PROBE_SKIP_PREFIX):
            key = btc_leadership_probe_label_key(row)
            merged[key] = _merge_rows(merged[key], row) if key in merged else row
        else:
            passthrough.append(row)
    added = 0
    for record in expected_records:
        reason = str(record.get("skip_reason") or "")
        if reason not in BTC_LEADERSHIP_PROBE_NOT_OBSERVABLE_SKIP_REASONS:
            continue
        key = btc_leadership_probe_label_key(record)
        if key in merged:
            continue
        merged[key] = _not_observable_outcome(record)
        added += 1
    if added or rows:
        fieldnames = _default_btc_outcome_fieldnames(fieldnames)
        deduped = passthrough + sorted(
            merged.values(),
            key=lambda row: (
                str(row.get("ts_utc") or ""),
                str(row.get("run_id") or ""),
                str(row.get("symbol") or ""),
                str(row.get("skip_reason") or ""),
            ),
        )
        _write_csv_rows(path, deduped, fieldnames)
    return added


def _load_issues(path: Path) -> dict[str, Any]:
    payload = _load_json(path, {})
    return payload if isinstance(payload, dict) else {}


def _refresh_issue_counts(payload: dict[str, Any]) -> None:
    issues = [issue for issue in payload.get("issues", []) if isinstance(issue, dict)]
    payload["issues"] = issues
    payload["high_issue_count"] = sum(1 for issue in issues if issue.get("severity") == "high")
    payload["medium_issue_count"] = sum(1 for issue in issues if issue.get("severity") == "medium")
    payload["warning_count"] = sum(1 for issue in issues if issue.get("severity") == "warning")


def update_btc_leadership_label_issues(bundle_root: str | Path) -> dict[str, Any]:
    root = Path(bundle_root)
    expected_records = _expected_records(root)
    label_dedupe_result = _dedupe_skipped_candidate_labels(root)
    dedupe_result = _dedupe_btc_blocked_outcomes(root)
    not_observable_added = _ensure_not_observable_outcomes(root, expected_records)
    observed_keys = _observed_label_keys(root)
    missing_records = [
        record
        for record in expected_records
        if btc_leadership_probe_label_key(record) not in observed_keys
        and str(record.get("skip_reason") or "") not in BTC_LEADERSHIP_PROBE_NOT_OBSERVABLE_SKIP_REASONS
    ]

    issue_path = root / "summaries" / "issues_to_fix.json"
    payload = _load_issues(issue_path)
    existing_issues = payload.get("issues", [])
    if not isinstance(existing_issues, list):
        existing_issues = []
    payload["issues"] = [
        issue
        for issue in existing_issues
        if not (isinstance(issue, dict) and issue.get("code") == ISSUE_CODE)
    ]
    for record in missing_records:
        payload["issues"].append(
            {
                "severity": "high",
                "code": ISSUE_CODE,
                "message": "BTC leadership probe skip decision was not found in skipped candidate labels or outcomes.",
                "evidence": {
                    "unique_key_fields": list(BTC_LEADERSHIP_PROBE_LABEL_KEY_FIELDS),
                    "run_id": record.get("run_id"),
                    "symbol": record.get("symbol"),
                    "skip_reason": record.get("skip_reason"),
                    "ts_utc": record.get("ts_utc"),
                },
            }
        )
    _refresh_issue_counts(payload)
    issue_path.parent.mkdir(parents=True, exist_ok=True)
    issue_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    statuses = _observed_status_by_key(root)
    expected_keys = {btc_leadership_probe_label_key(record) for record in expected_records}
    total_duplicates_removed = int(dedupe_result.get("duplicate_rows_removed") or 0) + int(
        label_dedupe_result.get("label_duplicate_rows_removed") or 0
    )
    summary = {
        "total_blocked": len(expected_records),
        "labeled_complete": sum(1 for key in expected_keys if statuses.get(key) == "complete"),
        "not_observable": sum(1 for key in expected_keys if statuses.get(key) == "not_observable"),
        "pending": sum(1 for key in expected_keys if statuses.get(key) == "pending"),
        "duplicated_removed": total_duplicates_removed,
        "unlabeled_high_issue_count": len(missing_records),
        "not_observable_added": not_observable_added,
    }
    summary_path = root / "summaries" / SUMMARY_FILE
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        **summary,
        "expected_btc_leadership_blocked": len(expected_records),
        "observed_btc_leadership_labels": len(observed_keys),
        "missing_btc_leadership_labels": len(missing_records),
        "issues_path": str(issue_path),
        "summary_path": str(summary_path),
        **label_dedupe_result,
        **dedupe_result,
    }
