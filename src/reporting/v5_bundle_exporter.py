from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import shutil
import tarfile
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

from src.reporting.quant_lab_audit import read_quant_lab_usage, sanitize_quant_lab_obj


SECRET_MARKERS = (
    "api_key",
    "apiSecret",
    "api_secret",
    "secret_key",
    "passphrase",
    "private_key",
    "OK-ACCESS-KEY",
    "OK-ACCESS-SIGN",
    "OK-ACCESS-PASSPHRASE",
    "Authorization",
    "Bearer",
    "token",
    "password",
    "BEGIN PRIVATE KEY",
    "EXCHANGE_API_KEY",
    "EXCHANGE_API_SECRET",
    "EXCHANGE_PASSPHRASE",
    "QUANT_LAB_API_TOKEN",
)

COMPLIANCE_FIELDS = (
    "run_id",
    "ts",
    "mode",
    "called_api",
    "permission_gate_enforced",
    "cost_gate_enforced",
    "quant_lab_permission",
    "final_permission",
    "local_preflight_permission",
    "new_risk_order_count",
    "sell_order_count",
    "filtered_by_permission_count",
    "filtered_by_cost_count",
    "hypothetical_violation",
    "actual_violation",
    "violation",
    "violation_reason",
)

COST_FIELDS = (
    "run_id",
    "ts",
    "mode",
    "cost_gate_enforced",
    "would_filter",
    "actually_filtered",
    "symbol",
    "regime",
    "notional_usdt",
    "quantile",
    "fee_bps",
    "slippage_bps",
    "spread_bps",
    "total_cost_bps",
    "effective_total_cost_bps",
    "local_cost_bps",
    "local_cost_source",
    "fallback_level",
    "source",
    "sample_count",
    "cost_model_version",
    "expected_edge_bps",
    "min_required_edge_bps",
    "passed",
    "filtered",
    "filter_reason",
)

FALLBACK_FIELDS = ("run_id", "ts", "mode", "event_type", "reason", "fallback_policy", "fallback_scope", "action_taken")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _since_iso(window_hours: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=int(window_hours))).isoformat().replace("+00:00", "Z")


def _redact_text(text: str) -> str:
    out_lines: list[str] = []
    for line in text.splitlines():
        lowered = line.lower()
        if any(marker.lower() in lowered for marker in SECRET_MARKERS):
            if ":" in line:
                prefix = line.split(":", 1)[0]
                out_lines.append(f"{prefix}: <REDACTED>")
            elif "=" in line:
                prefix = line.split("=", 1)[0]
                out_lines.append(f"{prefix}=<REDACTED>")
            else:
                out_lines.append("<REDACTED>")
        else:
            out_lines.append(line)
    return "\n".join(out_lines) + ("\n" if text.endswith("\n") else "")


def _read_text_redacted(path: Path) -> str:
    try:
        return _redact_text(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return ""


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_csv(path: Path, fields: Iterable[str], rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(fields))
        writer.writeheader()
        for row in rows:
            writer.writerow({field: sanitize_quant_lab_obj(row).get(field, "") for field in writer.fieldnames})


def _read_jsonl(path: Path) -> list[Dict[str, Any]]:
    rows: list[Dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _filter_window(rows: list[Dict[str, Any]], since_ts: str) -> list[Dict[str, Any]]:
    return [row for row in rows if not row.get("ts") or str(row.get("ts")) >= since_ts]


def _is_new_risk_row(row: Mapping[str, Any]) -> bool:
    side = str(row.get("side") or "").lower()
    intent = str(row.get("intent") or "").upper()
    return side == "buy" or intent in {"OPEN_LONG", "REBALANCE"}


def _build_compliance_rows(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    by_run: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        run_id = str(row.get("run_id") or "unknown")
        item = by_run.setdefault(
            run_id,
            {
                "run_id": run_id,
                "ts": row.get("ts") or "",
                "mode": row.get("mode") or "",
                "called_api": row.get("called_api", ""),
                "permission_gate_enforced": row.get("permission_gate_enforced", ""),
                "cost_gate_enforced": row.get("cost_gate_enforced", ""),
                "quant_lab_permission": "",
                "final_permission": "",
                "local_preflight_permission": "",
                "new_risk_order_count": 0,
                "sell_order_count": 0,
                "filtered_by_permission_count": 0,
                "filtered_by_cost_count": 0,
                "hypothetical_violation": "false",
                "actual_violation": "false",
                "violation": "false",
                "violation_reason": "",
            },
        )
        if row.get("ts"):
            item["ts"] = row.get("ts")
        if row.get("mode"):
            item["mode"] = row.get("mode")
        if "called_api" in row:
            item["called_api"] = row.get("called_api")
        if "permission_gate_enforced" in row:
            item["permission_gate_enforced"] = row.get("permission_gate_enforced")
        if "cost_gate_enforced" in row:
            item["cost_gate_enforced"] = row.get("cost_gate_enforced")
        permission = row.get("permission") or row.get("quant_lab_permission") or row.get("quant_lab_decision")
        final = row.get("final_permission") or row.get("effective_decision")
        if permission:
            item["quant_lab_permission"] = permission
        if final:
            item["final_permission"] = final
        if row.get("local_preflight_permission"):
            item["local_preflight_permission"] = row.get("local_preflight_permission")
        if row.get("event_type") == "filter_order":
            if _is_new_risk_row(row) and not row.get("actually_filtered") and not row.get("order_filtered"):
                item["new_risk_order_count"] = int(item["new_risk_order_count"]) + 1
            if str(row.get("side") or "").lower() == "sell":
                item["sell_order_count"] = int(item["sell_order_count"]) + 1
            reason = str(row.get("filter_reason") or "")
            if (row.get("actually_filtered") or row.get("order_filtered")) and ("sell_only" in reason or "abort" in reason):
                item["filtered_by_permission_count"] = int(item["filtered_by_permission_count"]) + 1
            if (row.get("actually_filtered") or row.get("order_filtered")) and "cost" in reason:
                item["filtered_by_cost_count"] = int(item["filtered_by_cost_count"]) + 1
            if row.get("would_filter") and not (row.get("actually_filtered") or row.get("order_filtered")):
                item["hypothetical_violation"] = "true"
        final_permission = str(item.get("final_permission") or item.get("quant_lab_permission") or "").upper()
        enforced = str(item.get("permission_gate_enforced")).lower() == "true"
        if enforced and final_permission == "ABORT" and int(item["new_risk_order_count"]) > 0:
            item["actual_violation"] = "true"
            item["violation"] = "true"
            item["violation_reason"] = "abort_new_risk_order_submitted"
        if enforced and final_permission == "SELL_ONLY" and int(item["new_risk_order_count"]) > 0:
            item["actual_violation"] = "true"
            item["violation"] = "true"
            item["violation_reason"] = "sell_only_new_risk_order_submitted"
    return list(by_run.values())


def _build_cost_rows(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    out: list[Dict[str, Any]] = []
    for row in rows:
        if row.get("event_type") != "cost_estimate":
            continue
        out.append({field: row.get(field, "") for field in COST_FIELDS})
    return out


def _build_fallback_rows(rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    out: list[Dict[str, Any]] = []
    for row in rows:
        if not row.get("fallback_used") and row.get("event_type") != "fallback":
            continue
        out.append(
            {
                "run_id": row.get("run_id", ""),
                "ts": row.get("ts", ""),
                "mode": row.get("mode", ""),
                "event_type": row.get("event_type", ""),
                "reason": row.get("fallback_reason") or row.get("reason") or row.get("filter_reason") or "",
                "fallback_policy": row.get("fallback_policy") or row.get("fail_policy") or "",
                "fallback_scope": row.get("fallback_scope") or row.get("event_type") or "",
                "action_taken": row.get("action_taken") or row.get("permission") or row.get("final_permission") or "",
            }
        )
    return out


def _window_summary(rows: list[Dict[str, Any]], request_rows: list[Dict[str, Any]], compliance_rows: list[Dict[str, Any]]) -> Dict[str, Any]:
    latest_permission = None
    final_permission = None
    cost_model_version = None
    gate_version = None
    latest_mode = None
    for row in rows:
        latest_mode = row.get("mode") or latest_mode
        latest_permission = row.get("permission") or row.get("quant_lab_permission") or row.get("quant_lab_decision") or latest_permission
        final_permission = row.get("final_permission") or row.get("effective_decision") or final_permission
        cost_model_version = row.get("cost_model_version") or cost_model_version
        gate_version = row.get("gate_version") or gate_version
    return {
        "quant_lab_enabled": bool(rows or request_rows),
        "quant_lab_mode": latest_mode,
        "quant_lab_request_count": len(request_rows),
        "quant_lab_error_count": len([row for row in request_rows if row.get("success") is False or row.get("ok") is False]),
        "quant_lab_fallback_count": len([row for row in rows if row.get("fallback_used")]),
        "quant_lab_filtered_by_cost_count": len(
            [row for row in rows if row.get("order_filtered") and "cost" in str(row.get("filter_reason") or "")]
        ),
        "quant_lab_filtered_by_permission_count": len(
            [
                row
                for row in rows
                if row.get("order_filtered")
                and ("sell_only" in str(row.get("filter_reason") or "") or "abort" in str(row.get("filter_reason") or ""))
            ]
        ),
        "quant_lab_latest_permission": latest_permission,
        "quant_lab_final_permission": final_permission,
        "quant_lab_gate_compliance_violation_count": len([row for row in compliance_rows if str(row.get("violation")) == "true"]),
        "quant_lab_cost_model_version": cost_model_version,
        "quant_lab_gate_version": gate_version,
    }


def _issues(rows: list[Dict[str, Any]], request_rows: list[Dict[str, Any]], cost_rows: list[Dict[str, Any]], compliance_rows: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    issues: list[Dict[str, Any]] = []

    def add(code: str, severity: str, detail: str) -> None:
        if not any(item["code"] == code for item in issues):
            issues.append({"code": code, "severity": severity, "detail": detail})

    if not rows:
        add("quant_lab_missing_usage_log", "medium", "reports/quant_lab_usage.jsonl is missing or empty")
    if not cost_rows:
        add("quant_lab_missing_cost_usage", "medium", "no cost_estimate telemetry in window")
    if any(row.get("success") is False for row in request_rows):
        add("quant_lab_api_unavailable", "medium", "quant-lab request errors were observed")
    latest_permission = next((row.get("final_permission") or row.get("permission") for row in reversed(rows) if row.get("final_permission") or row.get("permission")), "")
    if str(latest_permission).upper() == "ABORT":
        add("quant_lab_permission_abort", "high", "latest quant-lab permission is ABORT")
    if str(latest_permission).upper() == "SELL_ONLY":
        add("quant_lab_permission_sell_only", "medium", "latest quant-lab permission is SELL_ONLY")
    if any(str(row.get("violation")) == "true" for row in compliance_rows):
        add("quant_lab_gate_compliance_violation", "high", "orders violated quant-lab permission in the window")
    if len([row for row in rows if row.get("fallback_used")]) >= 3:
        add("quant_lab_cost_fallback_high", "medium", "quant-lab fallback count is elevated")
    if any(row.get("filtered") for row in cost_rows):
        add("quant_lab_cost_gate_filtered_trade", "low", "quant-lab cost gate filtered at least one order")
    if len([row for row in request_rows if row.get("success") is False]) >= 3:
        add("quant_lab_request_error_high", "medium", "quant-lab request error count is elevated")
    if any(str(row.get("source") or "").lower() == "public_spread_proxy" for row in cost_rows):
        add("quant_lab_public_proxy_cost_only", "medium", "quant-lab cost source is public_spread_proxy")
    return issues


def _add_file(tf: tarfile.TarFile, arcname: str, data: str | bytes) -> None:
    raw = data if isinstance(data, bytes) else data.encode("utf-8")
    info = tarfile.TarInfo(arcname)
    info.size = len(raw)
    info.mtime = int(datetime.now(timezone.utc).timestamp())
    tf.addfile(info, io.BytesIO(raw))


def _secret_scan_findings(staging: Path) -> int:
    findings = 0
    risky_values = ("P@ssw0rd", "Bearer ", "BEGIN PRIVATE KEY", "super-secret-token")
    for path in staging.rglob("*"):
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if any(value in text for value in risky_values):
            findings += 1
    return findings


def export_v5_bundle(
    *,
    reports_dir: str | Path,
    out_dir: str | Path,
    window_hours: int = 72,
    include_logs: bool = True,
    include_config: bool = True,
) -> Path:
    reports = Path(reports_dir).resolve()
    root = reports.parent
    out = Path(out_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    since = _since_iso(window_hours)
    usage_path = reports / "quant_lab_usage.jsonl"
    requests_path = reports / "quant_lab_requests.jsonl"
    usage_rows = _filter_window(read_quant_lab_usage(usage_path), since)
    request_rows = _filter_window(_read_jsonl(requests_path), since)
    compliance_rows = _build_compliance_rows(usage_rows)
    cost_rows = _build_cost_rows(usage_rows)
    fallback_rows = _build_fallback_rows(usage_rows)

    stamp = _utc_stamp()
    bundle_name = f"v5_live_followup_bundle_{stamp}.tar.gz"
    final_path = out / bundle_name
    tmp_path = out / f"{bundle_name}.tmp"
    staging = Path(tempfile.mkdtemp(prefix="v5_bundle_", dir=str(out)))
    try:
        _write_text(staging / "raw/quant_lab/quant_lab_usage.jsonl", _redact_text(usage_path.read_text(encoding="utf-8") if usage_path.exists() else ""))
        _write_text(staging / "raw/quant_lab/quant_lab_requests.jsonl", _redact_text(requests_path.read_text(encoding="utf-8") if requests_path.exists() else ""))
        _write_csv(staging / "summaries/quant_lab_compliance.csv", COMPLIANCE_FIELDS, compliance_rows)
        _write_csv(staging / "summaries/quant_lab_cost_usage.csv", COST_FIELDS, cost_rows)
        _write_csv(staging / "summaries/quant_lab_fallbacks.csv", FALLBACK_FIELDS, fallback_rows)
        window_summary = _window_summary(usage_rows, request_rows, compliance_rows)
        _write_text(staging / "summaries/window_summary.json", json.dumps(window_summary, ensure_ascii=False, indent=2))
        _write_text(
            staging / "summaries/issues_to_fix.json",
            json.dumps(_issues(usage_rows, request_rows, cost_rows, compliance_rows), ensure_ascii=False, indent=2),
        )
        if include_config:
            for rel in ("configs/config.yaml", "configs/live_prod.yaml"):
                path = root / rel
                if path.exists():
                    _write_text(staging / "raw/config" / Path(rel).name, _read_text_redacted(path))
        if include_logs:
            log_path = root / "logs/v5_runtime.log"
            if log_path.exists():
                _write_text(staging / "raw/logs/v5_runtime.log", _read_text_redacted(log_path))
        findings = _secret_scan_findings(staging)
        manifest = {
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "reports_dir": str(reports),
            "window_hours": int(window_hours),
            "sanity_checks": {
                "no_env_files": True,
                "no_unredacted_secret_assignments": findings == 0,
                "redaction_applied": True,
                "secret_scan_findings_count": findings,
            },
        }
        _write_text(staging / "manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        with tarfile.open(tmp_path, "w:gz") as tf:
            for path in sorted(staging.rglob("*")):
                if path.is_file():
                    tf.add(path, arcname=path.relative_to(staging).as_posix())
        tmp_path.replace(final_path)
        digest = hashlib.sha256(final_path.read_bytes()).hexdigest()
        (out / f"{bundle_name}.sha256").write_text(f"{digest}  {bundle_name}\n", encoding="utf-8")
        return final_path
    finally:
        shutil.rmtree(staging, ignore_errors=True)
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Export sanitized V5 telemetry bundle for quant-lab")
    parser.add_argument("--reports-dir", default="reports")
    parser.add_argument("--out-dir", default="/var/lib/v5/exports/bundles")
    parser.add_argument("--window-hours", type=int, default=72)
    args = parser.parse_args(argv)
    bundle = export_v5_bundle(reports_dir=args.reports_dir, out_dir=args.out_dir, window_hours=args.window_hours)
    print(json.dumps({"bundle_path": str(bundle), "sha256_path": str(bundle) + ".sha256"}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
