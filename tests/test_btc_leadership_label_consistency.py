from __future__ import annotations

import csv
import json
from pathlib import Path

from src.reporting.btc_leadership_label_consistency import (
    ISSUE_CODE,
    update_btc_leadership_label_issues,
)


def _write_audit(bundle_root: Path, run_id: str, *, now_ts: int, decisions: list[dict]) -> None:
    run_dir = bundle_root / "raw" / "recent_runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "now_ts": now_ts,
                "router_decisions": decisions,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def test_btc_leadership_labeled_decision_clears_not_labeled_issue(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    run_id = "20260421_12"
    ts_utc = "2026-04-21T12:00:00Z"
    now_ts = 1_776_772_800
    _write_audit(
        bundle,
        run_id,
        now_ts=now_ts,
        decisions=[
            {
                "symbol": "BTC/USDT",
                "action": "skip",
                "reason": "btc_leadership_probe_alpha6_score_too_low",
            }
        ],
    )
    labels_path = bundle / "raw" / "reports" / "skipped_candidate_labels.jsonl"
    labels_path.parent.mkdir(parents=True, exist_ok=True)
    labels_path.write_text(
        json.dumps(
            {
                "ts_utc": ts_utc,
                "run_id": run_id,
                "symbol": "BTC/USDT",
                "skip_reason": "btc_leadership_probe_alpha6_score_too_low",
                "entry_px": 100.0,
                "label_status": "pending",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    issues_path = bundle / "summaries" / "issues_to_fix.json"
    issues_path.parent.mkdir(parents=True, exist_ok=True)
    issues_path.write_text(
        json.dumps(
            {
                "issues": [
                    {
                        "severity": "high",
                        "code": ISSUE_CODE,
                        "message": "stale",
                        "evidence": {
                            "run_id": run_id,
                            "symbol": "BTC/USDT",
                            "skip_reason": "btc_leadership_probe_alpha6_score_too_low",
                            "ts_utc": ts_utc,
                        },
                    }
                ],
                "high_issue_count": 1,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = update_btc_leadership_label_issues(bundle)

    assert result["expected_btc_leadership_blocked"] == 1
    assert result["missing_btc_leadership_labels"] == 0
    issues = json.loads(issues_path.read_text(encoding="utf-8"))
    assert not any(issue.get("code") == ISSUE_CODE for issue in issues["issues"])
    assert issues["high_issue_count"] == 0


def test_btc_leadership_price_context_uses_closed_bar_ts_for_key(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    run_id = "20260421_15"
    _write_audit(
        bundle,
        run_id,
        now_ts=1_776_783_615,
        decisions=[
            {
                "symbol": "BTC/USDT",
                "action": "skip",
                "reason": "btc_leadership_probe_alpha6_score_too_low",
                "latest_px": 100.0,
            }
        ],
    )
    audit_path = bundle / "raw" / "recent_runs" / run_id / "decision_audit.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    payload["window_end_ts"] = 1_776_783_600
    audit_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    labels_path = bundle / "raw" / "reports" / "skipped_candidate_labels.jsonl"
    labels_path.parent.mkdir(parents=True, exist_ok=True)
    labels_path.write_text(
        json.dumps(
            {
                "ts_utc": "2026-04-21T14:00:00Z",
                "run_id": run_id,
                "symbol": "BTC/USDT",
                "skip_reason": "btc_leadership_probe_alpha6_score_too_low",
                "entry_px": 100.0,
                "label_status": "pending",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    result = update_btc_leadership_label_issues(bundle)

    assert result["expected_btc_leadership_blocked"] == 1
    assert result["missing_btc_leadership_labels"] == 0


def test_btc_leadership_missing_label_writes_high_issue_with_unique_key(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    run_id = "20260421_13"
    _write_audit(
        bundle,
        run_id,
        now_ts=1_776_776_400,
        decisions=[
            {
                "symbol": "BTC/USDT",
                "action": "skip",
                "reason": "btc_leadership_probe_no_alpha6_buy",
            }
        ],
    )

    result = update_btc_leadership_label_issues(bundle)

    assert result["expected_btc_leadership_blocked"] == 1
    assert result["missing_btc_leadership_labels"] == 1
    issues = json.loads((bundle / "summaries" / "issues_to_fix.json").read_text(encoding="utf-8"))
    issue = next(issue for issue in issues["issues"] if issue.get("code") == ISSUE_CODE)
    assert issue["evidence"]["unique_key_fields"] == ["run_id", "symbol", "skip_reason", "ts_utc"]
    assert issue["evidence"]["run_id"] == run_id
    assert issue["evidence"]["symbol"] == "BTC/USDT"
    assert issue["evidence"]["skip_reason"] == "btc_leadership_probe_no_alpha6_buy"


def test_btc_leadership_blocked_outcomes_summary_is_deduped_by_same_key(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    summary_path = bundle / "summaries" / "btc_leadership_probe_blocked_outcomes.csv"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "ts_utc",
        "run_id",
        "symbol",
        "skip_reason",
        "entry_px",
        "label_status",
    ]
    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "ts_utc": "2026-04-21T14:00:00Z",
                "run_id": "20260421_14",
                "symbol": "BTC/USDT",
                "skip_reason": "btc_leadership_probe_cooldown",
                "entry_px": "",
                "label_status": "not_observable",
            }
        )
        writer.writerow(
            {
                "ts_utc": "2026-04-21T14:00:00Z",
                "run_id": "20260421_14",
                "symbol": "BTC/USDT",
                "skip_reason": "btc_leadership_probe_cooldown",
                "entry_px": "",
                "label_status": "not_observable",
            }
        )

    result = update_btc_leadership_label_issues(bundle)

    assert result["duplicate_rows_removed"] == 1
    with summary_path.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["skip_reason"] == "btc_leadership_probe_cooldown"
