from __future__ import annotations

import json
from pathlib import Path

from src.reporting.decision_audit import DecisionAudit, load_decision_audit
from src.reporting import metrics, summary_writer


def test_decision_audit_saves_and_loads_quant_lab(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    audit = DecisionAudit(run_id="r1")
    audit.quant_lab = {"enabled": True, "permission": "SELL_ONLY", "final_permission": "SELL_ONLY"}
    audit.save(str(run_dir))

    loaded = load_decision_audit(str(run_dir))

    assert loaded is not None
    assert loaded.quant_lab["permission"] == "SELL_ONLY"


def test_summary_json_includes_quant_lab(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)
    run_dir = tmp_path / "reports/runs/r1"
    run_dir.mkdir(parents=True)
    (run_dir / "equity.jsonl").write_text(json.dumps({"ts": "2026-05-11T00:00:00Z", "equity": 100}) + "\n", encoding="utf-8")
    (run_dir / "trades.csv").write_text(
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,slippage_usdt,realized_pnl_usdt,realized_pnl_pct\n",
        encoding="utf-8",
    )
    (run_dir / "decision_audit.json").write_text(
        json.dumps(
            {
                "run_id": "r1",
                "quant_lab": {
                    "enabled": True,
                    "permission": "SELL_ONLY",
                    "final_permission": "SELL_ONLY",
                    "cost_model_version": "cost_bucket_daily:2026-05-11",
                    "gate_version": "bootstrap.quarantine.v1",
                    "cost_request_count": 2,
                    "cost_fallback_count": 0,
                    "filtered_by_cost_count": 0,
                    "filtered_by_permission_count": 1,
                },
            }
        ),
        encoding="utf-8",
    )

    summary = summary_writer.write_summary("reports/runs/r1")

    assert summary["quant_lab"]["permission"] == "SELL_ONLY"
    assert summary["quant_lab"]["final_permission"] == "SELL_ONLY"
    assert summary["quant_lab"]["cost_request_count"] == 2
