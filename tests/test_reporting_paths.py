from __future__ import annotations

import json

from src.core import run_logger
from src.reporting import decision_audit, metrics, reporting, summary_writer, trade_log


def test_trade_log_writer_resolves_relative_run_dir_from_project_root(monkeypatch, tmp_path):
    monkeypatch.setattr(trade_log, "PROJECT_ROOT", tmp_path)
    writer = trade_log.TradeLogWriter("reports/runs/test_run")
    assert writer.run_dir == (tmp_path / "reports" / "runs" / "test_run").resolve()
    assert writer.path == (tmp_path / "reports" / "runs" / "test_run" / "trades.csv").resolve()


def test_write_summary_resolves_relative_run_dir_from_project_root(monkeypatch, tmp_path):
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)

    run_dir = tmp_path / "reports" / "runs" / "test_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "equity.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"ts": "2026-04-15T00:00:00Z", "equity": 100.0}),
                json.dumps({"ts": "2026-04-15T01:00:00Z", "equity": 110.0}),
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "trades.csv").write_text(
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,slippage_usdt,realized_pnl_usdt,realized_pnl_pct\n",
        encoding="utf-8",
    )

    summary = summary_writer.write_summary("reports/runs/test_run")

    assert summary["run_id"] == "test_run"
    assert (run_dir / "summary.json").exists()


def test_reporting_write_json_resolves_relative_path_from_project_root(monkeypatch, tmp_path):
    monkeypatch.setattr(reporting, "PROJECT_ROOT", tmp_path)
    reporting.write_json("reports/example.json", {"ok": True})
    assert ((tmp_path / "reports" / "example.json").resolve()).exists()


def test_decision_audit_resolves_relative_run_dir_from_project_root(monkeypatch, tmp_path):
    monkeypatch.setattr(decision_audit, "PROJECT_ROOT", tmp_path)
    audit = decision_audit.DecisionAudit(run_id="test_run")
    audit.save("reports/runs/test_run")
    loaded = decision_audit.load_decision_audit("reports/runs/test_run")
    assert loaded is not None
    assert loaded.run_id == "test_run"


def test_run_logger_resolves_relative_run_dir_from_project_root(monkeypatch, tmp_path):
    monkeypatch.setattr(run_logger, "PROJECT_ROOT", tmp_path)
    logger = run_logger.RunLogger("reports/runs/test_run_logger")
    logger.log_equity({"equity": 100.0})
    logger.log_position({"symbol": "BTC/USDT", "qty": 1.0})
    assert (tmp_path / "reports" / "runs" / "test_run_logger" / "equity.jsonl").exists()
    assert (tmp_path / "reports" / "runs" / "test_run_logger" / "positions.jsonl").exists()
