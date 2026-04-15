from __future__ import annotations

import json

from src.reporting import metrics, reporting, summary_writer, trade_log


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
