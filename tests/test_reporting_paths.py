from __future__ import annotations

import json

import pytest

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
    (run_dir / "decision_audit.json").write_text(
        json.dumps(
            {
                "counts": {
                    "negative_expectancy_score_penalty": 2,
                    "negative_expectancy_cooldown": 3,
                    "negative_expectancy_open_block": 4,
                    "negative_expectancy_fast_fail_open_block": 5,
                }
            }
        ),
        encoding="utf-8",
    )

    summary = summary_writer.write_summary("reports/runs/test_run")

    assert summary["run_id"] == "test_run"
    assert summary["negative_expectancy_penalty_count"] == 2
    assert summary["negative_expectancy_cooldown_count"] == 3
    assert summary["negative_expectancy_open_block_count"] == 4
    assert summary["negative_expectancy_fast_fail_open_block_count"] == 5
    assert summary["negative_expectancy_probation_release_count"] == 0
    assert summary["trades_file_exists"] is True
    assert summary["trades_file_rows"] == 0
    assert summary["trades_counted_rows"] == 0
    assert summary["trade_metrics_source"] == "trades_csv_empty"
    assert summary["num_trades"] == 0
    assert (run_dir / "summary.json").exists()


def test_write_summary_counts_single_bnb_trade_from_trades_csv(monkeypatch, tmp_path):
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)

    run_dir = tmp_path / "reports" / "runs" / "test_run_bnb_buy"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "equity.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"ts": "2026-05-13T00:00:00Z", "equity": 100.0}),
                json.dumps({"ts": "2026-05-13T01:00:00Z", "equity": 100.0}),
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "trades.csv").write_text(
        "\n".join(
            [
                "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,slippage_usdt,realized_pnl_usdt,realized_pnl_pct",
                "2026-05-13T00:10:00Z,test_run_bnb_buy,BNB/USDT,OPEN_LONG,buy,0.02,600,12,0.012,,,"
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    summary = summary_writer.write_summary("reports/runs/test_run_bnb_buy")

    assert summary["trades_file_exists"] is True
    assert summary["trades_file_rows"] == 1
    assert summary["trades_counted_rows"] == 1
    assert summary["trade_metrics_source"] == "trades_csv"
    assert summary["num_trades"] == 1
    assert summary["notional_usdt_total"] == 12.0
    assert summary["turnover_usdt"] == 12.0
    assert summary["fees_usdt_total"] == 0.012
    assert summary["cost_usdt_total"] == 0.012


def test_write_summary_counts_bnb_buy_sell_turnover_and_fee(monkeypatch, tmp_path):
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)

    run_dir = tmp_path / "reports" / "runs" / "test_run_bnb_roundtrip"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "equity.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"ts": "2026-05-13T00:00:00Z", "equity": 100.0}),
                json.dumps({"ts": "2026-05-13T01:00:00Z", "equity": 100.0}),
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "trades.csv").write_text(
        "\n".join(
            [
                "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,slippage_usdt,realized_pnl_usdt,realized_pnl_pct",
                "2026-05-13T00:10:00Z,test_run_bnb_roundtrip,BNB/USDT,OPEN_LONG,buy,0.02,600,12,0.012,0.001,,",
                "2026-05-13T00:40:00Z,test_run_bnb_roundtrip,BNB/USDT,CLOSE_LONG,sell,0.02,610,12.2,0.0122,0.0012,0.176,0.0144",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    summary = summary_writer.write_summary("reports/runs/test_run_bnb_roundtrip")

    assert summary["num_trades"] == 2
    assert summary["trades_counted_rows"] == 2
    assert summary["notional_usdt_total"] == 24.2
    assert summary["turnover_usdt"] == 24.2
    assert summary["turnover_ratio"] == 0.242
    assert summary["fees_usdt_total"] == 0.0242
    assert summary["slippage_usdt_total"] == pytest.approx(0.0022)
    assert summary["cost_usdt_total"] == pytest.approx(0.0264)


def test_write_summary_warns_on_malformed_trades_csv(monkeypatch, tmp_path):
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)

    run_dir = tmp_path / "reports" / "runs" / "test_run_malformed_trades"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "equity.jsonl").write_text(
        json.dumps({"ts": "2026-05-13T00:00:00Z", "equity": 100.0}) + "\n",
        encoding="utf-8",
    )
    (run_dir / "trades.csv").write_text(
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt\n"
        "2026-05-13T00:10:00Z,test_run_malformed_trades,BNB/USDT,OPEN_LONG,buy,\"0.02,600,12,0.012\n",
        encoding="utf-8",
    )

    summary = summary_writer.write_summary("reports/runs/test_run_malformed_trades")

    assert summary["trades_file_exists"] is True
    assert summary["trades_counted_rows"] == 0
    assert summary["num_trades"] == 0
    assert summary["trade_metrics_source"] == "trades_csv_parse_error"
    assert summary["trade_metrics_warning_count"] > 0
    assert any("parse failed" in item for item in summary["trade_metrics_warnings"])


def test_write_summary_sorts_unsorted_equity_rows_by_timestamp(monkeypatch, tmp_path):
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)

    run_dir = tmp_path / "reports" / "runs" / "test_run_unsorted"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "equity.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"ts": "2026-04-15T01:00:00Z", "equity": 110.0}),
                json.dumps({"ts": "2026-04-15T00:00:00Z", "equity": 100.0}),
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "trades.csv").write_text(
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,slippage_usdt,realized_pnl_usdt,realized_pnl_pct\n",
        encoding="utf-8",
    )

    summary = summary_writer.write_summary("reports/runs/test_run_unsorted")

    assert summary["start_ts"] == "2026-04-15T00:00:00Z"
    assert summary["end_ts"] == "2026-04-15T01:00:00Z"
    assert summary["equity_start"] == 100.0
    assert summary["equity_end"] == 110.0


def test_write_summary_deduplicates_same_timestamp_equity_rows_preferring_latest_value(monkeypatch, tmp_path):
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)

    run_dir = tmp_path / "reports" / "runs" / "test_run_duplicate_equity"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "equity.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"ts": "2026-04-15T00:00:00Z", "equity": 100.0}),
                json.dumps({"ts": "2026-04-15T00:00:00Z", "equity": 110.0}),
                json.dumps({"ts": "2026-04-15T01:00:00Z", "equity": 120.0}),
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "trades.csv").write_text(
        "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,slippage_usdt,realized_pnl_usdt,realized_pnl_pct\n",
        encoding="utf-8",
    )

    summary = summary_writer.write_summary("reports/runs/test_run_duplicate_equity")

    assert summary["start_ts"] == "2026-04-15T00:00:00Z"
    assert summary["end_ts"] == "2026-04-15T01:00:00Z"
    assert summary["equity_start"] == 110.0
    assert summary["equity_end"] == 120.0


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
