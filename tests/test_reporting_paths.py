from __future__ import annotations

import json

import pytest

import main as main_module
from src.core import run_logger
from src.reporting import budget_state, decision_audit, fill_trade_exporter, metrics, reporting, summary_writer, trade_log


TRADE_HEADER = (
    "ts,run_id,symbol,intent,side,qty,price,notional_usdt,fee_usdt,"
    "slippage_usdt,realized_pnl_usdt,realized_pnl_pct"
)


def _write_equity(run_dir, ts="2026-05-12T00:00:00Z", equity=100.0):
    (run_dir / "equity.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"ts": ts, "equity": equity}),
                json.dumps({"ts": "2026-05-12T01:00:00Z", "equity": equity}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _write_trades(run_dir, rows):
    (run_dir / "trades.csv").write_text("\n".join([TRADE_HEADER, *rows]) + "\n", encoding="utf-8")


def test_trade_log_writer_resolves_relative_run_dir_from_project_root(monkeypatch, tmp_path):
    monkeypatch.setattr(trade_log, "PROJECT_ROOT", tmp_path)
    writer = trade_log.TradeLogWriter("reports/runs/test_run")
    assert writer.run_dir == (tmp_path / "reports" / "runs" / "test_run").resolve()
    assert writer.path == (tmp_path / "reports" / "runs" / "test_run" / "trades.csv").resolve()
    header = writer.path.read_text(encoding="utf-8").splitlines()[0].split(",")
    for field in (
        "run_id",
        "ts_utc",
        "symbol",
        "normalized_symbol",
        "side",
        "action",
        "qty",
        "price",
        "notional_usdt",
        "fee",
        "fee_ccy",
        "fee_usdt",
        "slippage_usdt",
        "order_id",
        "trade_id",
        "strategy_id",
        "position_id",
    ):
        assert field in header
    writer.append_fill(
        trade_log.Fill(
            ts="2026-05-13T00:00:00Z",
            run_id="test_run",
            symbol="BNBUSDT",
            intent="OPEN_LONG",
            side="buy",
            qty=0.02,
            price=600.0,
            notional_usdt=12.0,
            fee_usdt=None,
            slippage_usdt=None,
        )
    )
    row = dict(zip(header, writer.path.read_text(encoding="utf-8").splitlines()[1].split(",")))
    assert row["ts_utc"] == "2026-05-13T00:00:00Z"
    assert row["normalized_symbol"] == "BNB-USDT"
    assert row["fee_usdt"] == "null"
    assert row["slippage_usdt"] == "null"
    assert row["order_id"] == "null"


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
    assert summary["fills_count_today"] == 1
    assert summary["notional_usdt_total"] == 12.0
    assert summary["turnover_usdt"] == 12.0
    assert summary["fees_usdt_total"] == 0.012
    assert summary["cost_usdt_total"] == 0.012


def test_write_summary_counts_20260512_06_bnb_buy_fixture(monkeypatch, tmp_path):
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)

    run_dir = tmp_path / "reports" / "runs" / "20260512_06"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_equity(run_dir, ts="2026-05-12T06:00:00Z", equity=100.0)
    _write_trades(
        run_dir,
        [
            "2026-05-11T22:01:00.596000Z,20260512_06,BNB/USDT,OPEN_LONG,buy,"
            "0.0241,663.9,15.99999,0.01599999,0.001205,,"
        ],
    )

    summary = summary_writer.write_summary("reports/runs/20260512_06")

    assert summary["trades_file_exists"] is True
    assert summary["trades_file_rows"] == 1
    assert summary["trades_counted_rows"] == 1
    assert summary["trade_metrics_source"] == "trades_csv"
    assert summary["trade_metrics_warning"] == ""
    assert summary["num_trades"] == 1
    assert summary["fills_count_today"] == 1
    assert summary["turnover_usdt"] == pytest.approx(15.99999)
    assert summary["fees_usdt_total"] == pytest.approx(0.01599999)
    assert summary["slippage_usdt_total"] == pytest.approx(0.001205)
    assert summary["cost_usdt_total"] == pytest.approx(0.01720499)


def test_write_summary_counts_20260512_11_bnb_sell_fixture(monkeypatch, tmp_path):
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)

    run_dir = tmp_path / "reports" / "runs" / "20260512_11"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_equity(run_dir, ts="2026-05-12T11:00:00Z", equity=100.0)
    _write_trades(
        run_dir,
        [
            "2026-05-12T03:00:41.218000Z,20260512_11,BNB/USDT,CLOSE_LONG,sell,"
            "0.024075,662.8,15.95691,0.01595691,0.00120375,,"
        ],
    )

    summary = summary_writer.write_summary("reports/runs/20260512_11")

    assert summary["trades_file_exists"] is True
    assert summary["trades_file_rows"] == 1
    assert summary["trades_counted_rows"] == 1
    assert summary["trade_metrics_source"] == "trades_csv"
    assert summary["trade_metrics_warning"] == ""
    assert summary["num_trades"] == 1
    assert summary["fills_count_today"] == 1
    assert summary["turnover_usdt"] == pytest.approx(15.95691)
    assert summary["fees_usdt_total"] == pytest.approx(0.01595691)
    assert summary["slippage_usdt_total"] == pytest.approx(0.00120375)
    assert summary["cost_usdt_total"] == pytest.approx(0.01716066)


def test_daily_budget_counts_20260512_buy_sell_fills_from_summary_metrics(monkeypatch, tmp_path):
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(budget_state, "PROJECT_ROOT", tmp_path)

    fixtures = {
        "20260512_06": (
            "2026-05-11T22:01:00.596000Z,20260512_06,BNB/USDT,OPEN_LONG,buy,"
            "0.0241,663.9,15.99999,0.01599999,0.001205,,"
        ),
        "20260512_11": (
            "2026-05-12T03:00:41.218000Z,20260512_11,BNB/USDT,CLOSE_LONG,sell,"
            "0.024075,662.8,15.95691,0.01595691,0.00120375,,"
        ),
    }

    state = None
    for run_id, trade_row in fixtures.items():
        run_dir = tmp_path / "reports" / "runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        _write_equity(run_dir, ts="2026-05-12T00:00:00Z", equity=100.0)
        _write_trades(run_dir, [trade_row])
        summary = summary_writer.write_summary(f"reports/runs/{run_id}", window_end_ts=1778544000)
        trade_read = metrics.read_trades_csv_detailed(str(run_dir / "trades.csv"))
        notionals = [abs(float(row["notional_usdt"])) for row in trade_read.rows]
        state = budget_state.update_daily_budget_state(
            base_dir="reports/budget_state",
            ymd_utc="20260512",
            run_id=run_id,
            turnover_inc=float(summary["turnover_usdt"]),
            cost_inc_usdt=float(summary["cost_usdt_total"]),
            fills_count_inc=int(summary["trades_counted_rows"]),
            notionals_inc=notionals,
            avg_equity=summary.get("avg_equity"),
            turnover_budget_per_day=1.0,
            cost_budget_bps_per_day=100.0,
            small_trade_notional_cutoff=10.0,
        )

    assert state is not None
    assert state.fills_count_today == 2
    assert state.turnover_used == pytest.approx(31.9569)
    assert state.cost_used_usdt == pytest.approx(0.03436565)
    assert state.to_dict()["fills_count_today"] == 2


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
    assert summary["fills_count_today"] == 2
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
    assert "parse failed" in summary["trade_metrics_warning"]
    assert any("parse failed" in item for item in summary["trade_metrics_warnings"])


def test_attach_budget_keeps_fills_count_today_in_sync_with_trades(monkeypatch, tmp_path):
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)

    run_dir = tmp_path / "reports" / "runs" / "test_budget_reconcile"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_equity(run_dir, ts="2026-05-13T00:00:00Z", equity=100.0)
    _write_trades(
        run_dir,
        ["2026-05-13T00:10:00Z,test_budget_reconcile,BNB/USDT,OPEN_LONG,buy,0.02,600,12,0.012,0.001,,"],
    )
    summary_writer.write_summary("reports/runs/test_budget_reconcile")

    summary = summary_writer.attach_budget(
        "reports/runs/test_budget_reconcile",
        {"fills_count_today": 0, "turnover_used": 0.0},
    )

    assert summary["num_trades"] == 1
    assert summary["fills_count_today"] == 1
    assert summary["budget"]["fills_count_today"] == 1


def test_live_finalize_refreshes_summary_after_trades_flush(tmp_path):
    run_dir = tmp_path / "reports" / "runs" / "live_finalize_trade_flush"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_equity(run_dir, ts="2026-05-13T00:00:00Z", equity=100.0)
    (run_dir / "summary.json").write_text(
        json.dumps({"run_id": run_dir.name, "num_trades": 0, "budget": {"fills_count_today": 0}}),
        encoding="utf-8",
    )
    _write_trades(
        run_dir,
        ["2026-05-13T00:10:00Z,live_finalize_trade_flush,BNB/USDT,OPEN_LONG,buy,0.02,600,12,0.012,0.001,,"],
    )

    audit = decision_audit.DecisionAudit(run_id=run_dir.name)
    summary = main_module._finalize_live_run_summary_metrics(
        run_dir,
        audit=audit,
        current_summary={"num_trades": 0},
    )
    persisted = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))

    assert summary["num_trades"] == 1
    assert summary["fills_count_today"] == 1
    assert summary["budget"]["fills_count_today"] == 1
    assert summary["fees_usdt_total"] == pytest.approx(0.012)
    assert summary["slippage_usdt_total"] == pytest.approx(0.001)
    assert summary["cost_usdt_total"] == pytest.approx(0.013)
    assert persisted["num_trades"] == 1
    assert persisted["budget"]["fills_count_today"] == 1
    assert persisted["fees_usdt_total"] == pytest.approx(0.012)
    assert persisted["slippage_usdt_total"] == pytest.approx(0.001)
    assert persisted["cost_usdt_total"] == pytest.approx(0.013)
    assert audit.issues_to_fix == []


def test_live_finalize_refreshes_summary_after_attach_budget(tmp_path):
    run_dir = tmp_path / "reports" / "runs" / "live_finalize_budget_refresh"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_equity(run_dir, ts="2026-05-13T00:00:00Z", equity=100.0)
    (run_dir / "summary.json").write_text(
        json.dumps({"run_id": run_dir.name, "num_trades": 0, "budget": {"fills_count_today": 0}}),
        encoding="utf-8",
    )
    _write_trades(
        run_dir,
        ["2026-05-13T00:10:00Z,live_finalize_budget_refresh,BNB/USDT,OPEN_LONG,buy,0.02,600,12,0.012,0.001,,"],
    )

    audit = decision_audit.DecisionAudit(run_id=run_dir.name)
    summary = main_module._finalize_live_run_summary_metrics(run_dir, audit=audit, current_summary={"num_trades": 0})
    assert summary["num_trades"] == 1

    summary_writer.attach_budget(str(run_dir), {"fills_count_today": 0, "turnover_used": 0.0})
    final_summary = main_module._finalize_live_run_summary_metrics(run_dir, audit=audit, current_summary=summary)
    persisted = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))

    assert final_summary["num_trades"] == 1
    assert final_summary["trades_counted_rows"] == 1
    assert final_summary["budget"]["fills_count_today"] == 1
    assert final_summary["fees_usdt_total"] == pytest.approx(0.012)
    assert final_summary["slippage_usdt_total"] == pytest.approx(0.001)
    assert final_summary["cost_usdt_total"] == pytest.approx(0.013)
    assert persisted["num_trades"] == 1
    assert persisted["budget"]["fills_count_today"] == 1
    assert persisted["cost_usdt_total"] == pytest.approx(0.013)
    assert audit.issues_to_fix == []


def test_live_finalize_records_high_issue_when_summary_remains_stale(monkeypatch, tmp_path):
    run_dir = tmp_path / "reports" / "runs" / "live_finalize_stale_summary"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_equity(run_dir, ts="2026-05-13T00:00:00Z", equity=100.0)
    (run_dir / "summary.json").write_text(
        json.dumps({"run_id": run_dir.name, "num_trades": 0, "budget": {"fills_count_today": 0}}),
        encoding="utf-8",
    )
    _write_trades(
        run_dir,
        ["2026-05-13T00:10:00Z,live_finalize_stale_summary,BNB/USDT,OPEN_LONG,buy,0.02,600,12,0.012,0.001,,"],
    )

    def stale_refresh(_run_dir):
        return {"run_id": run_dir.name, "num_trades": 0, "budget": {"fills_count_today": 0}}

    monkeypatch.setattr(summary_writer, "refresh_summary_metrics", stale_refresh)
    audit = decision_audit.DecisionAudit(run_id=run_dir.name)
    summary = main_module._finalize_live_run_summary_metrics(run_dir, audit=audit, current_summary={"num_trades": 0})
    loaded = decision_audit.load_decision_audit(str(run_dir))
    persisted = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))

    assert summary["num_trades"] == 0
    assert persisted["summary_metrics_finalize_warning"] == "trades_csv_nonempty_summary_num_trades_zero_after_live_finalize"
    assert loaded is not None
    high_issues = [item for item in loaded.issues_to_fix if item.get("code") == "summary_trade_count_mismatch_after_live_finalize"]
    assert len(high_issues) == 1
    assert high_issues[0]["severity"] == "high"
    assert high_issues[0]["evidence"]["trades_counted_rows"] == 1


def test_export_fill_refreshes_summary_for_open_long_trade(tmp_path):
    run_dir = tmp_path / "reports" / "runs" / "20260514_23"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_equity(run_dir, ts="2026-05-14T23:00:00Z", equity=100.0)
    (run_dir / "summary.json").write_text(
        json.dumps({"run_id": run_dir.name, "num_trades": 0}),
        encoding="utf-8",
    )

    fill_trade_exporter.export_fill(
        fill_ts_ms=1778780400000,
        inst_id="BTC-USDT",
        side="buy",
        fill_px="78000",
        fill_sz="0.000205128205128",
        fee="-0.016",
        fee_ccy="USDT",
        run_id=run_dir.name,
        intent="OPEN_LONG",
        window_start_ts=None,
        window_end_ts=None,
        run_dir=str(run_dir),
    )

    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["num_trades"] == 1
    assert summary["trades_counted_rows"] == 1
    assert summary["turnover_usdt"] == pytest.approx(16.0)
    assert summary["fees_usdt_total"] == pytest.approx(0.016)
    assert summary["slippage_usdt_total"] == pytest.approx(0.0)


def test_export_fill_refreshes_summary_for_close_long_trade(tmp_path):
    run_dir = tmp_path / "reports" / "runs" / "20260515_02"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_equity(run_dir, ts="2026-05-15T02:00:00Z", equity=100.0)
    (run_dir / "summary.json").write_text(
        json.dumps({"run_id": run_dir.name, "num_trades": 0}),
        encoding="utf-8",
    )

    fill_trade_exporter.export_fill(
        fill_ts_ms=1778791200000,
        inst_id="BTC-USDT",
        side="sell",
        fill_px="78500",
        fill_sz="0.000203821656051",
        fee="-0.016",
        fee_ccy="USDT",
        run_id=run_dir.name,
        intent="CLOSE_LONG",
        window_start_ts=None,
        window_end_ts=None,
        run_dir=str(run_dir),
    )

    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["num_trades"] == 1
    assert summary["trades_counted_rows"] == 1
    assert summary["turnover_usdt"] == pytest.approx(16.0)
    assert summary["fees_usdt_total"] == pytest.approx(0.016)
    assert summary["slippage_usdt_total"] == pytest.approx(0.0)


def test_export_fill_fallback_refreshes_summary_when_summary_writer_unavailable(monkeypatch, tmp_path):
    run_dir = tmp_path / "reports" / "runs" / "20260515_03"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "summary.json").write_text(
        json.dumps({"run_id": run_dir.name, "num_trades": 0, "budget": {"fills_count_today": 0}}),
        encoding="utf-8",
    )

    def fail_summary_writer(_run_dir):
        raise ImportError("numpy unavailable")

    monkeypatch.setattr(fill_trade_exporter, "_refresh_summary_metrics_with_summary_writer", fail_summary_writer)

    fill_trade_exporter.export_fill(
        fill_ts_ms=1778794800000,
        inst_id="BTC-USDT",
        side="buy",
        fill_px="80000",
        fill_sz="0.0002",
        fee="-0.016",
        fee_ccy="USDT",
        run_id=run_dir.name,
        intent="OPEN_LONG",
        window_start_ts=None,
        window_end_ts=None,
        run_dir=str(run_dir),
    )

    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["num_trades"] == 1
    assert summary["trades_counted_rows"] == 1
    assert summary["turnover_usdt"] == pytest.approx(16.0)
    assert summary["fees_usdt_total"] == pytest.approx(0.016)
    assert summary["budget"]["fills_count_today"] == 1


def test_write_summary_keeps_zero_trades_when_trades_csv_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(summary_writer, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(metrics, "PROJECT_ROOT", tmp_path)

    run_dir = tmp_path / "reports" / "runs" / "test_no_trades"
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_equity(run_dir, ts="2026-05-15T00:00:00Z", equity=100.0)
    _write_trades(run_dir, [])

    summary = summary_writer.write_summary("reports/runs/test_no_trades")

    assert summary["num_trades"] == 0
    assert summary["trades_counted_rows"] == 0
    assert summary["turnover_usdt"] == 0.0
    assert summary["fees_usdt_total"] == 0.0
    assert summary["slippage_usdt_total"] == 0.0


def test_live_finalize_refresh_failure_records_high_issue(monkeypatch, tmp_path):
    run_dir = tmp_path / "reports" / "runs" / "live_finalize_refresh_fail"
    run_dir.mkdir(parents=True, exist_ok=True)
    audit = decision_audit.DecisionAudit(run_id=run_dir.name)

    def fail_refresh(_run_dir):
        raise RuntimeError("refresh broke")

    monkeypatch.setattr(summary_writer, "refresh_summary_metrics", fail_refresh)
    summary = main_module._refresh_live_summary_metrics_after_trades(
        run_dir,
        audit=audit,
        current_summary={"num_trades": 0},
    )
    loaded = decision_audit.load_decision_audit(str(run_dir))

    assert summary["num_trades"] == 0
    assert loaded is not None
    assert loaded.issues_to_fix
    issue = loaded.issues_to_fix[0]
    assert issue["severity"] == "high"
    assert issue["code"] == "summary_metrics_refresh_failed"
    assert issue["evidence"]["error_type"] == "RuntimeError"


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
