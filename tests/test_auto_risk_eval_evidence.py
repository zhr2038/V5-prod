from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

import scripts.auto_risk_eval as evaluation


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    now = datetime(2026, 9, 5, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(evaluation, "_utc_now", lambda: now)
    import src.risk.live_equity_fetcher as live
    monkeypatch.setattr(live, "get_live_equity_from_okx", lambda **kwargs: 96.0)
    paths = evaluation.AutoRiskEvalPaths(
        reports_dir=tmp_path, runs_dir=tmp_path / "runs", auto_risk_eval_path=tmp_path / "eval.json",
        positions_db=tmp_path / "positions.sqlite", auto_risk_guard_path=tmp_path / "guard.json",
        env_path=tmp_path / ".env",
    )
    with sqlite3.connect(paths.positions_db) as con:
        con.execute("CREATE TABLE account_state(k TEXT, equity_peak_usdt REAL)")
        con.execute("INSERT INTO account_state VALUES ('default',100)")
    with sqlite3.connect(paths.orders_db) as con:
        con.execute("CREATE TABLE orders(cl_ord_id TEXT, ord_id TEXT, inst_id TEXT, run_id TEXT, side TEXT)")
    with sqlite3.connect(paths.fills_db) as con:
        con.execute("CREATE TABLE fills(trade_id TEXT,cl_ord_id TEXT,ord_id TEXT,inst_id TEXT,side TEXT,fill_sz TEXT,fill_px TEXT,fee TEXT,fee_ccy TEXT,ts_ms INTEGER)")
        con.execute("CREATE TABLE sync_state(k TEXT,v TEXT)")
        con.execute("INSERT INTO sync_state VALUES ('last_sync_ts_ms',?)", (str(int(now.timestamp() * 1000)),))
    return paths


def run(offset=0, selected=1, proposed=1, pnl=None):
    stamp = datetime(2026, 9, 5, 10, tzinfo=timezone.utc) + timedelta(hours=offset)
    return {"run_id": stamp.strftime("%Y%m%d_%H"), "timestamp": stamp.timestamp(),
            "counts": {"selected": selected, "orders_rebalance": proposed}, "realized_pnl": pnl}


def add_fill(runtime, *, trade_id="t1", clid="c1", oid="o1", side="buy", qty="1", price="10", fee="0", fee_ccy="USDT", offset=0):
    ts = int((datetime(2026, 9, 5, 10, tzinfo=timezone.utc) + timedelta(minutes=offset)).timestamp() * 1000)
    with sqlite3.connect(runtime.fills_db) as con:
        con.execute("INSERT INTO fills VALUES (?,?,?,?,?,?,?,?,?,?)", (trade_id, clid, oid, "SOL-USDT", side, qty, price, fee, fee_ccy, ts))


def test_actual_conversion_requires_fills_and_deduplicates_partial_executions(runtime):
    with sqlite3.connect(runtime.orders_db) as con:
        con.execute("INSERT INTO orders VALUES ('c1','o1','SOL-USDT','20260905_10','buy')")
    first = evaluation.calculate_metrics([run(selected=2, proposed=2)], runtime_paths=runtime)
    assert first["conversion_rate"] == 0
    assert first["proposal_conversion_rate"] == 1
    add_fill(runtime, trade_id="part1", qty="0.4")
    add_fill(runtime, trade_id="part2", qty="0.6", offset=1)
    second = evaluation.calculate_metrics([run(selected=2, proposed=2)], runtime_paths=runtime)
    assert second["conversion_rate"] == 0.5
    assert second["filled_opportunities"] == 1
    assert second["conversion_source"] == "fills_matched_to_orders"


def test_no_selected_candidates_differs_from_selected_but_unfilled(runtime):
    empty = evaluation.calculate_metrics([run(selected=0, proposed=0)], runtime_paths=runtime)
    blocked = evaluation.calculate_metrics([run(selected=1, proposed=0)], runtime_paths=runtime)
    assert empty["opportunity_status"] == "no_opportunities"
    assert empty["conversion_rate"] is None
    assert empty["recovery_evidence_ok"] is True
    assert blocked["opportunity_status"] == "observed"
    assert blocked["conversion_rate"] == 0


def test_missing_fills_db_is_unknown_and_does_not_create_a_database(runtime):
    missing = runtime.fills_db.with_name("absent.sqlite")
    runtime.fills_db = missing
    metrics = evaluation.calculate_metrics([run()], runtime_paths=runtime)
    assert metrics["conversion_rate"] is None
    assert metrics["recovery_evidence_ok"] is False
    assert any("actual_fills_unavailable" in warning for warning in metrics["warnings"])
    assert not missing.exists()


def test_missing_candidate_counts_are_not_no_opportunities(runtime):
    record = run()
    record["counts"] = {}
    metrics = evaluation.calculate_metrics([record], runtime_paths=runtime)
    assert metrics["opportunity_status"] == "candidate_evidence_unavailable"
    assert metrics["recovery_evidence_ok"] is False


def test_stale_standalone_cursor_accepts_current_reconciled_runtime(runtime):
    with sqlite3.connect(runtime.fills_db) as con:
        con.execute("UPDATE sync_state SET v='1'")
    now_ms = int(evaluation._utc_now().timestamp() * 1000)
    for name in ("reconcile_status", "ledger_status"):
        (runtime.reports_dir / f"{name}.json").write_text(json.dumps({"ts_ms": now_ms, "ok": True}), encoding="utf-8")
    metrics = evaluation.calculate_metrics([run(selected=0, proposed=0)], runtime_paths=runtime)
    assert metrics["fills_freshness_source"] == "reconcile_and_ledger_current"
    assert metrics["recovery_evidence_ok"] is True
    assert any("standalone_fills_sync_cursor_stale" in warning for warning in metrics["warnings"])
    (runtime.reports_dir / "ledger_status.json").write_text(json.dumps({"ts_ms": now_ms, "ok": False}), encoding="utf-8")
    assert evaluation.calculate_metrics([run()], runtime_paths=runtime)["recovery_evidence_ok"] is False


def test_audit_fallback_sorts_newest_losses_correctly_but_cannot_promote(runtime):
    runtime.fills_db = runtime.fills_db.with_name("missing.sqlite")
    rows = [run(offset=-i, pnl=-1 if i < 3 else 1) for i in range(6)]
    metrics = evaluation.calculate_metrics(rows, runtime_paths=runtime)
    assert metrics["pnl_trend"] == "down"
    assert metrics["consecutive_losses"] == 3
    assert metrics["pnl_source"] == "audit_realized_pnl_fallback"
    assert metrics["recovery_evidence_ok"] is False


def test_fill_pnl_includes_entry_before_window_base_fee_and_exit_quote_fee(runtime):
    add_fill(runtime, trade_id="buy", qty="1", price="100", fee="-0.001", fee_ccy="SOL", offset=-60)
    add_fill(runtime, trade_id="sell", clid="c2", oid="o2", side="sell", qty="0.999", price="110", fee="-0.10989", offset=5)
    metrics = evaluation.calculate_metrics([run(proposed=0)], runtime_paths=runtime)
    assert metrics["pnl_source"] == "fills_fifo_exit_orders"
    assert metrics["consecutive_losses"] == 0
    assert metrics["recovery_evidence_ok"] is True
    evidence = evaluation._filled_opportunity_metrics([run()], runtime)
    assert evidence["pnl_values"] == pytest.approx([9.78011])


@pytest.mark.parametrize("kind", ["missing_entry", "unknown_fee"])
def test_unverifiable_fifo_does_not_allow_recovery(runtime, kind):
    if kind == "unknown_fee":
        add_fill(runtime, trade_id="buy", fee="-0.01", fee_ccy="OTHER", offset=-60)
    add_fill(runtime, trade_id="sell", clid="c2", oid="o2", side="sell", offset=5)
    metrics = evaluation.calculate_metrics([run()], runtime_paths=runtime)
    assert metrics["recovery_evidence_ok"] is False
    assert any("fill_pnl_unavailable" in warning for warning in metrics["warnings"])


def test_peak_is_not_replaced_with_initial_capital_or_erased_for_large_drawdown():
    assert evaluation._sanitize_peak_equity(96, 100) == 100
    assert evaluation._sanitize_peak_equity(40, 132) == 132
    with pytest.raises(ValueError, match="peak"):
        evaluation._sanitize_peak_equity(96, 0)


def test_unknown_equity_never_authorizes_recovery(runtime, monkeypatch):
    import src.risk.live_equity_fetcher as live
    monkeypatch.setattr(live, "get_live_equity_from_okx", lambda **kwargs: None)
    metrics = evaluation.calculate_metrics([run(selected=0, proposed=0)], runtime_paths=runtime)
    assert metrics["recovery_evidence_ok"] is False
    assert metrics["drawdown_source"] == "audit_fallback"
