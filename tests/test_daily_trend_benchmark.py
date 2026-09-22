import json
import sqlite3

import pytest

from src.research.daily_trend_benchmark import build_daily_trend_benchmark
from src.research.daily_trend_paper import DAY_SECONDS, digest, epoch, process_snapshot
from test_daily_trend_paper import config, snapshot


def append(study, cfg, day, *, long=True, price=None):
    value = snapshot(cfg, epoch(cfg.start_utc) + day * DAY_SECONDS, long=long)
    if price is not None:
        for row in value["symbols"].values():
            row["quote"].update(bid=price, ask=price + .1)
        value.pop("input_sha256")
        value["input_sha256"] = digest(value)
    inputs = study / "inputs"
    inputs.mkdir(exist_ok=True, parents=True)
    (study / "manifest.json").write_text(json.dumps({
        "identity": "frozen-pilot", "config_sha256": digest(cfg.__dict__),
    }), encoding="utf-8")
    (inputs / f"{value['input_sha256']}.json").write_text(json.dumps(value), encoding="utf-8")
    return process_snapshot(study / "ledger.sqlite", cfg, value, source_identity="frozen-pilot")


def test_unchanged_hold_matches_passive_with_costs_and_no_ledger_writes(tmp_path):
    cfg = config()
    append(tmp_path, cfg, 0)
    report = append(tmp_path, cfg, 1, price=220)
    original = (tmp_path / "ledger.sqlite").read_bytes()
    result = build_daily_trend_benchmark(tmp_path, report)
    assert (tmp_path / "ledger.sqlite").read_bytes() == original
    assert result["strategy"]["equity_usdt"] == pytest.approx(result["passive"]["equity_usdt"])
    assert result["excess_vs_passive_usdt"] == pytest.approx(0)
    assert result["strategy"]["realized_pnl_usdt"] == 0
    assert result["strategy"]["unrealized_pnl_usdt"] > 0
    assert result["curve"][0]["passive_equity_usdt"] < 100
    assert result["cash"]["equity_usdt"] == 100
    assert "no_closed_strategy_campaigns" in result["limitations"]
    assert result["live_execution_eligible"] is False


def test_strategy_exit_and_passive_loss_create_computed_excess(tmp_path):
    cfg = config()
    append(tmp_path, cfg, 0)
    append(tmp_path, cfg, 1, long=False, price=210)
    report = append(tmp_path, cfg, 2, long=False, price=150)
    result = build_daily_trend_benchmark(tmp_path, report)
    assert result["excess_vs_passive_usdt"] > 0
    assert result["strategy"]["realized_pnl_usdt"] > 0
    assert result["strategy"]["independent_closed_campaign_count"] == 2
    assert result["passive"]["observed_maximum_drawdown_fraction"] > .2
    assert result["strategy"]["observed_maximum_drawdown_fraction"] < .01
    assert result["strategy"]["net_equity_increment_usdt"] == pytest.approx(
        result["strategy"]["realized_pnl_usdt"] + result["strategy"]["unrealized_pnl_usdt"]
    )


def test_cash_signal_does_not_control_passive_entry_and_review_liquidates(tmp_path):
    cfg = config()
    append(tmp_path, cfg, 0, long=False)
    report = append(tmp_path, cfg, 90, long=False)
    result = build_daily_trend_benchmark(tmp_path, report)
    assert result["strategy"]["net_equity_increment_usdt"] == 0
    assert result["passive"]["net_equity_increment_usdt"] < 0
    assert result["passive"]["independent_closed_campaign_count"] == 2
    assert "missing_registered_decision_days" in result["limitations"]


def test_input_tamper_or_uncommitted_report_cannot_produce_a_baseline(tmp_path):
    report = append(tmp_path, config(), 0)
    altered = {**report, "total_equity_usdt": 999}
    with pytest.raises(ValueError, match="committed event"):
        build_daily_trend_benchmark(tmp_path, altered)
    source = next((tmp_path / "inputs").glob("*.json"))
    value = json.loads(source.read_text(encoding="utf-8"))
    value["symbols"]["BTC/USDT"]["quote"]["bid"] += .01
    source.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        build_daily_trend_benchmark(tmp_path, report)


def test_missing_day_and_mismatched_count_are_not_hidden(tmp_path):
    append(tmp_path, config(), 0)
    report = append(tmp_path, config(), 2)
    assert "missing_registered_decision_days" in build_daily_trend_benchmark(tmp_path, report)["limitations"]
    with sqlite3.connect(tmp_path / "ledger.sqlite") as con:
        con.execute("DELETE FROM events WHERE bar_ts=(SELECT MIN(bar_ts) FROM events)")
    with pytest.raises(ValueError, match="count mismatch"):
        build_daily_trend_benchmark(tmp_path, report)
