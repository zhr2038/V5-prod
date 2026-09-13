from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from src.research.daily_trend_paper import (
    DAY_SECONDS,
    DailyTrendConfig,
    collect_snapshot,
    digest,
    epoch,
    process_snapshot,
)


PROJECT = Path(__file__).resolve().parents[1]


def config() -> DailyTrendConfig:
    return DailyTrendConfig.load(PROJECT / "configs/research/daily_trend_paper_v1.json")


def snapshot(cfg: DailyTrendConfig, decision_ts: float, *, long: bool, delay: float = 10) -> dict:
    closes = [100 + index if long else 300 - index for index in range(100)]
    end = int(decision_ts - DAY_SECONDS)
    bars = []
    for index, close in enumerate(closes):
        ts = (end - (99 - index) * DAY_SECONDS) * 1000
        bars.append([ts, close, close + 1, close - 1, close, 10, 1])
    observed_at = decision_ts + delay
    symbols = {}
    for symbol in cfg.symbols:
        base = symbol.split("/")[0]
        sma = sum(closes) / 100
        momentum = closes[-1] / closes[-31] - 1
        symbols[symbol] = {
            "bars": bars,
            "signal": {
                "long": closes[-1] > sma and momentum > 0,
                "close": closes[-1],
                "sma": sma,
                "momentum_return": momentum,
                "sma_days": 100,
                "momentum_days": 30,
            },
            "quote": {"bid": closes[-1] - 0.1, "ask": closes[-1] + 0.1, "ts": observed_at - 1},
            "instrument": {
                "symbol": symbol,
                "lot_size": "0.00000001",
                "minimum_qty": "0.00000001",
                "minimum_notional_usdt": 0,
                "minimum_notional_source": "not_reported_no_additional_notional_limit_assumed",
                "buy_fee_currency": base,
                "sell_fee_currency": "USDT",
                "observed_ts": observed_at,
                "source_hash": "a" * 64,
            },
            "entry_minimum_notional_usdt": 10,
        }
    value = {
        "schema_version": cfg.schema_version,
        "experiment_id": cfg.experiment_id,
        "observed_at": observed_at,
        "completed_bar_ts": end,
        "decision_ts": decision_ts,
        "symbols": symbols,
        "source": "fixture",
        "public_only": True,
        "live_order_effect": "none",
        "raw_response_sha256": "b" * 64,
    }
    value["input_sha256"] = digest(value)
    assert all(row["signal"]["long"] is long for row in symbols.values())
    return value


def test_config_freezes_cost_universe_window_and_no_live_effect() -> None:
    cfg = config()
    assert cfg.symbols == ("BTC/USDT", "ETH/USDT")
    assert cfg.sma_days == 100 and cfg.momentum_days == 30
    assert 2 * (cfg.fee_bps_per_side + cfg.slippage_bps_per_side) == 30
    assert cfg.maximum_future_quote_skew_seconds == 5
    assert epoch(cfg.review_utc) - epoch(cfg.start_utc) == 90 * DAY_SECONDS
    assert cfg.paper_only is True and cfg.live_order_effect == "none"
    assert cfg.allow_live_promotion is False


def test_superseded_participation_producers_are_disabled_in_live_config() -> None:
    production = yaml.safe_load((PROJECT / "configs/live_prod.yaml").read_text(encoding="utf-8"))
    assert production["participation"]["enabled"] is False
    assert production["participation"]["quote_execution_enabled"] is False


def test_independent_daily_sleeves_buy_dedupe_then_sell_without_live_effect(tmp_path: Path) -> None:
    cfg = config()
    first = epoch(cfg.start_utc)
    entered = process_snapshot(tmp_path / "ledger.sqlite", cfg, snapshot(cfg, first, long=True), source_identity="source-v1")
    assert entered["status"] == "OBSERVING"
    assert [row["action"] for row in entered["actions"]] == ["buy", "buy"]
    assert entered["total_equity_usdt"] < 100
    assert entered["paper_only"] is True and entered["live_order_effect"] == "none"
    assert entered["live_execution_eligible"] is False and entered["automatic_live_scaling"] is False
    assert all(float(value["portfolio"]["cash_usdt"]) < 1 for value in entered["sleeves"].values())

    duplicate = process_snapshot(tmp_path / "ledger.sqlite", cfg, snapshot(cfg, first, long=True), source_identity="source-v1")
    assert duplicate == entered

    exited = process_snapshot(
        tmp_path / "ledger.sqlite",
        cfg,
        snapshot(cfg, first + DAY_SECONDS, long=False),
        source_identity="source-v1",
    )
    assert [row["action"] for row in exited["actions"]] == ["sell", "sell"]
    assert all(value["independent_closed_campaign_count"] == 1 for value in exited["sleeves"].values())
    assert all(value["actual_simulated_fill_count"] == 2 for value in exited["sleeves"].values())


def test_late_entry_is_skipped_but_first_observable_exit_is_recorded(tmp_path: Path) -> None:
    cfg = config()
    first = epoch(cfg.start_utc)
    late = process_snapshot(
        tmp_path / "ledger.sqlite",
        cfg,
        snapshot(cfg, first, long=True, delay=cfg.entry_execution_window_seconds + 1),
        source_identity="source-v1",
    )
    assert late["status"] == "OBSERVING_LATE"
    assert {row["reason"] for row in late["actions"]} == {"entry_skipped_late"}
    entered = process_snapshot(
        tmp_path / "ledger.sqlite",
        cfg,
        snapshot(cfg, first + DAY_SECONDS, long=True),
        source_identity="source-v1",
    )
    assert [row["action"] for row in entered["actions"]] == ["buy", "buy"]
    exited = process_snapshot(
        tmp_path / "ledger.sqlite",
        cfg,
        snapshot(cfg, first + 2 * DAY_SECONDS, long=False, delay=cfg.entry_execution_window_seconds + 1),
        source_identity="source-v1",
    )
    assert {row["reason"] for row in exited["actions"]} == {"delayed_signal_exit"}


def test_missing_decision_days_are_counted_without_backfilled_trades(tmp_path: Path) -> None:
    cfg = config()
    first = epoch(cfg.start_utc)
    initial = process_snapshot(
        tmp_path / "ledger.sqlite",
        cfg,
        snapshot(cfg, first, long=False),
        source_identity="source-v1",
    )
    assert initial["observation_count"] == 1
    assert initial["cumulative_missed_decision_days"] == 0
    after_gap = process_snapshot(
        tmp_path / "ledger.sqlite",
        cfg,
        snapshot(cfg, first + 2 * DAY_SECONDS, long=True),
        source_identity="source-v1",
    )
    assert after_gap["observation_gap_days"] == 1
    assert after_gap["cumulative_missed_decision_days"] == 1
    assert after_gap["observation_count"] == 2
    assert [row["action"] for row in after_gap["actions"]] == ["buy", "buy"]


def test_review_deadline_blocks_new_entries_and_finalizes(tmp_path: Path) -> None:
    cfg = config()
    due = process_snapshot(
        tmp_path / "ledger.sqlite",
        cfg,
        snapshot(cfg, epoch(cfg.review_utc), long=True),
        source_identity="source-v1",
    )
    assert due["status"] == "REVIEW_DUE_FROZEN" and due["finalized"] is True
    assert due["cumulative_missed_decision_days"] == 90
    assert due["evidence_status"] == "INSUFFICIENT_OBSERVATION_COVERAGE"
    assert {row["reason"] for row in due["actions"]} == {"cash"}
    later = process_snapshot(
        tmp_path / "ledger.sqlite",
        cfg,
        snapshot(cfg, epoch(cfg.review_utc) + DAY_SECONDS, long=True),
        source_identity="source-v1",
    )
    assert later == due


def test_snapshot_signal_and_source_identity_fail_closed(tmp_path: Path) -> None:
    cfg = config()
    value = snapshot(cfg, epoch(cfg.start_utc), long=True)
    bad_signal = copy.deepcopy(value)
    bad_signal["symbols"]["BTC/USDT"]["signal"]["long"] = False
    bad_signal["input_sha256"] = digest({key: item for key, item in bad_signal.items() if key != "input_sha256"})
    with pytest.raises(ValueError, match="stored signal"):
        process_snapshot(tmp_path / "bad.sqlite", cfg, bad_signal, source_identity="source-v1")

    process_snapshot(tmp_path / "identity.sqlite", cfg, value, source_identity="source-v1")
    with pytest.raises(ValueError, match="source identity changed"):
        process_snapshot(tmp_path / "identity.sqlite", cfg, value, source_identity="source-v2")


def test_collect_uses_confirmed_contiguous_utc_daily_bars_and_public_spot_quotes() -> None:
    cfg = config()
    now = epoch(cfg.start_utc) + 600
    latest = int(epoch(cfg.start_utc) - DAY_SECONDS) * 1000
    rows = []
    for index in range(110):
        close = 100 + index
        ts = latest - (109 - index) * DAY_SECONDS * 1000
        rows.append([str(ts), str(close), str(close + 1), str(close - 1), str(close), "10", "0", "0", "1"])

    def fake_get(path: str, params: dict[str, str]) -> dict:
        inst_id = params["instId"]
        if path.endswith("history-candles"):
            assert params["bar"] == "1Dutc"
            return {"code": "0", "data": list(reversed(rows))}
        if path.endswith("instruments"):
            base = inst_id.split("-")[0]
            return {"code": "0", "data": [{"instId": inst_id, "instType": "SPOT", "state": "live", "baseCcy": base, "quoteCcy": "USDT", "lotSz": "0.00000001", "minSz": "0.00000001"}]}
        return {"code": "0", "data": [{"instId": inst_id, "bidPx": "208.9", "askPx": "209.1", "ts": str(int((now - 1) * 1000))}]}

    value = collect_snapshot(cfg, now=now, public_get=fake_get)
    assert value["completed_bar_ts"] == latest // 1000
    assert set(value["symbols"]) == set(cfg.symbols)
    assert all(row["signal"]["long"] is True for row in value["symbols"].values())
    assert value["public_only"] is True and value["live_order_effect"] == "none"
