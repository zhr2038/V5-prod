"""Read-only reconstruction of cash and passive holdings beside the frozen pilot.

This reader never calls the strategy processor or writes to its ledger. The
passive curve is a retrospective benchmark using the pilot's archived quotes,
quantity rules and costs, not another forward experiment.
"""
from __future__ import annotations

import json
import math
import re
import sqlite3
from contextlib import closing
from pathlib import Path

from src.research.daily_trend_paper import DailyTrendConfig, _validate_snapshot, digest
from src.research.quote_portfolio import QuotePortfolio

MAX_OBSERVATIONS = 100
MAX_INPUT_BYTES = 2 * 1024 * 1024
MAX_TOTAL_INPUT_BYTES = 16 * 1024 * 1024


def _number(value):
    if isinstance(value, bool):
        raise ValueError("invalid benchmark number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("nonfinite benchmark number")
    return number


def _path(root: Path, name: str) -> Path:
    path = (root / name).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("benchmark source outside experiment directory")
    return path


def _account(equity, initial, realized, unrealized, maximum_drawdown, campaigns):
    return {
        "equity_usdt": equity,
        "net_equity_increment_usdt": equity - initial,
        "net_return_fraction": equity / initial - 1,
        "realized_pnl_usdt": realized,
        "unrealized_pnl_usdt": unrealized,
        "observed_maximum_drawdown_fraction": maximum_drawdown,
        "independent_closed_campaign_count": campaigns,
    }


def build_daily_trend_benchmark(study: Path, report: dict) -> dict:
    """Bind archived events and inputs to the displayed report, with bounded IO."""
    study = Path(study).resolve()
    database = _path(study, "ledger.sqlite")
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as con:
        con.execute("PRAGMA query_only=ON")
        con.execute("BEGIN")
        metadata = dict(con.execute("SELECT key,value FROM meta WHERE key IN ('source_identity','config')"))
        if metadata.get("source_identity") != report.get("identity"):
            raise ValueError("benchmark ledger identity mismatch")
        raw_config = json.loads(metadata["config"])
        config = DailyTrendConfig(**{
            key: tuple(raw_config[key]) if key == "symbols" else raw_config[key]
            for key in DailyTrendConfig.__dataclass_fields__
        })
        config.validate()
        if config.experiment_id != report.get("experiment_id"):
            raise ValueError("benchmark experiment mismatch")
        manifest_path = _path(study, "manifest.json")
        if manifest_path.stat().st_size > MAX_INPUT_BYTES:
            raise ValueError("benchmark manifest exceeds limit")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (manifest.get("identity") != report.get("identity")
                or manifest.get("config_sha256") != digest(config.__dict__)):
            raise ValueError("benchmark frozen config identity mismatch")
        rows = con.execute(
            "SELECT bar_ts,observed_at,input_hash,report FROM events WHERE bar_ts<=? ORDER BY bar_ts LIMIT ?",
            (report["completed_bar_ts"], MAX_OBSERVATIONS + 1),
        ).fetchall()
    if not rows:
        return {"status": "waiting", "reason": "no_committed_daily_observations"}
    if len(rows) > MAX_OBSERVATIONS or len(rows) != report.get("observation_count"):
        raise ValueError("benchmark observation count mismatch")
    if json.loads(rows[-1][3]) != report:
        raise ValueError("benchmark latest report differs from committed event")

    initial = config.initial_cash_per_symbol_usdt * len(config.symbols)
    books = {symbol: QuotePortfolio(
        initial_cash=config.initial_cash_per_symbol_usdt,
        fee_bps=config.fee_bps_per_side,
        slippage_bps=config.slippage_bps_per_side,
        maximum_quote_age_seconds=config.maximum_quote_age_seconds,
    ) for symbol in config.symbols}
    curve, realized, input_bytes = [], 0.0, 0
    strategy_peak = passive_peak = initial
    strategy_drawdown = passive_drawdown = 0.0
    for index, (bar_ts, observed_at, input_hash, encoded_report) in enumerate(rows):
        event = json.loads(encoded_report)
        if (event.get("identity") != report["identity"]
                or event.get("input_sha256") != input_hash
                or event.get("completed_bar_ts") != bar_ts
                or event.get("observed_at") != observed_at):
            raise ValueError("benchmark event provenance mismatch")
        if not re.fullmatch(r"[a-f0-9]{64}", input_hash):
            raise ValueError("benchmark input hash invalid")
        source = _path(study, f"inputs/{input_hash}.json")
        size = source.stat().st_size
        input_bytes += size
        if size > MAX_INPUT_BYTES or input_bytes > MAX_TOTAL_INPUT_BYTES:
            raise ValueError("benchmark input exceeds limit")
        snapshot = json.loads(source.read_text(encoding="utf-8"))
        _validate_snapshot(config, snapshot)
        if (snapshot["input_sha256"] != input_hash
                or snapshot["completed_bar_ts"] != bar_ts
                or snapshot["observed_at"] != observed_at):
            raise ValueError("benchmark input differs from committed event")
        for symbol, book in books.items():
            market = snapshot["symbols"][symbol]
            if index == 0:
                book.fill({
                    "intent_id": f"passive:{input_hash}:{symbol}:buy", "symbol": symbol,
                    "side": "buy", "decision_ts": snapshot["decision_ts"],
                    "notional_usdt": book.cash, "reason": "passive_benchmark_entry",
                }, market, now=observed_at)
            if event.get("finalized"):
                position = book.positions.get(symbol)
                if position and position.get("management_status", "active") == "active":
                    book.fill({
                        "intent_id": f"passive:{input_hash}:{symbol}:sell", "symbol": symbol,
                        "side": "sell", "decision_ts": snapshot["decision_ts"],
                        "notional_usdt": float(position["qty"]) * float(market["quote"]["bid"]),
                        "quantity": position["qty"], "reason": "passive_benchmark_review_end",
                    }, market, now=observed_at)
        marks = [book.mark({symbol: snapshot["symbols"][symbol]}, now=observed_at)
                 for symbol, book in books.items()]
        passive_equity = sum(_number(mark["equity_usdt"]) for mark in marks)
        strategy_equity = _number(event["total_equity_usdt"])
        strategy_peak = max(strategy_peak, strategy_equity)
        passive_peak = max(passive_peak, passive_equity)
        strategy_drawdown = max(strategy_drawdown, 1 - strategy_equity / strategy_peak)
        passive_drawdown = max(passive_drawdown, 1 - passive_equity / passive_peak)
        realized += sum(_number(action["fill"].get("realized_pnl_usdt", 0))
                        for action in event["actions"] if action.get("fill"))
        curve.append({
            "observed_at": observed_at, "decision_ts": snapshot["decision_ts"],
            "strategy_equity_usdt": strategy_equity, "passive_equity_usdt": passive_equity,
            "cash_equity_usdt": initial,
            "strategy_net_return_fraction": strategy_equity / initial - 1,
            "passive_net_return_fraction": passive_equity / initial - 1,
            "excess_vs_passive_usdt": strategy_equity - passive_equity,
            "strategy_drawdown_fraction": 1 - strategy_equity / strategy_peak,
            "passive_drawdown_fraction": 1 - passive_equity / passive_peak,
        })
    campaigns = sum(int(sleeve["independent_closed_campaign_count"]) for sleeve in report["sleeves"].values())
    limitations = ["daily_marks_only_no_intraday_risk", "manual_review_not_live_eligibility",
                   "retrospective_benchmark_not_new_forward_strategy"]
    if not campaigns:
        limitations.append("no_closed_strategy_campaigns")
    if report.get("cumulative_missed_decision_days"):
        limitations.append("missing_registered_decision_days")
    latest = curve[-1]
    return {
        "schema_version": "v5.daily_trend_benchmark.v1", "status": "observed",
        "identity": report["identity"], "source_report_sha256": digest(report),
        "initial_capital_usdt": initial, "first_observed_at": rows[0][1],
        "latest_observed_at": rows[-1][1], "observation_count": len(rows),
        "benchmark_method": "first_archived_quote_equal_initial_sleeves_same_costs_no_rebalance",
        "cost_model": {"fee_bps_per_side": config.fee_bps_per_side,
                       "slippage_bps_per_side": config.slippage_bps_per_side,
                       "observed_bid_ask_spread_additional": True},
        "strategy": _account(latest["strategy_equity_usdt"], initial, realized,
                             sum(_number(sleeve["portfolio"]["unrealized_pnl_usdt"])
                                 for sleeve in report["sleeves"].values()), strategy_drawdown, campaigns),
        "passive": _account(latest["passive_equity_usdt"], initial,
                            sum(float(lot["net_pnl_usdt"]) for book in books.values() for lot in book.closed_lots),
                            sum(float(mark["unrealized_pnl_usdt"]) for mark in marks), passive_drawdown,
                            sum(len({lot["campaign_id"] for lot in book.closed_lots if lot.get("campaign_closed")})
                                for book in books.values())),
        "cash": _account(initial, initial, 0.0, 0.0, 0.0, 0),
        "excess_vs_passive_usdt": latest["excess_vs_passive_usdt"],
        "excess_vs_cash_usdt": latest["strategy_equity_usdt"] - initial,
        "evidence_status": "INSUFFICIENT_FOR_LIVE_PROMOTION",
        "limitations": limitations, "curve": curve,
        "read_only": True, "live_order_effect": "none", "live_execution_eligible": False,
    }
