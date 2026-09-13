"""Frozen BTC/ETH daily-trend forward paper ledger using OKX public GET data only."""
from __future__ import annotations

import copy
import hashlib
import json
import math
import sqlite3
import time
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests

from src.research.quote_portfolio import QuotePortfolio, serializable


DAY_SECONDS = 86_400
DAY_MS = DAY_SECONDS * 1000
ALLOWED_PUBLIC_PATHS = frozenset(
    {
        "/api/v5/market/history-candles",
        "/api/v5/market/ticker",
        "/api/v5/public/instruments",
    }
)


def digest(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(raw).hexdigest()


def epoch(value: str) -> float:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("UTC experiment timestamp required")
    return parsed.astimezone(timezone.utc).timestamp()


@dataclass(frozen=True)
class DailyTrendConfig:
    schema_version: str
    experiment_id: str
    symbols: tuple[str, ...]
    initial_cash_per_symbol_usdt: float
    sma_days: int
    momentum_days: int
    fee_bps_per_side: float
    slippage_bps_per_side: float
    minimum_explicit_roundtrip_cost_bps: float
    entry_minimum_notional_usdt: float
    maximum_quote_age_seconds: float
    maximum_future_quote_skew_seconds: float
    entry_execution_window_seconds: float
    start_utc: str
    review_utc: str
    review_window_days: int
    paper_only: bool
    live_order_effect: str
    allow_live_promotion: bool

    @classmethod
    def load(cls, path: Path) -> "DailyTrendConfig":
        value = json.loads(path.read_text(encoding="utf-8"))
        fields = cls.__dataclass_fields__
        missing = set(fields) - set(value)
        if missing:
            raise ValueError("daily trend config missing: " + ",".join(sorted(missing)))
        config = cls(**{key: tuple(value[key]) if key == "symbols" else value[key] for key in fields})
        config.validate()
        return config

    def validate(self) -> None:
        if self.schema_version != "v5.daily_trend_paper.v1":
            raise ValueError("unsupported daily trend schema")
        if self.symbols != ("BTC/USDT", "ETH/USDT") or len(set(self.symbols)) != 2:
            raise ValueError("frozen BTC/ETH universe required")
        if self.sma_days != 100 or self.momentum_days != 30:
            raise ValueError("frozen SMA100 and momentum30 parameters required")
        if self.initial_cash_per_symbol_usdt != 50:
            raise ValueError("two independent 50 USDT sleeves required")
        numbers = (
            self.fee_bps_per_side,
            self.slippage_bps_per_side,
            self.minimum_explicit_roundtrip_cost_bps,
            self.entry_minimum_notional_usdt,
            self.maximum_quote_age_seconds,
            self.maximum_future_quote_skew_seconds,
            self.entry_execution_window_seconds,
        )
        if not all(isinstance(item, (int, float)) and not isinstance(item, bool) and math.isfinite(item) and item > 0 for item in numbers):
            raise ValueError("positive finite cost, size and timing assumptions required")
        explicit_roundtrip = 2 * (self.fee_bps_per_side + self.slippage_bps_per_side)
        if explicit_roundtrip < self.minimum_explicit_roundtrip_cost_bps:
            raise ValueError("explicit modeled cost is below frozen minimum")
        start, review = epoch(self.start_utc), epoch(self.review_utc)
        if review - start != self.review_window_days * DAY_SECONDS or self.review_window_days != 90:
            raise ValueError("exact 90-day forward review window required")
        if self.paper_only is not True or self.live_order_effect != "none" or self.allow_live_promotion is not False:
            raise ValueError("daily trend experiment must remain paper-only with no promotion")


def okx_public_get(path: str, params: dict[str, str]) -> dict[str, Any]:
    if path not in ALLOWED_PUBLIC_PATHS:
        raise ValueError("non-public or unapproved OKX path")
    response = requests.get(
        "https://www.okx.com" + path,
        params=params,
        timeout=(3, 15),
        allow_redirects=False,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") != "0" or not isinstance(payload.get("data"), list):
        raise ValueError("OKX public response rejected")
    return payload


def _number(value: Any) -> float:
    if isinstance(value, bool) or value is None:
        raise ValueError("finite numeric input required")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("finite numeric input required")
    return result


def _validate_bars(rows: list[Any], *, expected_latest_ms: int, required: int) -> list[list[float | int]]:
    unique: dict[int, list[float | int]] = {}
    for row in rows:
        if not isinstance(row, list):
            continue
        confirmed = row[8] if len(row) >= 9 else row[6] if len(row) == 7 else None
        if str(confirmed) != "1":
            continue
        ts = int(row[0])
        values = [_number(item) for item in row[1:6]]
        normalized: list[float | int] = [ts, *values, 1]
        if ts in unique and unique[ts] != normalized:
            raise ValueError("conflicting duplicate daily candle")
        unique[ts] = normalized
    bars = [unique[key] for key in sorted(unique)]
    if len(bars) < required:
        raise ValueError("insufficient confirmed daily candles")
    bars = bars[-required:]
    timestamps = [int(row[0]) for row in bars]
    if timestamps[-1] != expected_latest_ms:
        raise ValueError("latest confirmed UTC daily candle missing")
    if any(right - left != DAY_MS for left, right in zip(timestamps, timestamps[1:])):
        raise ValueError("non-contiguous UTC daily candles")
    for _ts, open_px, high, low, close, volume in (row[:6] for row in bars):
        if min(open_px, high, low, close) <= 0 or volume < 0 or low > min(open_px, close) or high < max(open_px, close):
            raise ValueError("invalid daily OHLCV")
    return bars


def _signal(bars: list[list[float | int]], config: DailyTrendConfig) -> dict[str, Any]:
    closes = [float(row[4]) for row in bars]
    sma = sum(closes[-config.sma_days :]) / config.sma_days
    momentum = closes[-1] / closes[-1 - config.momentum_days] - 1
    return {
        "long": closes[-1] > sma and momentum > 0,
        "close": closes[-1],
        "sma": sma,
        "momentum_return": momentum,
        "sma_days": config.sma_days,
        "momentum_days": config.momentum_days,
    }


def collect_snapshot(
    config: DailyTrendConfig,
    *,
    now: float | None = None,
    public_get: Callable[[str, dict[str, str]], dict[str, Any]] = okx_public_get,
) -> dict[str, Any]:
    observed_at = float(time.time() if now is None else now)
    current_day = int(observed_at // DAY_SECONDS) * DAY_SECONDS
    latest_completed_ms = (current_day - DAY_SECONDS) * 1000
    required = max(config.sma_days, config.momentum_days + 1)
    symbols: dict[str, Any] = {}
    raw: dict[str, Any] = {}
    for symbol in config.symbols:
        inst_id = symbol.replace("/", "-")
        history = public_get(
            "/api/v5/market/history-candles",
            {"instId": inst_id, "bar": "1Dutc", "limit": str(min(300, required + 10))},
        )
        instrument = public_get(
            "/api/v5/public/instruments",
            {"instType": "SPOT", "instId": inst_id},
        )
        ticker = public_get("/api/v5/market/ticker", {"instId": inst_id})
        raw[symbol] = {"history": history, "instrument": instrument, "ticker": ticker}
        bars = _validate_bars(history["data"], expected_latest_ms=latest_completed_ms, required=required)
        specs = [row for row in instrument["data"] if row.get("instId") == inst_id]
        quotes = [row for row in ticker["data"] if row.get("instId") == inst_id]
        if len(specs) != 1 or len(quotes) != 1:
            raise ValueError("exact instrument and ticker row required")
        spec, quote = specs[0], quotes[0]
        if spec.get("instType") != "SPOT" or spec.get("state") != "live" or spec.get("quoteCcy") != "USDT":
            raise ValueError("live USDT spot instrument required")
        bid, ask, quote_ts = _number(quote.get("bidPx")), _number(quote.get("askPx")), _number(quote.get("ts")) / 1000
        local_received_at = observed_at if now is not None else time.time()
        quote_age = local_received_at - quote_ts
        if (
            bid <= 0
            or ask < bid
            or quote_age > config.maximum_quote_age_seconds
            or quote_age < -config.maximum_future_quote_skew_seconds
        ):
            raise ValueError("current non-crossed public quote required")
        observed_at = max(observed_at, local_received_at, quote_ts)
        source_hash = digest(spec)
        symbols[symbol] = {
            "bars": bars,
            "signal": _signal(bars, config),
            "quote": {"bid": bid, "ask": ask, "ts": quote_ts},
            "instrument": {
                "symbol": symbol,
                "lot_size": spec.get("lotSz"),
                "minimum_qty": spec.get("minSz"),
                "minimum_notional_usdt": 0,
                "minimum_notional_source": "not_reported_no_additional_notional_limit_assumed",
                "buy_fee_currency": spec.get("baseCcy"),
                "sell_fee_currency": spec.get("quoteCcy"),
                "observed_ts": observed_at,
                "source_hash": source_hash,
            },
            "entry_minimum_notional_usdt": config.entry_minimum_notional_usdt,
        }
    if int(observed_at // DAY_SECONDS) * DAY_SECONDS != current_day:
        raise ValueError("UTC day changed during daily snapshot collection; retry required")
    snapshot = {
        "schema_version": config.schema_version,
        "experiment_id": config.experiment_id,
        "observed_at": observed_at,
        "completed_bar_ts": latest_completed_ms // 1000,
        "decision_ts": current_day,
        "symbols": symbols,
        "source": "OKX public REST GET",
        "public_only": True,
        "live_order_effect": "none",
        "raw_response_sha256": digest(raw),
    }
    snapshot["input_sha256"] = digest(snapshot)
    return snapshot


def _validate_snapshot(config: DailyTrendConfig, snapshot: dict[str, Any]) -> None:
    if snapshot.get("schema_version") != config.schema_version or snapshot.get("experiment_id") != config.experiment_id:
        raise ValueError("snapshot experiment identity mismatch")
    if snapshot.get("public_only") is not True or snapshot.get("live_order_effect") != "none":
        raise ValueError("public paper-only snapshot required")
    observed_at, decision_ts, bar_ts = (_number(snapshot[key]) for key in ("observed_at", "decision_ts", "completed_bar_ts"))
    if decision_ts != bar_ts + DAY_SECONDS or not decision_ts <= observed_at:
        raise ValueError("closed daily decision clock required")
    if set(snapshot.get("symbols", {})) != set(config.symbols):
        raise ValueError("snapshot symbol set mismatch")
    required = max(config.sma_days, config.momentum_days + 1)
    for symbol in config.symbols:
        row = snapshot["symbols"][symbol]
        bars = _validate_bars(row.get("bars", []), expected_latest_ms=int(bar_ts * 1000), required=required)
        if row.get("signal") != _signal(bars, config):
            raise ValueError("stored signal does not match frozen rule")
        # QuotePortfolio applies the full quote, instrument, fee-currency and age contract.
        probe = QuotePortfolio(
            initial_cash=config.initial_cash_per_symbol_usdt,
            fee_bps=config.fee_bps_per_side,
            slippage_bps=config.slippage_bps_per_side,
            maximum_quote_age_seconds=config.maximum_quote_age_seconds,
        )
        probe._market(row, observed_at)
    expected_hash = snapshot.get("input_sha256")
    unsigned = {key: value for key, value in snapshot.items() if key != "input_sha256"}
    if expected_hash != digest(unsigned):
        raise ValueError("snapshot content hash mismatch")


def _new_book(config: DailyTrendConfig) -> QuotePortfolio:
    return QuotePortfolio(
        initial_cash=config.initial_cash_per_symbol_usdt,
        fee_bps=config.fee_bps_per_side,
        slippage_bps=config.slippage_bps_per_side,
        maximum_quote_age_seconds=config.maximum_quote_age_seconds,
    )


def _active_position(book: QuotePortfolio, symbol: str) -> dict[str, Any] | None:
    position = book.positions.get(symbol)
    return position if position and position.get("management_status", "active") == "active" else None


def _report(
    config: DailyTrendConfig,
    snapshot: dict[str, Any],
    books: dict[str, QuotePortfolio],
    actions: list[dict[str, Any]],
    *,
    identity: str,
    previous_metrics: dict[str, Any],
    observation_gap_days: int,
    cumulative_missed_decision_days: int,
    observation_count: int,
    status: str,
    finalized: bool,
) -> dict[str, Any]:
    observed_at = float(snapshot["observed_at"])
    sleeves, total_equity, total_cost = {}, 0.0, 0.0
    for symbol, book in books.items():
        mark = serializable(book.mark({symbol: snapshot["symbols"][symbol]}, now=observed_at))
        observed_maximum_drawdown = max(
            float(previous_metrics.get("symbol_maximum_drawdown_fraction", {}).get(symbol, 0)),
            float(mark["drawdown_fraction"]),
        )
        sleeves[symbol] = {
            "signal": copy.deepcopy(snapshot["symbols"][symbol]["signal"]),
            "portfolio": mark,
            "observed_maximum_drawdown_fraction": observed_maximum_drawdown,
            "actual_simulated_fill_count": len(book.fills),
            "independent_closed_campaign_count": len({lot["campaign_id"] for lot in book.closed_lots if lot.get("campaign_closed")}),
        }
        total_equity += float(mark["equity_usdt"])
        total_cost += float(mark["fee_usdt"])
    combined_peak = max(float(previous_metrics.get("combined_peak_equity_usdt", 0)), total_equity)
    combined_drawdown = 1 - total_equity / combined_peak if combined_peak else 0
    observed_maximum_drawdown = max(
        float(previous_metrics.get("observed_maximum_drawdown_fraction", 0)),
        combined_drawdown,
    )
    return {
        "schema_version": config.schema_version,
        "experiment_id": config.experiment_id,
        "identity": identity,
        "status": status,
        "finalized": finalized,
        "observed_at": observed_at,
        "completed_bar_ts": snapshot["completed_bar_ts"],
        "decision_ts": snapshot["decision_ts"],
        "input_sha256": snapshot["input_sha256"],
        "raw_response_sha256": snapshot["raw_response_sha256"],
        "actions": serializable(actions),
        "sleeves": sleeves,
        "initial_capital_usdt": config.initial_cash_per_symbol_usdt * len(config.symbols),
        "total_equity_usdt": total_equity,
        "net_equity_increment_usdt": total_equity - config.initial_cash_per_symbol_usdt * len(config.symbols),
        "combined_peak_equity_usdt": combined_peak,
        "current_drawdown_fraction": combined_drawdown,
        "observed_maximum_drawdown_fraction": observed_maximum_drawdown,
        "observation_gap_days": observation_gap_days,
        "cumulative_missed_decision_days": cumulative_missed_decision_days,
        "observation_count": observation_count,
        "evidence_status": (
            "INSUFFICIENT_OBSERVATION_COVERAGE"
            if finalized and cumulative_missed_decision_days
            else "READY_FOR_MANUAL_REVIEW_NOT_PROFITABILITY_EVIDENCE"
            if finalized
            else "COLLECTING_FORWARD_EVIDENCE"
        ),
        "modeled_fee_usdt": total_cost,
        "explicit_roundtrip_cost_bps": 2 * (config.fee_bps_per_side + config.slippage_bps_per_side),
        "observed_bid_ask_spread_additional": True,
        "start_utc": config.start_utc,
        "review_utc": config.review_utc,
        "paper_only": True,
        "live_order_effect": "none",
        "live_execution_eligible": False,
        "automatic_live_scaling": False,
    }


def process_snapshot(
    database: Path,
    config: DailyTrendConfig,
    snapshot: dict[str, Any],
    *,
    source_identity: str,
) -> dict[str, Any]:
    _validate_snapshot(config, snapshot)
    database.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database)) as con:
        con.execute("PRAGMA synchronous=FULL")
        con.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        con.execute(
            "CREATE TABLE IF NOT EXISTS events (bar_ts INTEGER PRIMARY KEY, observed_at REAL NOT NULL, input_hash TEXT NOT NULL UNIQUE, report TEXT NOT NULL)"
        )
        con.execute("BEGIN IMMEDIATE")
        saved = dict(con.execute("SELECT key,value FROM meta"))
        if saved.get("source_identity") not in {None, source_identity}:
            raise ValueError("source identity changed; new experiment directory required")
        con.execute("INSERT OR REPLACE INTO meta VALUES('source_identity',?)", (source_identity,))
        if "config" in saved and json.loads(saved["config"]) != serializable(config.__dict__):
            raise ValueError("frozen config changed; new experiment directory required")
        con.execute("INSERT OR REPLACE INTO meta VALUES('config',?)", (json.dumps(serializable(config.__dict__), sort_keys=True),))
        checkpoint = json.loads(saved["checkpoint"]) if "checkpoint" in saved else {
            "books": {symbol: _new_book(config).checkpoint() for symbol in config.symbols},
            "last_bar_ts": None,
            "finalized": False,
            "combined_peak_equity_usdt": config.initial_cash_per_symbol_usdt * len(config.symbols),
            "observed_maximum_drawdown_fraction": 0,
            "symbol_maximum_drawdown_fraction": {symbol: 0 for symbol in config.symbols},
            "cumulative_missed_decision_days": 0,
            "observation_count": 0,
        }
        books = {symbol: QuotePortfolio.restore(checkpoint["books"][symbol]) for symbol in config.symbols}
        start_ts, review_ts = epoch(config.start_utc), epoch(config.review_utc)
        decision_ts = float(snapshot["decision_ts"])
        if decision_ts < start_ts:
            report = _report(
                config,
                snapshot,
                books,
                [],
                identity=source_identity,
                previous_metrics=checkpoint,
                observation_gap_days=0,
                cumulative_missed_decision_days=int(checkpoint.get("cumulative_missed_decision_days", 0)),
                observation_count=int(checkpoint.get("observation_count", 0)),
                status="WAITING_FOR_START",
                finalized=False,
            )
            con.execute("INSERT OR REPLACE INTO meta VALUES('checkpoint',?)", (json.dumps(checkpoint, sort_keys=True),))
            con.commit()
            return report
        if checkpoint.get("finalized"):
            row = con.execute("SELECT report FROM events ORDER BY bar_ts DESC LIMIT 1").fetchone()
            if not row:
                raise ValueError("finalized checkpoint without final event")
            con.commit()
            return json.loads(row[0])
        bar_ts = int(snapshot["completed_bar_ts"])
        existing = con.execute("SELECT report FROM events WHERE bar_ts=?", (bar_ts,)).fetchone()
        if existing:
            con.commit()
            return json.loads(existing[0])
        previous_bar = checkpoint.get("last_bar_ts")
        if previous_bar is not None and bar_ts <= int(previous_bar):
            raise ValueError("reversed daily observation")
        expected_previous_bar = int(previous_bar) if previous_bar is not None else int(start_ts - 2 * DAY_SECONDS)
        observation_gap_days = max(0, (bar_ts - expected_previous_bar) // DAY_SECONDS - 1)
        cumulative_missed_decision_days = int(checkpoint.get("cumulative_missed_decision_days", 0)) + observation_gap_days
        observation_count = int(checkpoint.get("observation_count", 0)) + 1
        observed_at = float(snapshot["observed_at"])
        entry_window_open = 0 <= observed_at - decision_ts <= config.entry_execution_window_seconds
        review_due = decision_ts >= review_ts
        actions: list[dict[str, Any]] = []
        for symbol in config.symbols:
            book, row = books[symbol], snapshot["symbols"][symbol]
            active = _active_position(book, symbol)
            wants_long = bool(row["signal"]["long"]) and not review_due
            if active and not wants_long:
                reason = "review_window_end" if review_due else ("signal_exit" if entry_window_open else "delayed_signal_exit")
                intent = {
                    "intent_id": f"{config.experiment_id}:{bar_ts}:{symbol}:sell",
                    "symbol": symbol,
                    "side": "sell",
                    "decision_ts": decision_ts,
                    "notional_usdt": float(active["qty"]) * float(row["quote"]["bid"]),
                    "quantity": active["qty"],
                    "reason": reason,
                }
                fill = book.fill(intent, row, now=observed_at)
                actions.append({"symbol": symbol, "action": "sell", "reason": reason, "fill": fill})
            elif not active and wants_long and entry_window_open:
                intent = {
                    "intent_id": f"{config.experiment_id}:{bar_ts}:{symbol}:buy",
                    "symbol": symbol,
                    "side": "buy",
                    "decision_ts": decision_ts,
                    "notional_usdt": book.cash,
                    "reason": "daily_trend_entry",
                }
                fill = book.fill(intent, row, now=observed_at)
                actions.append({"symbol": symbol, "action": "buy", "reason": "daily_trend_entry", "fill": fill})
            else:
                reason = "hold" if active else ("entry_skipped_late" if wants_long else "cash")
                actions.append({"symbol": symbol, "action": "none", "reason": reason})
        finalized = bool(review_due)
        status = "REVIEW_DUE_FROZEN" if finalized else ("OBSERVING" if entry_window_open else "OBSERVING_LATE")
        report = _report(
            config,
            snapshot,
            books,
            actions,
            identity=source_identity,
            previous_metrics=checkpoint,
            observation_gap_days=observation_gap_days,
            cumulative_missed_decision_days=cumulative_missed_decision_days,
            observation_count=observation_count,
            status=status,
            finalized=finalized,
        )
        checkpoint = {
            "books": {symbol: books[symbol].checkpoint() for symbol in config.symbols},
            "last_bar_ts": bar_ts,
            "finalized": finalized,
            "combined_peak_equity_usdt": report["combined_peak_equity_usdt"],
            "observed_maximum_drawdown_fraction": report["observed_maximum_drawdown_fraction"],
            "symbol_maximum_drawdown_fraction": {
                symbol: report["sleeves"][symbol]["observed_maximum_drawdown_fraction"]
                for symbol in config.symbols
            },
            "cumulative_missed_decision_days": cumulative_missed_decision_days,
            "observation_count": observation_count,
        }
        con.execute("INSERT INTO events VALUES(?,?,?,?)", (bar_ts, observed_at, snapshot["input_sha256"], json.dumps(report, sort_keys=True)))
        con.execute("INSERT OR REPLACE INTO meta VALUES('checkpoint',?)", (json.dumps(checkpoint, sort_keys=True),))
        con.commit()
        return report


def source_identity(project: Path, config_path: Path) -> str:
    paths = (
        config_path,
        project / "src/research/daily_trend_paper.py",
        project / "src/research/quote_portfolio.py",
        project / "scripts/run_daily_trend_paper.py",
    )
    return digest({path.relative_to(project).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths})
