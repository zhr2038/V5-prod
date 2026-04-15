from __future__ import annotations

from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
import json
import math
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_config_path, resolve_runtime_env_path
from src.backtest.cost_factory import make_cost_model_from_cfg
from src.core.models import MarketSeries
import src.core.pipeline as pipeline_module
from src.core.pipeline import V5Pipeline
from src.core.run_logger import RunLogger
from src.execution.position_store import Position
from src.reporting.decision_audit import DecisionAudit
from src.research.cache_loader import load_cached_market_data
from src.research.recorder import ResearchRecorder
from src.research.task_runner import load_task_config
from src.research.trend_quality_experiment import (
    build_baseline_config,
    sandbox_working_directory,
    seed_sandbox_read_only_artifacts,
)


@lru_cache(maxsize=8)
def _load_base_config_cached(base_config_path: str, env_path: str):
    return load_config(base_config_path, env_path=env_path)


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    return path


def _append_jsonl(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, default=_json_default) + "\n")
    return path


@contextmanager
def _sandbox_reports_dir(path: Path):
    sandbox_reports = Path(path).resolve() / "reports"
    sandbox_reports.mkdir(parents=True, exist_ok=True)
    original_reports_dir = pipeline_module.REPORTS_DIR
    pipeline_module.REPORTS_DIR = sandbox_reports
    try:
        yield sandbox_reports
    finally:
        pipeline_module.REPORTS_DIR = original_reports_dir


def _project_path(project_root: Path, raw_path: str, fallback: str) -> Path:
    value = str(raw_path or fallback).strip() or fallback
    path = Path(value)
    if path.is_absolute():
        return path
    return (project_root / path).resolve()


def _resolve_workers(raw_workers: Any, window_count: int) -> int:
    if window_count <= 1:
        return 1
    value = str(raw_workers or "1").strip().lower()
    if value in {"auto", "max"}:
        cpu_count = max(1, int(os.cpu_count() or 1))
        multiplier_raw = str(os.getenv("RESEARCH_WORKER_MULTIPLIER", "2.0") or "2.0").strip()
        try:
            multiplier = max(1.0, float(multiplier_raw))
        except Exception:
            multiplier = 2.0
        target = int(math.ceil(cpu_count * multiplier))
        return max(1, min(target, window_count))
    if value.endswith("x"):
        try:
            multiplier = max(1.0, float(value[:-1]))
        except Exception:
            return 1
        cpu_count = max(1, int(os.cpu_count() or 1))
        target = int(math.ceil(cpu_count * multiplier))
        return max(1, min(target, window_count))
    try:
        workers = int(value)
    except Exception:
        return 1
    return max(1, min(workers, window_count))


def _resolve_ohlcv_limit(raw_limit: Any, *, available_bars: int, shift: int, fallback: int = 720) -> int:
    text = str(raw_limit or "").strip().lower()
    if text in {"full", "all", "auto", "max"}:
        return max(1, int(available_bars) - int(shift))
    try:
        return max(1, int(raw_limit or fallback))
    except Exception:
        return max(1, int(fallback))


def _apply_overrides(cfg, overrides: dict[str, object]) -> None:
    for raw_path, value in (overrides or {}).items():
        target = cfg
        parts = str(raw_path).split(".")
        for attr in parts[:-1]:
            target = getattr(target, attr)
        setattr(target, parts[-1], value)


def _slice_market_data_tail_window(
    market_data: dict[str, MarketSeries],
    *,
    limit: int,
    shift: int = 0,
) -> dict[str, MarketSeries]:
    shift = max(0, int(shift or 0))
    limit = max(1, int(limit or 1))
    sliced: dict[str, MarketSeries] = {}
    for symbol, series in market_data.items():
        end = None if shift == 0 else -shift
        start = -limit - shift
        sliced[symbol] = MarketSeries(
            symbol=series.symbol,
            timeframe=series.timeframe,
            ts=series.ts[start:end],
            open=series.open[start:end],
            high=series.high[start:end],
            low=series.low[start:end],
            close=series.close[start:end],
            volume=series.volume[start:end],
        )
    return sliced


class _BacktestClock:
    def __init__(self, timestamp_ms: int | None = None):
        self._now = datetime.now(timezone.utc)
        if timestamp_ms is not None:
            self.set_timestamp_ms(timestamp_ms)

    def set_timestamp_ms(self, timestamp_ms: int) -> None:
        self._now = datetime.fromtimestamp(int(timestamp_ms) / 1000.0, tz=timezone.utc)

    def now(self) -> datetime:
        return self._now


def _initial_equity_usdt(cfg) -> float:
    try:
        value = float(getattr(getattr(cfg, "backtest", None), "initial_equity_usdt", 20.0) or 20.0)
        if value > 0:
            return value
    except Exception:
        pass
    return 20.0


def _serialize_positions(
    positions: Dict[str, Position],
    exec_px: Dict[str, float],
    equity_now: float,
) -> dict[str, dict[str, float]]:
    payload: dict[str, dict[str, float]] = {}
    equity_base = max(float(equity_now), 1e-12)
    for symbol, position in positions.items():
        mark_px = float(exec_px.get(symbol, getattr(position, "last_mark_px", 0.0)) or 0.0)
        notional = float(position.qty) * mark_px if mark_px > 0 else 0.0
        payload[symbol] = {
            "qty": float(position.qty),
            "avg_px": float(position.avg_px),
            "mark_px": float(mark_px),
            "notional_usdt": float(notional),
            "weight": float(notional / equity_base),
            "unrealized_pnl_pct": float(getattr(position, "unrealized_pnl_pct", 0.0) or 0.0),
        }
    return payload


def _nonzero_counter(counter_like: Dict[str, int]) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in (counter_like or {}).items()
        if int(value or 0) != 0
    }


def _summarize_series(values: Iterable[float]) -> dict[str, float]:
    arr = np.asarray(list(values), dtype=float)
    if arr.size == 0:
        return {
            "count": 0,
            "mean": 0.0,
            "std": 0.0,
            "min": 0.0,
            "max": 0.0,
        }
    return {
        "count": int(arr.size),
        "mean": float(np.mean(arr)),
        "std": float(np.std(arr)),
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
    }


def _cost_meta_to_dict(cost_meta: Any) -> dict[str, Any]:
    if cost_meta is None:
        return {}
    if isinstance(cost_meta, dict):
        return dict(cost_meta)
    to_dict = getattr(cost_meta, "to_dict", None)
    if callable(to_dict):
        try:
            payload = to_dict()
            return dict(payload) if isinstance(payload, dict) else {}
        except Exception:
            return {}
    return {}


def _performance_summary(
    *,
    initial_equity: float,
    equity_curve: list[float],
    turnovers: list[float],
    gains: float,
    losses: float,
) -> dict[str, float]:
    eq = np.asarray(equity_curve, dtype=float)
    if eq.size < 5:
        return {
            "sharpe": 0.0,
            "cagr": 0.0,
            "max_dd": 0.0,
            "profit_factor": 0.0,
            "turnover": float(np.mean(np.asarray(turnovers, dtype=float))) if turnovers else 0.0,
            "final_equity": float(eq[-1]) if eq.size else float(initial_equity),
            "total_return": 0.0,
        }
    rets = eq[1:] / eq[:-1] - 1.0
    max_eq = np.maximum.accumulate(eq)
    dd = 1.0 - (eq / max_eq)
    ann = np.sqrt(24 * 365)
    sharpe = float(np.mean(rets) / (np.std(rets) + 1e-12) * ann)
    total_return = float(eq[-1]) / max(float(initial_equity), 1e-12) - 1.0
    total_return_ratio = float(eq[-1]) / max(float(initial_equity), 1e-12)
    cagr = float(max(total_return_ratio, 1e-12) ** (365 * 24 / max(1, len(rets))) - 1.0)
    pf = float(gains / (losses + 1e-12))
    turnover = float(np.mean(np.asarray(turnovers, dtype=float))) if turnovers else 0.0
    return {
        "sharpe": sharpe,
        "cagr": cagr,
        "max_dd": float(np.max(dd)),
        "profit_factor": pf,
        "turnover": turnover,
        "final_equity": float(eq[-1]),
        "total_return": float(total_return),
    }


def _sorted_counter(counter: Counter[str]) -> dict[str, int]:
    return {key: int(value) for key, value in sorted(counter.items(), key=lambda item: (-item[1], item[0]))}


def run_window_diagnostic(
    *,
    market_data: dict[str, MarketSeries],
    cfg,
    window_name: str,
    output_dir: Path,
) -> dict[str, Any]:
    syms = list(market_data.keys())
    if not syms:
        return {
            "status": "empty",
            "window_name": window_name,
        }
    n = min(len(market_data[symbol].close) for symbol in syms)
    if n < 80:
        return {
            "status": "too_short",
            "window_name": window_name,
            "bars": int(n),
        }

    cost_model, cost_meta = make_cost_model_from_cfg(cfg)
    initial_ts = int(market_data[syms[0]].ts[0]) if market_data[syms[0]].ts else None
    clock = _BacktestClock(initial_ts)
    with _sandbox_reports_dir(output_dir):
        pipeline = V5Pipeline(cfg, clock=clock, data_provider=None)
        run_logger = RunLogger(str(output_dir / "run_logger"))

        initial_equity = _initial_equity_usdt(cfg)
        cash = float(initial_equity)
        peak = float(initial_equity)
        positions: Dict[str, Position] = {}
        equity_curve: list[float] = []
        turnovers: list[float] = []
        gains = 0.0
        losses = 0.0

        regime_counts: Counter[str] = Counter()
        skip_reason_counts: Counter[str] = Counter()
        reject_counts: Counter[str] = Counter()
        order_symbol_counts: Counter[str] = Counter()
        order_intent_counts: Counter[str] = Counter()
        order_side_counts: Counter[str] = Counter()
        symbol_realized_pnl: defaultdict[str, float] = defaultdict(float)
        symbol_trade_notional: defaultdict[str, float] = defaultdict(float)
        symbol_order_count: defaultdict[str, int] = defaultdict(int)
        regime_returns: defaultdict[str, list[float]] = defaultdict(list)

        add_premiums: list[float] = []
        averaging_down_add_count = 0
        chasing_add_count = 0
        add_order_count = 0
        new_buy_count = 0
        bars_with_orders = 0
        bars_with_positions = 0
        top1_scores: list[float] = []

        bars_path = output_dir / "bars.jsonl"
        orders_path = output_dir / "orders.jsonl"
        bars_path.unlink(missing_ok=True)
        orders_path.unlink(missing_ok=True)

        for i in range(60, n - 2):
            md_slice = {
                symbol: MarketSeries(
                    symbol=symbol,
                    timeframe=market_data[symbol].timeframe,
                    ts=market_data[symbol].ts[: i + 1],
                    open=market_data[symbol].open[: i + 1],
                    high=market_data[symbol].high[: i + 1],
                    low=market_data[symbol].low[: i + 1],
                    close=market_data[symbol].close[: i + 1],
                    volume=market_data[symbol].volume[: i + 1],
                )
                for symbol in syms
            }
            signal_ts = int(md_slice[syms[0]].ts[-1])
            exec_ts = int(market_data[syms[0]].ts[i + 1])
            clock.set_timestamp_ms(signal_ts)

            audit = DecisionAudit(
                run_id=f"{window_name}_bar_{i}",
                window_start_ts=int(md_slice[syms[0]].ts[0]),
                window_end_ts=signal_ts,
            )
            out = pipeline.run(
                md_slice,
                positions=list(positions.values()),
                cash_usdt=float(cash),
                equity_peak_usdt=float(peak),
                run_logger=run_logger,
                audit=audit,
            )
            regime_state = str(out.regime.state.value if hasattr(out.regime.state, "value") else out.regime.state)
            regime_counts[regime_state] += 1
            for reason, count in (audit.rejects or {}).items():
                if int(count or 0) > 0:
                    reject_counts[str(reason)] += int(count)
            for router_decision in audit.router_decisions or []:
                if str(router_decision.get("action") or "").lower() != "skip":
                    continue
                reason = str(router_decision.get("reason") or "unknown")
                skip_reason_counts[reason] += 1
            if audit.top_scores:
                top1_scores.append(float((audit.top_scores[0] or {}).get("score") or 0.0))

            exec_px = {symbol: float(market_data[symbol].close[i + 1]) for symbol in syms}
            traded_notional = 0.0
            order_events: list[dict[str, Any]] = []
            if out.orders:
                bars_with_orders += 1

            for order in out.orders:
                symbol = str(order.symbol)
                order_symbol_counts[symbol] += 1
                order_intent_counts[str(order.intent)] += 1
                order_side_counts[str(order.side)] += 1
                symbol_order_count[symbol] += 1

                px = float(exec_px.get(symbol, order.signal_price) or 0.0)
                if px <= 0:
                    continue

                fee_bps = float(cfg.backtest.fee_bps)
                slippage_bps = float(cfg.backtest.slippage_bps)
                cost_meta_event: dict[str, Any] = {}
                if cost_model is not None:
                    try:
                        resolved = cost_model.resolve(symbol, regime_state, "fill", float(order.notional_usdt))
                        if isinstance(resolved, tuple) and len(resolved) == 3:
                            fee_bps, slippage_bps, cost_meta_event = resolved
                        else:
                            fee_bps, slippage_bps = resolved
                    except Exception as exc:
                        cost_meta_event = {"error": str(exc)}

                cost = (float(fee_bps) + float(slippage_bps)) / 10_000.0
                notional = float(order.notional_usdt)
                event: dict[str, Any] = {
                    "bar_index": int(i),
                    "signal_ts": int(signal_ts),
                    "exec_ts": int(exec_ts),
                    "signal_dt": datetime.fromtimestamp(signal_ts / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                    "exec_dt": datetime.fromtimestamp(exec_ts / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                    "symbol": symbol,
                    "side": str(order.side),
                    "intent": str(order.intent),
                    "requested_notional_usdt": float(notional),
                    "exec_px": float(px),
                    "fee_bps": float(fee_bps),
                    "slippage_bps": float(slippage_bps),
                    "meta": dict(order.meta or {}),
                }

                if order.side == "buy":
                    held = positions.get(symbol)
                    held_value_before = float(held.qty) * float(px) if held is not None else 0.0
                    is_add = held is not None
                    premium_vs_entry = None
                    add_ratio = None
                    if held is not None and float(getattr(held, "avg_px", 0.0) or 0.0) > 0:
                        premium_vs_entry = float(px) / float(held.avg_px) - 1.0
                        add_ratio = float(notional) / max(held_value_before, 1e-12)
                        add_premiums.append(float(premium_vs_entry))
                        add_order_count += 1
                        if float(premium_vs_entry) < 0:
                            averaging_down_add_count += 1
                        elif float(premium_vs_entry) > 0:
                            chasing_add_count += 1
                    else:
                        new_buy_count += 1
                    event["is_add"] = bool(is_add)
                    event["premium_vs_entry"] = premium_vs_entry
                    event["add_ratio_vs_held_value"] = add_ratio

                    if cash < notional:
                        event["status"] = "skipped_insufficient_cash"
                        order_events.append(event)
                        _append_jsonl(orders_path, event)
                        continue

                    cash -= notional
                    qty = (notional / px) * (1.0 - cost)
                    traded_notional += abs(notional)
                    symbol_trade_notional[symbol] += abs(notional)

                    if held is None:
                        positions[symbol] = Position(
                            symbol=symbol,
                            qty=qty,
                            avg_px=px,
                            entry_ts=datetime.fromtimestamp(exec_ts / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                            highest_px=px,
                            last_update_ts=datetime.fromtimestamp(exec_ts / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                            last_mark_px=px,
                            unrealized_pnl_pct=0.0,
                        )
                    else:
                        new_qty = float(held.qty) + qty
                        avg_px = (float(held.avg_px) * float(held.qty) + px * qty) / new_qty if new_qty else px
                        positions[symbol] = Position(
                            symbol=symbol,
                            qty=new_qty,
                            avg_px=avg_px,
                            entry_ts=held.entry_ts,
                            highest_px=max(float(held.highest_px), px),
                            last_update_ts=datetime.fromtimestamp(exec_ts / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                            last_mark_px=px,
                            unrealized_pnl_pct=((px - avg_px) / avg_px) if avg_px > 0 else 0.0,
                            tags_json=getattr(held, "tags_json", "{}"),
                        )
                    event["filled_qty"] = float(qty)
                    event["status"] = "filled"
                else:
                    held = positions.get(symbol)
                    if held is None:
                        event["status"] = "skipped_no_position"
                        order_events.append(event)
                        _append_jsonl(orders_path, event)
                        continue

                    gross_proceeds = min(float(held.qty) * px, max(0.0, notional))
                    if gross_proceeds <= 0.0:
                        event["status"] = "skipped_zero_proceeds"
                        order_events.append(event)
                        _append_jsonl(orders_path, event)
                        continue

                    qty_sold = min(float(held.qty), gross_proceeds / px)
                    if qty_sold <= 0.0:
                        event["status"] = "skipped_zero_qty"
                        order_events.append(event)
                        _append_jsonl(orders_path, event)
                        continue

                    traded_notional += abs(gross_proceeds)
                    symbol_trade_notional[symbol] += abs(gross_proceeds)
                    pnl = (px - float(held.avg_px)) * qty_sold
                    symbol_realized_pnl[symbol] += float(pnl)
                    if pnl >= 0:
                        gains += float(pnl)
                    else:
                        losses += float(-pnl)

                    cash += gross_proceeds * (1.0 - cost)
                    remaining_qty = max(0.0, float(held.qty) - qty_sold)
                    if remaining_qty <= 1e-12:
                        positions.pop(symbol, None)
                    else:
                        positions[symbol] = Position(
                            symbol=symbol,
                            qty=remaining_qty,
                            avg_px=float(held.avg_px),
                            entry_ts=held.entry_ts,
                            highest_px=max(float(held.highest_px), px),
                            last_update_ts=datetime.fromtimestamp(exec_ts / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                            last_mark_px=px,
                            unrealized_pnl_pct=((px - float(held.avg_px)) / float(held.avg_px)) if float(held.avg_px) > 0 else 0.0,
                            tags_json=getattr(held, "tags_json", "{}"),
                        )
                    event["filled_qty"] = float(qty_sold)
                    event["gross_proceeds_usdt"] = float(gross_proceeds)
                    event["realized_pnl_usdt"] = float(pnl)
                    event["status"] = "filled"

                order_events.append(event)
                _append_jsonl(orders_path, event)

            turnovers.append(float(traded_notional) / max(float(initial_equity), 1e-12))
            eq_now = float(cash)
            for position in positions.values():
                mark_px = float(exec_px.get(position.symbol, 0.0) or 0.0)
                if mark_px > 0:
                    eq_now += float(position.qty) * mark_px
            equity_curve.append(eq_now)
            peak = max(float(peak), float(eq_now))
            if positions:
                bars_with_positions += 1

            bar_return = 0.0
            if len(equity_curve) >= 2 and float(equity_curve[-2]) > 0:
                bar_return = float(equity_curve[-1] / equity_curve[-2] - 1.0)
            regime_returns[regime_state].append(float(bar_return))

            bar_record = {
                "bar_index": int(i),
                "signal_ts": int(signal_ts),
                "exec_ts": int(exec_ts),
                "signal_dt": datetime.fromtimestamp(signal_ts / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                "exec_dt": datetime.fromtimestamp(exec_ts / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                "regime": regime_state,
                "regime_multiplier": float(out.regime.multiplier),
                "atr_pct": float(out.regime.atr_pct),
                "ma20": float(out.regime.ma20),
                "ma60": float(out.regime.ma60),
                "equity_after": float(eq_now),
                "cash_after": float(cash),
                "bar_return": float(bar_return),
                "positions": _serialize_positions(positions, exec_px, eq_now),
                "top_scores": list(audit.top_scores[:5]),
                "targets_post_risk": dict(audit.targets_post_risk or {}),
                "counts": dict(audit.counts or {}),
                "rejects": _nonzero_counter(audit.rejects or {}),
                "skip_reasons": _sorted_counter(Counter(
                    str(item.get("reason") or "unknown")
                    for item in (audit.router_decisions or [])
                    if str(item.get("action") or "").lower() == "skip"
                )),
                "rebalance_deadband_pct": audit.rebalance_deadband_pct,
                "rebalance_skipped_deadband_count": int(audit.rebalance_skipped_deadband_count or 0),
                "rebalance_drift_by_symbol": dict(audit.rebalance_drift_by_symbol or {}),
                "rebalance_effective_deadband_by_symbol": dict(audit.rebalance_effective_deadband_by_symbol or {}),
                "orders": order_events,
                "notes_tail": list((audit.notes or [])[-6:]),
            }
            _append_jsonl(bars_path, bar_record)

    metrics = _performance_summary(
        initial_equity=float(initial_equity),
        equity_curve=equity_curve,
        turnovers=turnovers,
        gains=float(gains),
        losses=float(losses),
    )
    return {
        "status": "completed",
        "window_name": window_name,
        "bars": int(n),
        "diagnostic_bars": int(max(0, n - 62)),
        "cost_model_meta": _cost_meta_to_dict(cost_meta),
        "metrics": metrics,
        "regime_counts": _sorted_counter(regime_counts),
        "skip_reason_counts": _sorted_counter(skip_reason_counts),
        "reject_counts": _sorted_counter(reject_counts),
        "order_symbol_counts": _sorted_counter(order_symbol_counts),
        "order_intent_counts": _sorted_counter(order_intent_counts),
        "order_side_counts": _sorted_counter(order_side_counts),
        "symbol_realized_pnl_usdt": {
            key: round(float(value), 8)
            for key, value in sorted(symbol_realized_pnl.items(), key=lambda item: item[1])
        },
        "symbol_trade_notional_usdt": {
            key: round(float(value), 8)
            for key, value in sorted(symbol_trade_notional.items(), key=lambda item: (-item[1], item[0]))
        },
        "symbol_order_count": {
            key: int(value)
            for key, value in sorted(symbol_order_count.items(), key=lambda item: (-item[1], item[0]))
        },
        "activity": {
            "bars_with_orders": int(bars_with_orders),
            "bars_with_positions": int(bars_with_positions),
            "order_bar_ratio": float(bars_with_orders / max(1, n - 62)),
            "position_bar_ratio": float(bars_with_positions / max(1, n - 62)),
            "new_buy_count": int(new_buy_count),
            "add_buy_count": int(add_order_count),
            "averaging_down_add_count": int(averaging_down_add_count),
            "chasing_add_count": int(chasing_add_count),
            "add_buy_averaging_down_ratio": float(averaging_down_add_count / max(1, add_order_count)),
            "add_buy_premium_summary": _summarize_series(add_premiums),
            "top1_score_summary": _summarize_series(top1_scores),
        },
        "regime_bar_return_summary": {
            key: _summarize_series(values)
            for key, values in sorted(regime_returns.items(), key=lambda item: item[0])
        },
        "artifacts": {
            "bars_path": str(bars_path),
            "orders_path": str(orders_path),
            "run_logger_dir": str(output_dir / "run_logger"),
        },
    }


@dataclass
class WindowDiagnosticTaskResult:
    run_dir: Path
    report_path: Path
    latest_report_path: Path
    result: dict[str, Any]


def _run_window_job(
    *,
    project_root: str,
    base_config_path: str,
    env_path: str,
    cache_dir: str,
    symbols: list[str],
    overrides: dict[str, object],
    evaluation: dict[str, Any],
    window_dir: str,
) -> dict[str, Any]:
    project_root_path = Path(project_root)
    evaluation_name = str(evaluation.get("name") or "window")
    shift = int(evaluation.get("window_shift_bars") or 0)
    base_cfg = _load_base_config_cached(str(base_config_path), env_path)
    cfg = build_baseline_config(base_cfg, project_root=project_root_path, research_symbols=symbols)
    _apply_overrides(cfg, overrides)
    window_market_data = load_cached_market_data(
        Path(cache_dir),
        symbols,
        cfg.timeframe_main,
        limit=100_000 if str(evaluation.get("ohlcv_limit", "")).strip().lower() in {"full", "all", "auto", "max"} else max(1, int(evaluation.get("ohlcv_limit") or 720) + shift),
    )
    available_bars = min((len(series.close) for series in window_market_data.values()), default=0)
    ohlcv_limit = _resolve_ohlcv_limit(
        evaluation.get("ohlcv_limit", 720),
        available_bars=int(available_bars),
        shift=int(shift),
        fallback=720,
    )
    window_market_data = _slice_market_data_tail_window(
        window_market_data,
        limit=ohlcv_limit,
        shift=shift,
    )
    sandbox_dir = Path(window_dir)
    sandbox_dir.mkdir(parents=True, exist_ok=True)
    seed_sandbox_read_only_artifacts(project_root_path, sandbox_dir)
    log_path = sandbox_dir / "scenario.log"
    with log_path.open("w", encoding="utf-8") as handle:
        with sandbox_working_directory(sandbox_dir):
            with redirect_stdout(handle), redirect_stderr(handle):
                diagnostic = run_window_diagnostic(
                    market_data=window_market_data,
                    cfg=cfg,
                    window_name=evaluation_name,
                    output_dir=sandbox_dir,
                )
    return {
        "name": evaluation_name,
        "ohlcv_limit": int(ohlcv_limit),
        "window_shift_bars": int(shift),
        "summary": diagnostic,
        "window_dir": str(sandbox_dir),
        "scenario_log_path": str(log_path),
    }


def run_window_diagnostic_task(
    *,
    project_root: Path,
    task_config_path: str,
) -> WindowDiagnosticTaskResult:
    task_config = load_task_config(project_root / task_config_path)
    if not task_config:
        raise FileNotFoundError(f"unable to load task config: {task_config_path}")

    task_meta = task_config.get("task") or {}
    paths_cfg = task_config.get("paths") or {}
    exp_cfg = task_config.get("experiment") or {}

    symbols = [str(symbol) for symbol in (exp_cfg.get("symbols") or [])]
    evaluations = list(exp_cfg.get("evaluations") or [])
    overrides = dict(exp_cfg.get("overrides") or {})

    recorder = ResearchRecorder(
        base_dir=_project_path(project_root, str(paths_cfg.get("runs_dir", "reports/runs")), "reports/runs")
    )
    run = recorder.start_run(
        task_name=str(task_meta.get("name", "window_diagnostics")),
        task_config=task_config,
    )

    try:
        base_config_path = Path(
            resolve_runtime_config_path(
                str(exp_cfg.get("base_config_path", "")).strip() or None,
                project_root=project_root,
            )
        )
        env_path = resolve_runtime_env_path(str(exp_cfg.get("env_path", ".env")), project_root=project_root)
        cache_dir = _project_path(project_root, str(exp_cfg.get("cache_dir", "data/cache")), "data/cache")
        latest_report_path = _project_path(
            project_root,
            str(paths_cfg.get("output_report_path", "reports/research/core6_window_diagnostics/latest.json")),
            "reports/research/core6_window_diagnostics/latest.json",
        )
        workers = _resolve_workers(exp_cfg.get("workers", 1), len(evaluations))

        if not evaluations:
            raise ValueError("window diagnostics requires at least one evaluation")

        ordered_results: dict[int, dict[str, Any]] = {}
        if workers <= 1:
            for idx, evaluation in enumerate(evaluations):
                window_name = str(evaluation.get("name") or "window")
                ordered_results[idx] = _run_window_job(
                    project_root=str(project_root),
                    base_config_path=str(base_config_path),
                    env_path=env_path,
                    cache_dir=str(cache_dir),
                    symbols=symbols,
                    overrides=overrides,
                    evaluation=evaluation,
                    window_dir=str(run.run_dir / "windows" / window_name),
                )
        else:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                future_map = {
                    executor.submit(
                        _run_window_job,
                        project_root=str(project_root),
                        base_config_path=str(base_config_path),
                        env_path=env_path,
                        cache_dir=str(cache_dir),
                        symbols=symbols,
                        overrides=overrides,
                        evaluation=evaluation,
                        window_dir=str(run.run_dir / "windows" / str(evaluation.get("name") or f"window_{idx}")),
                    ): idx
                    for idx, evaluation in enumerate(evaluations)
                }
                for future in as_completed(future_map):
                    ordered_results[int(future_map[future])] = future.result()
        window_results = [ordered_results[idx] for idx in range(len(evaluations))]

        result = {
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "task_config_path": str(project_root / task_config_path),
            "base_config_path": str(base_config_path),
            "symbols": symbols,
            "overrides": overrides,
            "workers": int(workers),
            "windows": window_results,
        }
        report_path = run.write_json("result.json", result)
        _write_json(latest_report_path, result)
        recorder.finalize_run(
            run,
            status="completed",
            summary={
                "output_report_path": str(latest_report_path),
                "windows": [window["name"] for window in window_results],
            },
        )
        return WindowDiagnosticTaskResult(
            run_dir=run.run_dir,
            report_path=report_path,
            latest_report_path=latest_report_path,
            result=result,
        )
    except Exception as exc:
        failure_summary = {
            "reason": "window_diagnostics_failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        run.write_json("error.json", failure_summary)
        recorder.finalize_run(run, status="failed", summary=failure_summary)
        raise
