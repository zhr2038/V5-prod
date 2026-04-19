from __future__ import annotations

from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
import json
from pathlib import Path
from typing import Any

from configs.loader import load_config
from configs.runtime_config import resolve_runtime_env_path
from src.core.models import Order
from src.core.pipeline import V5Pipeline
from src.core.run_logger import RunLogger
from src.reporting.decision_audit import DecisionAudit
from src.research.cache_loader import load_cached_market_data
from src.research.trend_quality_experiment import (
    build_baseline_config,
    sandbox_working_directory,
    seed_sandbox_read_only_artifacts,
)
from src.research.window_diagnostics import (
    _apply_overrides,
    _sandbox_reports_dir,
)


@lru_cache(maxsize=8)
def _load_base_config_cached(base_config_path: str, env_path: str):
    return load_config(base_config_path, env_path=env_path)


def _iso_from_ts(timestamp_ms: int | None) -> str | None:
    if timestamp_ms is None:
        return None
    return datetime.fromtimestamp(int(timestamp_ms) / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _state_value(raw_state: Any) -> str:
    return str(getattr(raw_state, "value", raw_state))


def _nonzero_counter(counter_like: dict[str, int] | Counter[str]) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in (counter_like or {}).items()
        if int(value or 0) != 0
    }


def _skip_reason_counts(router_decisions: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for item in router_decisions or []:
        if str(item.get("action") or "").lower() != "skip":
            continue
        counter[str(item.get("reason") or "unknown")] += 1
    return {key: int(value) for key, value in sorted(counter.items(), key=lambda pair: (-pair[1], pair[0]))}


def _selected_zero_reason_buckets(
    counts: dict[str, int] | Counter[str],
    router_decisions: list[dict[str, Any]],
) -> dict[str, int]:
    buckets: Counter[str] = Counter()
    c = counts or {}

    mappings = {
        "risk_off_suppressed_count": "risk_off_target_zero",
        "target_zero_after_regime_count": "target_zero_after_regime",
        "target_zero_after_dd_throttle_count": "target_zero_after_dd_throttle",
        "protect_entry_block_count": "protect_entry_block",
        "protect_entry_trend_only_block_count": "protect_entry_trend_only_block",
        "protect_entry_alpha6_rsi_block_count": "protect_entry_alpha6_rsi_block",
        "negative_expectancy_cooldown": "negative_expectancy_block",
        "negative_expectancy_open_block": "negative_expectancy_block",
        "negative_expectancy_fast_fail_open_block": "negative_expectancy_block",
    }
    for key, bucket_name in mappings.items():
        value = int(c.get(key, 0) or 0)
        if value > 0:
            buckets[bucket_name] += value

    for item in (router_decisions or []):
        if str(item.get("action") or "").lower() != "skip":
            continue
        reason = str(item.get("reason") or "").strip()
        if reason == "deadband":
            buckets["deadband"] += 1
        elif reason == "target_zero_no_order":
            bucket_name = str(item.get("target_zero_reason") or "target_zero_no_order")
            buckets[bucket_name] += 1
        elif reason == "cost_aware_edge":
            buckets["cost_aware_block"] += 1
        elif reason.startswith("negative_expectancy_"):
            buckets["negative_expectancy_block"] += 1

    return {
        key: int(value)
        for key, value in sorted(buckets.items(), key=lambda pair: (-pair[1], pair[0]))
        if int(value or 0) > 0
    }


def _serialize_orders(orders: list[Order]) -> list[dict[str, Any]]:
    return [
        {
            "symbol": str(order.symbol),
            "side": str(order.side),
            "intent": str(order.intent),
            "notional_usdt": float(order.notional_usdt),
            "signal_price": float(order.signal_price),
            "meta": dict(order.meta or {}),
        }
        for order in (orders or [])
    ]


def _sorted_target_weights(target_weights: dict[str, float]) -> dict[str, float]:
    items = [
        (str(symbol), float(weight))
        for symbol, weight in (target_weights or {}).items()
        if abs(float(weight or 0.0)) > 1e-12
    ]
    items.sort(key=lambda item: (-abs(item[1]), item[0]))
    return {symbol: weight for symbol, weight in items}


class _Clock:
    def __init__(self, timestamp_ms: int):
        self._now = datetime.fromtimestamp(int(timestamp_ms) / 1000.0, tz=timezone.utc)

    def now(self) -> datetime:
        return self._now


def run_latest_signal_variant(
    *,
    variant: dict[str, Any],
    base_config_path: str,
    env_path: str,
    cache_dir: str,
    project_root: Path,
    output_dir: Path,
    ohlcv_limit: int,
    initial_equity_usdt: float,
    top_scores_limit: int = 5,
) -> dict[str, Any]:
    variant_name = str(variant.get("name") or "variant")
    symbols = [str(symbol) for symbol in (variant.get("symbols") or [])]
    overrides = dict(variant.get("overrides") or {})

    resolved_env_path = resolve_runtime_env_path(env_path, project_root=project_root)
    base_cfg = _load_base_config_cached(base_config_path, resolved_env_path)
    cfg = build_baseline_config(base_cfg, project_root=project_root, research_symbols=symbols)
    _apply_overrides(cfg, overrides)
    cfg.backtest.initial_equity_usdt = float(initial_equity_usdt)

    market_data = load_cached_market_data(Path(cache_dir), symbols, cfg.timeframe_main, limit=int(ohlcv_limit))
    first_series = next(iter(market_data.values()))
    signal_ts = int(first_series.ts[-1])
    audit = DecisionAudit(
        run_id=variant_name,
        window_start_ts=int(first_series.ts[0]),
        window_end_ts=signal_ts,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    seed_sandbox_read_only_artifacts(project_root, output_dir)
    log_path = output_dir / "scenario.log"
    clock = _Clock(signal_ts)

    with log_path.open("w", encoding="utf-8") as handle:
        with sandbox_working_directory(output_dir):
            with _sandbox_reports_dir(output_dir):
                with redirect_stdout(handle), redirect_stderr(handle):
                    pipeline = V5Pipeline(cfg, clock=clock, data_provider=None)
                    run_logger = RunLogger(str(output_dir / "run_logger"))
                    out = pipeline.run(
                        market_data,
                        positions=[],
                        cash_usdt=float(initial_equity_usdt),
                        equity_peak_usdt=float(initial_equity_usdt),
                        run_logger=run_logger,
                        audit=audit,
                    )

    regime_state = _state_value(out.regime.state)
    regime_payload = {
        "state": regime_state,
        "multiplier": float(out.regime.multiplier),
        "atr_pct": float(out.regime.atr_pct),
        "ma20": float(out.regime.ma20),
        "ma60": float(out.regime.ma60),
    }
    if getattr(out.regime, "hmm_state", None) is not None:
        regime_payload["hmm_state"] = str(out.regime.hmm_state)
    if getattr(out.regime, "hmm_probability", None) is not None:
        regime_payload["hmm_probability"] = float(out.regime.hmm_probability)

    target_weights = _sorted_target_weights(dict(out.portfolio.target_weights or {}))
    payload = {
        "name": variant_name,
        "signal_ts": int(signal_ts),
        "signal_dt": _iso_from_ts(signal_ts),
        "lookback_bars": int(min(len(series.close) for series in market_data.values())),
        "symbols": symbols,
        "overrides": overrides,
        "regime": regime_payload,
        "selected": list(out.portfolio.selected or []),
        "entry_candidates": list(getattr(out.portfolio, "entry_candidates", []) or []),
        "target_weights": target_weights,
        "top_scores": list((audit.top_scores or [])[: max(1, int(top_scores_limit))]),
        "counts": dict(audit.counts or {}),
        "rejects": _nonzero_counter(audit.rejects or {}),
        "skip_reasons": _skip_reason_counts(audit.router_decisions or []),
        "selected_zero_reason_buckets": _selected_zero_reason_buckets(
            dict(audit.counts or {}),
            audit.router_decisions or [],
        ),
        "orders": _serialize_orders(out.orders or []),
        "notes_tail": list((audit.notes or [])[-8:]),
        "output_dir": str(output_dir),
        "scenario_log_path": str(log_path),
    }
    return payload


def build_latest_signal_summary(
    *,
    generated_at: str,
    baseline: dict[str, Any],
    champion: dict[str, Any],
    baseline_name: str,
    champion_name: str,
) -> dict[str, Any]:
    baseline_selected = set(str(symbol) for symbol in (baseline.get("selected") or []))
    champion_selected = set(str(symbol) for symbol in (champion.get("selected") or []))
    baseline_orders = {
        (str(item.get("symbol")), str(item.get("side")), str(item.get("intent")))
        for item in (baseline.get("orders") or [])
    }
    champion_orders = {
        (str(item.get("symbol")), str(item.get("side")), str(item.get("intent")))
        for item in (champion.get("orders") or [])
    }
    all_symbols = sorted(set((baseline.get("target_weights") or {}).keys()) | set((champion.get("target_weights") or {}).keys()))
    weight_deltas = {
        symbol: round(
            float((champion.get("target_weights") or {}).get(symbol, 0.0))
            - float((baseline.get("target_weights") or {}).get(symbol, 0.0)),
            6,
        )
        for symbol in all_symbols
    }
    weight_deltas = {
        symbol: delta
        for symbol, delta in sorted(weight_deltas.items(), key=lambda item: (-abs(item[1]), item[0]))
        if abs(float(delta)) > 1e-9
    }
    l1_weight_diff = float(
        sum(
            abs(float((champion.get("target_weights") or {}).get(symbol, 0.0)) - float((baseline.get("target_weights") or {}).get(symbol, 0.0)))
            for symbol in all_symbols
        )
    )

    return {
        "generated_at": str(generated_at),
        "baseline_name": str(baseline_name),
        "champion_name": str(champion_name),
        "baseline": baseline,
        "champion": champion,
        "compare": {
            "same_signal_ts": baseline.get("signal_ts") == champion.get("signal_ts"),
            "same_regime": ((baseline.get("regime") or {}).get("state") == (champion.get("regime") or {}).get("state")),
            "baseline_selected_only": sorted(baseline_selected - champion_selected),
            "champion_selected_only": sorted(champion_selected - baseline_selected),
            "selected_overlap": sorted(baseline_selected & champion_selected),
            "baseline_order_only": [
                {"symbol": symbol, "side": side, "intent": intent}
                for symbol, side, intent in sorted(baseline_orders - champion_orders)
            ],
            "champion_order_only": [
                {"symbol": symbol, "side": side, "intent": intent}
                for symbol, side, intent in sorted(champion_orders - baseline_orders)
            ],
            "target_weight_delta": weight_deltas,
            "l1_target_weight_diff": round(l1_weight_diff, 6),
            "selection_changed": bool(baseline_selected != champion_selected),
            "orders_changed": bool(baseline_orders != champion_orders),
            "needs_review": bool(
                baseline_selected != champion_selected
                or baseline_orders != champion_orders
                or l1_weight_diff >= 0.20
                or ((baseline.get("regime") or {}).get("state") != (champion.get("regime") or {}).get("state"))
            ),
        },
    }


def build_latest_signal_markdown(summary: dict[str, Any]) -> str:
    baseline = summary["baseline"]
    champion = summary["champion"]
    compare = summary["compare"]
    lines = [
        "# Core6 Latest Signal Monitor",
        "",
        f"- generated_at: `{summary.get('generated_at')}`",
        f"- champion: `{summary.get('champion_name')}`",
        f"- baseline: `{summary.get('baseline_name')}`",
        f"- same_signal_ts: `{str(bool(compare.get('same_signal_ts'))).lower()}`",
        f"- same_regime: `{str(bool(compare.get('same_regime'))).lower()}`",
        f"- selection_changed: `{str(bool(compare.get('selection_changed'))).lower()}`",
        f"- orders_changed: `{str(bool(compare.get('orders_changed'))).lower()}`",
        f"- l1_target_weight_diff: `{compare.get('l1_target_weight_diff')}`",
        f"- needs_review: `{str(bool(compare.get('needs_review'))).lower()}`",
        "",
        "## Champion",
        "",
        f"- signal_dt: `{champion.get('signal_dt')}`",
        f"- regime: `{(champion.get('regime') or {}).get('state')}` x `{(champion.get('regime') or {}).get('multiplier')}`",
        f"- selected: `{', '.join(champion.get('selected') or [])}`",
        f"- entry_candidates: `{', '.join(champion.get('entry_candidates') or [])}`",
        f"- orders: `{len(champion.get('orders') or [])}`",
        "",
        "## Baseline",
        "",
        f"- signal_dt: `{baseline.get('signal_dt')}`",
        f"- regime: `{(baseline.get('regime') or {}).get('state')}` x `{(baseline.get('regime') or {}).get('multiplier')}`",
        f"- selected: `{', '.join(baseline.get('selected') or [])}`",
        f"- entry_candidates: `{', '.join(baseline.get('entry_candidates') or [])}`",
        f"- orders: `{len(baseline.get('orders') or [])}`",
        "",
        "## Diff",
        "",
        f"- champion_selected_only: `{', '.join(compare.get('champion_selected_only') or [])}`",
        f"- baseline_selected_only: `{', '.join(compare.get('baseline_selected_only') or [])}`",
        f"- selected_overlap: `{', '.join(compare.get('selected_overlap') or [])}`",
        "",
        "| symbol | champion_tw | baseline_tw | delta |",
        "|---|---:|---:|---:|",
    ]
    champion_weights = champion.get("target_weights") or {}
    baseline_weights = baseline.get("target_weights") or {}
    for symbol in sorted(set(champion_weights.keys()) | set(baseline_weights.keys())):
        c_weight = float(champion_weights.get(symbol, 0.0))
        b_weight = float(baseline_weights.get(symbol, 0.0))
        delta = c_weight - b_weight
        if abs(delta) <= 1e-9 and abs(c_weight) <= 1e-9 and abs(b_weight) <= 1e-9:
            continue
        lines.append(f"| {symbol} | {c_weight:.6f} | {b_weight:.6f} | {delta:.6f} |")
    return "\n".join(lines) + "\n"
