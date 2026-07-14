from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import statistics
import subprocess
import tempfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from configs.schema import AppConfig
from src.paper_runtime.contracts import (
    PAPER_STRATEGY_CONTRACT_VERSION,
    PaperRuntimeState,
    PaperStrategyAck,
    PaperStrategyProposal,
    assert_runtime_transition,
)
from src.paper_runtime.dsl import PaperRuleInterpreter
from src.paper_runtime.store import PaperRuntimeStore
from src.paper_runtime.source import read_paper_strategy_proposals

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PAPER_RUNTIME_SCHEMA_VERSION = "v5.generic_paper_runtime.v1"

ACK_FIELDS = (
    "proposal_id",
    "proposal_hash",
    "paper_tracker_id",
    "tracker_id",
    "accepted",
    "accepted_at",
    "paper_only",
    "max_live_notional_usdt",
    "recommended_mode",
    "symbol",
    "strategy_candidate",
    "strategy_version",
    "suggested_horizon",
    "proposal_source",
    "reject_reason",
    "contract_version",
    "rules_locked",
    "live_order_effect",
    "expires_at",
    "source_v5_commit",
    "source_v5_bundle_sha256",
    "schema_version",
)


def paper_runtime_observation_symbols(
    cfg: AppConfig,
    *,
    run_dir: str | Path,
) -> list[str]:
    runtime_cfg = cfg.quant_lab.paper_runtime
    if not runtime_cfg.enabled:
        return []
    symbols: set[str] = set()
    try:
        state = PaperRuntimeStore(_state_path(runtime_cfg.state_path)).load()
        for tracker in state.get("trackers", {}).values():
            symbol = str((tracker.get("proposal") or {}).get("symbol") or "")
            if symbol:
                symbols.add(symbol)
    except Exception:
        pass
    try:
        for row in _proposal_rows(cfg, Path(run_dir)):
            symbol = str(row.get("symbol") or row.get("v5_symbol") or "")
            if symbol:
                symbols.add(_slash_symbol(symbol))
    except Exception:
        pass
    return sorted(symbols)[: runtime_cfg.max_observation_symbols]


def supplement_paper_runtime_market_data(
    *,
    provider: Any,
    market_data_1h: Mapping[str, Any],
    observation_symbols: Iterable[str],
    timeframe: str,
    limit: int,
    end_ts_ms: int | None = None,
) -> dict[str, Any]:
    """Fetch proposal-only bars without changing the live scoring universe."""
    merged = dict(market_data_1h or {})
    missing = [
        _slash_symbol(symbol)
        for symbol in observation_symbols
        if _slash_symbol(symbol) and _lookup_symbol(merged, symbol) is None
    ]
    missing = list(dict.fromkeys(missing))
    if not missing:
        return merged
    fetch = getattr(provider, "fetch_ohlcv", None)
    if not callable(fetch):
        raise AttributeError("market data provider does not support fetch_ohlcv")
    fetched = fetch(
        missing,
        timeframe=timeframe,
        limit=limit,
        end_ts_ms=end_ts_ms,
    )
    if not isinstance(fetched, Mapping):
        raise TypeError("paper runtime OHLCV fetch must return a mapping")
    for key, value in fetched.items():
        if _lookup_symbol(merged, key) is None:
            merged[str(key)] = value
    return merged


def run_generic_paper_runtime(
    *,
    run_dir: str | Path,
    market_data_1h: Mapping[str, Any],
    top_of_book: Mapping[str, Any] | None,
    cfg: AppConfig,
    audit: Any = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    runtime_cfg = cfg.quant_lab.paper_runtime
    current = (now or datetime.now(UTC)).astimezone(UTC)
    run_path = Path(run_dir)
    reports_dir = _reports_dir(run_path)
    summaries_dir = reports_dir / "summaries"
    summaries_dir.mkdir(parents=True, exist_ok=True)
    store = PaperRuntimeStore(_state_path(runtime_cfg.state_path))
    errors: list[dict[str, Any]] = []
    recovery_rows: list[dict[str, Any]] = []
    try:
        state = store.load()
    except Exception as exc:
        errors.append(_error_row(current, "state_load_failed", exc))
        contract_status = _contract_status(
            cfg=cfg,
            runtime_cfg=runtime_cfg,
            now=current,
            ack_rows=[],
            trackers=[],
            state_loaded=False,
            state_persisted=False,
            failure_stage="state_load_failed",
        )
        _write_failure_reports(
            summaries_dir,
            error_rows=errors,
            contract_status=contract_status,
            history_limit=runtime_cfg.max_history_records,
        )
        return _failure_result(
            runtime_cfg=runtime_cfg,
            store=store,
            errors=errors,
            failure_stage="state_load_failed",
        )
    trackers: dict[str, dict[str, Any]] = state.setdefault("trackers", {})
    loaded_tracker_count = len(trackers)
    for tracker in trackers.values():
        if tracker.get("state") in {
            PaperRuntimeState.PAPER_OPEN.value,
            PaperRuntimeState.PAPER_EXIT_PENDING.value,
        }:
            recovery_rows.append(
                {
                    "recovered_at": current.isoformat(),
                    "tracker_id": tracker.get("tracker_id"),
                    "proposal_id": (tracker.get("proposal") or {}).get("proposal_id"),
                    "state": tracker.get("state"),
                    "open_trade_preserved": bool(tracker.get("open_trade")),
                    "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
                }
            )

    try:
        proposal_rows = _proposal_rows(cfg, run_path)
    except Exception as exc:
        proposal_rows = []
        errors.append(_error_row(current, "proposal_source_read_failed", exc))
    ack_rows: list[dict[str, Any]] = []
    accepted_proposals: dict[str, PaperStrategyProposal] = {}
    current_proposal_ids: set[str] = set()
    seen_proposals: set[tuple[str, str]] = set()
    for raw in proposal_rows[: runtime_cfg.max_trackers * 2]:
        proposal_key = (
            str(raw.get("proposal_id") or ""),
            str(raw.get("proposal_hash") or ""),
        )
        if proposal_key in seen_proposals:
            continue
        seen_proposals.add(proposal_key)
        proposal, reject_reason = _parse_proposal(raw, current)
        raw_proposal_id = str(raw.get("proposal_id") or "")
        if raw_proposal_id:
            current_proposal_ids.add(raw_proposal_id)
        if proposal is None:
            existing = trackers.get(proposal_key[0])
            if (
                reject_reason == "proposal_expired"
                and existing is not None
                and str(existing.get("proposal_hash") or "") == proposal_key[1]
            ):
                locked = PaperStrategyProposal.model_validate(existing.get("proposal"))
                accepted_proposals[locked.proposal_id] = locked
                ack_rows.append(_accepted_ack_row(locked, existing, current))
                continue
            ack_rows.append(_rejected_ack_row(raw, reject_reason, current))
            continue
        if not runtime_cfg.enabled:
            ack_rows.append(_rejected_ack_row(raw, "config_disabled", current))
            continue
        existing = trackers.get(proposal.proposal_id)
        conflict = _version_conflict(trackers.values(), proposal)
        if conflict:
            ack_rows.append(
                _rejected_ack_row(raw, "duplicate_version_conflict", current)
            )
            continue
        if existing is None and len(trackers) >= runtime_cfg.max_trackers:
            ack_rows.append(
                _rejected_ack_row(raw, "tracker_capacity_exceeded", current)
            )
            continue
        tracker = existing or _new_tracker(proposal, current)
        if (
            existing is not None
            and str(existing.get("proposal_hash")) != proposal.proposal_hash
        ):
            ack_rows.append(
                _rejected_ack_row(raw, "duplicate_version_conflict", current)
            )
            continue
        trackers[proposal.proposal_id] = tracker
        accepted_proposals[proposal.proposal_id] = proposal
        ack_rows.append(_accepted_ack_row(proposal, tracker, current))

    for proposal_id, tracker in trackers.items():
        current_member = proposal_id in accepted_proposals
        open_position = bool(tracker.get("open_trade"))
        tracker["current_proposal_member"] = current_member
        tracker["current_cohort_member"] = current_member
        if current_member:
            tracker["supersession_status"] = "CURRENT_ACTIVE"
            tracker["new_entry_allowed"] = True
            tracker["exit_allowed"] = True
        elif open_position:
            tracker["supersession_status"] = "SUPERSEDED_EXIT_ONLY"
            tracker["new_entry_allowed"] = False
            tracker["exit_allowed"] = True
        else:
            tracker["supersession_status"] = "SUPERSEDED_CLOSED"
            tracker["new_entry_allowed"] = False
            tracker["exit_allowed"] = False

    signal_rows: list[dict[str, Any]] = []
    new_run_rows: list[dict[str, Any]] = []
    interpreter = PaperRuleInterpreter()
    for proposal_id, tracker in list(trackers.items()):
        if not runtime_cfg.enabled:
            continue
        try:
            proposal = accepted_proposals.get(
                proposal_id
            ) or PaperStrategyProposal.model_validate(tracker.get("proposal"))
            _advance_tracker(
                tracker=tracker,
                proposal=proposal,
                market_data_1h=market_data_1h,
                top_of_book=top_of_book or {},
                audit=audit,
                cfg=cfg,
                interpreter=interpreter,
                now=current,
                signal_rows=signal_rows,
                new_run_rows=new_run_rows,
            )
        except Exception as exc:
            errors.append(
                _error_row(
                    current, "tracker_advance_failed", exc, proposal_id=proposal_id
                )
            )

    max_history = runtime_cfg.max_history_records
    combined_signals = [*(state.get("signals") or []), *signal_rows]
    state_signals = _dedupe_rows(combined_signals, "signal_id")[-max_history:]
    state_runs = [*(state.get("runs") or []), *new_run_rows][-max_history:]
    daily_buckets = state.get("daily_buckets")
    rebuild_daily_buckets = not isinstance(daily_buckets, dict) or len(
        state_signals
    ) != len(combined_signals)
    if rebuild_daily_buckets:
        daily_buckets = {}
    _update_daily_buckets(
        daily_buckets,
        trackers.values(),
        state_signals if rebuild_daily_buckets else signal_rows,
        state_runs if rebuild_daily_buckets else new_run_rows,
        current,
    )
    state["daily_buckets"] = _bounded_daily_buckets(daily_buckets, max_history)
    state["signals"] = state_signals
    state["runs"] = state_runs
    state["updated_at"] = current.isoformat()
    state["schema_version"] = "v5.paper_runtime_state.v1"
    try:
        store.save(state)
    except Exception as exc:
        errors.append(_error_row(current, "state_write_failed", exc))
        contract_status = _contract_status(
            cfg=cfg,
            runtime_cfg=runtime_cfg,
            now=current,
            ack_rows=[],
            trackers=list(trackers.values())[:loaded_tracker_count],
            state_loaded=True,
            state_persisted=False,
            failure_stage="state_write_failed",
        )
        _write_failure_reports(
            summaries_dir,
            error_rows=errors,
            contract_status=contract_status,
            history_limit=max_history,
        )
        return _failure_result(
            runtime_cfg=runtime_cfg,
            store=store,
            errors=errors,
            failure_stage="state_write_failed",
            tracker_count=loaded_tracker_count,
        )

    registry_history_rows = [_registry_row(tracker) for tracker in trackers.values()]
    registry_current_rows = [
        row for row in registry_history_rows if _as_bool(row.get("current_proposal_member"))
    ]
    state_history_rows = [_state_row(tracker) for tracker in trackers.values()]
    state_current_rows = [
        row for row in state_history_rows if _as_bool(row.get("current_proposal_member"))
    ]
    daily_rows = _daily_rows(state["daily_buckets"])
    quote_coverage_rows = _quote_coverage_rows(state_signals)
    cost_evidence_rows = _cost_evidence_rows(state_runs, trackers.values())
    exit_quality_rows = _exit_quality_rows(state_runs)
    canonical_ack_history_rows = _canonical_ack_rows(trackers.values(), ack_rows, current)
    canonical_ack_current_rows = [
        row
        for row in ack_rows
        if str(row.get("proposal_id") or "") in current_proposal_ids
    ]
    contract_status = _contract_status(
        cfg=cfg,
        runtime_cfg=runtime_cfg,
        now=current,
        ack_rows=canonical_ack_current_rows,
        trackers=trackers.values(),
        state_loaded=True,
        state_persisted=True,
    )
    try:
        _write_runtime_reports(
            summaries_dir,
            ack_rows=canonical_ack_current_rows,
            ack_history_rows=canonical_ack_history_rows,
            registry_rows=registry_current_rows,
            registry_history_rows=registry_history_rows,
            state_rows=state_current_rows,
            state_history_rows=state_history_rows,
            signal_rows=state_signals,
            run_rows=state_runs,
            daily_rows=daily_rows,
            quote_coverage_rows=quote_coverage_rows,
            cost_evidence_rows=cost_evidence_rows,
            exit_quality_rows=exit_quality_rows,
            error_rows=errors,
            recovery_rows=recovery_rows,
            contract_status=contract_status,
            history_limit=max_history,
        )
    except Exception as exc:
        errors.append(_error_row(current, "report_write_failed", exc))
    return {
        "enabled": bool(runtime_cfg.enabled),
        "proposal_rows": len(proposal_rows),
        "accepted": contract_status["accepted_proposal_count"],
        "rejected": contract_status["rejected_proposal_count"],
        "trackers": len(trackers),
        "signals": len(signal_rows),
        "closed_trades": len(new_run_rows),
        "errors": len(errors),
        "state_path": str(store.path),
        "fail_closed": False,
        "state_persisted": True,
        "live_order_effect": "none",
    }


def _advance_tracker(
    *,
    tracker: dict[str, Any],
    proposal: PaperStrategyProposal,
    market_data_1h: Mapping[str, Any],
    top_of_book: Mapping[str, Any],
    audit: Any,
    cfg: AppConfig,
    interpreter: PaperRuleInterpreter,
    now: datetime,
    signal_rows: list[dict[str, Any]],
    new_run_rows: list[dict[str, Any]],
) -> None:
    state = PaperRuntimeState(
        str(tracker.get("state") or PaperRuntimeState.WAITING_SIGNAL.value)
    )
    if proposal.expires_at <= now and state not in {
        PaperRuntimeState.PAPER_OPEN,
        PaperRuntimeState.PAPER_EXIT_PENDING,
    }:
        _set_state(tracker, PaperRuntimeState.EXPIRED)
        return
    context, quote = _market_context(
        proposal.symbol,
        market_data_1h,
        top_of_book,
        audit=audit,
        now=now,
    )
    if not context:
        observation_key = _signal_observation_key(context, now)
        if (
            str(tracker.get("last_unobservable_observation_key") or "")
            == observation_key
        ):
            return
        tracker["last_unobservable_observation_key"] = observation_key
        signal_rows.append(
            _signal_row(proposal, tracker, now, {}, {}, False, "NO_MARKET_DATA")
        )
        return
    context["real_mode_would_allow"] = bool(
        cfg.quant_lab.mode == "enforce"
        and cfg.quant_lab.canary.enabled
        and proposal.strategy_id in set(cfg.quant_lab.canary.strategy_whitelist)
    )
    context["real_cost_canary_ready"] = _audit_cost_canary_ready(audit)
    bar_key = str(context.get("bar_ts") or "")
    if bar_key and bar_key == str(tracker.get("last_processed_bar_ts") or ""):
        return
    tracker["last_processed_bar_ts"] = bar_key
    history = list(tracker.get("context_history") or [])[-512:]
    opened_now = False

    consumed_cooldown_bar = state == PaperRuntimeState.COOLDOWN
    if consumed_cooldown_bar:
        remaining = max(int(tracker.get("cooldown_remaining_bars") or 0) - 1, 0)
        tracker["cooldown_remaining_bars"] = remaining
        if remaining <= 0:
            _set_state(tracker, PaperRuntimeState.WAITING_SIGNAL)
            state = PaperRuntimeState.WAITING_SIGNAL

    if state in {
        PaperRuntimeState.ACK_ACCEPTED,
        PaperRuntimeState.PAPER_CLOSED,
    }:
        _set_state(tracker, PaperRuntimeState.WAITING_SIGNAL)
        state = PaperRuntimeState.WAITING_SIGNAL

    if state == PaperRuntimeState.WAITING_SIGNAL and not consumed_cooldown_bar:
        if not _as_bool(tracker.get("new_entry_allowed")):
            tracker["updated_at"] = now.isoformat()
            return
        triggered = interpreter.evaluate(proposal.entry_rule, context, history=history)
        confirmation = int(tracker.get("entry_confirmation_count") or 0)
        confirmation = confirmation + 1 if triggered else 0
        tracker["entry_confirmation_count"] = confirmation
        confirmed = triggered and confirmation >= proposal.signal_confirmation_bars
        observability = _entry_observability(
            proposal,
            context,
            quote,
            cfg.quant_lab.paper_runtime.max_quote_age_seconds,
        )
        signal_rows.append(
            _signal_row(
                proposal, tracker, now, context, quote, confirmed, observability
            )
        )
        if confirmed and observability == "OBSERVABLE":
            trade = _open_trade(proposal, context, quote, cfg, now)
            tracker["open_trade"] = trade
            tracker["entry_confirmation_count"] = 0
            _set_state(tracker, PaperRuntimeState.PAPER_OPEN)
            state = PaperRuntimeState.PAPER_OPEN
            opened_now = True

    if (
        state
        in {
            PaperRuntimeState.PAPER_OPEN,
            PaperRuntimeState.PAPER_EXIT_PENDING,
        }
        and tracker.get("open_trade")
        and not opened_now
        and _as_bool(tracker.get("exit_allowed", True))
    ):
        trade = tracker["open_trade"]
        _mark_to_market(trade, context, quote, cfg)
        if state == PaperRuntimeState.PAPER_OPEN:
            exit_context = {
                **context,
                "holding_bars": int(trade.get("holding_bars") or 0),
                "gross_pnl_bps": trade.get("gross_pnl_bps"),
                "net_pnl_bps": trade.get("net_pnl_bps"),
                "peak_pnl_bps": trade.get("max_favorable_excursion"),
            }
            should_exit = int(
                trade.get("holding_bars") or 0
            ) >= proposal.min_holding_bars and interpreter.evaluate(
                proposal.exit_rule, exit_context, history=history
            )
            if should_exit:
                trade["pending_exit_reason"] = interpreter.match_reason(
                    proposal.exit_rule,
                    exit_context,
                    history=history,
                ) or "structured_exit_rule"
                _set_state(tracker, PaperRuntimeState.PAPER_EXIT_PENDING)
                state = PaperRuntimeState.PAPER_EXIT_PENDING
        if state == PaperRuntimeState.PAPER_EXIT_PENDING:
            observability = _quote_observability(
                quote,
                cfg.quant_lab.paper_runtime.max_quote_age_seconds,
            )
            if observability == "OBSERVABLE":
                closed = _close_trade(proposal, trade, context, quote, cfg, now)
                new_run_rows.append(closed)
                tracker.setdefault("closed_trades", []).append(closed)
                tracker["closed_trades"] = tracker["closed_trades"][-100:]
                tracker["open_trade"] = None
                _set_state(tracker, PaperRuntimeState.PAPER_CLOSED)
                tracker["cooldown_remaining_bars"] = proposal.cooldown_bars
                _set_state(
                    tracker,
                    PaperRuntimeState.COOLDOWN
                    if proposal.cooldown_bars > 0
                    else PaperRuntimeState.WAITING_SIGNAL,
                )
                if not _as_bool(tracker.get("current_proposal_member")):
                    tracker["supersession_status"] = "SUPERSEDED_CLOSED"
                    tracker["exit_allowed"] = False

    tracker["context_history"] = [*history, _bounded_context(context)][-512:]
    tracker["updated_at"] = now.isoformat()


def _new_tracker(proposal: PaperStrategyProposal, now: datetime) -> dict[str, Any]:
    tracker_id = f"paper:{proposal.proposal_id}"
    state = PaperRuntimeState.PROPOSAL_RECEIVED
    state = assert_runtime_transition(state, PaperRuntimeState.VALIDATED)
    state = assert_runtime_transition(state, PaperRuntimeState.ACK_ACCEPTED)
    state = assert_runtime_transition(state, PaperRuntimeState.WAITING_SIGNAL)
    return {
        "tracker_id": tracker_id,
        "proposal_id": proposal.proposal_id,
        "proposal_hash": proposal.proposal_hash,
        "proposal": proposal.model_dump(mode="json"),
        "state": state.value,
        "rules_locked": True,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "last_processed_bar_ts": "",
        "entry_confirmation_count": 0,
        "cooldown_remaining_bars": 0,
        "context_history": [],
        "open_trade": None,
        "closed_trades": [],
        "current_proposal_member": True,
        "current_cohort_member": True,
        "supersession_status": "CURRENT_ACTIVE",
        "new_entry_allowed": True,
        "exit_allowed": True,
    }


def _set_state(tracker: dict[str, Any], target: PaperRuntimeState) -> None:
    current = PaperRuntimeState(str(tracker.get("state") or target.value))
    tracker["state"] = assert_runtime_transition(current, target).value


def _open_trade(
    proposal: PaperStrategyProposal,
    context: Mapping[str, Any],
    quote: Mapping[str, Any],
    cfg: AppConfig,
    now: datetime,
) -> dict[str, Any]:
    entry_mid = float(quote["mid"])
    side_price = float(quote["ask"] if proposal.direction == "long" else quote["bid"])
    slippage_bps = float(cfg.quant_lab.paper_runtime.default_slippage_bps)
    multiplier = (
        1.0 + slippage_bps / 10_000.0
        if proposal.direction == "long"
        else 1.0 - slippage_bps / 10_000.0
    )
    entry_price = side_price * multiplier
    trade_id = hashlib.sha256(
        f"{proposal.proposal_hash}|{context.get('bar_ts')}".encode("utf-8")
    ).hexdigest()[:32]
    return {
        "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
        "paper_trade_id": trade_id,
        "proposal_id": proposal.proposal_id,
        "strategy_id": proposal.strategy_id,
        "strategy_version": proposal.strategy_version,
        "symbol": proposal.symbol,
        "direction": proposal.direction,
        "entry_signal_ts": context.get("bar_ts"),
        "entry_decision_ts": now.isoformat(),
        "entry_arrival_mid": entry_mid,
        "entry_bid": quote.get("bid"),
        "entry_ask": quote.get("ask"),
        "virtual_entry_price": entry_price,
        "paper_notional": proposal.paper_notional_usdt,
        "virtual_quantity": proposal.paper_notional_usdt / entry_price,
        "price_source": "top_of_book_directional",
        "quote_timestamp": quote.get("quote_timestamp"),
        "quote_age_seconds": quote.get("quote_age_seconds"),
        "spread_bps": quote.get("spread_bps"),
        "fallback_level": "NONE",
        "fee_estimate": 0.0,
        "slippage_estimate": slippage_bps,
        "total_cost_bps": 0.0,
        "gross_pnl_bps": 0.0,
        "net_pnl_bps": 0.0,
        "max_favorable_excursion": 0.0,
        "max_adverse_excursion": 0.0,
        "mfe_bps": 0.0,
        "mae_bps": 0.0,
        "holding_bars": 0,
        "cost_source": "configured_conservative_paper",
        "required_cost_trust_level": proposal.required_cost_trust_level,
        "cost_trust_level": "PAPER_ONLY",
        "cost_model_version": "v5.paper_cost.v1",
        "market_regime": context.get("market_regime"),
        "real_permission_would_allow": context.get("real_permission_would_allow"),
        "real_mode_would_allow": context.get("real_mode_would_allow"),
        "real_cost_canary_ready": context.get("real_cost_canary_ready"),
        "real_funds_sufficient": context.get("real_funds_sufficient"),
    }


def _mark_to_market(
    trade: dict[str, Any],
    context: Mapping[str, Any],
    quote: Mapping[str, Any],
    cfg: AppConfig,
) -> None:
    if (
        _quote_observability(quote, cfg.quant_lab.paper_runtime.max_quote_age_seconds)
        != "OBSERVABLE"
    ):
        return
    exit_side = _virtual_exit_price(
        direction=str(trade.get("direction") or "long"),
        quote=quote,
        slippage_bps=float(cfg.quant_lab.paper_runtime.default_slippage_bps),
    )
    entry = float(trade["virtual_entry_price"])
    gross = (
        (exit_side / entry - 1.0) * 10_000.0
        if trade.get("direction") == "long"
        else (entry / exit_side - 1.0) * 10_000.0
    )
    fees = float(cfg.quant_lab.paper_runtime.default_fee_bps) * 2.0
    trade["holding_bars"] = int(trade.get("holding_bars") or 0) + 1
    trade["gross_pnl_bps"] = gross
    trade["net_pnl_bps"] = gross - fees
    trade["max_favorable_excursion"] = max(
        float(trade.get("max_favorable_excursion") or 0.0), gross - fees
    )
    trade["max_adverse_excursion"] = min(
        float(trade.get("max_adverse_excursion") or 0.0), gross - fees
    )
    trade["mfe_bps"] = trade["max_favorable_excursion"]
    trade["mae_bps"] = trade["max_adverse_excursion"]


def _close_trade(
    proposal: PaperStrategyProposal,
    trade: dict[str, Any],
    context: Mapping[str, Any],
    quote: Mapping[str, Any],
    cfg: AppConfig,
    now: datetime,
) -> dict[str, Any]:
    slippage_bps = float(cfg.quant_lab.paper_runtime.default_slippage_bps)
    exit_price = _virtual_exit_price(
        direction=proposal.direction,
        quote=quote,
        slippage_bps=slippage_bps,
    )
    entry_price = float(trade["virtual_entry_price"])
    gross = (
        (exit_price / entry_price - 1.0) * 10_000.0
        if proposal.direction == "long"
        else (entry_price / exit_price - 1.0) * 10_000.0
    )
    fee_bps = float(cfg.quant_lab.paper_runtime.default_fee_bps) * 2.0
    total_cost_bps = (
        fee_bps + slippage_bps * 2.0 + float(trade.get("spread_bps") or 0.0)
    )
    net_pnl_bps = gross - fee_bps
    mfe_bps = max(float(trade.get("mfe_bps") or 0.0), net_pnl_bps)
    mae_bps = min(float(trade.get("mae_bps") or 0.0), net_pnl_bps)
    profit_giveback_bps = max(0.0, mfe_bps - net_pnl_bps)
    exit_efficiency = net_pnl_bps / mfe_bps if mfe_bps > 0 else None
    exit_reason = str(
        trade.get("pending_exit_reason") or "structured_exit_rule"
    )
    holding_period_seconds = _elapsed_seconds(
        trade.get("entry_decision_ts"),
        now,
    )
    return {
        **trade,
        "ts_utc": now.isoformat(),
        "as_of_date": now.date().isoformat(),
        "paper_tracker_id": f"paper:{proposal.proposal_id}",
        "strategy_candidate": proposal.strategy_family,
        "recommended_mode": "paper",
        "board_decision": "PAPER_TRACKER_ACTIVE",
        "suggested_horizon": f"{proposal.max_holding_bars}h",
        "horizon_hours": proposal.max_holding_bars,
        "would_enter": True,
        "would_exit": True,
        "would_size": proposal.paper_notional_usdt,
        "would_size_usdt": proposal.paper_notional_usdt,
        "paper_source": "generic_contract_runtime",
        "paper_count_scope": "closed_trade",
        "exit_signal_ts": context.get("bar_ts"),
        "exit_decision_ts": now.isoformat(),
        "exit_arrival_mid": quote.get("mid"),
        "virtual_exit_price": exit_price,
        "fee_estimate": fee_bps,
        "slippage_estimate": slippage_bps * 2.0,
        "total_cost_bps": total_cost_bps,
        "gross_pnl_bps": gross,
        "net_pnl_bps": net_pnl_bps,
        "mfe_bps": mfe_bps,
        "mae_bps": mae_bps,
        "profit_giveback_bps": profit_giveback_bps,
        "exit_efficiency": exit_efficiency,
        "exit_timing_bars": int(trade.get("holding_bars") or 0),
        "holding_period_seconds": holding_period_seconds,
        "paper_pnl_bps": net_pnl_bps,
        "paper_pnl_usdt": proposal.paper_notional_usdt * net_pnl_bps / 10_000.0,
        "estimated_spread_bps": trade.get("spread_bps"),
        "exit_reason": exit_reason,
        "exit_timing_state": _exit_timing_state(exit_reason),
        "valid_for_promotion": True,
        "closed_at": now.isoformat(),
    }


def _virtual_exit_price(
    *,
    direction: str,
    quote: Mapping[str, Any],
    slippage_bps: float,
) -> float:
    side_price = float(quote["bid"] if direction == "long" else quote["ask"])
    multiplier = (
        1.0 - slippage_bps / 10_000.0
        if direction == "long"
        else 1.0 + slippage_bps / 10_000.0
    )
    return side_price * multiplier


def _elapsed_seconds(start: Any, end: datetime) -> float | None:
    parsed = _datetime(start)
    if parsed is None:
        return None
    return max((end - parsed).total_seconds(), 0.0)


def _exit_timing_state(reason: str) -> str:
    if "take_profit" in reason:
        return "profit_target"
    if "stop_loss" in reason:
        return "hard_stop"
    if "trailing_exit" in reason:
        return "profit_giveback"
    if "signal_invalid" in reason:
        return "signal_invalidation"
    if "max_holding_bars" in reason:
        return "time_horizon"
    return "structured_rule"


def _market_context(
    symbol: str,
    market_data_1h: Mapping[str, Any],
    top_of_book: Mapping[str, Any],
    *,
    audit: Any,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    series = _lookup_symbol(market_data_1h, symbol)
    closes = list(getattr(series, "close", []) or []) if series is not None else []
    volumes = list(getattr(series, "volume", []) or []) if series is not None else []
    timestamps = list(getattr(series, "ts", []) or []) if series is not None else []
    if not closes:
        return {}, {}
    context: dict[str, Any] = {
        "open": _last(getattr(series, "open", [])),
        "high": _last(getattr(series, "high", [])),
        "low": _last(getattr(series, "low", [])),
        "close": closes[-1],
        "volume": _last(volumes),
        "return_1": _return(closes, 1),
        "return_4": _return(closes, 4),
        "return_8": _return(closes, 8),
        "return_24": _return(closes, 24),
        "momentum_4": _return(closes, 4),
        "momentum_8": _return(closes, 8),
        "momentum_24": _return(closes, 24),
        "volatility_8": _volatility(closes, 8),
        "volatility_24": _volatility(closes, 24),
        "volume_zscore_24": _zscore(volumes, 24),
        "market_regime": str(getattr(audit, "regime", "UNKNOWN") or "UNKNOWN").upper(),
        "bar_ts": _timestamp_text(
            timestamps[-1] if timestamps else now.timestamp() * 1000
        ),
        "real_permission_would_allow": _audit_permission_allows(audit),
        "real_mode_would_allow": False,
        "real_cost_canary_ready": False,
        "real_funds_sufficient": "not_evaluated_for_paper",
    }
    raw_quote = _lookup_symbol(top_of_book, symbol) or {}
    bid = _float(raw_quote.get("bid"))
    ask = _float(raw_quote.get("ask"))
    mid = _float(raw_quote.get("mid"))
    if mid is None and bid and ask:
        mid = (bid + ask) / 2.0
    quote_ts = (
        raw_quote.get("timestamp")
        or raw_quote.get("ts")
        or raw_quote.get("quote_timestamp")
        or raw_quote.get("quote_ts")
    )
    quote_dt = _datetime(quote_ts) if quote_ts not in (None, "") else None
    age = max((now - quote_dt).total_seconds(), 0.0) if quote_dt is not None else None
    spread = (ask - bid) / mid * 10_000.0 if bid and ask and mid else None
    quote = {
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread_bps": spread,
        "quote_timestamp": quote_dt.isoformat() if quote_dt is not None else None,
        "quote_age_seconds": age,
    }
    context.update(
        {
            key: value
            for key, value in quote.items()
            if key in {"bid", "ask", "mid", "spread_bps"}
        }
    )
    return context, quote


def _parse_proposal(
    row: Mapping[str, Any],
    now: datetime,
) -> tuple[PaperStrategyProposal | None, str]:
    payload: dict[str, Any] = {}
    for field in PaperStrategyProposal.model_fields:
        if field not in row:
            continue
        value = row.get(field)
        if field in {
            "entry_rule",
            "exit_rule",
            "source_dataset_versions",
            "required_market_fields",
            "blocked_reasons",
            "next_required_actions",
        }:
            value = _json_value(value)
        if field in {"paper_only"}:
            value = _as_bool(value)
        if field in {
            "max_holding_bars",
            "min_holding_bars",
            "cooldown_bars",
            "signal_confirmation_bars",
        }:
            value = _int(value)
        if field in {
            "minimum_expected_edge_bps",
            "paper_notional_usdt",
            "max_live_notional_usdt",
        }:
            value = _float(value)
        payload[field] = value
    if str(payload.get("contract_version") or "") != PAPER_STRATEGY_CONTRACT_VERSION:
        return None, "unsupported_contract_version"
    if (
        ("paper_only" in payload and not _as_bool(payload.get("paper_only")))
        or (
            "live_order_effect" in payload
            and str(payload.get("live_order_effect") or "").strip().lower() != "none"
        )
        or (
            "max_live_notional_usdt" in payload
            and (_float(payload.get("max_live_notional_usdt")) or 0.0) != 0.0
        )
    ):
        return None, "unsafe_live_effect"
    try:
        proposal = PaperStrategyProposal.model_validate(payload)
    except ValidationError as exc:
        text = str(exc)
        if (
            "unsupported paper rule" in text
            or "literal_error" in text
            and "operator" in text
        ):
            return None, "unsupported_operator"
        if "invalid_symbol" in text:
            return None, "invalid_symbol"
        if "invalid_timeframe" in text:
            return None, "invalid_timeframe"
        if "missing_market_field" in text:
            return None, "missing_market_field"
        return None, "invalid_schema"
    if proposal.expires_at <= now:
        return None, "proposal_expired"
    if not proposal.paper_only or proposal.live_order_effect != "none":
        return None, "unsafe_live_effect"
    return proposal, ""


def _proposal_rows(cfg: AppConfig, run_path: Path) -> list[dict[str, Any]]:
    return read_paper_strategy_proposals(
        run_path=run_path,
        reports_dir=_reports_dir(run_path),
        diagnostics=cfg.diagnostics,
        now_ms=int(datetime.now(UTC).timestamp() * 1000),
    )


def _accepted_ack_row(
    proposal: PaperStrategyProposal,
    tracker: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    ack = PaperStrategyAck(
        proposal_id=proposal.proposal_id,
        proposal_hash=proposal.proposal_hash,
        accepted=True,
        tracker_id=str(tracker.get("tracker_id") or ""),
        strategy_version=proposal.strategy_version,
        rules_locked=True,
        paper_only=True,
        live_order_effect="none",
        accepted_at=_datetime(tracker.get("created_at")) or now,
        expires_at=proposal.expires_at,
        source_v5_commit=_source_v5_commit(),
    )
    return {
        **ack.model_dump(mode="json"),
        "paper_tracker_id": ack.tracker_id,
        "max_live_notional_usdt": 0.0,
        "recommended_mode": "paper",
        "symbol": proposal.symbol,
        "strategy_candidate": proposal.strategy_family,
        "suggested_horizon": f"{proposal.max_holding_bars}h",
        "proposal_source": "generic_contract_runtime",
        "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
    }


def _rejected_ack_row(
    row: Mapping[str, Any], reason: str, now: datetime
) -> dict[str, Any]:
    return {
        "proposal_id": str(row.get("proposal_id") or row.get("strategy_id") or ""),
        "proposal_hash": str(row.get("proposal_hash") or ""),
        "paper_tracker_id": "",
        "tracker_id": "",
        "accepted": False,
        "accepted_at": now.isoformat(),
        "paper_only": True,
        "max_live_notional_usdt": 0.0,
        "recommended_mode": "paper",
        "symbol": _slash_symbol(row.get("symbol")),
        "strategy_candidate": str(row.get("strategy_candidate") or ""),
        "strategy_version": str(row.get("strategy_version") or ""),
        "suggested_horizon": str(row.get("suggested_horizon") or ""),
        "proposal_source": "generic_contract_runtime",
        "reject_reason": reason or "invalid_schema",
        "contract_version": str(row.get("contract_version") or ""),
        "rules_locked": False,
        "live_order_effect": "none",
        "expires_at": str(row.get("expires_at") or ""),
        "source_v5_commit": _source_v5_commit(),
        "source_v5_bundle_sha256": "",
        "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
    }


def _version_conflict(
    trackers: Iterable[Mapping[str, Any]], proposal: PaperStrategyProposal
) -> bool:
    for tracker in trackers:
        existing = tracker.get("proposal") or {}
        if str(existing.get("strategy_id") or "") != proposal.strategy_id:
            continue
        if str(existing.get("strategy_version") or "") != proposal.strategy_version:
            continue
        if str(existing.get("proposal_hash") or "") != proposal.proposal_hash:
            return True
    return False


def _signal_row(
    proposal: PaperStrategyProposal,
    tracker: Mapping[str, Any],
    now: datetime,
    context: Mapping[str, Any],
    quote: Mapping[str, Any],
    triggered: bool,
    observability: str,
) -> dict[str, Any]:
    return {
        "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
        "signal_id": hashlib.sha256(
            f"{proposal.proposal_hash}|{_signal_observation_key(context, now)}|entry".encode(
                "utf-8"
            )
        ).hexdigest()[:32],
        "proposal_id": proposal.proposal_id,
        "tracker_id": tracker.get("tracker_id"),
        "strategy_id": proposal.strategy_id,
        "strategy_version": proposal.strategy_version,
        "strategy_candidate": proposal.strategy_family,
        "symbol": proposal.symbol,
        "signal_ts": context.get("bar_ts") or now.isoformat(),
        "decision_ts": now.isoformat(),
        "signal_type": "ENTRY",
        "triggered": triggered,
        "observability": observability,
        "arrival_bid": quote.get("bid"),
        "arrival_ask": quote.get("ask"),
        "arrival_mid": quote.get("mid"),
        "spread_bps": quote.get("spread_bps"),
        "quote_timestamp": quote.get("quote_timestamp"),
        "quote_age_seconds": quote.get("quote_age_seconds"),
        "price_source": "top_of_book"
        if observability == "OBSERVABLE"
        else "NOT_OBSERVABLE",
        "fallback_level": "NONE" if observability == "OBSERVABLE" else "NOT_OBSERVABLE",
        "valid_for_promotion": observability == "OBSERVABLE",
        "real_permission_would_allow": context.get("real_permission_would_allow"),
        "real_mode_would_allow": context.get("real_mode_would_allow"),
        "real_cost_canary_ready": context.get("real_cost_canary_ready"),
        "real_funds_sufficient": context.get("real_funds_sufficient"),
    }


def _signal_observation_key(context: Mapping[str, Any], now: datetime) -> str:
    bar_ts = str(context.get("bar_ts") or "").strip()
    if bar_ts:
        return bar_ts
    hour = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    return hour.isoformat().replace("+00:00", "Z")


def _dedupe_rows(rows: Iterable[Mapping[str, Any]], key: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    indexes: dict[str, int] = {}
    for raw in rows:
        row = dict(raw)
        value = str(row.get(key) or "").strip()
        if not value:
            output.append(row)
            continue
        if value in indexes:
            output[indexes[value]] = row
            continue
        indexes[value] = len(output)
        output.append(row)
    return output


def _source_v5_commit() -> str:
    return str(os.environ.get("V5_GIT_COMMIT") or "").strip() or _repo_commit()


@lru_cache(maxsize=1)
def _repo_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout.strip()


def _registry_row(tracker: Mapping[str, Any]) -> dict[str, Any]:
    proposal = tracker.get("proposal") or {}
    return {
        "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
        "proposal_id": proposal.get("proposal_id"),
        "proposal_hash": proposal.get("proposal_hash"),
        "tracker_id": tracker.get("tracker_id"),
        "strategy_id": proposal.get("strategy_id"),
        "strategy_version": proposal.get("strategy_version"),
        "strategy_family": proposal.get("strategy_family"),
        "symbol": proposal.get("symbol"),
        "timeframe": proposal.get("timeframe"),
        "state": tracker.get("state"),
        "rules_locked": tracker.get("rules_locked"),
        "paper_only": True,
        "live_order_effect": "none",
        "created_at": tracker.get("created_at"),
        "updated_at": tracker.get("updated_at"),
        "current_proposal_member": tracker.get("current_proposal_member", False),
        "current_cohort_member": tracker.get("current_cohort_member", False),
        "supersession_status": tracker.get("supersession_status") or "HISTORY_ONLY",
        "new_entry_allowed": tracker.get("new_entry_allowed", False),
        "exit_allowed": tracker.get("exit_allowed", False),
    }


def _state_row(tracker: Mapping[str, Any]) -> dict[str, Any]:
    proposal = tracker.get("proposal") or {}
    open_trade = tracker.get("open_trade") or {}
    return {
        "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
        "tracker_id": tracker.get("tracker_id"),
        "proposal_id": proposal.get("proposal_id"),
        "strategy_id": proposal.get("strategy_id"),
        "symbol": proposal.get("symbol"),
        "state": tracker.get("state"),
        "paper_trade_id": open_trade.get("paper_trade_id"),
        "open_paper_position": bool(open_trade),
        "cooldown_remaining_bars": tracker.get("cooldown_remaining_bars"),
        "last_processed_bar_ts": tracker.get("last_processed_bar_ts"),
        "updated_at": tracker.get("updated_at"),
        "current_proposal_member": tracker.get("current_proposal_member", False),
        "current_cohort_member": tracker.get("current_cohort_member", False),
        "supersession_status": tracker.get("supersession_status") or "HISTORY_ONLY",
        "new_entry_allowed": tracker.get("new_entry_allowed", False),
        "exit_allowed": tracker.get("exit_allowed", False),
    }


def _update_daily_buckets(
    buckets: dict[str, dict[str, Any]],
    trackers: Iterable[Mapping[str, Any]],
    signals: Iterable[Mapping[str, Any]],
    runs: Iterable[Mapping[str, Any]],
    now: datetime,
) -> None:
    for tracker in trackers:
        proposal = tracker.get("proposal") or {}
        proposal_id = str(proposal.get("proposal_id") or "")
        if not proposal_id:
            continue
        _daily_bucket(
            buckets,
            proposal_id=proposal_id,
            day=now.date().isoformat(),
            tracker_id=str(tracker.get("tracker_id") or f"paper:{proposal_id}"),
            strategy_id=str(proposal.get("strategy_id") or ""),
            strategy_candidate=str(proposal.get("strategy_family") or ""),
            symbol=str(proposal.get("symbol") or ""),
            updated_at=now.isoformat(),
        )["active"] = True

    for signal in signals:
        proposal_id = str(signal.get("proposal_id") or "")
        day = str(signal.get("decision_ts") or "")[:10]
        if not proposal_id or not day:
            continue
        bucket = _daily_bucket(
            buckets,
            proposal_id=proposal_id,
            day=day,
            tracker_id=str(signal.get("tracker_id") or f"paper:{proposal_id}"),
            strategy_id=str(signal.get("strategy_id") or ""),
            strategy_candidate=str(signal.get("strategy_candidate") or ""),
            symbol=str(signal.get("symbol") or ""),
            updated_at=str(signal.get("decision_ts") or now.isoformat()),
        )
        bucket["signal_count"] += 1
        if signal.get("observability") == "OBSERVABLE":
            bucket["observable_quote_count"] += 1
        if _as_bool(signal.get("triggered")) and _as_bool(
            signal.get("valid_for_promotion")
        ):
            bucket["entry_count"] += 1

    for run in runs:
        proposal_id = str(run.get("proposal_id") or "")
        day = str(run.get("closed_at") or run.get("exit_decision_ts") or "")[:10]
        if not proposal_id or not day:
            continue
        bucket = _daily_bucket(
            buckets,
            proposal_id=proposal_id,
            day=day,
            tracker_id=f"paper:{proposal_id}",
            strategy_id=str(run.get("strategy_id") or ""),
            strategy_candidate=str(run.get("strategy_candidate") or ""),
            symbol=str(run.get("symbol") or ""),
            updated_at=str(run.get("closed_at") or now.isoformat()),
        )
        bucket["closed_entries"] += 1
        pnl = _float(run.get("net_pnl_bps"))
        if pnl is not None:
            bucket["net_pnl_sum_bps"] += pnl
            bucket["net_pnl_count"] += 1
        source = str(run.get("cost_source") or "").strip()
        if source and source not in bucket["cost_sources"]:
            bucket["cost_sources"].append(source)


def _daily_bucket(
    buckets: dict[str, dict[str, Any]],
    *,
    proposal_id: str,
    day: str,
    tracker_id: str,
    strategy_id: str,
    strategy_candidate: str,
    symbol: str,
    updated_at: str,
) -> dict[str, Any]:
    key = f"{proposal_id}|{day}"
    bucket = buckets.setdefault(
        key,
        {
            "proposal_id": proposal_id,
            "paper_date": day,
            "paper_tracker_id": tracker_id,
            "strategy_id": strategy_id,
            "strategy_candidate": strategy_candidate,
            "symbol": symbol,
            "active": True,
            "signal_count": 0,
            "observable_quote_count": 0,
            "entry_count": 0,
            "closed_entries": 0,
            "net_pnl_sum_bps": 0.0,
            "net_pnl_count": 0,
            "cost_sources": [],
            "updated_at": updated_at,
        },
    )
    bucket["paper_tracker_id"] = tracker_id or bucket.get("paper_tracker_id")
    bucket["strategy_id"] = strategy_id or bucket.get("strategy_id")
    bucket["strategy_candidate"] = strategy_candidate or bucket.get(
        "strategy_candidate"
    )
    bucket["symbol"] = symbol or bucket.get("symbol")
    bucket["updated_at"] = max(str(bucket.get("updated_at") or ""), updated_at)
    return bucket


def _bounded_daily_buckets(
    buckets: Mapping[str, dict[str, Any]],
    limit: int,
) -> dict[str, dict[str, Any]]:
    ordered = sorted(
        buckets.items(),
        key=lambda item: (
            str(item[1].get("paper_date") or ""),
            str(item[1].get("proposal_id") or ""),
        ),
    )
    return dict(ordered[-max(int(limit), 1) :])


def _daily_rows(buckets: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for bucket in buckets.values():
        proposal_id = str(bucket.get("proposal_id") or "")
        if proposal_id:
            grouped[proposal_id].append(bucket)

    output: list[dict[str, Any]] = []
    for proposal_id, proposal_buckets in sorted(grouped.items()):
        paper_days = 0
        entry_day_count = 0
        cumulative_entries = 0
        closed_entries = 0
        paper_pnl_day_count = 0
        pnl_sum = 0.0
        pnl_count = 0
        signal_count = 0
        observable_count = 0
        cost_sources: set[str] = set()
        for bucket in sorted(
            proposal_buckets,
            key=lambda row: str(row.get("paper_date") or ""),
        ):
            paper_days += int(_as_bool(bucket.get("active")))
            daily_entries = _int(bucket.get("entry_count"))
            daily_closed = _int(bucket.get("closed_entries"))
            entry_day_count += int(daily_entries > 0)
            cumulative_entries += daily_entries
            closed_entries += daily_closed
            paper_pnl_day_count += int(daily_closed > 0)
            pnl_sum += _float(bucket.get("net_pnl_sum_bps")) or 0.0
            pnl_count += _int(bucket.get("net_pnl_count"))
            signal_count += _int(bucket.get("signal_count"))
            observable_count += _int(bucket.get("observable_quote_count"))
            cost_sources.update(
                str(value) for value in (bucket.get("cost_sources") or []) if str(value)
            )
            output.append(
                {
                    "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
                    "paper_date": bucket.get("paper_date"),
                    "proposal_id": proposal_id,
                    "paper_tracker_id": bucket.get("paper_tracker_id")
                    or f"paper:{proposal_id}",
                    "strategy_id": bucket.get("strategy_id"),
                    "strategy_candidate": bucket.get("strategy_candidate"),
                    "symbol": bucket.get("symbol"),
                    "paper_days": paper_days,
                    "heartbeat_day_count": paper_days,
                    "entry_day_count": entry_day_count,
                    "daily_would_enter_count": daily_entries,
                    "cumulative_would_enter_count": cumulative_entries,
                    "would_enter_count": cumulative_entries,
                    "closed_entries": closed_entries,
                    "daily_paper_pnl_observed_count": daily_closed,
                    "cumulative_paper_pnl_observed_count": pnl_count,
                    "paper_pnl_observed_count": pnl_count,
                    "paper_pnl_day_count": paper_pnl_day_count,
                    "avg_paper_pnl_bps": pnl_sum / pnl_count if pnl_count else None,
                    "arrival_mid_coverage": (
                        observable_count / signal_count if signal_count else 0.0
                    ),
                    "spread_observation_coverage": (
                        observable_count / signal_count if signal_count else 0.0
                    ),
                    "cost_source_mix": json.dumps(sorted(cost_sources)),
                    "created_at": bucket.get("updated_at"),
                }
            )
    return output


def _quote_coverage_rows(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in signals:
        grouped[str(row.get("proposal_id") or "")].append(row)
    return [
        {
            "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
            "proposal_id": proposal_id,
            "strategy_id": rows[-1].get("strategy_id"),
            "symbol": rows[-1].get("symbol"),
            "candidate_signal_count": len(rows),
            "observable_quote_count": sum(
                row.get("observability") == "OBSERVABLE" for row in rows
            ),
            "arrival_mid_coverage": _coverage(rows, proposal_id),
            "stale_quote_rate": _rate(rows, "observability", "STALE"),
            "quote_fallback_rate": _rate(rows, "fallback_level", "NOT_OBSERVABLE"),
            "target_coverage": 0.95,
        }
        for proposal_id, rows in sorted(grouped.items())
    ]


def _cost_evidence_rows(
    runs: list[dict[str, Any]], trackers: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in runs:
        grouped[str(row.get("proposal_id") or "")].append(row)
    output = []
    for tracker in trackers:
        proposal = tracker.get("proposal") or {}
        proposal_id = str(proposal.get("proposal_id") or "")
        rows = grouped.get(proposal_id, [])
        output.append(
            {
                "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
                "proposal_id": proposal_id,
                "strategy_id": proposal.get("strategy_id"),
                "symbol": proposal.get("symbol"),
                "required_cost_trust_level": proposal.get("required_cost_trust_level"),
                "closed_trade_count": len(rows),
                "cost_observed_count": sum(
                    row.get("total_cost_bps") is not None for row in rows
                ),
                "cost_source": (
                    "configured_conservative_paper" if rows else "not_observed"
                ),
                "cost_trust_level": "PAPER_ONLY" if rows else "BLOCK",
                "valid_for_live_coverage": False,
            }
        )
    return output


def _exit_quality_rows(runs: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in runs:
        grouped[str(row.get("proposal_id") or "")].append(row)
    output: list[dict[str, Any]] = []
    for proposal_id, rows in sorted(grouped.items()):
        latest = rows[-1]
        net_values = _numeric_values(rows, "net_pnl_bps")
        mfe_values = _numeric_values(rows, "mfe_bps")
        mae_values = _numeric_values(rows, "mae_bps")
        giveback_values = _numeric_values(rows, "profit_giveback_bps")
        efficiency_values = _numeric_values(rows, "exit_efficiency")
        holding_values = _numeric_values(rows, "exit_timing_bars")
        reason_mix = Counter(str(row.get("exit_reason") or "unknown") for row in rows)
        timing_mix = Counter(
            str(row.get("exit_timing_state") or "unknown") for row in rows
        )
        high_giveback_count = sum(
            (_float(row.get("mfe_bps")) or 0.0) > 0
            and (_float(row.get("profit_giveback_bps")) or 0.0)
            >= (_float(row.get("mfe_bps")) or 0.0) * 0.5
            for row in rows
        )
        output.append(
            {
                "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
                "proposal_id": proposal_id,
                "strategy_id": latest.get("strategy_id"),
                "strategy_version": latest.get("strategy_version"),
                "symbol": latest.get("symbol"),
                "closed_trade_count": len(rows),
                "avg_net_pnl_bps": _average(net_values),
                "avg_mfe_bps": _average(mfe_values),
                "avg_mae_bps": _average(mae_values),
                "avg_profit_giveback_bps": _average(giveback_values),
                "avg_exit_efficiency": _average(efficiency_values),
                "avg_holding_bars": _average(holding_values),
                "high_profit_giveback_count": high_giveback_count,
                "exit_reason_mix": json.dumps(reason_mix, sort_keys=True),
                "exit_timing_state_mix": json.dumps(timing_mix, sort_keys=True),
                "diagnosis": (
                    "high_profit_giveback"
                    if high_giveback_count > len(rows) / 2
                    else "observe_more_closed_paper_trades"
                    if len(rows) < 20
                    else "no_dominant_exit_defect"
                ),
                "valid_for_live_orders": False,
                "live_order_effect": "none",
            }
        )
    return output


def _numeric_values(
    rows: Iterable[Mapping[str, Any]],
    field: str,
) -> list[float]:
    output: list[float] = []
    for row in rows:
        value = _float(row.get(field))
        if value is not None and math.isfinite(value):
            output.append(value)
    return output


def _average(values: Iterable[float]) -> float | None:
    materialized = list(values)
    return sum(materialized) / len(materialized) if materialized else None


def _canonical_ack_rows(
    trackers: Iterable[Mapping[str, Any]],
    current_ack_rows: Iterable[Mapping[str, Any]],
    now: datetime,
) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for tracker in trackers:
        try:
            proposal = PaperStrategyProposal.model_validate(tracker.get("proposal"))
        except (TypeError, ValidationError):
            continue
        row = _accepted_ack_row(proposal, tracker, now)
        rows[(proposal.proposal_id, proposal.proposal_hash)] = row
    for raw in current_ack_rows:
        row = dict(raw)
        if _as_bool(row.get("accepted")):
            continue
        key = (
            str(row.get("proposal_id") or ""),
            str(row.get("proposal_hash") or ""),
        )
        if key not in rows:
            rows[key] = row
    return list(rows.values())


def _contract_status(
    *,
    cfg: AppConfig,
    runtime_cfg: Any,
    now: datetime,
    ack_rows: Iterable[Mapping[str, Any]],
    trackers: Iterable[Mapping[str, Any]],
    state_loaded: bool,
    state_persisted: bool,
    failure_stage: str = "",
) -> dict[str, Any]:
    ack_list = list(ack_rows)
    tracker_list = list(trackers)
    current_trackers = [
        row for row in tracker_list if _as_bool(row.get("current_proposal_member"))
    ]
    superseded_exit_only = [
        row
        for row in tracker_list
        if str(row.get("supersession_status") or "") == "SUPERSEDED_EXIT_ONLY"
    ]
    superseded_closed = [
        row
        for row in tracker_list
        if str(row.get("supersession_status") or "") == "SUPERSEDED_CLOSED"
    ]
    current_tracker_ids = {
        str(
            row.get("proposal_id")
            or (row.get("proposal") or {}).get("proposal_id")
            or ""
        )
        for row in current_trackers
    }
    accepted_current_ids = {
        str(row.get("proposal_id") or "")
        for row in ack_list
        if _as_bool(row.get("accepted"))
    }
    return {
        "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
        "contract_version": PAPER_STRATEGY_CONTRACT_VERSION,
        "paper_runtime_enabled": bool(runtime_cfg.enabled),
        "paper_runtime_live_order_effect": runtime_cfg.live_order_effect,
        "quant_lab_mode": cfg.quant_lab.mode,
        "canary_enabled": bool(cfg.quant_lab.canary.enabled),
        "accepted_proposal_count": sum(
            _as_bool(row.get("accepted")) for row in ack_list
        ),
        "rejected_proposal_count": sum(
            not _as_bool(row.get("accepted")) for row in ack_list
        ),
        "loaded_tracker_count": len(tracker_list),
        "current_active_tracker_count": len(current_trackers),
        "current_pending_tracker_count": len(
            {
                item
                for item in accepted_current_ids
                if item and item not in current_tracker_ids
            }
        ),
        "superseded_exit_only_count": len(superseded_exit_only),
        "superseded_closed_count": len(superseded_closed),
        "active_tracker_count": len(tracker_list),
        "active_tracker_count_deprecated": True,
        "active_tracker_count_semantics": "deprecated_alias_of_loaded_tracker_count",
        "open_paper_position_count": sum(
            bool(row.get("open_trade")) for row in tracker_list
        ),
        "state_loaded": state_loaded,
        "state_persisted": state_persisted,
        "fail_closed": not state_loaded or not state_persisted,
        "failure_stage": failure_stage,
        "real_order_calls": 0,
        "real_position_mutations": 0,
        "generated_at": now.isoformat(),
    }


def _write_failure_reports(
    summaries: Path,
    *,
    error_rows: list[dict[str, Any]],
    contract_status: dict[str, Any],
    history_limit: int,
) -> None:
    try:
        _write_csv_atomic(
            summaries / "paper_strategy_errors.csv",
            _merge_csv_rows(
                summaries / "paper_strategy_errors.csv",
                error_rows,
                ("ts_utc", "proposal_id", "error_code"),
            )[-history_limit:],
        )
        _write_json_atomic(
            summaries / "quant_lab_contract_status.json",
            contract_status,
        )
    except Exception:
        return


def _failure_result(
    *,
    runtime_cfg: Any,
    store: PaperRuntimeStore,
    errors: list[dict[str, Any]],
    failure_stage: str,
    tracker_count: int = 0,
) -> dict[str, Any]:
    return {
        "enabled": bool(runtime_cfg.enabled),
        "proposal_rows": 0,
        "accepted": 0,
        "rejected": 0,
        "trackers": tracker_count,
        "signals": 0,
        "closed_trades": 0,
        "errors": len(errors),
        "state_path": str(store.path),
        "fail_closed": True,
        "failure_stage": failure_stage,
        "state_persisted": False,
        "live_order_effect": "none",
    }


def _write_runtime_reports(
    summaries: Path,
    *,
    ack_rows: list[dict[str, Any]],
    ack_history_rows: list[dict[str, Any]],
    registry_rows: list[dict[str, Any]],
    registry_history_rows: list[dict[str, Any]],
    state_rows: list[dict[str, Any]],
    state_history_rows: list[dict[str, Any]],
    signal_rows: list[dict[str, Any]],
    run_rows: list[dict[str, Any]],
    daily_rows: list[dict[str, Any]],
    quote_coverage_rows: list[dict[str, Any]],
    cost_evidence_rows: list[dict[str, Any]],
    exit_quality_rows: list[dict[str, Any]],
    error_rows: list[dict[str, Any]],
    recovery_rows: list[dict[str, Any]],
    contract_status: dict[str, Any],
    history_limit: int,
) -> None:
    _write_csv_atomic(
        summaries / "paper_strategy_proposal_ack.csv",
        ack_rows,
        preferred_fields=ACK_FIELDS,
    )
    _write_csv_atomic(
        summaries / "paper_strategy_proposal_ack_current.csv",
        ack_rows,
        preferred_fields=ACK_FIELDS,
    )
    _write_csv_atomic(
        summaries / "paper_strategy_proposal_ack_history.csv",
        ack_history_rows,
        preferred_fields=ACK_FIELDS,
    )
    _write_csv_atomic(summaries / "paper_strategy_registry.csv", registry_rows)
    _write_csv_atomic(
        summaries / "paper_strategy_registry_current.csv", registry_rows
    )
    _write_csv_atomic(
        summaries / "paper_strategy_registry_history.csv", registry_history_rows
    )
    _write_csv_atomic(
        summaries / "paper_strategy_trackers_current.csv", registry_rows
    )
    _write_csv_atomic(summaries / "paper_strategy_state.csv", state_rows)
    _write_csv_atomic(
        summaries / "paper_strategy_state_history.csv", state_history_rows
    )
    _write_csv_atomic(summaries / "paper_strategy_signals.csv", signal_rows)
    _write_csv_atomic(
        summaries / "paper_strategy_runs.csv",
        run_rows,
    )
    _write_csv_atomic(
        summaries / "paper_strategy_daily.csv",
        daily_rows,
    )
    _write_csv_atomic(
        summaries / "paper_strategy_quote_coverage.csv", quote_coverage_rows
    )
    _write_csv_atomic(
        summaries / "paper_strategy_cost_evidence.csv", cost_evidence_rows
    )
    _write_csv_atomic(
        summaries / "paper_strategy_exit_quality.csv", exit_quality_rows
    )
    _write_csv_atomic(
        summaries / "paper_strategy_errors.csv",
        _merge_csv_rows(
            summaries / "paper_strategy_errors.csv",
            error_rows,
            ("ts_utc", "proposal_id", "error_code"),
        )[-history_limit:],
    )
    _write_csv_atomic(
        summaries / "paper_strategy_restart_recovery.csv",
        _merge_csv_rows(
            summaries / "paper_strategy_restart_recovery.csv",
            recovery_rows,
            ("recovered_at", "tracker_id", "state"),
        )[-history_limit:],
    )
    _write_json_atomic(summaries / "quant_lab_contract_status.json", contract_status)


def _merge_csv_rows(
    path: Path,
    incoming: list[dict[str, Any]],
    keys: tuple[str, ...],
) -> list[dict[str, Any]]:
    rows: dict[tuple[str, ...], dict[str, Any]] = {}
    if path.is_file():
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    key = tuple(str(row.get(field) or "") for field in keys)
                    rows[key] = dict(row)
        except (OSError, csv.Error):
            pass
    for row in incoming:
        key = tuple(str(row.get(field) or "") for field in keys)
        rows[key] = dict(row)
    return list(rows.values())


def _write_csv_atomic(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    preferred_fields: Iterable[str] = (),
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(preferred_fields)
    for row in rows:
        for field in row:
            if field not in fields:
                fields.append(field)
    if not fields:
        fields = ["schema_version"]
    handle, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({field: _csv_value(row.get(field)) for field in fields})
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, ensure_ascii=True, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def _reports_dir(run_path: Path) -> Path:
    for parent in [run_path, *run_path.parents]:
        if parent.name == "runs" and parent.parent.name == "reports":
            return parent.parent
    return PROJECT_ROOT / "reports"


def _state_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _quote_observability(quote: Mapping[str, Any], max_age_seconds: float) -> str:
    bid = _float(quote.get("bid"))
    ask = _float(quote.get("ask"))
    mid = _float(quote.get("mid"))
    age = _float(quote.get("quote_age_seconds"))
    if (
        bid is None
        or ask is None
        or mid is None
        or bid <= 0.0
        or ask <= 0.0
        or mid <= 0.0
        or ask < bid
        or not quote.get("quote_timestamp")
        or age is None
    ):
        return "NOT_OBSERVABLE"
    if age > max_age_seconds:
        return "STALE"
    return "OBSERVABLE"


def _entry_observability(
    proposal: PaperStrategyProposal,
    context: Mapping[str, Any],
    quote: Mapping[str, Any],
    max_age_seconds: float,
) -> str:
    quote_status = _quote_observability(quote, max_age_seconds)
    if quote_status != "OBSERVABLE":
        return quote_status
    missing = [
        field for field in proposal.required_market_fields if context.get(field) is None
    ]
    if missing:
        return "MISSING_MARKET_FIELD"
    return "OBSERVABLE"


def _lookup_symbol(values: Mapping[str, Any], symbol: str) -> Any:
    variants = {
        symbol,
        symbol.upper(),
        symbol.replace("/", "-"),
        symbol.replace("/", "-").upper(),
        symbol.replace("-", "/"),
        symbol.replace("-", "/").upper(),
    }
    for variant in variants:
        if variant in values:
            return values[variant]
    return None


def _return(values: list[float], bars: int) -> float | None:
    if len(values) <= bars or not values[-bars - 1]:
        return None
    return float(values[-1]) / float(values[-bars - 1]) - 1.0


def _volatility(values: list[float], bars: int) -> float | None:
    if len(values) <= bars:
        return None
    returns = [
        _return(values[: index + 1], 1)
        for index in range(len(values) - bars, len(values))
    ]
    clean = [value for value in returns if value is not None]
    return statistics.pstdev(clean) if len(clean) >= 2 else None


def _zscore(values: list[float], bars: int) -> float | None:
    clean = [float(value) for value in values[-bars:] if _float(value) is not None]
    if len(clean) < 2:
        return None
    deviation = statistics.pstdev(clean)
    return (clean[-1] - statistics.fmean(clean)) / deviation if deviation > 0 else 0.0


def _audit_permission_allows(audit: Any) -> bool:
    quant_lab = getattr(audit, "quant_lab", None)
    if not isinstance(quant_lab, Mapping):
        return False
    direct = quant_lab.get("permission_status")
    if direct is not None:
        return str(direct).upper() == "ACTIVE_ALLOW"
    permission = quant_lab.get("permission") or quant_lab.get("risk_permission") or {}
    value = (
        permission.get("permission_status")
        if isinstance(permission, Mapping)
        else permission
    )
    return str(value or "").upper() == "ACTIVE_ALLOW"


def _audit_cost_canary_ready(audit: Any) -> bool:
    quant_lab = getattr(audit, "quant_lab", None)
    if not isinstance(quant_lab, Mapping):
        return False
    value = (
        quant_lab.get("cost_trust_level")
        or quant_lab.get("strategy_cost_trust_level")
        or ""
    )
    return str(value).upper() in {"CANARY", "SCALE_READY"}


def _bounded_context(context: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in context.items()
        if key not in {"real_funds_sufficient"} and _json_scalar(value)
    }


def _json_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _coverage(rows: list[dict[str, Any]], proposal_id: str) -> float:
    selected = [row for row in rows if str(row.get("proposal_id") or "") == proposal_id]
    return (
        sum(_float(row.get("arrival_mid")) is not None for row in selected)
        / len(selected)
        if selected
        else 0.0
    )


def _rate(rows: list[dict[str, Any]], field: str, value: str) -> float:
    return (
        sum(str(row.get(field) or "") == value for row in rows) / len(rows)
        if rows
        else 0.0
    )


def _error_row(
    now: datetime,
    code: str,
    exc: Exception,
    *,
    proposal_id: str = "",
) -> dict[str, Any]:
    return {
        "schema_version": PAPER_RUNTIME_SCHEMA_VERSION,
        "ts_utc": now.isoformat(),
        "proposal_id": proposal_id,
        "error_code": code,
        "error_type": type(exc).__name__,
        "error_message": str(exc)[:500],
        "live_order_effect": "none",
    }


def _json_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    text = str(value or "").strip()
    if not text:
        return {} if text == "" else text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(
            value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
    return "" if value is None else value


def _slash_symbol(value: Any) -> str:
    text = str(value or "").strip().upper().replace("_", "/").replace("-", "/")
    parts = [part for part in text.split("/") if part]
    return f"{parts[0]}/{parts[1]}" if len(parts) >= 2 else text


def _timestamp_text(value: Any) -> str:
    parsed = _datetime(value)
    return parsed.isoformat() if parsed is not None else str(value or "")


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        if isinstance(value, (int, float)) or str(value).isdigit():
            number = float(value)
            if number > 10_000_000_000:
                number /= 1000.0
            return datetime.fromtimestamp(number, tz=UTC)
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except (ValueError, TypeError, OSError):
        return None


def _last(values: Iterable[Any]) -> Any:
    materialized = list(values or [])
    return materialized[-1] if materialized else None


def _float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
