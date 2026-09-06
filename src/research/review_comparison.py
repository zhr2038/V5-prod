"""Causal, version-isolated same-capital comparison. No real order executor.

A is the repaired hourly V5 *decision pipeline* under the common fill model.
This is deliberately not a reconstruction of the live event executor or account.
"""
from __future__ import annotations

import copy
import hashlib
import json
import statistics
from dataclasses import asdict
from pathlib import Path

import numpy as np

from src.core.models import MarketSeries
from src.reporting.participation_runtime import build_snapshot
from src.research.original_v5_adapter import OriginalV5Adapter
from src.research.participation_comparison import ParticipationComparison
from src.research.quote_portfolio import QuotePortfolio, serializable
from src.research.reference_contract import reference_use, validate_reference_contract
from src.research.review_acceptance import evaluate_acceptance
from src.research.review_integrity import observe_integrity, integrity_report


COHORTS = ("A_original_v5", "B_participation_v1", "C_hold24_only", "D_reference_only")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def validate_signal_data(market_data, symbols, bar):
    if set(market_data) != set(symbols):
        raise ValueError("incomplete shared input universe")
    for symbol, value in market_data.items():
        series = MarketSeries(**value)
        if series.symbol != symbol or series.timeframe != "1h":
            raise ValueError("market symbol or timeframe binding mismatch")
        columns = [getattr(series, key) for key in ("ts", "open", "high", "low", "close", "volume")]
        if len(series.ts) < 60 or len({len(c) for c in columns}) != 1:
            raise ValueError("incomplete market warmup")
        if any(b - a != 3600000 for a, b in zip(series.ts, series.ts[1:])):
            raise ValueError("non_contiguous_closed_candles")
        if series.ts[-1] / 1000 + 3600 != bar:
            raise ValueError("last candle must close at decision hour")
        if any(not np.isfinite(x) for c in columns for x in c):
            raise ValueError("invalid market values")


def validate_frame(frame):
    now, bar = frame["observed_at"], frame["bar_ts"]
    if isinstance(now, bool) or not np.isfinite(now) or bar % 3600 or not 0 <= now - bar < 3600:
        raise ValueError("invalid observable decision time")
    if frame.get("historical_backfill") is not False or frame.get("live_order_effect") != "none":
        raise ValueError("forward input boundary required")
    signal = frame.get("signal_data", {"status": "valid"})
    if signal.get("status") == "unavailable":
        if frame["market_data"] or not signal.get("reason"):
            raise ValueError("unavailable signals require empty data and an explicit reason")
    elif signal.get("status") == "valid":
        validate_signal_data(frame["market_data"], frame["symbols"], bar)
    else:
        raise ValueError("unknown signal availability")
    checker = QuotePortfolio(initial_cash=100, fee_bps=10, slippage_bps=5, maximum_quote_age_seconds=30)
    if not frame["symbols"]:
        raise ValueError("empty quote universe")
    for symbol, row in frame["symbols"].items():
        checker._market(row, now)
        if row["instrument"].get("symbol") != symbol:
            raise ValueError("instrument symbol binding mismatch")


class Comparison:
    def __init__(self, cfg, policy, experiment, *, root: Path, checkpoint=None):
        self.root, self.cfg, self.experiment = root, cfg, experiment
        if experiment.get("schema_version") == "v5.review_comparison.v2":
            validate_reference_contract(experiment.get("reference_contract"))
        self.policy = copy.deepcopy(policy)
        self.policy["initial_cash_usdt"] = experiment["initial_cash_usdt"]
        self.scenarios = {}
        self.last_observed = None
        self.last_bar = None
        self.last_decision_bar = None
        clock = experiment.get("decision_clock", {"offset_seconds": 0, "maximum_lateness_seconds": 3599})
        self.decision_offset, self.maximum_lateness = clock["offset_seconds"], clock["maximum_lateness_seconds"]
        if not 0 <= self.decision_offset < 3600 or not 0 <= self.maximum_lateness < 3600 - self.decision_offset:
            raise ValueError("invalid frozen common decision clock")
        self.metrics = {}
        for cost in experiment["roundtrip_cost_scenarios_bps"]:
            config = {**self.policy, "fee_bps": 10, "slippage_bps": cost / 2 - 10}
            if config["slippage_bps"] < 0:
                raise ValueError("cost scenario cannot be below explicit fee assumption")
            baseline_cfg = cfg.model_copy(deep=True)
            baseline_cfg.execution.fee_bps = config["fee_bps"]
            baseline_cfg.execution.slippage_bps = config["slippage_bps"]
            baseline_cfg.execution.cost_aware_roundtrip_cost_bps = cost
            scenario = {"adapter": OriginalV5Adapter(baseline_cfg, sandbox=root / str(cost) / "original"),
                        "A_original_v5": QuotePortfolio(initial_cash=experiment["initial_cash_usdt"], fee_bps=10,
                                                        slippage_bps=config["slippage_bps"], maximum_quote_age_seconds=30),
                        "B_participation_v1": ParticipationComparison(config),
                        "C_hold24_only": ParticipationComparison({**config, "maximum_holding_hours": 24}),
                        "D_reference_only": ParticipationComparison({**config, "maximum_holding_hours": 24}, use_reference=True,
                                                                    reference_contract=experiment.get("reference_contract")),
                        "pending": [], "audit": None}
            self.scenarios[str(cost)] = scenario
        if checkpoint:
            self.metrics = checkpoint.get("metrics", {})
            self.last_observed, self.last_bar = checkpoint["last_observed"], checkpoint["last_bar"]
            self.last_decision_bar = checkpoint.get("last_decision_bar")
            for cost, saved in checkpoint["scenarios"].items():
                scenario = self.scenarios[cost]
                scenario["pending"], scenario["audit"] = saved["pending"], saved["audit"]
                scenario["A_original_v5"] = QuotePortfolio.restore(saved["A_original_v5"])
                for name in COHORTS[1:]:
                    item = scenario[name]
                    item.book = QuotePortfolio.restore(saved[name]["book"])
                    item.state = saved[name]["state"]
                    item.last_observed, item.last_bar = self.last_observed, self.last_decision_bar

    def checkpoint(self):
        scenarios = {}
        for cost, scenario in self.scenarios.items():
            saved = {"pending": scenario["pending"], "audit": scenario["audit"],
                     "A_original_v5": scenario["A_original_v5"].checkpoint()}
            for name in COHORTS[1:]:
                saved[name] = {"book": scenario[name].book.checkpoint(), "state": scenario[name].state}
            scenarios[cost] = saved
        return serializable({"last_observed": self.last_observed, "last_bar": self.last_bar,
                             "last_decision_bar": self.last_decision_bar, "scenarios": scenarios, "metrics": self.metrics})

    def observe(self, frame):
        validate_frame(frame)
        if set(frame["symbols"]) != set(self.policy["symbols"]):
            raise ValueError("incomplete shared quote universe")
        previous_marks = {k: v.get("last_mark") for k, v in self.metrics.items() if ":" in k and "last_mark" in v}
        now, bar = frame["observed_at"], frame["bar_ts"]
        if self.last_observed is not None and (now <= self.last_observed or bar < self.last_bar):
            raise ValueError("duplicate_or_reversed_comparison_observation")
        cutoff = bar + self.decision_offset
        deadline = cutoff + self.maximum_lateness
        signals_valid = frame.get("signal_data", {"status": "valid"})["status"] == "valid"
        hourly = signals_valid and self.last_decision_bar != bar and cutoff <= now <= deadline
        phase = "DECIDED" if hourly else "ALREADY_DECIDED" if self.last_decision_bar == bar else "WAITING_COMMON_CUTOFF" if now < cutoff else "MISSED_COMMON_DEADLINE" if now > deadline else "SIGNAL_DATA_UNAVAILABLE"
        gap = self.last_observed is not None and now - self.last_observed > 180
        market = {symbol: MarketSeries(**value) for symbol, value in frame["market_data"].items()}
        rows = copy.deepcopy(frame["symbols"])
        raw_snapshot = {"now_ts": now, "bar_ts": bar, "symbols": rows}
        for symbol, value in market.items():
            rows[symbol]["close"] = value.close[-1]
        result = {"observed_at": now, "bar_ts": bar, "hourly_decision": hourly, "observation_gap": gap,
                  "signal_data": copy.deepcopy(frame.get("signal_data", {"status": "valid"})),
                  "exit_evaluation": {"A": "hourly_pipeline_only" if signals_valid else "pending_quote_execution_only_hourly_signals_unavailable",
                                      "BCD": "declared_rules" if signals_valid else "quote_trial_hard_time_and_last_observed_regime; current_hour_ema_unavailable"},
                  "decision_clock": {"status": phase, "cutoff_ts": cutoff, "deadline_ts": deadline,
                                     "actual_decision_ts": now if hourly else None, "scope": "all_cohorts_shared_cutoff"},
                  "input_hash": digest(frame), "scenarios": {}, "live_order_effect": "none"}
        counters = self.metrics.setdefault("observations", {"count": 0, "gaps": 0})
        counters["count"] += 1
        counters["gaps"] += int(gap)
        for cost, scenario in self.scenarios.items():
            book = scenario["A_original_v5"]
            executions = []
            for intent in scenario["pending"]:
                try:
                    if intent["side"] == "buy" and gap:
                        raise ValueError("missing_forward_observation")
                    row = rows[intent["symbol"]]
                    bid, ask = row["quote"]["bid"], row["quote"]["ask"]
                    if intent["side"] == "buy" and ((ask / bid - 1) * 10000 > self.policy["maximum_spread_bps"]
                            or ask / intent["signal_price"] - 1 > self.policy["maximum_signal_premium_fraction"]):
                        raise ValueError("entry_spread_or_signal_premium")
                    executions.append(serializable(book.fill(intent, row, now=now)))
                except ValueError as exc:
                    executions.append({"intent_id": intent["intent_id"], "action": "rejected", "reason": str(exc)})
            scenario["pending"] = []
            if hourly:
                orders, audit = scenario["adapter"].decide(snapshot=raw_snapshot, market_data=market, portfolio=book)
                scenario["audit"] = audit
                for index, order in enumerate(orders):
                    value = asdict(order)
                    scenario["pending"].append({**value, "intent_id": digest([cost, now, index, value]),
                                                "decision_ts": now, "metadata": order.meta})
            audit = scenario["audit"] or {"regime": "Unknown"}
            if not signals_valid:
                # Keep the existing regime observation, never fabricate fresh indicators.
                # The unchanged participation kernel can still mark, execute pending
                # sells, and evaluate trial, time and quoted hard stops.
                snapshot = {**copy.deepcopy(raw_snapshot), "regime": audit.get("regime", "Unknown"),
                            "operational_block": "signal_data_unavailable"}
            elif scenario["audit"] is None:
                # A fresh flat account can observe quotes before its first fixed
                # decision. No factor forecast exists yet and none is fabricated.
                if hourly or book.positions or any(scenario[n].book.positions for n in COHORTS[1:]):
                    raise ValueError("factor_audit_required_for_decision_or_existing_position")
                snapshot = {**copy.deepcopy(raw_snapshot), "regime": "Unknown",
                            "operational_block": "awaiting_first_common_factor_decision"}
            else:
                # Candidate ranking never inherits A's independent account vetoes.
                shared_audit = {**audit, "window_end_ts": bar, "router_decisions": [], "quant_lab": {}}
                snapshot = build_snapshot(market_data=market, top_of_book={s: r["quote"] for s, r in rows.items()},
                                          audit=shared_audit, config=self.policy, now=now)
                if snapshot["data_errors"]:
                    raise ValueError("shared factor input incomplete: " + json.dumps(snapshot["data_errors"], sort_keys=True))
            for symbol, row in snapshot["symbols"].items():
                row["instrument"] = rows[symbol]["instrument"]
                row["entry_minimum_notional_usdt"] = rows[symbol].get("entry_minimum_notional_usdt", self.experiment.get("entry_minimum_notional_usdt", 10))
                row["cost_bps"] = float(cost)
            entries = {"A_original_v5": {"portfolio": serializable(book.mark(rows, now=now)),
                                          "decision": copy.deepcopy(scenario["pending"]), "execution": executions,
                                          "audit_hash": digest(audit)}}
            for name in COHORTS[1:]:
                item = scenario[name]
                if gap and item.state["pending"] and item.state["pending"]["action"] == "entry_intent":
                    item.state["pending"] = None
                entries[name] = item.observe(snapshot, references=frame.get("references", {}), allow_new_signal=hourly)
                item.events.clear()  # The immutable event ledger owns history, not process memory.
            funnel = self.metrics.setdefault("reference_funnel:" + cost, {
                "candidates": 0, "valid_reference_coverage": 0, "matched_candidates": 0,
                "defer_eligible": 0, "decisions_changed": 0, "statuses": {}, "late_arrivals": 0,
                "independent_changed_opportunities": 0, "last_changed_until": {}, "uncovered_opportunities": {}})
            late_arrivals = []
            for opportunity_id, opportunity in list(funnel["uncovered_opportunities"].items()):
                ref = frame.get("references", {}).get(opportunity["symbol"])
                use = reference_use(ref, now, self.experiment.get("reference_contract"))
                if use["valid"] and opportunity["decision_ts"] < ref["first_received_ts"] < opportunity["bar_ts"] + 3600:
                    late_arrivals.append({**opportunity, "opportunity_id": opportunity_id, "advice_id": ref["advice_id"],
                                          "first_received_ts": ref["first_received_ts"], "observed_at": now, "decision_unchanged": True})
                    funnel["late_arrivals"] += 1
                    del funnel["uncovered_opportunities"][opportunity_id]
                elif now >= opportunity["bar_ts"] + 3600:
                    del funnel["uncovered_opportunities"][opportunity_id]
            reference_decisions = []
            control = entries["C_hold24_only"].get("decision") or {}
            treatment = entries["D_reference_only"].get("decision") or {}
            control_symbol = control.get("symbol") if control.get("action") == "entry_intent" else None
            treatment_symbol = treatment.get("symbol") if treatment.get("action") in {"entry_intent", "reference_deferred_entry"} else None
            # Count the union of real C/D candidates once per symbol/hour. When
            # account histories diverge, D-only changes must not disappear.
            for symbol in sorted({s for s in (control_symbol, treatment_symbol) if s}) if hourly else []:
                use = reference_use(frame.get("references", {}).get(symbol), now, self.experiment.get("reference_contract"))
                matched = control_symbol == treatment_symbol == symbol
                changed = treatment_symbol == symbol and treatment.get("action") == "reference_deferred_entry"
                reference_decision = {**use, "opportunity_id": digest([cost, bar, symbol]), "symbol": symbol,
                                      "matched_candidate": matched, "decision_changed": changed,
                                      "control_candidate": control_symbol == symbol, "treatment_candidate": treatment_symbol == symbol,
                                      "decision_quote_hash": digest(rows[symbol]["quote"]),
                                      "control_snapshot_hash": entries["C_hold24_only"]["snapshot_hash"],
                                      "treatment_snapshot_hash": entries["D_reference_only"]["snapshot_hash"]}
                for key, count in {"candidates": 1, "valid_reference_coverage": int(use["valid"]),
                                   "matched_candidates": int(matched), "defer_eligible": int(use["defer"] and treatment_symbol == symbol),
                                   "decisions_changed": int(changed)}.items():
                    funnel[key] += count
                funnel["statuses"][use["status"]] = funnel["statuses"].get(use["status"], 0) + 1
                if changed and matched and now >= funnel["last_changed_until"].get(symbol, 0):
                    funnel["independent_changed_opportunities"] += 1
                    funnel["last_changed_until"][symbol] = now + 86400
                if not use["valid"]:
                    funnel["uncovered_opportunities"][reference_decision["opportunity_id"]] = {"symbol": symbol, "decision_ts": now, "bar_ts": bar}
                reference_decisions.append(reference_decision)
            result["scenarios"][cost] = {"regime": audit.get("regime"), "cohorts": entries,
                                         "reference_decisions": reference_decisions, "late_reference_arrivals": late_arrivals}
            for name, entry in entries.items():
                mark = entry["portfolio"]
                key = cost + ":" + name
                total = self.metrics.setdefault(key, {"maximum_drawdown_fraction": 0., "duration": 0., "exposed_seconds": 0., "utilized_seconds": 0., "last_mark": None})
                total["maximum_drawdown_fraction"] = max(total["maximum_drawdown_fraction"], float(mark["drawdown_fraction"]))
                if total["last_mark"] is not None:
                    seconds = now - self.last_observed
                    prior = total["last_mark"]
                    total["duration"] += seconds
                    total["exposed_seconds"] += seconds * (float(prior["gross_exposure_usdt"]) > 0)
                    total["utilized_seconds"] += seconds * float(prior["gross_exposure_usdt"]) / max(float(prior["equity_usdt"]), 1e-9)
                total["last_mark"] = mark
        observe_integrity(self.metrics, result, self.last_observed, self.experiment, previous_marks, self.last_decision_bar)
        self.last_observed, self.last_bar = now, bar
        if hourly:
            self.last_decision_bar = bar
        return result


def summarize(events, checkpoint, experiment, *, legacy_observation_integrity=None):
    """Account statistics and paired daily block bootstrap; never price-label trade counts."""
    result = {"experiment_id": experiment["experiment_id"], "status": "INSUFFICIENT_FORWARD_EVIDENCE",
              "live_execution_eligible": False, "automatic_live_scaling": False,
              "fixed_monthly_cloud_api_cost_usdt": experiment["fixed_monthly_cloud_api_cost_usdt"],
              "project_profit_verified": False, "scenarios": {}, "comparisons": {},
              "baseline_scope": "fixed_hourly_V5_decisions_with_shared_quote_execution_not_full_live_executor",
              "fee_basis": "explicit_10bps_per_side_research_assumption_not_calibrated_fills"}
    result["reference_contract"] = experiment.get("reference_contract")
    result["decision_clock"] = experiment.get("decision_clock")
    result["observation_integrity"] = integrity_report(checkpoint.get("metrics", {}))
    result["legacy_observation_integrity"] = copy.deepcopy(legacy_observation_integrity)
    result["reference_funnel"] = {}
    for cost in experiment["roundtrip_cost_scenarios_bps"]:
        funnel = copy.deepcopy(checkpoint.get("metrics", {}).get("reference_funnel:" + str(cost), {}))
        funnel.pop("uncovered_opportunities", None)
        funnel.pop("last_changed_until", None)
        funnel["coverage_rate"] = funnel.get("valid_reference_coverage", 0) / funnel["candidates"] if funnel.get("candidates") else None
        funnel["interpretation"] = "NO_EFFECTIVE_REFERENCE_COMPARISON" if not funnel.get("decisions_changed") else "TREATMENT_OBSERVED_NOT_PROFIT_PROVEN"
        result["reference_funnel"][str(cost)] = funnel
    if not events:
        result["acceptance"] = evaluate_acceptance(result, experiment)
        return result
    result.update(start_ts=events[0]["observed_at"], end_ts=events[-1]["observed_at"],
                  calendar_days=(events[-1]["observed_at"] - events[0]["observed_at"]) / 86400,
                  observation_gaps=sum(e["observation_gap"] for e in events), observations=len(events))
    counters = checkpoint.get("metrics", {}).get("observations")
    if counters:
        result.update(observation_gaps=counters["gaps"], observations=counters["count"])
    for cost, saved in checkpoint["scenarios"].items():
        stats, daily_curves = {}, {}
        for name in COHORTS:
            value = saved[name] if name == COHORTS[0] else saved[name]["book"]
            book = QuotePortfolio.restore(value)
            curve = [e["scenarios"][cost]["cohorts"][name]["portfolio"] for e in events]
            last, duration, exposed, utilized = curve[-1], 0., 0., 0.
            daily, segments = {}, {}
            prev_equity = float(experiment["initial_cash_usdt"])
            for i, (event, mark) in enumerate(zip(events, curve)):
                equity = float(mark["equity_usdt"])
                delta = equity - prev_equity
                day = str(int(event["observed_at"]) // 86400)
                daily[day] = daily.get(day, 0.) + delta
                regime = str(event["scenarios"][cost]["regime"])
                segments[regime] = segments.get(regime, 0.) + delta
                if i:
                    seconds = event["observed_at"] - events[i - 1]["observed_at"]
                    prior = curve[i - 1]
                    duration += seconds
                    exposed += seconds * (float(prior["gross_exposure_usdt"]) > 0)
                    utilized += seconds * float(prior["gross_exposure_usdt"]) / max(float(prior["equity_usdt"]), 1e-9)
                prev_equity = equity
            # Aggregate partial exits of one opening campaign before the robustness check.
            campaigns, completed = {}, set()
            for lot in book.closed_lots:
                key = (lot["symbol"], str(lot["entry_ts"]))
                campaigns[key] = campaigns.get(key, 0.) + float(lot["net_pnl_usdt"])
                if lot.get("campaign_closed"):
                    completed.add(key)
            net = float(last["net_equity_increment_usdt"])
            realized = [value for key, value in campaigns.items() if key in completed]
            stats[name] = {**last, "actual_simulated_fills": len(book.fills), "real_live_trades": 0,
                           "closed_campaigns": len(realized), "partially_realized_open_campaigns": len(campaigns) - len(completed),
                           "net_expectancy_per_realized_campaign_usdt": statistics.mean(realized) if realized else None,
                           "maximum_drawdown_fraction": max(float(x["drawdown_fraction"]) for x in curve),
                           "net_equity_without_largest_profitable_campaign_usdt": net - max([0., *realized]),
                           "exposure_time_fraction": exposed / duration if duration else 0,
                           "time_weighted_capital_utilization": utilized / duration if duration else 0,
                           "daily_equity_increments": daily, "regime_equity_increments": segments}
            total = checkpoint.get("metrics", {}).get(cost + ":" + name)
            if total:
                stats[name].update(maximum_drawdown_fraction=total["maximum_drawdown_fraction"],
                                   exposure_time_fraction=total["exposed_seconds"] / total["duration"] if total["duration"] else 0,
                                   time_weighted_capital_utilization=total["utilized_seconds"] / total["duration"] if total["duration"] else 0,
                                   accounting_sampling="all_observed_minutes")
            stats[name].update(observed_maximum_drawdown_fraction=stats[name]["maximum_drawdown_fraction"],
                               drawdown_scope="observed_quotes_only_unknown_between_observations",
                               exposure_scope="last_observed_holdings_carried_between_quotes_estimate_not_monitoring_proof")
            daily_curves[name] = daily
        result["scenarios"][cost] = stats
        comparisons = {}
        for treatment, control in experiment["primary_comparisons"]:
            days = sorted(daily_curves[treatment])
            deltas = [daily_curves[treatment][d] - daily_curves[control][d] for d in days]
            interval = None
            if len(deltas) >= 10:
                rng = np.random.default_rng(20260906)
                # Paired contiguous 3-day blocks preserve short serial dependence.
                samples = []
                for _ in range(2000):
                    sampled = []
                    while len(sampled) < len(deltas):
                        start = int(rng.integers(0, len(deltas)))
                        sampled.extend(deltas[(start + j) % len(deltas)] for j in range(3))
                    samples.append(sum(sampled[:len(deltas)]))
                interval = np.quantile(samples, [.025, .975]).tolist()
            comparisons[treatment + "_minus_" + control] = {
                "net_equity_delta_usdt": float(stats[treatment]["net_equity_increment_usdt"]) - float(stats[control]["net_equity_increment_usdt"]),
                "paired_daily_block_bootstrap_95pct_delta_usdt": interval,
                "observed_day_blocks": len(days), "price_labels_counted_as_trades": False,
                "veto_avoided_loss_and_missed_profit": "requires_same_opportunity_completed_counterfactual_pair; not_inferred_from_MFE"}
        result["comparisons"][cost] = comparisons
        control_book = QuotePortfolio.restore(saved["C_hold24_only"]["book"])
        pairs = {}
        for event in events:
            cohorts = event["scenarios"][cost]["cohorts"]
            control = cohorts["C_hold24_only"].get("decision") or {}
            treatment = cohorts["D_reference_only"].get("decision") or {}
            if control.get("action") != "entry_intent" or treatment.get("action") != "reference_deferred_entry" or control["symbol"] != treatment["symbol"]:
                continue
            key = (control["symbol"], event["observed_at"])
            fill = next((f for f in control_book.fills if f["side"] == "buy" and f["symbol"] == key[0] and f["decision_ts"] == key[1]), None)
            lots = [lot for lot in control_book.closed_lots if fill and lot["symbol"] == key[0] and lot["entry_ts"] == fill["observed_at"]]
            completed = any(lot.get("campaign_closed") for lot in lots)
            pairs[str(key)] = {"symbol": key[0], "decision_ts": key[1], "advice_id": treatment["reference_advice_id"],
                               "control_entry_executed": fill is not None, "campaign_completed": completed,
                               "control_net_pnl_usdt": sum(float(lot["net_pnl_usdt"]) for lot in lots) if completed else None}
        complete = [p["control_net_pnl_usdt"] for p in pairs.values() if p["campaign_completed"]]
        result["comparisons"][cost]["D_reference_only_minus_C_hold24_only"]["paired_reference_veto_attribution"] = {
            "matched_opportunities": len(pairs), "completed_control_campaigns": len(complete),
            "avoided_net_loss_usdt": -sum(min(0., pnl) for pnl in complete),
            "missed_net_profit_usdt": sum(max(0., pnl) for pnl in complete), "pairs": list(pairs.values()),
            "interpretation": "matched_control_simulation_campaigns; incremental_account_equity_remains_primary"}
    opportunities = {}
    for event in events:
        value = event["scenarios"][str(experiment["roundtrip_cost_scenarios_bps"][0])]["cohorts"]["C_hold24_only"].get("decision") or {}
        if value.get("action") == "entry_intent":
            opportunities[(value["symbol"], event["bar_ts"])] = event["observed_at"]
    last_exit, independent = {}, []
    for (symbol, bar), now in sorted(opportunities.items(), key=lambda item: item[1]):
        if now >= last_exit.get(symbol, 0):
            independent.append(now)
            last_exit[symbol] = now + 86400
    result["independent_24h_entry_opportunities"] = len(independent)
    result["overlapping_entry_observations"] = len(opportunities) - len(independent)
    result["distinct_independent_entry_days"] = len({int(t) // 86400 for t in independent})
    result["acceptance"] = evaluate_acceptance(result, experiment)
    result["status"] = result["acceptance"]["status"]
    result["promotion_status"] = "RESEARCH_REVIEW_ONLY_LIVE_AUTHORIZATION_SEPARATE"
    return serializable(result)
