"""Independent, causal V5 accounts; the only treatment is a new-entry DEFER.

No exchange executor is imported. Original strategy code runs in isolated
account sandboxes with its own simulated fills and state, never the live ledger.
"""
from __future__ import annotations

import copy
import json
import re
import statistics
from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import numpy as np

from src.core.models import MarketSeries
from src.execution.fill_store import FillRow, FillStore, derive_fill_store_path
from src.execution.order_store import OrderStore
from src.execution.same_symbol_reentry_guard import record_same_symbol_exit_memory
from src.reporting.budget_state import BudgetState
from src.reporting.decision_audit import DecisionAudit
from src.reporting.participation_runtime import _save_report
from src.research.original_v5_adapter import OriginalV5Adapter
from src.research.paired_reference_risk import record_risk_audit, update_risk
from src.research.quote_portfolio import QuotePortfolio, number, serializable
from src.research.reference_contract import FIELDS, reference_use
from src.research.review_comparison import digest, validate_frame


ACCOUNTS = ("A_original_v5", "B_defer_4h")


class PairedV5Adapter(OriginalV5Adapter):
    """Supply pipeline budget inputs from this account's own paper executions."""
    @contextmanager
    def _environment(self, now):
        from src.data.okx_instruments import OKXSpotInstrumentsCache
        # Bind the actual cache reader as well as its writer. This prevents a
        # test/global seed cache or working-directory fallback from supplying
        # unrecorded instrument specifications to the experiment.
        with patch.object(OKXSpotInstrumentsCache, "_resolve_cache_path", return_value=self.root / "reports/okx_spot_instruments.json"), \
                patch.object(OKXSpotInstrumentsCache, "_resolve_seed_cache_path", return_value=None), \
                super()._environment(now):
            yield

    def decide(self, *, snapshot, market_data, portfolio, regime_result=None):
        now = snapshot["now_ts"]
        day = int(now // 86400)
        fills = [fill for fill in portfolio.fills if int(float(fill["observed_at"]) // 86400) == day]
        notionals = [float(fill["notional_usdt"]) for fill in fills]
        equity = float(portfolio.mark(snapshot["symbols"], now=now)["equity_usdt"])
        state = BudgetState(ymd_utc=datetime.fromtimestamp(now, timezone.utc).strftime("%Y%m%d"),
                            turnover_budget_per_day=self.cfg.budget.turnover_budget_per_day,
                            turnover_budget_unit="ratio", cost_budget_bps_per_day=self.cfg.budget.cost_budget_bps_per_day,
                            turnover_used=sum(notionals), avg_equity_est=equity,
                            cost_used_usdt=sum(float(fill["fee_usdt"]) + float(fill["notional_usdt"]) * float(portfolio.slippage) for fill in fills),
                            fills_count_today=len(fills), median_notional_usdt_today=statistics.median(notionals) if notionals else None,
                            small_trade_ratio_today=sum(value < self.cfg.budget.min_trade_notional_base for value in notionals) / len(notionals) if notionals else None,
                            small_trade_notional_cutoff=self.cfg.budget.min_trade_notional_base)
        budget = {**state.to_dict(), "current_equity_usdt": equity,
                  "equity_cap_usdt": self.cfg.budget.live_equity_cap_usdt,
                  "source": "own_paper_fills_and_current_paper_equity",
                  "cost_basis": "explicit_modeled_fee_plus_slippage; observed_spread_already_in_account_equity"}
        def audit_factory(*args, **kwargs):
            audit = DecisionAudit(*args, **kwargs)
            audit.budget = copy.deepcopy(budget)
            return audit
        # The original adapter creates a DecisionAudit internally; provide the
        # same class with account-specific inputs without changing frozen legacy
        # experiments or ever reading the real account's daily budget.
        with patch("src.research.original_v5_adapter.DecisionAudit", audit_factory):
            return super().decide(snapshot=snapshot, market_data=market_data, portfolio=portfolio, regime_result=regime_result)


def validate_experiment(experiment):
    if experiment.get("schema_version") != "v5.paired_reference_paper.v1":
        raise ValueError("unsupported_paired_experiment_schema")
    if (experiment.get("paper_only") is not True or experiment.get("live_order_effect") != "none"
            or experiment.get("live_execution_eligible") is not False
            or experiment.get("automatic_live_scaling") is not False):
        raise ValueError("paper_only_no_live_authority_required")
    contract = experiment.get("reference_contract", {})
    if (set(contract) != set(FIELDS) or contract.get("horizon_hours") != 4
            or any(not isinstance(contract.get(k), str) or not contract[k] for k in FIELDS[:-1])
            or not re.fullmatch(r"[a-f0-9]{40}", contract.get("analysis_source_identity", ""))):
        raise ValueError("exact_frozen_four_hour_reference_contract_required")
    if experiment.get("initial_cash_usdt") != 100 or experiment.get("fee_bps_per_side") != 10:
        raise ValueError("frozen_equal_100_usdt_and_10bps_fee_required")
    if experiment.get("roundtrip_cost_scenarios_bps") != [30, 60, 120]:
        raise ValueError("declared_30_60_120bps_cost_scenarios_required")
    clock = experiment["decision_clock"]
    if not 0 <= clock["offset_seconds"] < 3600 or not 0 <= clock["maximum_lateness_seconds"] < 3600 - clock["offset_seconds"]:
        raise ValueError("invalid_common_decision_clock")


def _active(book, symbol):
    position = book.positions.get(symbol)
    return position is not None and position.get("management_status", "active") == "active"


def record_paper_fill(adapter, fill, intent, *, pre_position=None):
    """Feed only this account's paper executions into account-dependent guards."""
    orders_path = Path(adapter.cfg.execution.order_store_path).resolve()
    if not orders_path.is_relative_to(adapter.root):
        raise ValueError("paper_fill_store_escaped_account_sandbox")
    oid = "paper-" + fill["intent_id"]
    stamp = float(fill["observed_at"])
    with patch("time.time", lambda: stamp):
        store = OrderStore(str(orders_path))
        store.upsert_new(cl_ord_id=oid, run_id="paper-" + str(int(intent["decision_ts"])),
                         inst_id=fill["symbol"].replace("/", "-"), side=fill["side"],
                         intent=intent.get("intent", "OPEN_LONG" if fill["side"] == "buy" else "CLOSE_LONG"),
                         decision_hash=fill["intent_id"], td_mode="cash", ord_type="market",
                         notional_usdt=float(fill["notional_usdt"]), px=str(fill["price"]), sz=str(fill["quantity"]),
                         req={"paper_only": True, "meta": intent.get("metadata", {})}, submit_gate="PAPER_ONLY")
        # A local paper execution must be visible to existing cooldown readers.
        import sqlite3
        with sqlite3.connect(orders_path) as con:
            con.execute("UPDATE orders SET state='FILLED',ord_id=?,acc_fill_sz=?,avg_px=?,fee=?,updated_ts=? WHERE cl_ord_id=?",
                        (oid, str(fill["quantity"]), str(fill["price"]), str(-float(fill["fee_amount"])), int(stamp * 1000), oid))
        FillStore(str(derive_fill_store_path(orders_path))).upsert_many([FillRow(
            inst_id=fill["symbol"].replace("/", "-"), trade_id=oid, ts_ms=int(stamp * 1000),
            ord_id=oid, cl_ord_id=oid, side=fill["side"], exec_type="T", fill_px=str(fill["price"]),
            fill_sz=str(fill["quantity"]), fill_notional=str(fill["notional_usdt"]),
            fee=str(-float(fill["fee_amount"])), fee_ccy=fill["fee_currency"], source="paper_simulation",
            raw_json=json.dumps({"paper_only": True, "live_order_effect": "none", "campaign_id": fill["campaign_id"]}))])
        if fill["side"] == "sell" and intent.get("intent") == "CLOSE_LONG":
            before = pre_position or {}
            metadata = intent.get("metadata", {})
            basis = float(before.get("entry_price", 0)) * float(fill["quantity"])
            net_bps = metadata.get("net_bps")
            if net_bps is None and basis > 0:
                net_bps = float(fill["realized_pnl_usdt"]) / basis * 10000
            record_same_symbol_exit_memory(path=orders_path.parent / "same_symbol_reentry_exit_memory.json",
                                           symbol=fill["symbol"], exit_ts_ms=int(stamp * 1000), exit_px=float(fill["price"]),
                                           exit_reason=str(metadata.get("exit_reason") or intent.get("reason") or ""),
                                           highest_px_before_exit=metadata.get("highest_px_before_exit") or float(before.get("highest_px", 0)) or float(fill["price"]),
                                           net_bps=net_bps,
                                           memory_status="filled", source="paired_paper_simulation")
            for name in ("stop_loss_state", "fixed_stop_loss_state", "profit_taking_state", "highest_px_state"):
                path = orders_path.parent / (name + ".json")
                if path.exists():
                    state = json.loads(path.read_text(encoding="utf-8"))
                    if fill["symbol"] in state:
                        del state[fill["symbol"]]
                        _save_report(path, state)


class PairedComparison:
    def __init__(self, cfg, experiment, *, root: Path, checkpoint=None, adapter_factory=PairedV5Adapter):
        validate_experiment(experiment)
        self.experiment = experiment
        self.last_observed = self.last_bar = self.last_decision_bar = self.start = None
        self.metrics = {"observations": 0, "gaps": 0, "unobserved_seconds": 0, "decision_hours": [],
                        "first_quote_slot": None, "last_quote_slot": None, "quote_slots_observed": 0,
                        "maximum_quote_interval_seconds": 0}
        self.scenarios = {}
        for cost in experiment["roundtrip_cost_scenarios_bps"]:
            scenario = {"vetoes": [], "reference_counts": {}, "matched_candidates": 0, "daily": {}, "regimes": {}, "max_drawdown": {}}
            for name in ACCOUNTS:
                baseline = cfg.model_copy(deep=True)
                baseline.execution.fee_bps = 10
                baseline.execution.slippage_bps = cost / 2 - 10
                baseline.execution.cost_aware_roundtrip_cost_bps = cost
                scenario[name] = {"adapter": adapter_factory(baseline, sandbox=root / str(cost) / name),
                                  "book": QuotePortfolio(initial_cash=100, fee_bps=10, slippage_bps=cost / 2 - 10,
                                                         maximum_quote_age_seconds=experiment["maximum_quote_age_seconds"]),
                                  "pending": [], "audit": {}, "last_equity": 100}
            self.scenarios[str(cost)] = scenario
        if checkpoint:
            for name in ("last_observed", "last_bar", "last_decision_bar", "start", "metrics"):
                setattr(self, name, copy.deepcopy(checkpoint[name]))
            for cost, saved in checkpoint["scenarios"].items():
                scenario = self.scenarios[cost]
                for key in ("vetoes", "reference_counts", "matched_candidates", "daily", "regimes", "max_drawdown"):
                    scenario[key] = copy.deepcopy(saved[key])
                for name in ACCOUNTS:
                    value = scenario[name]
                    value.update({k: copy.deepcopy(v) for k, v in saved[name].items() if k != "book"})
                    value["book"] = QuotePortfolio.restore(saved[name]["book"])

    def checkpoint(self):
        scenarios = {}
        for cost, scenario in self.scenarios.items():
            scenarios[cost] = {k: copy.deepcopy(v) for k, v in scenario.items() if k not in ACCOUNTS}
            for name in ACCOUNTS:
                scenarios[cost][name] = {k: (v.checkpoint() if k == "book" else copy.deepcopy(v))
                                         for k, v in scenario[name].items() if k != "adapter"}
        return serializable({"last_observed": self.last_observed, "last_bar": self.last_bar,
                             "last_decision_bar": self.last_decision_bar, "start": self.start,
                             "metrics": self.metrics, "scenarios": scenarios})

    def observe(self, frame):
        validate_frame(frame)
        if set(frame["symbols"]) != set(self.experiment["symbols"]):
            raise ValueError("shared_quote_universe_mismatch")
        now, bar = frame["observed_at"], frame["bar_ts"]
        if self.last_observed is not None and (now <= self.last_observed or bar < self.last_bar):
            raise ValueError("duplicate_or_reversed_observation")
        if self.start is None:
            self.start = now
        interval = now - self.last_observed if self.last_observed is not None else 0
        gap = interval > self.experiment["maximum_observation_gap_seconds"]
        clock = self.experiment["decision_clock"]
        cutoff = bar + clock["offset_seconds"]
        deadline = cutoff + clock["maximum_lateness_seconds"]
        signals = frame.get("signal_data", {"status": "valid"})["status"] == "valid"
        hourly = signals and self.last_decision_bar != bar and cutoff <= now <= deadline
        phase = ("DECIDED" if hourly else "ALREADY_DECIDED" if self.last_decision_bar == bar
                 else "WAITING_COMMON_CUTOFF" if now < cutoff else "MISSED_COMMON_DEADLINE"
                 if now > deadline else "SIGNAL_DATA_UNAVAILABLE")
        rows = copy.deepcopy(frame["symbols"])
        market = {symbol: MarketSeries(**value) for symbol, value in frame["market_data"].items()}
        for symbol, series in market.items():
            rows[symbol]["close"] = series.close[-1]
        snapshot = {"now_ts": now, "bar_ts": bar, "symbols": rows}
        event = {"observed_at": now, "bar_ts": bar, "input_hash": digest(frame), "hourly_decision": hourly,
                 "observation_gap": gap, "decision_clock": {"status": phase, "cutoff_ts": cutoff, "deadline_ts": deadline},
                 "scenarios": {}, "paper_only": True, "live_order_effect": "none"}
        self.metrics["observations"] += 1
        self.metrics["gaps"] += int(gap)
        slot = int(now // 60)
        if self.metrics["first_quote_slot"] is None:
            self.metrics["first_quote_slot"] = slot
        if slot != self.metrics["last_quote_slot"]:
            self.metrics["quote_slots_observed"] += 1
        self.metrics["last_quote_slot"] = slot
        self.metrics["unobserved_seconds"] = 60 * max(0, slot - self.metrics["first_quote_slot"] + 1 - self.metrics["quote_slots_observed"])
        self.metrics["maximum_quote_interval_seconds"] = max(self.metrics["maximum_quote_interval_seconds"], interval)
        if hourly:
            self.metrics["decision_hours"].append(bar)
        for cost, scenario in self.scenarios.items():
            entries = {}
            for name in ACCOUNTS:
                account = scenario[name]
                book = account["book"]
                executions = []
                for intent in account["pending"]:
                    pre_position = copy.deepcopy(book.positions.get(intent["symbol"]))
                    try:
                        if gap and intent["side"] == "buy":
                            raise ValueError("entry_cancelled_after_observation_gap_no_chase")
                        fill = book.fill(intent, rows[intent["symbol"]], now=now)
                    except ValueError as exc:
                        executions.append({"intent_id": intent["intent_id"], "action": "rejected", "reason": str(exc)})
                    else:
                        if fill:
                            record_paper_fill(account["adapter"], fill, intent, pre_position=pre_position)
                            executions.append(serializable(fill))
                account["pending"] = []
                decisions = []
                if hourly:
                    update_risk(account, rows, now, observation_gap=gap)
                    orders, audit = account["adapter"].decide(snapshot=snapshot, market_data=market, portfolio=book)
                    account["audit"] = audit
                    for index, order in enumerate(orders):
                        value = asdict(order)
                        new_entry = order.side == "buy" and not _active(book, order.symbol)
                        use = reference_use(frame.get("references", {}).get(order.symbol), now, self.experiment["reference_contract"])
                        intent = {**value, "intent_id": digest([cost, name, now, index, value]),
                                  "decision_ts": now, "metadata": value.pop("meta"), "new_entry": new_entry,
                                  "reason": order.meta.get("exit_reason") or order.meta.get("reason")}
                        if order.side == "sell":
                            position = book.positions.get(order.symbol)
                            if position:
                                # Live CLOSE_LONG sells the held base quantity;
                                # REBALANCE sizes base at the decision price.
                                # Price moves before execution must not turn a
                                # full exit into a partial rebalance or vice versa.
                                if order.intent != "CLOSE_LONG" and order.signal_price <= 0:
                                    raise ValueError("sell_rebalance_requires_positive_decision_price")
                                quantity = position["qty"] if order.intent == "CLOSE_LONG" else min(position["qty"],
                                    number(order.notional_usdt) / number(order.signal_price))
                                intent["quantity"] = str(quantity)
                        veto = name == ACCOUNTS[1] and new_entry and use["defer"]
                        decisions.append({**intent, "action": "reference_deferred_entry" if veto else "original_order_intent",
                                          "reference": use if new_entry else None})
                        if not veto:
                            account["pending"].append(intent)
                    record_risk_audit(account, audit, now=now, bar=bar, decisions=decisions)
                mark = serializable(book.mark(rows, now=now))
                delta = float(mark["equity_usdt"]) - account["last_equity"]
                day = str(int(now) // 86400)
                regime = account["audit"].get("regime", "Unknown")
                scenario["daily"].setdefault(name, {})[day] = scenario["daily"].get(name, {}).get(day, 0) + delta
                scenario["regimes"].setdefault(name, {})[str(regime)] = scenario["regimes"].get(name, {}).get(str(regime), 0) + delta
                scenario["max_drawdown"][name] = max(scenario["max_drawdown"].get(name, 0), float(mark["drawdown_fraction"]))
                account["last_equity"] = float(mark["equity_usdt"])
                entries[name] = {"portfolio": mark, "decisions": decisions, "executions": executions,
                                 "audit": account["audit"] if hourly else None}
            a_entries = {r["symbol"]: r for r in entries[ACCOUNTS[0]]["decisions"] if r["new_entry"]}
            b_entries = {r["symbol"]: r for r in entries[ACCOUNTS[1]]["decisions"] if r["new_entry"]}
            for symbol in sorted(set(a_entries) | set(b_entries)):
                use = reference_use(frame.get("references", {}).get(symbol), now, self.experiment["reference_contract"])
                counts = scenario["reference_counts"]
                for key, value in (("candidates", 1), ("valid", int(use["valid"])), ("status:" + use["status"], 1)):
                    counts[key] = counts.get(key, 0) + value
                matched = symbol in a_entries and symbol in b_entries
                scenario["matched_candidates"] += int(matched)
                if symbol in b_entries and b_entries[symbol]["action"] == "reference_deferred_entry":
                    counts["deferred"] = counts.get("deferred", 0) + 1
                    scenario["vetoes"].append({"symbol": symbol, "decision_ts": now, "advice_id": use["advice_id"],
                                               "matched": matched, "control_intent_id": a_entries[symbol]["intent_id"] if matched else None})
            event["scenarios"][cost] = entries
        self.last_observed, self.last_bar = now, bar
        if hourly:
            self.last_decision_bar = bar
        return event


def _interval(daily_a, daily_b):
    days = sorted(set(daily_a) | set(daily_b))
    values = [daily_b.get(day, 0) - daily_a.get(day, 0) for day in days]
    if len(values) < 10:
        return None
    rng = np.random.default_rng(20260922)
    starts = rng.integers(0, len(values), size=(2000, (len(values) + 2) // 3))
    indices = ((starts[..., None] + np.arange(3)) % len(values)).reshape(2000, -1)[:, :len(values)]
    return np.quantile(np.asarray(values)[indices].sum(axis=1), [.025, .975]).tolist()


def summarize(comparison, event):
    exp, metrics = comparison.experiment, comparison.metrics
    now, start = comparison.last_observed, comparison.start
    clock = exp["decision_clock"]
    first_bar = int(start // 3600) * 3600
    if start > first_bar + clock["offset_seconds"] + clock["maximum_lateness_seconds"]:
        first_bar += 3600
    due = int((now - clock["offset_seconds"] - clock["maximum_lateness_seconds"]) // 3600) * 3600
    expected = max(0, (due - first_bar) // 3600 + 1)
    observed = len([hour for hour in metrics["decision_hours"] if hour <= due])
    duration = now - start
    expected_quote_slots = metrics["last_quote_slot"] - metrics["first_quote_slot"] + 1
    coverage = {"expected_decision_hours": expected, "observed_decision_hours": observed,
                "missed_decision_hours": max(0, expected - observed),
                "decision_coverage_rate": observed / expected if expected else None,
                "observation_count": metrics["observations"], "observation_gaps": metrics["gaps"],
                "unobserved_seconds": metrics["unobserved_seconds"],
                "expected_quote_slots": expected_quote_slots,
                "observed_quote_slots": metrics["quote_slots_observed"],
                "missed_quote_slots": max(0, expected_quote_slots - metrics["quote_slots_observed"]),
                "quote_coverage_rate": metrics["quote_slots_observed"] / expected_quote_slots,
                "quote_cadence_seconds": 60, "maximum_quote_interval_seconds": metrics["maximum_quote_interval_seconds"],
                "scope": "actual_UTC_minute_slots_counted_once; decision_startup_partial_hour_excluded; no_history_backfill"}
    report = {"schema_version": exp["schema_version"], "experiment_id": exp["experiment_id"],
              "latest_observed_at": now, "ledger_start_ts": start, "calendar_days": duration / 86400,
              "paper_only": True, "live_order_effect": "none", "live_execution_eligible": False,
              "automatic_live_scaling": False, "baseline_scope": exp["baseline_scope"],
              "defer_rule": exp["defer_rule"], "reference_contract": exp["reference_contract"],
              "risk_evaluation_clock": exp["risk_evaluation_clock"],
              "cost_model": {"fee_bps_per_side": 10, "slippage_bps_per_side": 5,
                             "explicit_roundtrip_cost_bps": 30, "bid_ask_spread": "additional_observed_cost",
                             "calibrated_to_real_fills": False, "fixed_operating_cost_included": False},
              "coverage": coverage, "scenarios": {}, "latest_decision_clock": event["decision_clock"],
              "drawdown_scope": "observed_quotes_only_unknown_between_observations",
              "profit_claim": "research_account_estimate_not_realized_live_profit_or_project_profit"}
    for cost, scenario in comparison.scenarios.items():
        accounts = {}
        for name in ACCOUNTS:
            book = scenario[name]["book"]
            campaigns = Counter()
            closed = set()
            for lot in book.closed_lots:
                cid = lot["campaign_id"]
                campaigns[cid] += float(lot["net_pnl_usdt"])
                if lot["campaign_closed"]:
                    closed.add(cid)
            accounts[name] = {**event["scenarios"][cost][name]["portfolio"],
                              "realized_pnl_usdt": sum(campaigns.values()), "actual_simulated_fills": len(book.fills),
                              "independent_closed_campaign_count": len(closed), "exit_allocation_segment_count": len(book.closed_lots),
                              "maximum_drawdown_fraction": scenario["max_drawdown"].get(name, 0),
                              "risk_snapshot": scenario[name].get("risk_snapshot"),
                              "daily_equity_increments": scenario["daily"].get(name, {}),
                              "regime_equity_increments": scenario["regimes"].get(name, {}),
                              "net_equity_without_largest_profitable_campaign_usdt": float(event["scenarios"][cost][name]["portfolio"]["net_equity_increment_usdt"]) - max([0, *(campaigns[cid] for cid in closed)]),
                              "price_labels_counted_as_trades": False, "real_live_trades": 0}
        control = scenario[ACCOUNTS[0]]["book"]
        pairs, seen = [], set()
        for veto in scenario["vetoes"]:
            if not veto["matched"]:
                continue
            fill = next((f for f in control.fills if f["intent_id"] == veto["control_intent_id"]), None)
            cid = fill["campaign_id"] if fill else None
            lots = [lot for lot in control.closed_lots if cid and lot["campaign_id"] == cid]
            completed = any(lot["campaign_closed"] for lot in lots)
            if cid and cid in seen:
                continue
            if cid:
                seen.add(cid)
            pairs.append({**veto, "campaign_id": cid, "control_entry_executed": fill is not None,
                          "campaign_completed": completed,
                          "control_net_pnl_usdt": sum(float(lot["net_pnl_usdt"]) for lot in lots) if completed else None})
        values = [row["control_net_pnl_usdt"] for row in pairs if row["campaign_completed"]]
        counts = scenario["reference_counts"]
        refs = {"candidates": counts.get("candidates", 0), "valid": counts.get("valid", 0),
                "valid_coverage_rate": counts.get("valid", 0) / counts["candidates"] if counts.get("candidates") else None,
                "deferred": counts.get("deferred", 0), "matched_candidates": scenario["matched_candidates"],
                "matched_vetoes": len(pairs), "status_counts": {k.removeprefix("status:"): v for k, v in counts.items() if k.startswith("status:")}}
        delta = float(accounts[ACCOUNTS[1]]["equity_usdt"]) - float(accounts[ACCOUNTS[0]]["equity_usdt"])
        difference = {"net_equity_delta_usdt": delta, "avoided_net_loss_usdt": -sum(min(0, value) for value in values),
                      "missed_net_profit_usdt": sum(max(0, value) for value in values),
                      "completed_matched_veto_campaigns": len(values), "matched_veto_campaigns": pairs,
                      "paired_daily_block_bootstrap_95pct_delta_usdt": _interval(scenario["daily"].get(ACCOUNTS[0], {}), scenario["daily"].get(ACCOUNTS[1], {})),
                      "attribution_scope": "matched_control_completed_campaigns_only; cash_redeployment_and_diverged_candidates_are_account_delta_not_veto_attribution"}
        report["scenarios"][cost] = {"accounts": accounts, "comparison": difference, "references": refs}
    report.update(report["scenarios"]["30"])
    report["acceptance"] = evaluate_acceptance(report, exp)
    report["status"] = report["acceptance"]["status"]
    return serializable(report)


def evaluate_acceptance(report, exp):
    rows = []
    def minimum(name, actual, required, sample=True, strict=False):
        passed = actual is not None and (actual > required if strict else actual >= required)
        rows.append({"criterion": name, "actual": actual, "required": (">" if strict else ">=") + str(required),
                     "result": "PASS" if passed else "INSUFFICIENT" if sample or actual is None else "FAIL"})
    minimum("forward_calendar_days", report["calendar_days"], exp["minimum_forward_calendar_days"])
    minimum("completed_control_campaigns", report["accounts"][ACCOUNTS[0]]["independent_closed_campaign_count"], exp["minimum_closed_campaigns_control"])
    minimum("matched_candidates", report["references"]["matched_candidates"], exp["minimum_matched_candidates"])
    minimum("completed_matched_veto_campaigns", report["comparison"]["completed_matched_veto_campaigns"], exp["minimum_completed_matched_veto_campaigns"])
    minimum("valid_reference_coverage", report["references"]["valid_coverage_rate"], exp["minimum_valid_reference_coverage"])
    minimum("decision_coverage", report["coverage"]["decision_coverage_rate"], exp["minimum_decision_coverage"])
    minimum("quote_coverage", report["coverage"]["quote_coverage_rate"], exp["minimum_quote_coverage"])
    samples_ready = all(row["result"] == "PASS" for row in rows)
    for cost in ("30", "60", "120"):
        minimum("net_account_delta_" + cost + "bps", report["scenarios"][cost]["comparison"]["net_equity_delta_usdt"], 0, False, True)
    interval = report["comparison"]["paired_daily_block_bootstrap_95pct_delta_usdt"]
    minimum("paired_95pct_lower_bound", interval[0] if interval else None, 0, False, True)
    a, b = (report["accounts"][name]["maximum_drawdown_fraction"] for name in ACCOUNTS)
    rows.append({"criterion": "observed_drawdown_within_budget_and_control", "actual": b, "required": min(a, exp["maximum_drawdown_fraction"]), "result": "PASS" if b <= min(a, exp["maximum_drawdown_fraction"]) else "FAIL"})
    minimum("treatment_positive_without_largest_profitable_campaign", report["accounts"][ACCOUNTS[1]]["net_equity_without_largest_profitable_campaign_usdt"], 0, False, True)
    status = ("COLLECTING_FORWARD_EVIDENCE" if not samples_ready else "STOP_VERSION_NO_NET_BENEFIT"
              if any(row["result"] == "FAIL" for row in rows) else "INSUFFICIENT_EVIDENCE"
              if any(row["result"] == "INSUFFICIENT" for row in rows) else "READY_FOR_MANUAL_RESEARCH_REVIEW")
    return {"status": status, "requirements": rows, "live_execution_eligible": False,
            "automatic_live_scaling": False, "scope": "research_review_only_never_live_authorization"}
