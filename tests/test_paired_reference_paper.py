import copy
import json
import sqlite3
from pathlib import Path

import pytest

from src.core.models import Order
from src.research.paired_reference_forward import process, references_at, strategy_dependencies
from src.research.paired_reference_paper import ACCOUNTS, PairedComparison, PairedV5Adapter, summarize, validate_experiment
from src.research.paired_reference_risk import record_risk_audit, update_risk
from tests.test_review_comparison import config, frame as original_frame


PROJECT = Path(__file__).resolve().parents[1]
EXPERIMENT = json.loads((PROJECT / "configs/research/paired_reference_paper_v1.json").read_text())
BAR = 1788652800


def frame(now=BAR + 360, references=None):
    value = original_frame(now)
    value["references"] = references or {}
    # The synthetic strategy below trades one test symbol. Constant observed
    # bid/ask makes closed losses attributable to actual modeled costs.
    for row in value["symbols"].values():
        row["quote"].update(bid=100, ask=100.01)
    return value


def reference(now=BAR + 360, **overrides):
    return {**EXPERIMENT["reference_contract"], "advice_id": "advice-" + "a" * 64,
            "published_ts": now - 30, "first_received_ts": now - 10, "expires_ts": now + 1800,
            "action": "DEFER", "research_evaluable": True, "live_execution_eligible": False,
            "live_order_effect": "none", "receipt_reason": "record_only_not_adopted", **overrides}


class FakeAdapter:
    def __init__(self, cfg, *, sandbox):
        self.root = sandbox.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.cfg = cfg.model_copy(deep=True)
        self.cfg.execution.order_store_path = str(self.root / "reports/orders.sqlite")

    def decide(self, *, snapshot, market_data, portfolio):
        symbol = "BTC/USDT"
        active = portfolio.positions.get(symbol)
        active = active and active.get("management_status") != "residual_after_exit"
        side = "sell" if active else "buy"
        return [Order(symbol, side, "CLOSE_LONG" if active else "OPEN_LONG", 50, 100, {})], {"regime": "RiskOn", "own_positions": sorted(portfolio.positions)}


def factory(cfg, experiment, **kwargs):
    return PairedComparison(cfg, experiment, adapter_factory=FakeAdapter, **kwargs)


def test_only_valid_previously_received_4h_defer_changes_new_entry(tmp_path):
    comparison = factory(config(), EXPERIMENT, root=tmp_path)
    event = comparison.observe(frame(references={"BTC/USDT": reference()}))
    for scenario in event["scenarios"].values():
        assert scenario[ACCOUNTS[0]]["decisions"][0]["action"] == "original_order_intent"
        assert scenario[ACCOUNTS[1]]["decisions"][0]["action"] == "reference_deferred_entry"
    fill_event = comparison.observe(frame(BAR + 420))
    assert len(fill_event["scenarios"]["30"][ACCOUNTS[0]]["executions"]) == 1
    assert fill_event["scenarios"]["30"][ACCOUNTS[1]]["executions"] == []
    assert comparison.scenarios["30"][ACCOUNTS[0]]["book"].cash < 51
    assert comparison.scenarios["30"][ACCOUNTS[1]]["book"].cash == 100
    # Each account calculates its own position-dependent decision. A exits;
    # B may enter only on the next genuine candidate, without queuing the veto.
    event = comparison.observe(frame(BAR + 3960))
    assert event["scenarios"]["30"][ACCOUNTS[0]]["decisions"][0]["side"] == "sell"
    assert event["scenarios"]["30"][ACCOUNTS[1]]["decisions"][0]["side"] == "buy"
    event = comparison.observe(frame(BAR + 4020))
    report = summarize(comparison, event)
    assert report["accounts"][ACCOUNTS[0]]["independent_closed_campaign_count"] == 1
    assert report["comparison"]["completed_matched_veto_campaigns"] == 1
    assert report["comparison"]["avoided_net_loss_usdt"] > 0
    assert report["comparison"]["missed_net_profit_usdt"] == 0
    assert report["status"] == "COLLECTING_FORWARD_EVIDENCE"
    assert report["live_execution_eligible"] is False
    # History-dependent strategy guards see this paper account's fills only.
    path = comparison.scenarios["30"][ACCOUNTS[0]]["adapter"].root / "reports/fills.sqlite"
    with sqlite3.connect(path) as con:
        assert con.execute("SELECT count(*) FROM fills WHERE source='paper_simulation'").fetchone()[0] == 2


@pytest.mark.parametrize("change,status", [
    ({"expires_ts": BAR + 350}, "expired"),
    ({"first_received_ts": BAR + 400}, "late_or_future"),
    ({"horizon_hours": 24}, "version_mismatch"),
    ({"analysis_source_identity": "b" * 40}, "version_mismatch"),
    ({"reference_schema": "qlab.decision.result.v2"}, "version_mismatch"),
    ({"research_evaluable": False}, "no_view"),
    ({"receipt_reason": "reference_contract_invalid"}, "invalid_receipt"),
])
def test_inadmissible_advice_preserves_baseline_with_reason(tmp_path, change, status):
    comparison = factory(config(), EXPERIMENT, root=tmp_path)
    event = comparison.observe(frame(references={"BTC/USDT": reference(**change)}))
    decision = event["scenarios"]["30"][ACCOUNTS[1]]["decisions"][0]
    assert decision["action"] == "original_order_intent"
    assert decision["reference"]["status"] == status


def test_defer_never_blocks_an_existing_position_exit(tmp_path):
    comparison = factory(config(), EXPERIMENT, root=tmp_path)
    comparison.observe(frame())
    comparison.observe(frame(BAR + 420))
    event = comparison.observe(frame(BAR + 3960, {"BTC/USDT": reference(BAR + 3960)}))
    assert all(event["scenarios"]["30"][name]["decisions"][0]["side"] == "sell" for name in ACCOUNTS)
    assert event["scenarios"]["30"][ACCOUNTS[1]]["decisions"][0]["action"] == "original_order_intent"


def test_gap_drops_pending_entries_and_never_fills_old_signal(tmp_path):
    comparison = factory(config(), EXPERIMENT, root=tmp_path)
    comparison.observe(frame())
    event = comparison.observe(frame(BAR + 600))
    assert event["observation_gap"]
    assert event["scenarios"]["30"][ACCOUNTS[0]]["executions"][0]["reason"] == "entry_cancelled_after_observation_gap_no_chase"
    assert comparison.scenarios["30"][ACCOUNTS[0]]["book"].cash == 100


def test_restart_idempotency_atomic_generation_and_version_freeze(tmp_path):
    kwargs = dict(cfg=config(), experiment=EXPERIMENT, root=tmp_path, identity={"identity": "a" * 64}, comparison_factory=factory)
    first = process(frame=frame(), **kwargs)
    assert process(frame=frame(), **kwargs) == first
    with sqlite3.connect(tmp_path / "comparison.sqlite") as con:
        assert con.execute("SELECT count(*) FROM events").fetchone()[0] == 1
        generation = con.execute("SELECT value FROM meta WHERE key='generation'").fetchone()[0]
    class Broken(PairedComparison):
        def observe(self, value):
            raise RuntimeError("synthetic_crash_after_staging")
    with pytest.raises(RuntimeError, match="synthetic_crash"):
        process(frame=frame(BAR + 420), **{**kwargs, "comparison_factory": Broken})
    with sqlite3.connect(tmp_path / "comparison.sqlite") as con:
        assert con.execute("SELECT value FROM meta WHERE key='generation'").fetchone()[0] == generation
        assert con.execute("SELECT count(*) FROM acquisitions WHERE status='failed'").fetchone()[0] == 1
    # Next natural observation can continue from committed state after the crash.
    report = process(frame=frame(BAR + 480), **kwargs)
    assert report["accounts"][ACCOUNTS[0]]["actual_simulated_fills"] == 1
    with pytest.raises(ValueError, match="frozen_strategy_or_config"):
        process(frame=frame(BAR + 540), **{**kwargs, "identity": {"identity": "b" * 64}})
    changed = frame(BAR + 480)
    changed["references"] = {"BTC/USDT": reference()}
    with pytest.raises(ValueError, match="conflicting_same_time"):
        process(frame=changed, **kwargs)


def test_coverage_counts_missed_hour_without_backfill(tmp_path):
    comparison = factory(config(), EXPERIMENT, root=tmp_path)
    comparison.observe(frame(BAR + 500))  # Start after the first deadline.
    event = comparison.observe(frame(BAR + 7800))  # Skip next full decision.
    report = summarize(comparison, event)
    assert report["coverage"]["expected_decision_hours"] == 2
    assert report["coverage"]["missed_decision_hours"] == 2
    assert report["coverage"]["observed_decision_hours"] == 0
    assert not report["accounts"][ACCOUNTS[0]]["actual_simulated_fills"]


def test_reader_uses_4h_v3_and_does_not_fall_back_past_new_unknown_version(tmp_path):
    path = tmp_path / "receipts.sqlite"
    with sqlite3.connect(path) as con:
        con.execute("CREATE TABLE advice(advice_id TEXT,first_received REAL,payload TEXT,receipt TEXT)")
        for index, horizon in enumerate((4, 24, 4)):
            advice = {"advice_id": str(index), "symbol": "BTCUSDT", "horizon_hours": horizon,
                      "action": "DEFER", "expires_at": "2026-09-06T01:00:00Z", "cost": {"version": "current-cost-v1"},
                      "eligibility": {"research_evaluable": True, "live_execution_eligible": False}, "live_order_effect": "none"}
            receipt = {**EXPERIMENT["reference_contract"], "published_at": "2026-09-06T00:00:00Z",
                       "reason": "record_only_not_adopted", "analysis_source_identity": "b" * 40 if index == 2 else EXPERIMENT["reference_contract"]["analysis_source_identity"]}
            con.execute("INSERT INTO advice VALUES(?,?,?,?)", (str(index), BAR + index, json.dumps(advice), json.dumps(receipt)))
    diagnostic = {}
    refs = references_at(path, BAR + 360, diagnostic)
    assert refs["BTC/USDT"]["advice_id"] == "2"
    assert refs["BTC/USDT"]["horizon_hours"] == 4
    assert diagnostic["status"] == "observed"


def test_experiment_requires_paper_boundary_and_real_frozen_worker_identity():
    value = copy.deepcopy(EXPERIMENT)
    value["reference_contract"]["analysis_source_identity"] = "pending"
    with pytest.raises(ValueError, match="exact_frozen"):
        validate_experiment(value)
    value = copy.deepcopy(EXPERIMENT)
    value["live_execution_eligible"] = True
    with pytest.raises(ValueError, match="paper_only"):
        validate_experiment(value)


def test_real_v5_adapter_runs_independently_with_observed_instruments(tmp_path):
    # This smoke case exercises the actual strategy pipeline, rather than only
    # the deterministic strategy used in causal account tests above.
    report = process(cfg=config(), experiment=EXPERIMENT, root=tmp_path, frame=frame(), identity={"identity": "a" * 64})
    assert report["coverage"]["observation_count"] == 1
    assert set(report["accounts"]) == set(ACCOUNTS)
    assert report["baseline_scope"].startswith("frozen_hourly_V5_pipeline")
    assert report["live_order_effect"] == "none"


def test_frozen_dependencies_cover_pipeline_but_exclude_unrelated_dashboard_and_cost_report():
    paths = {path.relative_to(PROJECT).as_posix() for path in strategy_dependencies(PROJECT)}
    assert {"src/core/pipeline.py", "configs/schema.py", "src/alpha/alpha_engine.py",
            "src/regime/ensemble_regime_engine.py", "src/execution/fill_store.py",
            "src/research/quote_portfolio.py", "src/reporting/budget_state.py"} <= paths
    assert "src/reporting/dashboard_command_center.py" not in paths
    assert "src/reporting/live_cost_evidence.py" not in paths
    assert "src/research/daily_trend_benchmark.py" not in paths


def test_own_fill_budget_is_injected_into_real_pipeline_audit(tmp_path, monkeypatch):
    from src.research.quote_portfolio import QuotePortfolio
    from src.research import original_v5_adapter
    book = QuotePortfolio(initial_cash=100, fee_bps=10, slippage_bps=5)
    intent = {"intent_id": "budget-fill", "side": "buy", "symbol": "BTC/USDT", "notional_usdt": 70,
              "decision_ts": BAR + 300}
    book.fill(intent, frame()["symbols"]["BTC/USDT"], now=BAR + 360)
    def capture(self, **kwargs):
        return [], original_v5_adapter.DecisionAudit(run_id="test", now_ts=BAR + 360, window_end_ts=BAR).to_dict()
    monkeypatch.setattr(original_v5_adapter.OriginalV5Adapter, "decide", capture)
    adapter = PairedV5Adapter(config(), sandbox=tmp_path)
    _, audit = adapter.decide(snapshot={"now_ts": BAR + 360, "symbols": frame()["symbols"]}, market_data={}, portfolio=book)
    assert audit["budget"]["fills_count_today"] == 1
    assert audit["budget"]["turnover_exceeded"] is True
    assert audit["budget"]["source"] == "own_paper_fills_and_current_paper_equity"
    assert audit["budget"]["current_equity_usdt"] < 100


def test_archived_market_inputs_reconstruct_exact_original_hash(tmp_path):
    import zlib
    from src.research.review_comparison import digest
    value = frame()
    process(cfg=config(), experiment=EXPERIMENT, root=tmp_path, frame=value, identity={"identity": "a" * 64}, comparison_factory=factory)
    with sqlite3.connect(tmp_path / "comparison.sqlite") as con:
        expected_hash, raw = con.execute("SELECT input_hash,frame FROM acquisitions").fetchone()
    restored = json.loads(raw)
    restored.pop("frame_storage")
    for key in ("market_data", "instrument_raw"):
        pointer = restored[key]
        restored[key] = json.loads(zlib.decompress((tmp_path / pointer["path"]).read_bytes()))
    assert digest(restored) == expected_hash == digest(value)


@pytest.mark.parametrize("next_price", [200, 50])
def test_full_exit_keeps_quantity_when_next_quote_moves_and_across_restart(tmp_path, next_price):
    comparison = factory(config(), EXPERIMENT, root=tmp_path)
    comparison.observe(frame())
    comparison.observe(frame(BAR + 420))
    event = comparison.observe(frame(BAR + 3960))
    original_qty = comparison.scenarios["30"][ACCOUNTS[0]]["book"].positions["BTC/USDT"]["qty"]
    assert float(event["scenarios"]["30"][ACCOUNTS[0]]["decisions"][0]["quantity"]) == float(original_qty)
    restored = factory(config(), EXPERIMENT, root=tmp_path, checkpoint=comparison.checkpoint())
    value = frame(BAR + 4020)
    value["symbols"]["BTC/USDT"]["quote"].update(bid=next_price, ask=next_price * 1.0001)
    report = summarize(restored, restored.observe(value))
    assert report["accounts"][ACCOUNTS[0]]["independent_closed_campaign_count"] == 1
    remaining = restored.scenarios["30"][ACCOUNTS[0]]["book"].positions.get("BTC/USDT")
    assert not remaining or remaining["management_status"] == "residual_after_exit"
    assert report["accounts"][ACCOUNTS[0]]["actual_simulated_fills"] == 2


def test_partial_rebalance_uses_decision_quantity_and_does_not_create_close_cooldown(tmp_path):
    class PartialAdapter(FakeAdapter):
        def decide(self, *, snapshot, market_data, portfolio):
            orders, audit = super().decide(snapshot=snapshot, market_data=market_data, portfolio=portfolio)
            if orders[0].side == "sell":
                orders[0].intent, orders[0].notional_usdt = "REBALANCE", 20
            return orders, audit
    comparison = PairedComparison(config(), EXPERIMENT, root=tmp_path, adapter_factory=PartialAdapter)
    comparison.observe(frame())
    comparison.observe(frame(BAR + 420))
    event = comparison.observe(frame(BAR + 3960))
    assert float(event["scenarios"]["30"][ACCOUNTS[0]]["decisions"][0]["quantity"]) == .2
    value = frame(BAR + 4020)
    value["symbols"]["BTC/USDT"]["quote"].update(bid=200, ask=200.01)
    event = comparison.observe(value)
    fill = event["scenarios"]["30"][ACCOUNTS[0]]["executions"][0]
    assert float(fill["quantity"]) == .2
    assert summarize(comparison, event)["accounts"][ACCOUNTS[0]]["independent_closed_campaign_count"] == 0
    assert not (comparison.scenarios["30"][ACCOUNTS[0]]["adapter"].root / "reports/same_symbol_reentry_exit_memory.json").exists()


def test_close_memory_preserves_campaign_peak_and_net_result(tmp_path):
    comparison = factory(config(), EXPERIMENT, root=tmp_path)
    comparison.observe(frame())
    comparison.observe(frame(BAR + 420))
    account = comparison.scenarios["30"][ACCOUNTS[0]]
    account["book"].positions["BTC/USDT"]["highest_px"] = 180
    comparison.observe(frame(BAR + 3960))
    comparison.observe(frame(BAR + 4020))
    memory = json.loads((account["adapter"].root / "reports/same_symbol_reentry_exit_memory.json").read_text())
    assert memory["symbols"]["BTC/USDT"]["highest_px_before_exit"] == 180
    assert memory["symbols"]["BTC/USDT"]["net_bps"] < 0


def test_own_drawdown_and_recovery_change_risk_without_other_account_history(tmp_path):
    comparison = factory(config(), EXPERIMENT, root=tmp_path)
    a, b = (comparison.scenarios["30"][name] for name in ACCOUNTS)
    for account in (a, b):
        for offset in (360, 3960, 7560):
            record_risk_audit(account, {"counts": {"selected": 0}}, now=BAR + offset, bar=BAR + offset - 360, decisions=[])
    a["book"].fill({"intent_id": "risk-entry", "side": "buy", "symbol": "BTC/USDT", "notional_usdt": 80,
                   "decision_ts": BAR + 300}, frame()["symbols"]["BTC/USDT"], now=BAR + 360)
    value = frame(BAR + 11160)
    value["symbols"]["BTC/USDT"]["quote"].update(bid=70, ask=70.01)
    arisk = update_risk(a, value["symbols"], value["observed_at"], observation_gap=False)
    brisk = update_risk(b, value["symbols"], value["observed_at"], observation_gap=False)
    assert arisk["current_level"] == "PROTECT"
    assert brisk["current_level"] == "NEUTRAL"
    assert arisk["metrics"]["dd_pct"] > .12
    assert brisk["metrics"]["dd_pct"] == 0
    # The real V5 pipeline consumes these files, not just the report presenter.
    from src.core.pipeline import V5Pipeline
    consumer = PairedV5Adapter(config(), sandbox=a["adapter"].root)
    with consumer._environment(value["observed_at"]):
        pipeline = V5Pipeline(consumer.cfg)
        assert pipeline._load_current_auto_risk_level() == "PROTECT"
    # Restart reads persisted risk state. A cannot skip directly to NEUTRAL,
    # and missing observation evidence never permits recovery.
    from src.risk.auto_risk_guard import _guard_instances
    _guard_instances.pop(str(a["adapter"].root / "reports/auto_risk_guard.json"), None)
    value["symbols"]["BTC/USDT"]["quote"].update(bid=100, ask=100.01)
    assert update_risk(a, value["symbols"], value["observed_at"], observation_gap=True)["current_level"] == "PROTECT"
    assert update_risk(a, value["symbols"], value["observed_at"], observation_gap=False)["current_level"] == "DEFENSE"


def test_three_own_realized_exit_losses_trigger_defense(tmp_path):
    comparison = factory(config(), EXPERIMENT, root=tmp_path)
    account = comparison.scenarios["30"][ACCOUNTS[0]]
    for offset in (360, 3960, 7560):
        record_risk_audit(account, {"counts": {"selected": 0}}, now=BAR + offset, bar=BAR + offset - 360, decisions=[])
        account["book"].closed_lots.append({"exit_ts": BAR + offset + 60, "net_pnl_usdt": -1})
    result = update_risk(account, frame(BAR + 11160)["symbols"], BAR + 11160, observation_gap=False)
    assert result["current_level"] == "DEFENSE"
    assert result["metrics"]["consecutive_losses"] == 3


def test_cold_start_never_disables_drawdown_protection(tmp_path):
    comparison = factory(config(), EXPERIMENT, root=tmp_path)
    account = comparison.scenarios["30"][ACCOUNTS[0]]
    account["book"].fill({"intent_id": "cold-start", "side": "buy", "symbol": "BTC/USDT", "notional_usdt": 80,
                         "decision_ts": BAR + 300}, frame()["symbols"]["BTC/USDT"], now=BAR + 360)
    value = frame(BAR + 3960)
    value["symbols"]["BTC/USDT"]["quote"].update(bid=70, ask=70.01)
    risk = update_risk(account, value["symbols"], value["observed_at"], observation_gap=False)
    assert risk["current_level"] == "PROTECT"
    assert risk["metrics"]["sample_size"] == 0
    assert risk["metrics"]["recovery_evidence_ok"] is False


@pytest.mark.parametrize("interval,expected", [(120, 3 / 5), (180, 3 / 7)])
def test_quote_coverage_counts_missing_minutes_even_before_entry_timeout(tmp_path, interval, expected):
    comparison = factory(config(), EXPERIMENT, root=tmp_path)
    for step in range(3):
        event = comparison.observe(frame(BAR + 500 + step * interval))
    report = summarize(comparison, event)
    assert report["coverage"]["quote_coverage_rate"] == pytest.approx(expected)
    assert report["coverage"]["observation_gaps"] == 0
    assert report["coverage"]["observed_quote_slots"] == 3
    assert report["coverage"]["missed_quote_slots"] == (2 if interval == 120 else 4)


def test_duplicate_quote_within_minute_is_not_extra_observation_coverage(tmp_path):
    comparison = factory(config(), EXPERIMENT, root=tmp_path)
    comparison.observe(frame(BAR + 500))
    event = comparison.observe(frame(BAR + 501))
    report = summarize(comparison, event)
    assert report["coverage"]["observation_count"] == 2
    assert report["coverage"]["observed_quote_slots"] == 1
    assert report["coverage"]["quote_coverage_rate"] == 1
