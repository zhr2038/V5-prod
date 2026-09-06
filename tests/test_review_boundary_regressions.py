"""R1-R3 real-module regressions; all quotes, times and accounts are synthetic."""
import copy
import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.quant_lab_client.reference_consumer import record_cycle
from src.research.participation_comparison import ParticipationComparison
from src.research.quote_portfolio import QuotePortfolio
from src.research.review_comparison import Comparison
from src.research.review_forward import references_at
from tests.test_decision_reference_consumer import NOW, payload
from tests.test_participation_comparison import observed
from tests.test_participation_policy import CONFIG
from tests.test_quote_portfolio import intent, row
from tests.test_review_comparison import EXPERIMENT, POLICY, config, frame

CONTRACT = {
    "reference_schema": "qlab.decision.result.v2",
    "experiment_version": "trend-reference-1h-v2",
    "strategy_version": "context-trend-24h-v1",
    "cost_version": "current-cost-v1",
    "analysis_source_identity": "a" * 40,
    "horizon_hours": 24,
}


def reference(now, **changes):
    return {**CONTRACT, "advice_id": "advice-" + "b" * 64,
            "first_received_ts": now - 10, "published_ts": now - 20,
            "expires_ts": now + 300, "research_evaluable": True,
            "live_execution_eligible": False, "live_order_effect": "none",
            "action": "DEFER", **changes}


def comparison(monkeypatch, tmp_path):
    import src.research.review_comparison as module

    monkeypatch.setattr(module, "OriginalV5Adapter", lambda cfg, sandbox: SimpleNamespace(
        decide=lambda **kwargs: ([], {"regime": "Trending"}), root=sandbox))

    def snapshot(**kwargs):
        value = observed(kwargs["now"])
        value["data_errors"] = []
        for symbol, item in value["symbols"].items():
            item["quote"] = copy.deepcopy(kwargs["top_of_book"][symbol])
        return value

    monkeypatch.setattr(module, "build_snapshot", snapshot)
    experiment = copy.deepcopy(EXPERIMENT)
    experiment["decision_clock"] = {"offset_seconds": 360, "maximum_lateness_seconds": 120,
                                    "scope": "all_cohorts_shared_cutoff"}
    experiment["reference_contract"] = CONTRACT
    result = Comparison(config(), POLICY, experiment, root=tmp_path)
    for scenario in result.scenarios.values():
        scenario["D_reference_only"].reference_contract = CONTRACT
    return result


def market(at, references=None):
    value = frame(at)
    for item in value["symbols"].values():
        item["quote"].update(bid=100, ask=100.01)
    value["references"] = references or {}
    return value


def test_r1_normal_nas_completion_has_one_common_later_decision(monkeypatch, tmp_path):
    runner = comparison(monkeypatch, tmp_path)
    hour = 1788652800
    early = runner.observe(market(hour + 50))
    assert early["scenarios"]["30"]["cohorts"]["C_hold24_only"]["decision"] is None
    available = reference(hour + 400)
    result = runner.observe(market(hour + 410, {"BTC/USDT": available}))
    cohorts = result["scenarios"]["30"]["cohorts"]
    assert cohorts["C_hold24_only"]["decision"]["action"] == "entry_intent"
    assert cohorts["D_reference_only"]["decision"]["action"] == "reference_deferred_entry"
    assert cohorts["C_hold24_only"]["snapshot_hash"] == cohorts["D_reference_only"]["snapshot_hash"]
    assert cohorts["C_hold24_only"]["decision"]["decision_ts"] == hour + 410
    assert cohorts["D_reference_only"]["decision"]["decision_ts"] == hour + 410
    funnel = runner.metrics["reference_funnel:30"]
    assert funnel["candidates"] == funnel["valid_reference_coverage"] == funnel["defer_eligible"] == funnel["decisions_changed"] == 1
    restored = Comparison(config(), POLICY, runner.experiment, root=tmp_path, checkpoint=runner.checkpoint())
    restored.observe(market(hour + 420, {"BTC/USDT": available}))
    assert restored.metrics["reference_funnel:30"]["candidates"] == 1


@pytest.mark.parametrize("case", ["expired_at_hour", "nas_late", "consumer_failure", "future_publication"])
def test_r1_unavailable_reference_never_shifts_only_treatment(monkeypatch, tmp_path, case):
    runner = comparison(monkeypatch, tmp_path)
    hour = 1788652800
    ref = reference(hour + 410)
    if case == "expired_at_hour":
        ref["expires_ts"] = hour
    elif case == "nas_late":
        ref["first_received_ts"] = hour + 500
    elif case == "future_publication":
        ref["published_ts"] = hour + 500
    refs = {} if case == "consumer_failure" else {"BTC/USDT": ref}
    runner.observe(market(hour + 50))
    event = runner.observe(market(hour + 410, refs))
    cohorts = event["scenarios"]["30"]["cohorts"]
    assert cohorts["C_hold24_only"]["decision"]["action"] == "entry_intent"
    assert cohorts["D_reference_only"]["decision"]["action"] == "entry_intent"
    assert cohorts["C_hold24_only"]["snapshot_hash"] == cohorts["D_reference_only"]["snapshot_hash"]
    late = runner.observe(market(hour + 530, {"BTC/USDT": reference(hour + 530)}))
    assert (late["scenarios"]["30"]["cohorts"]["D_reference_only"]["decision"] or {}).get("action") != "reference_deferred_entry"
    assert late["hourly_decision"] is False
    assert runner.metrics["reference_funnel:30"]["candidates"] == 1
    assert runner.metrics["reference_funnel:30"]["valid_reference_coverage"] == 0
    assert runner.metrics["reference_funnel:30"]["decisions_changed"] == 0
    assert runner.metrics["reference_funnel:30"]["late_arrivals"] == 1
    with pytest.raises(ValueError, match="duplicate_or_reversed"):
        runner.observe(market(hour + 530))


def test_r1_missed_cutoff_does_not_backfill_an_entry(monkeypatch, tmp_path):
    runner = comparison(monkeypatch, tmp_path)
    result = runner.observe(market(1788652800 + 600))
    assert result["scenarios"]["30"]["cohorts"]["C_hold24_only"]["decision"] is None


def test_r1_treatment_only_candidate_after_divergence_is_counted_but_not_matched(monkeypatch, tmp_path):
    runner = comparison(monkeypatch, tmp_path)
    at = 1788652800 + 410
    runner.observe(market(at, {"BTC/USDT": reference(at)}))
    runner.observe(market(at + 10))
    event = runner.observe(market(at + 3600, {"BTC/USDT": reference(at + 3600)}))
    funnel = runner.metrics["reference_funnel:30"]
    assert funnel["candidates"] == funnel["decisions_changed"] == 2
    assert funnel["matched_candidates"] == funnel["independent_changed_opportunities"] == 1
    use = event["scenarios"]["30"]["reference_decisions"][0]
    assert use["treatment_candidate"] and not use["control_candidate"]


def funded(min_notional="10", lot="0.000001"):
    book = QuotePortfolio(initial_cash=100, fee_bps=10, slippage_bps=0)
    market_row = row(buy_fee="BNB", lot=lot)
    market_row["instrument"]["minimum_notional_usdt"] = min_notional
    book.fill({**intent(notional=10.5), "metadata": {"entry_price": 100, "entry_bar_ts": 0, "stop_price": 90}}, market_row, now=101)
    book.slippage = Decimal("0.0005")
    return book, market_row


def test_r2_unexecutable_asset_keeps_economic_value_cost_and_active_management():
    book, market_row = funded()
    market_row["quote"].update(bid=95, ask=95, ts=102)
    mark = book.mark({"BNB/USDT": market_row}, now=102)
    assert float(mark["equity_usdt"]) == pytest.approx(99.4500824450125)
    assert mark["liquidation_value_usdt"] == 0
    assert book.positions["BNB/USDT"]["cash_cost"] == Decimal("10.5")
    candidate = ParticipationComparison(CONFIG)
    candidate.book = book
    candidate._sync({"now_ts": 102, "symbols": {"BNB/USDT": market_row}})
    assert candidate.state["position"]["symbol"] == "BNB/USDT"
    assert candidate.state["halted"] is False
    market_row["quote"].update(bid=96, ask=96, ts=103)
    assert float(book.mark({"BNB/USDT": market_row}, now=103)["equity_usdt"]) == pytest.approx(99.55482015496)
    assert book.positions["BNB/USDT"]["qty"] == Decimal("0.104895")


def test_r2_entry_floor_does_not_limit_sell_or_zero_residual_value():
    book, market_row = funded(min_notional="0", lot="0.001")
    market_row["entry_minimum_notional_usdt"] = 10
    market_row["quote"].update(bid=80, ask=80, ts=103)
    book.fill({**intent(side="sell", key="hard-stop", decision=102, notional=8.4),
               "quantity": book.positions["BNB/USDT"]["qty"], "reason": "hard_stop"}, market_row, now=103)
    mark = book.mark({"BNB/USDT": market_row}, now=103)
    assert book.fills[-1]["side"] == "sell"
    assert mark["equity_usdt"] > book.cash
    assert book.positions["BNB/USDT"]["qty"] > 0
    assert book.positions["BNB/USDT"]["cash_cost"] > 0


def test_r2_research_entry_floor_rejects_new_risk_independently():
    book = QuotePortfolio(initial_cash=100, fee_bps=10, slippage_bps=0)
    market_row = row()
    market_row["entry_minimum_notional_usdt"] = 10
    with pytest.raises(ValueError, match="entry_minimum"):
        book.fill(intent(notional=5), market_row, now=101)
    assert not book.fills


def test_r2_unexecutable_hard_exit_remains_pending_and_recovers_without_new_entry():
    book, item = funded()
    candidate = ParticipationComparison(CONFIG)
    candidate.book = book

    def source(now, bid):
        value = observed(now, bid)
        item["quote"].update(bid=bid, ask=bid, ts=now)
        value["symbols"]["BNB/USDT"].update(copy.deepcopy(item))
        return value

    event = candidate.observe(source(104, 80))
    assert event["decision"]["reason"] == "hard_stop"
    assert candidate.observe(source(105, 80))["execution"]["action"] == "execution_constraint_rejected"
    assert candidate.state["position"]["symbol"] == "BNB/USDT"
    assert candidate.state["pending"]["action"] == "exit_intent"
    assert not candidate.state["halted"]
    candidate.observe(source(106, 96))
    assert [fill["side"] for fill in book.fills] == ["buy", "sell"]
    assert not candidate.state["halted"]
    assert not book.positions  # This exact lot sells all units; no replacement buy.


@pytest.mark.parametrize("field", list(CONTRACT))
def test_r3_unknown_upstream_semantics_cannot_defer_original_experiment(field):
    candidate = ParticipationComparison(CONFIG, use_reference=True)
    candidate.reference_contract = CONTRACT
    changed = reference(360100)
    changed[field] = 4 if field == "horizon_hours" else "unknown-version"
    assert candidate._reference_defer(changed, 360100) is False


def test_r3_legal_content_refresh_and_display_metadata_keep_semantics():
    candidate = ParticipationComparison(CONFIG, use_reference=True)
    candidate.reference_contract = CONTRACT
    ref = reference(360100)
    assert candidate._reference_defer(ref, 360100)
    assert candidate._reference_defer({**ref, "display_version": "new-ui", "advice_id": "advice-" + "c" * 64}, 360100)
    assert not candidate._reference_defer({**ref, "first_received_ts": 360101}, 360100)


def test_r3_first_receipt_preserves_signed_source_and_all_semantic_versions(tmp_path):
    value = payload()
    value["worker_commit"] = CONTRACT["analysis_source_identity"]
    value["advice"][0]["strategy_version"] = CONTRACT["strategy_version"]
    event = record_cycle(payload=value, contexts=[], now=NOW, output=tmp_path)
    receipt = event["receipts"][0]
    for key in ("reference_schema", "analysis_source_identity", "strategy_version"):
        assert receipt.get(key) == CONTRACT[key]
    original = json.dumps(value, sort_keys=True)
    record_cycle(payload=value, contexts=[], now=NOW + 1, output=tmp_path)
    assert json.dumps(value, sort_keys=True) == original


def test_receipt_reader_keeps_expired_versions_and_never_blocks_on_consumer_failure(tmp_path):
    value = payload()
    value["worker_commit"] = CONTRACT["analysis_source_identity"]
    value["advice"][0]["strategy_version"] = CONTRACT["strategy_version"]
    value["advice"][0]["horizon_hours"] = 24
    record_cycle(payload=value, contexts=[], now=NOW, output=tmp_path)
    first = references_at(tmp_path / "receipts.sqlite", NOW + 1)["BNB/USDT"]
    assert all(first[k] == CONTRACT[k] for k in CONTRACT)
    assert first["first_received_ts"] == NOW
    expired = references_at(tmp_path / "receipts.sqlite", NOW + 3600)["BNB/USDT"]
    assert expired["expires_ts"] < NOW + 3600
    broken = tmp_path / "broken.sqlite"
    broken.write_bytes(b"invalid database")
    status = {}
    assert references_at(broken, NOW, diagnostics=status) == {}
    assert status["status"] == "reference_read_failed"
