from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from src.strategy.participation_policy import (
    apply_fill, check_pending, decide, market_feature_series, mark_to_market,
    new_state, policy_hash, validate_policy,
)

CONFIG = json.loads((Path(__file__).resolve().parents[1] / "configs/research/participation_policy_v1.json").read_text(encoding="utf-8"))


def snapshot(now=360002, price=100.0):
    bar = int(now // 3600) * 3600
    return {"now_ts": now, "regime": "Trending", "symbols": {
        symbol: {"bar_ts": bar, "close": price, "ema20": 99., "ema20_4h_ago": 98.,
                 "ret4_bps": 200., "atr14": 1., "volume": 100000,
                 "rank_score": -i * .1, "cost_bps": 30.,
                 "quote": {"bid": price - .01, "ask": price + .01, "ts": now - 1}}
        for i, symbol in enumerate(CONFIG["symbols"])
    }}


def opened(config=None):
    cfg = config or CONFIG
    state = new_state(cfg)
    intent = decide(snapshot(), state, cfg)
    fill = check_pending(intent, snapshot(360012), state, cfg)
    updated, trade = apply_fill(state, fill, cfg)
    assert trade is None
    return updated, fill


def test_policy_binding_and_config_validation_fail_explicitly():
    assert policy_hash(CONFIG) == policy_hash(dict(reversed(list(CONFIG.items()))))
    for change in ({"mode": "live"}, {"live_promotion_allowed": True}, {"stop_mode": "unknown"}, {"fee_bps": float("nan")}, {"maximum_positions": 2}, {"bar_seconds": 60}, {"typo_risk_setting": .25}):
        with pytest.raises(ValueError):
            validate_policy({**CONFIG, **change})
    with pytest.raises(ValueError, match="state schema or policy hash"):
        decide(snapshot(), new_state(CONFIG), {**CONFIG, "fee_bps": 15.})


def test_features_are_causal_and_require_ordered_confirmed_complete_bars():
    bars = [{"end": (i + 1) * 3600, "open": 100 + i, "high": 102 + i, "low": 99 + i,
             "close": 101 + i, "volume": 1000} for i in range(30)]
    short = market_feature_series(bars[:20])
    full = market_feature_series(bars)
    assert all(row == full[ts] for ts, row in short.items())
    for bad in (bars[:3] + bars[4:], list(reversed(bars)), [*bars[:-1], {**bars[-1], "confirmed": False}]):
        with pytest.raises(ValueError):
            market_feature_series(bad)


def test_negative_relative_rank_does_not_veto_positive_direction():
    snap = snapshot()
    for row in snap["symbols"].values():
        row["rank_score"] -= 20
    intent = decide(snap, new_state(CONFIG), CONFIG)
    assert intent["action"] == "entry_intent"
    assert intent["symbol"] == "BTC/USDT"
    assert intent["planned_loss_usdt"] <= intent["risk_budget_usdt"]


@pytest.mark.parametrize("kind", ["future_quote", "stale_quote", "future_bar", "unknown_cost"])
def test_unobservable_inputs_never_create_entry(kind):
    snap = snapshot()
    for row in snap["symbols"].values():
        if kind == "future_quote":
            row["quote"]["ts"] = snap["now_ts"] + 1
        elif kind == "stale_quote":
            row["quote"]["ts"] -= 60
        elif kind == "future_bar":
            row["bar_ts"] += 3600
        else:
            row["cost_bps"] = None
    assert decide(snap, new_state(CONFIG), CONFIG)["action"] == "hold"


def test_signal_quote_cannot_fill_and_expired_entry_does_not_wait_forever():
    state = new_state(CONFIG)
    intent = decide(snapshot(), state, CONFIG)
    assert check_pending(intent, snapshot(), state, CONFIG)["action"] == "wait"
    late = snapshot(370802)
    late["symbols"][intent["symbol"]]["quote"] = {}
    assert check_pending(intent, late, state, CONFIG)["reason"] == "entry_intent_expired"


def test_pending_entry_rechecks_direction_and_cost_without_requiring_top_rank():
    state = new_state(CONFIG)
    intent = decide(snapshot(), state, CONFIG)
    invalid = snapshot(363602, price=98.)
    assert check_pending(intent, invalid, state, CONFIG)["reason"] == "execution_direction_invalidated"
    changed = snapshot(363602)
    changed["symbols"][intent["symbol"]]["rank_score"] = -100
    assert check_pending(intent, changed, state, CONFIG)["action"] == "fill"
    changed["symbols"][intent["symbol"]]["cost_bps"] = 300
    assert check_pending(intent, changed, state, CONFIG)["reason"] == "execution_momentum_invalidated"


def test_price_stop_does_not_tighten_when_fee_scenario_increases():
    distances = []
    for fee in (15., 30., 60.):
        cfg = {**CONFIG, "fee_bps": fee, "slippage_bps": 0.}
        intent = decide(snapshot(), new_state(cfg), cfg)
        distances.append(1 - intent["stop_price"] / intent["price"])
    assert distances == pytest.approx([.012] * 3)
    cfg = {**CONFIG, "stop_mode": "legacy_net_120bps", "fee_bps": 60., "slippage_bps": 0.}
    intent = decide(snapshot(), new_state(cfg), cfg)
    assert (1 - intent["stop_price"] / intent["price"]) * 10000 < 1


def test_atr_stop_keeps_risk_budget_and_minimum_order_cannot_inflate_it():
    cfg = {**CONFIG, "stop_mode": "atr14_2x", "fee_bps": 30., "slippage_bps": 0.}
    snap = snapshot()
    for row in snap["symbols"].values():
        row["atr14"] = 10.
    state = new_state(cfg)
    original = copy.deepcopy(state)
    intent = decide(snap, state, cfg)
    assert intent["action"] == "hold"
    assert set(intent["candidate_reasons"].values()) == {"minimum_notional_exceeds_risk_budget"}
    assert state == original


def test_exact_cash_fee_identity_duplicate_fill_and_same_bar_reentry():
    state, buy = opened()
    original = copy.deepcopy(state)
    duplicate, trade = apply_fill(state, buy, CONFIG)
    assert duplicate == original and trade is None
    snap = snapshot(363602, price=98.)
    exit_intent = decide(snap, state, CONFIG)
    assert exit_intent["action"] == "exit_intent"
    sell = check_pending(exit_intent, snapshot(363612, price=97.), state, CONFIG)
    closed_state, trade = apply_fill(state, sell, CONFIG)
    fee = CONFIG["fee_bps"] / 10000
    expected = state["position"]["qty"] * sell["price"] * (1 - fee) - state["position"]["cash_spent"]
    assert trade["net_pnl_usdt"] == pytest.approx(expected)
    assert closed_state["cash_usdt"] == pytest.approx(CONFIG["initial_cash_usdt"] + expected)
    assert closed_state["position"] is None
    assert decide(snapshot(363622), closed_state, CONFIG)["action"] == "hold"
    assert state == original


def test_stale_source_and_missing_quote_do_not_erase_pending_exit():
    state, _ = opened()
    bad = snapshot(363602)
    bad["regime"] = "Risk-Off"
    bad["operational_block"] = True
    intent = decide(bad, state, CONFIG)
    assert intent["reason"] == "risk_off"
    bad["symbols"][intent["symbol"]]["quote"] = {}
    assert check_pending(intent, bad, state, CONFIG)["action"] == "wait"
    next_quote = snapshot(363622)
    next_quote["regime"] = "UNKNOWN"
    next_quote["operational_block"] = True
    assert check_pending(intent, next_quote, state, CONFIG)["action"] == "fill"


@pytest.mark.parametrize("change", [{"price": float("nan")}, {"fee_bps": float("inf")}, {"fee_bps": 12.}, {"quote_ts": float("nan")}, {"fill_ts": 0}, {"quote_ts": 360001}, {"side": "oops"}, {"fill_id": None}, {"stop_price": -1.}, {"planned_loss_usdt": -1.}])
def test_accounting_rejects_nonfinite_or_noncausal_fill(change):
    state = new_state(CONFIG)
    intent = decide(snapshot(), state, CONFIG)
    fill = check_pending(intent, snapshot(360012), state, CONFIG)
    with pytest.raises(ValueError):
        apply_fill(state, {**fill, **change}, CONFIG)


def test_trial_drawdown_detected_on_marked_liquidation_equity():
    state, _ = opened()
    marked = mark_to_market(state, snapshot(363602, price=60.), CONFIG)
    assert marked["halted"]
    assert decide(snapshot(363602, price=60.), marked, CONFIG)["reason"] == "trial_drawdown_stop"


def test_remaining_daily_loss_caps_entry_and_pending_fill_before_threshold():
    state = new_state(CONFIG)
    intent = decide(snapshot(), state, CONFIG)
    state["daily_realized_pnl"][str(360002 // 86400)] = -.95
    blocked = decide(snapshot(), state, CONFIG)
    assert blocked["action"] == "hold"
    assert set(blocked["candidate_reasons"].values()) == {"minimum_notional_exceeds_risk_budget"}
    assert check_pending(intent, snapshot(360012), state, CONFIG)["reason"] == "minimum_notional_exceeds_risk_budget"


def test_remaining_trial_loss_caps_new_trade_without_resetting_peak():
    state = new_state(CONFIG)
    state["peak_equity_usdt"] = (state["equity_usdt"] - .05) / .97
    intent = decide(snapshot(), state, CONFIG)
    assert intent["action"] == "hold"
    assert not state["halted"]
    assert set(intent["candidate_reasons"].values()) == {"minimum_notional_exceeds_risk_budget"}


def test_missing_mark_is_explicit_and_does_not_invent_a_liquidation_price():
    state, _ = opened()
    marked = mark_to_market(state, snapshot(360022), CONFIG)
    stale = snapshot(363602)
    stale["symbols"][state["position"]["symbol"]]["quote"]["ts"] = 360021
    unknown = mark_to_market(marked, stale, CONFIG)
    assert not unknown["valuation_valid"]
    assert unknown["valuation_status"] == "stale_quote"
    assert unknown["equity_usdt"] == marked["equity_usdt"]
    assert unknown["last_valuation_quote_ts"] == marked["last_valuation_quote_ts"]
