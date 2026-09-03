from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from src.research.rally_reentry_validation import (
    HOUR, SYMBOLS, Bar, build_signals, exit_price, prepare_market,
    quote_block, raw_policy_decision, simulate,
)

PROTOCOL = json.loads((Path(__file__).resolve().parents[1] / 'docs/rally_validation_protocol.json').read_text())
EXECUTION = PROTOCOL['shared_execution']
POLICIES = {row['name']: row for row in PROTOCOL['policies']}


def market_payload(prices: list[float]) -> dict:
    return {'symbols': {symbol.replace('/', '-'): {'bars': [
        [str(index * HOUR * 1000), str(px), str(px * 1.001), str(px * .999), str(px), '100000', '0', '0', '1']
        for index, px in enumerate(prices)]} for symbol in SYMBOLS}}


def record(end: int, *, target: bool = True, alpha: float = .45) -> dict:
    factors = {'f4_volume_expansion': .2, 'f5_rsi_trend_confirm': .4}
    quote = {'arrival_bid': 99.99, 'arrival_ask': 100.01, 'quote_age_ms': 1000,
             'quote_ts': end + 1, 'ts_utc': end + 2, 'current_px': 100,
             'selected_entry_gate_cost_bps': 30, 'final_score': alpha}
    return {'run_id': f'run-{end}', 'audit': {
        'regime': 'Trending', 'router_decisions': [],
        'targets_post_risk': {symbol: .1 for symbol in SYMBOLS} if target else {},
        'alpha_factor_snapshot': {symbol: {'z_factors': factors,
             'raw_factors': {'alpha6_final_score': 1.8, 'alpha6_relative_score': .01}} for symbol in SYMBOLS}},
        'candidates': {symbol: dict(quote) for symbol in SYMBOLS},
        'signals': {'Alpha6Factor': {symbol: {'side': 'buy', 'score': alpha, 'metadata': {}} for symbol in SYMBOLS}}}


def small_replay(*, cash: float = 106.86, prices: list[float] | None = None, overrides: dict | None = None) -> tuple:
    prices = prices or [100] * 5
    bars, _ = prepare_market(market_payload(prices))
    records = {stamp: record(stamp) for stamp in range(HOUR, (len(prices) + 1) * HOUR, HOUR)}
    signal = {'symbol': SYMBOLS[0], 'run_id': 'first', 'kind': 'normal', 'values': {'reference_px': prices[0]}}
    execution = {**EXECUTION, 'initial_cash_usdt': cash, **(overrides or {})}
    return {HOUR: [signal]}, bars, records, execution, HOUR, len(prices) * HOUR


def test_causal_features_and_signals_are_unchanged_when_future_prices_are_added():
    prefix = [100 + index * .3 for index in range(90)]
    _, before = prepare_market(market_payload(prefix))
    _, extended = prepare_market(market_payload(prefix + [70, 170, 40, 210]))
    records = {stamp: record(stamp) for stamp in range(30 * HOUR, 91 * HOUR, HOUR)}
    for symbol in SYMBOLS:
        assert all(extended[symbol][stamp] == values for stamp, values in before[symbol].items())
    first, _ = build_signals(records, before, POLICIES['direction_rank_simple'], EXECUTION, 30)
    second, _ = build_signals(records, extended, POLICIES['direction_rank_simple'], EXECUTION, 30)
    assert first == second
    assert any(first.values())


def test_missing_candle_and_unconfirmed_candle_fail_closed():
    payload = market_payload([100, 101, 102])
    payload['symbols']['BTC-USDT']['bars'].pop(1)
    with pytest.raises(ValueError, match='missing candle'):
        prepare_market(payload)
    payload = market_payload([100, 101])
    payload['symbols']['BTC-USDT']['bars'][-1][-1] = '0'
    with pytest.raises(ValueError, match='unconfirmed'):
        prepare_market(payload)


@pytest.mark.parametrize(('change', 'reason'), [
    ({'arrival_ask': None}, 'quote_unobservable'),
    ({'quote_age_ms': 31000}, 'stale_quote'),
    ({'quote_ts': HOUR + 20}, 'quote_or_decision_timestamp_invalid'),
    ({'arrival_ask': 101}, 'spread_too_wide'),
])
def test_bad_quotes_cannot_enter(change, reason):
    quote = record(HOUR)['candidates'][SYMBOLS[0]]
    assert quote_block({**quote, **change}, HOUR, EXECUTION) == reason


def test_missing_recorded_hour_cannot_count_as_consecutive_confirmation():
    records = {HOUR: record(HOUR), 3 * HOUR: record(3 * HOUR)}
    signals, _ = build_signals(records, {}, POLICIES['current_confirmation'], EXECUTION, 30)
    assert not any(signals.values())
    records[2 * HOUR] = record(2 * HOUR)
    signals, _ = build_signals(records, {}, POLICIES['current_confirmation'], EXECUTION, 30)
    assert len(signals[3 * HOUR]) == 4


def test_live_confirmation_uses_prior_alpha_even_without_prior_target():
    records = {HOUR: record(HOUR, target=False), 2 * HOUR: record(2 * HOUR)}
    signals, _ = build_signals(records, {}, POLICIES['current_confirmation'], EXECUTION, 30)
    assert not signals[HOUR]
    assert len(signals[2 * HOUR]) == 4


def test_relative_score_does_not_veto_positive_absolute_direction():
    _, features = prepare_market(market_payload([100 + i * .4 for i in range(80)]))
    end = 80 * HOUR
    source = record(end, alpha=.01)
    source['audit']['alpha_factor_snapshot'][SYMBOLS[0]]['raw_factors']['alpha6_relative_score'] = -.02
    source['signals']['Alpha6Factor'][SYMBOLS[0]]['side'] = 'sell'
    old = raw_policy_decision(POLICIES['current_confirmation'], source, SYMBOLS[0], end, features, 30)
    simplified = raw_policy_decision(POLICIES['direction_rank_simple'], source, SYMBOLS[0], end, features, 30)
    assert not old['eligible']
    assert simplified['eligible']
    assert simplified['rank_score'] == -.02


def test_delayed_entry_cannot_profit_from_earlier_intrabar_high():
    args = small_replay()
    bars = args[1]
    bars[SYMBOLS[0]][2 * HOUR] = Bar(2 * HOUR, 100, 150, 70, 100, 100000)
    result = simulate(*args, 30)
    assert len(result['trades']) == 1
    trade = result['trades'][0]
    assert trade['entry_ts'] == 2 * HOUR
    assert trade['exit_ts'] == 5 * HOUR
    assert trade['exit_reason'] == 'end_of_window'
    assert trade['net_pnl_usdt'] == pytest.approx(-10.5 * .003)
    assert result['metrics']['ending_cash_usdt'] == pytest.approx(106.86 - .0315)


def test_same_bar_stop_precedes_target_and_gap_uses_worse_open():
    ambiguous = Bar(HOUR, 100, 110, 90, 100, 1000)
    price, reason = exit_price(ambiguous, 100, .0015, -120, 80)
    assert reason == 'hard_stop'
    assert (price * (1 - .0015) / (100 * (1 + .0015)) - 1) * 10000 == pytest.approx(-120)
    gap = Bar(HOUR, 90, 110, 89, 100, 1000)
    assert exit_price(gap, 100, .0015, -120, 80) == (90, 'hard_stop')


def test_minimum_order_never_expands_the_risk_budget_or_borrows():
    too_small = simulate(*small_replay(cash=100), 30)
    assert too_small['metrics']['closed_trades'] == 0
    assert too_small['metrics']['execution_skips'] == {'minimum_notional_exceeds_risk_budget': 1}
    enough = simulate(*small_replay(), 30)
    assert enough['metrics']['closed_trades'] == 1
    assert enough['metrics']['maximum_entry_weight_pct'] <= 10
    assert all(row['cash_usdt'] >= 0 for row in enough['equity_curve'])


def test_risk_off_at_execution_cancels_pending_buy():
    args = small_replay()
    args[2][2 * HOUR]['audit']['regime'] = 'Risk-Off'
    result = simulate(*args, 30)
    assert result['trades'] == []
    assert result['metrics']['execution_skips']['execution_regime_unobservable_or_risk_off'] == 1


def test_cooldown_and_single_position_prevent_overlapping_repeated_entries():
    args = list(small_replay(prices=[100] * 15, overrides={'maximum_holding_hours': 1}))
    args[0] = {stamp: copy.deepcopy(args[0][HOUR]) for stamp in range(HOUR, 15 * HOUR, HOUR)}
    result = simulate(*args, 30)
    assert [trade['entry_ts'] // HOUR for trade in result['trades']] == [2, 12]
    first, second = result['trades']
    assert second['entry_ts'] >= first['exit_ts'] + 8 * HOUR
    assert second['entry_ts'] > first['exit_ts']
    assert result['metrics']['net_pnl_usdt'] == pytest.approx(sum(t['net_pnl_usdt'] for t in result['trades']))


def test_chasing_a_jump_cancels_entry():
    result = simulate(*small_replay(prices=[100, 102, 102, 102]), 30)
    assert result['trades'] == []
    assert result['metrics']['execution_skips']['entry_price_premium'] == 1


def test_trend_exit_can_hold_above_fixed_target_but_exits_below_causal_average():
    prices = [100, 100, 120, 130, 125, 110, 105, 104]
    args = small_replay(prices=prices, overrides={
        'exit_mode': 'ema20_trend', 'take_profit_net_bps': None, 'maximum_holding_hours': 72})
    _, features = prepare_market(market_payload(prices))
    result = simulate(*args, 30, features=features)
    trade = result['trades'][0]
    assert trade['entry_ts'] == 2 * HOUR
    assert trade['exit_ts'] == 7 * HOUR
    assert trade['exit_reason'] == 'ema20_trend_exit'
    assert trade['exit_px'] == 105


def test_trend_exit_preserves_hard_stop_and_requires_observable_features():
    args = small_replay(prices=[100, 100, 98, 100], overrides={
        'exit_mode': 'ema20_trend', 'take_profit_net_bps': None, 'maximum_holding_hours': 72})
    with pytest.raises(ValueError, match='causal market features'):
        simulate(*args, 30)
    _, features = prepare_market(market_payload([100, 100, 98, 100]))
    result = simulate(*args, 30, features=features)
    assert result['trades'][0]['exit_reason'] == 'hard_stop'
    assert result['trades'][0]['net_bps'] < -120
