"""Offline entry-policy comparisons using recorded signals and causal candles.

This module has no exchange client, credentials, production state writes, or
live promotion path. Its common execution model is not a full V5 backtest.
"""
from __future__ import annotations

import collections
import math
import random
import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Any

HOUR = 3600
SYMBOLS = ('BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT')


def number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def epoch(value: Any) -> float | None:
    if isinstance(value, str) and ('T' in value or '-' in value):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()
        except ValueError:
            return None
    result = number(value)
    return result / 1000 if result is not None and result > 10_000_000_000 else result


@dataclass(frozen=True)
class Bar:
    end: int
    open: float
    high: float
    low: float
    close: float
    volume: float


def prepare_market(payload: dict) -> tuple[dict, dict]:
    """All indicator values at t depend exclusively on candles closed by t."""
    bars, features = {}, {}
    for instrument, source in payload['symbols'].items():
        symbol = instrument.replace('-', '/')
        if symbol not in SYMBOLS:
            continue
        symbol_bars, symbol_features = {}, {}
        ema20 = ema60 = None
        previous_end = None
        for raw in sorted(source['bars'], key=lambda row: int(row[0])):
            if len(raw) != 9 or raw[-1] != '1':
                raise ValueError(f'{symbol}: unconfirmed candle is not admissible')
            if int(raw[0]) % (HOUR * 1000):
                raise ValueError(f'{symbol}: candle is not an hour boundary')
            end = int(raw[0]) // 1000 + HOUR
            values = [number(value) for value in raw[1:6]]
            if any(value is None for value in values):
                raise ValueError(f'{symbol}: nonfinite candle at {end}')
            o, h, low, close, volume = values
            if min(o, h, low, close) <= 0 or volume < 0 or h < max(o, close, low) or low > min(o, close):
                raise ValueError(f'{symbol}: invalid OHLCV at {end}')
            if previous_end is not None and end != previous_end + HOUR:
                raise ValueError(f'{symbol}: duplicate or missing candle before {end}')
            bar = Bar(end, o, h, low, close, volume)
            symbol_bars[end] = bar
            ema20 = close if ema20 is None else close * (2 / 21) + ema20 * (19 / 21)
            ema60 = close if ema60 is None else close * (2 / 61) + ema60 * (59 / 61)
            before4 = symbol_bars.get(end - 4 * HOUR)
            before24 = symbol_bars.get(end - 24 * HOUR)
            before_ema4 = symbol_features.get(end - 4 * HOUR)
            symbol_features[end] = {
                'close': close, 'ema20': ema20, 'ema60': ema60,
                'ema20_4h_ago': before_ema4['ema20'] if before_ema4 else None,
                'ret4_bps': (close / before4.close - 1) * 10000 if before4 else None,
                'ret24_bps': (close / before24.close - 1) * 10000 if before24 else None,
            }
            previous_end = end
        bars[symbol], features[symbol] = symbol_bars, symbol_features
    if set(bars) != set(SYMBOLS):
        raise ValueError('All four whitelist symbols are required')
    return bars, features


def prepare_records(payload: dict, bars: dict) -> tuple[dict, dict]:
    records = {}
    quality = {'runs': 0, 'quotes': 0, 'price_checks': 0, 'price_mismatches': [], 'unknown_regime_runs': [], 'fingerprints': {}}
    fingerprints = collections.Counter()
    for raw in payload['runs']:
        audit = raw['audit']
        end = int(audit['window_end_ts'])
        if end in records:
            raise ValueError(f'Duplicate decision window: {end}')
        if end % HOUR:
            raise ValueError(f'Decision window is not an hour boundary: {end}')
        candidates = {row['symbol']: row for row in raw.get('candidate_snapshot.csv', [])}
        if set(candidates) != set(SYMBOLS):
            raise ValueError(f'{raw["folder"]}: missing candidate rows')
        signals = {}
        for strategy in audit.get('strategy_signals', []):
            key = strategy.get('strategy') or strategy.get('type')
            signals[key] = {row['symbol']: row for row in strategy.get('signals', [])}
        records[end] = {'run_id': raw['folder'], 'audit': audit, 'candidates': candidates, 'signals': signals}
        quality['runs'] += 1
        fingerprints[audit.get('negative_expectancy_state', {}).get('config_fingerprint') or 'unknown'] += 1
        if audit.get('regime') not in ('Trending', 'Sideways', 'Risk-Off'):
            quality['unknown_regime_runs'].append(raw['folder'])
        for symbol, candidate in candidates.items():
            if number(candidate.get('arrival_bid')) and number(candidate.get('arrival_ask')):
                quality['quotes'] += 1
            close = number(candidate.get('current_px'))
            bar = bars[symbol].get(end)
            if bar is None:
                raise ValueError(f'{symbol}: no public candle for decision {end}')
            if bar and close is not None and candidate.get('price_source') == 'prices':
                quality['price_checks'] += 1
                if abs(close / bar.close - 1) > 0.000001:
                    quality['price_mismatches'].append({'run': raw['folder'], 'symbol': symbol, 'recorded': close, 'public_close': bar.close})
    quality['fingerprints'] = dict(fingerprints)
    return records, quality


def candidate_values(record: dict, symbol: str) -> dict:
    audit, candidate = record['audit'], record['candidates'][symbol]
    alpha = record['signals'].get('Alpha6Factor', {}).get(symbol, {})
    snapshot = audit.get('alpha_factor_snapshot', {}).get(symbol, {})
    raw = snapshot.get('raw_factors', {})
    metadata = alpha.get('metadata', {})
    z_factors = snapshot.get('z_factors', {}) or metadata.get('z_factors', {})
    return {
        'alpha_side': alpha.get('side'), 'alpha_score': number(alpha.get('score')),
        'f4': number(z_factors.get('f4_volume_expansion', candidate.get('f4_volume_expansion'))),
        'f5': number(z_factors.get('f5_rsi_trend_confirm', candidate.get('f5_rsi_trend_confirm'))),
        'absolute': number(raw.get('alpha6_final_score', metadata.get('final_score'))),
        'relative': number(record.get('research_rank_scores', {}).get(
            symbol, raw.get('alpha6_relative_score', metadata.get('relative_score')))),
        'final_score': number(candidate.get('final_score')),
        'cost_bps': number(candidate.get('selected_entry_gate_cost_bps', candidate.get('cost_bps'))),
        'reference_px': number(candidate.get('current_px')),
    }


def quote_block(candidate: dict, end: int, execution: dict) -> str | None:
    bid, ask = number(candidate.get('arrival_bid')), number(candidate.get('arrival_ask'))
    age = number(candidate.get('quote_age_ms'))
    quote_ts, decision_ts = epoch(candidate.get('quote_ts')), epoch(candidate.get('ts_utc'))
    if bid is None or ask is None or bid <= 0 or ask < bid or age is None or age < 0:
        return 'quote_unobservable'
    if age > execution['maximum_recorded_quote_age_ms']:
        return 'stale_quote'
    if decision_ts is None or quote_ts is None or quote_ts > decision_ts or not end <= decision_ts < end + HOUR:
        return 'quote_or_decision_timestamp_invalid'
    spread = (ask - bid) / ((ask + bid) / 2) * 10000
    return 'spread_too_wide' if spread > execution['maximum_spread_bps'] else None


def raw_policy_decision(policy: dict, record: dict, symbol: str, end: int, features: dict, cost_bps: float) -> dict:
    values = candidate_values(record, symbol)
    audit = record['audit']
    result = {'symbol': symbol, 'run_id': record['run_id'], 'signal_end': end, 'values': values,
              'eligible': False, 'reason': '', 'strong': False, 'kind': 'normal'}

    def blocked(reason: str) -> dict:
        result['reason'] = reason
        return result

    if audit.get('regime') not in ('Trending', 'Sideways'):
        return blocked('risk_off_or_unknown')
    if values['cost_bps'] is None or values['cost_bps'] < 0:
        return blocked('cost_unobservable')
    if values['reference_px'] is None or values['relative'] is None:
        return blocked('price_or_relative_rank_unobservable')
    gate_cost = max(cost_bps, values['cost_bps'])
    result['gate_cost_bps'] = gate_cost
    required = max(1.5 * gate_cost, 0.0)
    for route in audit.get('router_decisions', []):
        if route.get('symbol') != symbol or route.get('action') != 'skip':
            continue
        reason = route.get('reason', '')
        if ('negative_expectancy' in reason and 'no_closed' not in reason) or any(term in reason for term in ['kill_switch', 'reconcile_failed', 'ledger_failed']):
            return blocked('recorded_hard_gate:' + reason)

    name = policy['name']
    if name == 'current_confirmation':
        selected = float(audit.get('targets_post_risk', {}).get(symbol) or 0) > 0
        if not selected:
            breakout = any(r.get('symbol') == symbol and r.get('btc_leadership_probe') and r.get('breakout_met') for r in audit.get('router_decisions', []))
            if symbol != 'BTC/USDT' or not breakout:
                return blocked('not_selected_or_no_btc_breakout')
            result['kind'], result['strong'] = 'btc_probe', True
            if values['alpha_side'] != 'buy' or values['alpha_score'] is None or values['alpha_score'] < policy['btc_probe_alpha6_min_score']:
                return blocked('btc_alpha_confirmation')
            if values['f4'] is None or values['f4'] < policy['btc_probe_f4_min'] or values['f5'] is None or values['f5'] < policy['btc_probe_f5_min']:
                return blocked('btc_volume_or_rsi_confirmation')
        else:
            if values['alpha_side'] != 'buy' or values['alpha_score'] is None:
                return blocked('alpha_buy_missing')
            if values['f5'] is None or values['f5'] < policy['normal_f5_min']:
                return blocked('rsi_confirmation')
            if values['alpha_score'] < policy['normal_alpha6_min_score']:
                return blocked('alpha_score')
            if values['f4'] is None or values['f4'] < policy['normal_f4_min']:
                return blocked('volume_confirmation')
            result['strong'] = values['alpha_score'] >= policy['single_round_strong_alpha6'] and values['f5'] >= policy['single_round_strong_f5']
        score = values['final_score'] if values['final_score'] is not None else values['alpha_score']
        if score is None or (score - 0.18) / 0.003 < required:
            return blocked('score_proxy_below_cost')
        result['rank_score'] = score
    elif name == 'uncentered_alpha_control':
        if values['absolute'] is None or math.tanh(values['absolute']) < policy['absolute_alpha6_min_score']:
            return blocked('absolute_alpha_score')
        if values['f4'] is None or values['f4'] < policy['f4_min']:
            return blocked('volume_confirmation')
        if values['f5'] is None or values['f5'] < policy['f5_min']:
            return blocked('rsi_confirmation')
        if (math.tanh(values['absolute']) - 0.18) / 0.003 < required:
            return blocked('absolute_score_proxy_below_cost')
        result['rank_score'] = values['relative']
    elif name in ('direction_then_rank', 'direction_rank_simple'):
        current = features[symbol].get(end)
        if current is None or any(current.get(key) is None for key in ['ret4_bps', 'ret24_bps', 'ema20_4h_ago']):
            return blocked('causal_market_history_missing')
        if current['close'] <= current['ema20']:
            return blocked('price_below_ema20')
        if current['ret4_bps'] < max(policy['minimum_4h_momentum_bps'], gate_cost * policy['momentum_cost_multiplier']):
            return blocked('momentum_below_cost_buffer')
        if name == 'direction_rank_simple':
            if current['ema20'] <= current['ema20_4h_ago']:
                return blocked('ema20_slope_not_positive')
        else:
            if current['ema20'] <= current['ema60'] or current['ret24_bps'] <= 0:
                return blocked('absolute_direction_unconfirmed')
            if current['close'] / current['ema20'] - 1 > policy['maximum_ema20_premium_pct']:
                return blocked('ema20_premium_too_high')
            returns = [features[s].get(end, {}).get('ret4_bps') for s in SYMBOLS]
            if any(value is None for value in returns) or sum(value > 0 for value in returns) < policy['minimum_positive_whitelist_4h_count'] or returns[0] <= 0:
                return blocked('market_breadth_unconfirmed')
            if values['f4'] is None or values['f4'] < policy['f4_min'] or values['f5'] is None or values['f5'] < policy['f5_min']:
                return blocked('volume_or_rsi_confirmation')
        result['rank_score'] = values['relative']
    else:
        raise ValueError(f'Unknown offline policy: {name}')
    result['eligible'], result['reason'] = True, 'eligible_before_execution'
    return result


def build_signals(records: dict, features: dict, policy: dict, execution: dict, cost_bps: float) -> tuple[dict, dict]:
    signals, previous = {}, {}
    counts = collections.Counter()
    for end, record in sorted(records.items()):
        ready = []
        current = {}
        for symbol in SYMBOLS:
            decision = raw_policy_decision(policy, record, symbol, end, features, cost_bps)
            current[symbol] = decision['eligible']
            if policy['name'] == 'current_confirmation':
                # The live debounce checks previous Alpha6 evidence, even if the
                # prior target was not selected or a separate cost gate blocked it.
                v = decision['values']
                current[symbol] = (
                    v['alpha_side'] == 'buy'
                    and v['alpha_score'] is not None
                    and v['alpha_score'] >= policy['normal_alpha6_min_score']
                    and v['f4'] is not None and v['f4'] >= policy['normal_f4_min']
                    and v['f5'] is not None and v['f5'] >= policy['normal_f5_min']
                )
            if decision['eligible']:
                rounds = policy.get('confirmation_rounds', policy.get('normal_confirmation_rounds', 2))
                if rounds > 1 and not decision['strong'] and not previous.get((end - HOUR, symbol), False):
                    decision['eligible'], decision['reason'] = False, 'confirmation_not_stable'
                else:
                    block = quote_block(record['candidates'][symbol], end, execution)
                    if block:
                        decision['eligible'], decision['reason'] = False, block
            counts[decision['reason']] += 1
            if decision['eligible']:
                ready.append(decision)
        signals[end] = sorted(ready, key=lambda row: (-row['rank_score'], row['symbol']))
        previous.update({(end, symbol): value for symbol, value in current.items()})
    return signals, dict(counts)


def exit_price(bar: Bar, entry_px: float, leg_cost: float, stop_net_bps: float, take_net_bps: float | None) -> tuple[float, str] | None:
    adjustment = (1 + leg_cost) / (1 - leg_cost)
    stop = entry_px * (1 + stop_net_bps / 10000) * adjustment
    if bar.low <= stop:
        return min(bar.open, stop), 'hard_stop'
    if take_net_bps is not None:
        target = entry_px * (1 + take_net_bps / 10000) * adjustment
        if bar.high >= target:
            return target, 'take_profit'
    return None


def simulate(signals: dict, bars: dict, records: dict, execution: dict, start: int, end: int, cost_bps: float, *, features: dict | None = None) -> dict:
    """One position, delayed entries, real cash accounting, no overlapping trades."""
    if start >= end or not 0 <= cost_bps < 10000:
        raise ValueError('Invalid replay window or costs')
    if execution['maximum_positions'] != 1:
        raise ValueError('This comparison supports exactly one position')
    if not 0 < execution['target_weight'] <= execution['maximum_gross_weight'] <= 1:
        raise ValueError('Invalid position budget')
    exit_mode = execution.get('exit_mode', 'fixed')
    if exit_mode not in ('fixed', 'ema20_trend'):
        raise ValueError('Unknown exit policy')
    if exit_mode == 'ema20_trend' and features is None:
        raise ValueError('Trend exits require causal market features')
    leg_cost = cost_bps / 20000
    initial = float(execution['initial_cash_usdt'])
    cash, peak = initial, initial
    position = pending = None
    cooldown, loss_streak = {}, collections.Counter()
    daily_pnl, turnover = collections.defaultdict(float), collections.defaultdict(float)
    trades, curve = [], []
    skipped = collections.Counter()
    halted = False

    def close_trade(stamp: int, price: float, reason: str) -> None:
        nonlocal cash, position
        proceeds = position['qty'] * price * (1 - leg_cost)
        pnl = proceeds - position['cash_spent']
        cash += proceeds
        daily_pnl[stamp // 86400] += pnl
        turnover[stamp // 86400] += position['qty'] * price
        symbol = position['symbol']
        cooldown[symbol] = stamp + int(execution['same_symbol_cooldown_hours'] * HOUR)
        loss_streak[symbol] = loss_streak[symbol] + 1 if pnl < 0 else 0
        if loss_streak[symbol] >= 2:
            cooldown[symbol] = stamp + int(execution['two_losses_cooldown_hours'] * HOUR)
            loss_streak[symbol] = 0
        trades.append({**position, 'exit_ts': stamp, 'exit_px': price, 'exit_reason': reason,
                       'net_pnl_usdt': pnl, 'net_bps': pnl / position['cash_spent'] * 10000,
                       'hold_hours': (stamp - position['entry_ts']) / HOUR,
                       'cost_usdt': position['notional_usdt'] * leg_cost + position['qty'] * price * leg_cost})
        position = None

    for stamp in range(start, end + 1, HOUR):
        if any(stamp not in bars[symbol] for symbol in SYMBOLS):
            raise ValueError(f'Missing replay candle at {stamp}')
        day = stamp // 86400
        if position is not None and stamp > position['entry_ts']:
            bar = bars[position['symbol']][stamp]
            candidate_exit = exit_price(bar, position['entry_px'], leg_cost, execution['stop_loss_net_bps'], execution['take_profit_net_bps'])
            if candidate_exit is not None:
                close_trade(stamp, *candidate_exit)
            elif records.get(stamp, {}).get('audit', {}).get('regime') == 'Risk-Off':
                close_trade(stamp, bar.close, 'risk_off')
            elif exit_mode == 'ema20_trend':
                average = number(features.get(position['symbol'], {}).get(stamp, {}).get('ema20'))
                if average is None:
                    raise ValueError(f'Missing causal EMA20 for exit at {stamp}')
                if bar.close < average:
                    close_trade(stamp, bar.close, 'ema20_trend_exit')
                elif stamp - position['entry_ts'] >= execution['maximum_holding_hours'] * HOUR:
                    close_trade(stamp, bar.close, 'time_stop')
            elif stamp - position['entry_ts'] >= execution['maximum_holding_hours'] * HOUR:
                close_trade(stamp, bar.close, 'time_stop')

        equity = cash + (position['qty'] * bars[position['symbol']][stamp].close * (1 - leg_cost) if position else 0)
        peak = max(peak, equity)
        if 1 - equity / peak >= execution['maximum_trial_drawdown_pct']:
            halted = True
            if position is not None:
                close_trade(stamp, bars[position['symbol']][stamp].close, 'trial_drawdown_stop')
            equity = cash

        if pending is not None and pending['fill_ts'] == stamp:
            symbol = pending['symbol']
            bar = bars[symbol][stamp]
            notional = max(equity * execution['target_weight'], execution['minimum_order_notional_usdt'] * execution['minimum_executable_buffer'])
            reason = None
            if halted or position is not None:
                reason = 'trial_halted_or_position_open'
            elif stamp >= end:
                reason = 'insufficient_exit_horizon'
            elif records.get(stamp, {}).get('audit', {}).get('regime') not in ('Trending', 'Sideways'):
                reason = 'execution_regime_unobservable_or_risk_off'
            elif daily_pnl[day] <= -execution['maximum_daily_realized_loss_usdt']:
                reason = 'daily_loss_limit'
            elif stamp < cooldown.get(symbol, 0):
                reason = 'same_symbol_cooldown'
            elif notional > equity * execution['maximum_gross_weight'] + 1e-10 or notional * (1 + leg_cost) > cash:
                reason = 'minimum_notional_exceeds_risk_budget'
            elif bar.close / pending['reference_px'] - 1 > execution.get('maximum_signal_premium_pct', 0.006):
                reason = 'entry_price_premium'
            elif notional > bar.volume * bar.close * execution['maximum_volume_participation']:
                reason = 'volume_participation'
            elif turnover[day] + 2 * notional > equity * execution.get('daily_turnover_ratio', 0.6):
                reason = 'daily_turnover_budget'
            if reason:
                skipped[reason] += 1
            else:
                spent = notional * (1 + leg_cost)
                position = {'symbol': symbol, 'signal_ts': pending['signal_ts'], 'signal_run_id': pending['run_id'],
                            'entry_ts': stamp, 'entry_px': bar.close, 'notional_usdt': notional,
                            'cash_spent': spent, 'qty': notional / bar.close, 'entry_equity': equity,
                            'entry_weight': notional / equity, 'entry_kind': pending['kind']}
                cash -= spent
                turnover[day] += notional
            pending = None

        if position is None and pending is None and not halted and stamp < end:
            ready = [row for row in signals.get(stamp, []) if stamp >= cooldown.get(row['symbol'], 0)]
            if ready:
                selected = ready[0]
                pending = {'symbol': selected['symbol'], 'signal_ts': stamp, 'fill_ts': stamp + HOUR,
                           'reference_px': selected['values']['reference_px'], 'run_id': selected['run_id'], 'kind': selected['kind']}
        equity = cash + (position['qty'] * bars[position['symbol']][stamp].close * (1 - leg_cost) if position else 0)
        peak = max(peak, equity)
        curve.append({'ts': stamp, 'cash_usdt': cash, 'equity_usdt': equity, 'drawdown_pct': 1 - equity / peak,
                      'gross_weight': position['qty'] * bars[position['symbol']][stamp].close / equity if position else 0,
                      'symbol': position['symbol'] if position else '', 'halted': halted})
        if cash < -1e-8:
            raise AssertionError('Replay attempted to borrow cash')
    if position is not None:
        close_trade(end, bars[position['symbol']][end].close, 'end_of_window')
    positive = sum(max(0, t['net_pnl_usdt']) for t in trades)
    negative = -sum(min(0, t['net_pnl_usdt']) for t in trades)
    return {'trades': trades, 'equity_curve': curve, 'metrics': {
        'start_ts': start, 'end_ts': end, 'initial_cash_usdt': initial, 'ending_cash_usdt': cash,
        'net_pnl_usdt': cash - initial, 'return_pct': (cash / initial - 1) * 100,
        'max_drawdown_pct': max(row['drawdown_pct'] for row in curve) * 100,
        'closed_trades': len(trades), 'wins': sum(t['net_pnl_usdt'] > 0 for t in trades),
        'profit_factor': positive / negative if negative > 0 else None,
        'mean_trade_net_bps': statistics.mean(t['net_bps'] for t in trades) if trades else None,
        'cost_usdt': sum(t['cost_usdt'] for t in trades),
        'mean_gross_weight_pct': statistics.mean(row['gross_weight'] for row in curve) * 100,
        'maximum_entry_weight_pct': max((t['entry_weight'] for t in trades), default=0) * 100,
        'maximum_observed_gross_weight_pct': max(row['gross_weight'] for row in curve) * 100,
        'calendar_days_with_entries': len({t['entry_ts'] // 86400 for t in trades}),
        'largest_win_share': max((t['net_pnl_usdt'] for t in trades), default=0) / positive if positive else None,
        'trial_halted': halted, 'execution_skips': dict(skipped),
        'force_closed_at_end': sum(t['exit_reason'] == 'end_of_window' for t in trades),
    }}


def day_block_interval(trades: list[dict], start: int, end: int, repeats: int = 2000) -> dict:
    """Resample whole calendar days, including zero-trade days, not signal rows."""
    days = list(range(start // 86400, end // 86400 + 1))
    groups = {day: [trade for trade in trades if trade['exit_ts'] // 86400 == day] for day in days}
    if not trades:
        return {'lower_95_bps': None, 'upper_95_bps': None, 'days': len(days), 'resamples': 0}
    rng = random.Random(20260903)
    samples = []
    for _ in range(repeats):
        selected = [trade for _ in days for trade in groups[rng.choice(days)]]
        samples.append(statistics.mean(t['net_bps'] for t in selected) if selected else 0.0)
    samples.sort()
    return {'lower_95_bps': samples[int(0.025 * (repeats - 1))],
            'upper_95_bps': samples[int(0.975 * (repeats - 1))],
            'days': len(days), 'resamples': repeats}
