"""Factor removal experiments without rewriting original decision evidence."""
from __future__ import annotations

import statistics

from src.research.rally_reentry_validation import SYMBOLS, number

FACTORS = ('f1_mom_5d', 'f2_mom_20d', 'f3_vol_adj_ret',
           'f4_volume_expansion', 'f5_rsi_trend_confirm', 'f6_sentiment')


def ablate_records(records: dict, dropped: list[str], features: dict, *, price_only: bool = False) -> tuple[dict, dict]:
    unknown = set(dropped) - set(FACTORS)
    if unknown:
        raise ValueError(f'Unknown factors: {sorted(unknown)}')
    output = {}
    stats = {'complete_runs': 0, 'full_rank_changed_runs': 0, 'top_rank_changed_runs': 0,
             'max_original_score_error': 0.0, 'removed_factors': list(dropped), 'price_only': price_only}
    for stamp, record in records.items():
        audit = record['audit']
        snapshots = audit.get('alpha_factor_snapshot', {})
        if set(snapshots) != set(SYMBOLS):
            if audit.get('regime') in ('Trending', 'Sideways'):
                raise ValueError(f'{record["run_id"]}: incomplete factor evidence')
            output[stamp] = record
            continue
        weights = audit.get('effective_alpha6_weights', {})
        original, altered = {}, {}
        for symbol in SYMBOLS:
            z = snapshots[symbol].get('z_factors', {})
            values = {key: number(z.get(key)) for key in FACTORS}
            resolved = {key: number(weights.get(key)) for key in FACTORS}
            if any(v is None for v in [*values.values(), *resolved.values()]):
                raise ValueError(f'{record["run_id"]}: non-observable factor contribution')
            before = sum(resolved[key] * values[key] for key in FACTORS)
            recorded = number(snapshots[symbol].get('raw_factors', {}).get('alpha6_final_score'))
            if recorded is None or abs(before - recorded) > 1e-8:
                raise ValueError(f'{record["run_id"]}: reconstructed factors do not match recorded score')
            stats['max_original_score_error'] = max(stats['max_original_score_error'], abs(before - recorded))
            original[symbol] = before
            if price_only:
                altered[symbol] = number(features.get(symbol, {}).get(stamp, {}).get('ret4_bps'))
                if altered[symbol] is None:
                    raise ValueError('Price-only ranking requires completed 4h history')
            else:
                altered[symbol] = sum(resolved[key] * values[key] for key in FACTORS if key not in dropped)
        old_rank = sorted(SYMBOLS, key=lambda symbol: (-original[symbol], symbol))
        new_rank = sorted(SYMBOLS, key=lambda symbol: (-altered[symbol], symbol))
        stats['complete_runs'] += 1
        stats['full_rank_changed_runs'] += old_rank != new_rank
        stats['top_rank_changed_runs'] += old_rank[0] != new_rank[0]
        mean = statistics.mean(altered.values())
        # Keep original records immutable. This override is consumed only by
        # the offline relative ranker, never by a production strategy or order.
        output[stamp] = {**record, 'research_rank_scores': {s: altered[s] - mean for s in SYMBOLS}}
    return output, stats


def select_removals(results: list[dict], stats: dict, rules: dict) -> list[dict]:
    decisions = []
    primary = [(cell.split(':')[0], float(cell.split(':')[1])) for cell in rules['primary_cells']]
    baseline = {(r['window'], r['cost_bps']): r for r in results if r['factor_variant'] == 'all_recorded_factors'}
    for factor in FACTORS:
        variant = 'drop_' + factor.split('_')[0]
        cases = {(r['window'], r['cost_bps']): r for r in results if r['factor_variant'] == variant}
        if any(cell not in cases or cell not in baseline for cell in primary):
            raise ValueError('Cannot select removals with missing primary comparisons')
        deltas = {f'{window}:{cost:g}': cases[(window, cost)]['net_pnl_usdt'] - baseline[(window, cost)]['net_pnl_usdt']
                  for window, cost in primary}
        no_rank_effect = stats[variant]['full_rank_changed_runs'] == 0
        improves = (all(delta > 1e-9 for delta in deltas.values())
                    and sum(deltas.values()) >= rules['minimum_total_improvement_usdt']
                    and all(cases[cell]['max_drawdown_pct'] <= rules['maximum_drawdown_pct'] for cell in primary))
        decisions.append({'factor': factor, 'candidate_remove': no_rank_effect or improves,
                          'reason': 'no_observed_rank_effect' if no_rank_effect else 'consistent_after_cost_drag' if improves else 'mixed_or_insufficient_removal_evidence',
                          'pnl_improvement_usdt': deltas, 'live_change_authorized_by_results': False})
    return decisions
