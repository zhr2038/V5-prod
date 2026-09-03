from __future__ import annotations

import copy

import pytest

from src.research.factor_ablation import FACTORS, ablate_records, select_removals
from src.research.rally_reentry_validation import SYMBOLS, candidate_values


def source_record():
    weights = {key: .1 for key in FACTORS}
    snapshots = {}
    for index, symbol in enumerate(SYMBOLS):
        z = {key: 0.0 for key in FACTORS}
        z.update({'f1_mom_5d': float(index), 'f4_volume_expansion': 10.0 if index == 0 else 0.0, 'f6_sentiment': .5})
        score = sum(weights[key] * z[key] for key in FACTORS)
        snapshots[symbol] = {'z_factors': z, 'raw_factors': {'alpha6_final_score': score, 'alpha6_relative_score': score}}
    return {'run_id': 'example', 'audit': {'regime': 'Trending', 'effective_alpha6_weights': weights,
                                           'alpha_factor_snapshot': snapshots},
            'candidates': {symbol: {} for symbol in SYMBOLS}, 'signals': {}}


def test_factor_removal_changes_ranking_without_mutating_original_evidence():
    record = source_record()
    original = copy.deepcopy(record)
    altered, stats = ablate_records({3600: record}, ['f4_volume_expansion'], {})
    assert record == original
    assert altered[3600]['audit'] is record['audit']
    assert stats['top_rank_changed_runs'] == 1
    assert max(altered[3600]['research_rank_scores'], key=altered[3600]['research_rank_scores'].get) == 'BNB/USDT'
    assert candidate_values(altered[3600], 'BNB/USDT')['relative'] > candidate_values(altered[3600], 'BTC/USDT')['relative']


def test_common_component_has_no_relative_ranking_effect():
    records = {3600: source_record()}
    all_factors, _ = ablate_records(records, [], {})
    removed, stats = ablate_records(records, ['f6_sentiment'], {})
    assert stats['full_rank_changed_runs'] == 0
    assert removed[3600]['research_rank_scores'] == pytest.approx(all_factors[3600]['research_rank_scores'])


def test_unreconstructable_score_is_not_silently_accepted():
    record = source_record()
    record['audit']['alpha_factor_snapshot']['BTC/USDT']['raw_factors']['alpha6_final_score'] += .01
    with pytest.raises(ValueError, match='do not match'):
        ablate_records({3600: record}, [], {})


def test_unknown_factor_or_missing_feature_fails_explicitly():
    with pytest.raises(ValueError, match='Unknown factors'):
        ablate_records({3600: source_record()}, ['invented'], {})
    with pytest.raises(ValueError, match='completed 4h history'):
        ablate_records({3600: source_record()}, list(FACTORS), {}, price_only=True)


def test_removal_selection_cannot_hide_a_losing_period_behind_total_gain():
    cells = [('full60', 30), ('full60', 60), ('validation20', 30), ('validation20', 60)]
    results = [{'factor_variant': 'all_recorded_factors', 'window': w, 'cost_bps': c,
                'net_pnl_usdt': 0.0, 'max_drawdown_pct': 1.0} for w, c in cells]
    stats = {}
    for factor in FACTORS:
        variant = 'drop_' + factor.split('_')[0]
        stats[variant] = {'full_rank_changed_runs': 10}
        for w, c in cells:
            gain = .1 if factor == FACTORS[0] else 0.0
            if factor == FACTORS[1]:
                gain = 5.0 if w == 'full60' else -.1
            results.append({'factor_variant': variant, 'window': w, 'cost_bps': c,
                            'net_pnl_usdt': gain, 'max_drawdown_pct': 1.0})
    rules = {'primary_cells': [f'{w}:{c}' for w, c in cells],
             'minimum_total_improvement_usdt': .05, 'maximum_drawdown_pct': 3.0}
    decisions = {row['factor']: row for row in select_removals(results, stats, rules)}
    assert decisions[FACTORS[0]]['candidate_remove']
    assert not decisions[FACTORS[1]]['candidate_remove']
    assert not decisions[FACTORS[2]]['candidate_remove']
    assert not any(row['live_change_authorized_by_results'] for row in decisions.values())


def test_removal_selection_requires_all_primary_scenarios():
    with pytest.raises(ValueError, match='missing primary comparisons'):
        select_removals([], {}, {'primary_cells': ['full60:30']})
