#!/usr/bin/env python3
"""Compare six factor removals with fixed entries, exits and risk budgets."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.validate_rally_reentry import load_json, write_csv, write_json
from src.research.factor_ablation import ablate_records, select_removals
from src.research.rally_reentry_validation import build_signals, epoch, prepare_market, prepare_records, simulate


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--history', type=Path, required=True)
    parser.add_argument('--candles', type=Path, required=True)
    parser.add_argument('--protocol', type=Path, default=ROOT / 'docs/factor_ablation_protocol.json')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    protocol = load_json(args.protocol)
    if protocol['safety'] != {'offline_only': True, 'production_changes': False, 'live_order_effect': 'none', 'live_promotion_allowed': False}:
        raise ValueError('Factor comparisons must remain offline')
    history, market = load_json(args.history), load_json(args.candles)
    bars, features = prepare_market(market)
    records, quality = prepare_records(history, bars)
    if history.get('missing') or quality['price_mismatches']:
        raise ValueError('Incomplete or conflicting evidence')
    start, split, end = (int(epoch(protocol[key])) for key in ['evaluation_start_utc', 'chronological_split_utc', 'evaluation_end_utc'])
    windows = {'full60': (start, end), 'development40': (start, split), 'validation20': (split, end)}
    policy = protocol['policies'][0]
    if policy['name'] != 'direction_rank_simple':
        raise ValueError('This ablation isolates factor ranking under the fixed simple direction policy')
    execution = {**protocol['shared_execution'], **protocol['execution_scenarios'][0]['overrides']}
    stats, results, trades = {}, [], []

    def evaluate(variant: dict) -> None:
        altered, stats[variant['name']] = ablate_records(records, variant['drop'], features,
                                                        price_only=variant.get('ranking') == 'causal_4h_momentum')
        for cost in execution['roundtrip_cost_scenarios_bps']:
            signals, _ = build_signals(altered, features, policy, execution, cost)
            for window, (begin, finish) in windows.items():
                replay = simulate(signals, bars, altered, execution, begin, finish, cost, features=features)
                identity = {'factor_variant': variant['name'], 'cost_bps': cost, 'window': window}
                results.append({**identity, **replay['metrics']})
                trades.extend({**identity, **trade} for trade in replay['trades'])

    for variant in protocol['factor_variants']:
        evaluate(variant)
    decisions = select_removals(results, stats, protocol['removal_rule'])
    proposed = [row['factor'] for row in decisions if row['candidate_remove']]
    combined_accepted = False
    combined_delta = {}
    if proposed:
        evaluate({'name': 'combined_removal', 'drop': proposed})
        baseline = {(r['window'], r['cost_bps']): r for r in results if r['factor_variant'] == 'all_recorded_factors'}
        combined = {(r['window'], r['cost_bps']): r for r in results if r['factor_variant'] == 'combined_removal'}
        primary = [(cell.split(':')[0], float(cell.split(':')[1])) for cell in protocol['removal_rule']['primary_cells']]
        combined_delta = {f'{w}:{c:g}': combined[(w, c)]['net_pnl_usdt'] - baseline[(w, c)]['net_pnl_usdt'] for w, c in primary}
        combined_accepted = (all(value >= -1e-9 for value in combined_delta.values())
                             and all(combined[cell]['max_drawdown_pct'] <= protocol['removal_rule']['maximum_drawdown_pct'] for cell in primary))
    accepted = proposed if combined_accepted else []
    compact_result = None
    if protocol.get('compact_candidate'):
        compact = protocol['compact_candidate']
        reference = {(r['window'], r['cost_bps']): r for r in results if r['factor_variant'] == compact['baseline']}
        reduced = {(r['window'], r['cost_bps']): r for r in results if r['factor_variant'] == compact['name']}
        primary = [(cell.split(':')[0], float(cell.split(':')[1])) for cell in protocol['removal_rule']['primary_cells']]
        deltas = {f'{w}:{c:g}': reduced[(w, c)]['net_pnl_usdt'] - reference[(w, c)]['net_pnl_usdt'] for w, c in primary}
        compact_ok = (all(value >= -1e-9 for value in deltas.values())
                      and all(reduced[cell]['max_drawdown_pct'] <= protocol['removal_rule']['maximum_drawdown_pct'] for cell in primary))
        compact_result = {'name': compact['name'], 'accepted_for_candidate': compact_ok,
                          'pnl_improvement_over_pruned_baseline': deltas, 'removed_factors': compact['drop']}
        if compact_ok:
            accepted = compact['drop']
    args.output.mkdir(parents=True, exist_ok=True)
    sources = [args.protocol, args.history, args.candles, Path(__file__), ROOT / 'src/research/factor_ablation.py', ROOT / 'src/research/rally_reentry_validation.py']
    manifest = {'schema_version': 'v5.offline_factor_ablation_results.v1', 'safety': protocol['safety'],
                'sources': [{'path': str(path.resolve()), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()} for path in sources]}
    snapshot_dir = args.output / 'source_snapshots'
    snapshot_dir.mkdir(exist_ok=True)
    for source in manifest['sources']:
        path = Path(source['path'])
        if path.suffix in ('.py', '.json'):
            snapshot = snapshot_dir / (source['sha256'][:12] + '_' + path.name)
            snapshot.write_bytes(path.read_bytes())
            source['preserved_snapshot'] = str(snapshot.resolve())
    output = {'manifest': manifest, 'quality': quality, 'factor_stats': stats, 'results': results,
              'factor_decisions': decisions, 'proposed_removals': proposed, 'combined_removal_accepted_for_candidate': combined_accepted,
              'combined_pnl_improvement_usdt': combined_delta, 'accepted_candidate_removals': accepted,
              'compact_candidate_comparison': compact_result,
              'scope': 'exploratory_factor_ranking_only', 'live_promotion_allowed': False}
    write_json(args.output / 'factor_results.json', output)
    write_csv(args.output / 'factor_comparison.csv', results)
    write_csv(args.output / 'factor_simulated_trades.csv', trades)
    profile = {'schema_version': 'v5.reduced_factor_candidate.v1', 'mode': 'offline_research',
               'disabled_factors': accepted, 'ranking': 'remaining_recorded_factors',
               'absolute_direction_policy': policy, 'execution': execution,
               'live_promotion_allowed': False, 'requires_prospective_validation': True,
               'source_protocol_sha256': manifest['sources'][0]['sha256']}
    write_json(args.output / 'reduced_factor_candidate.json', profile)
    print(json.dumps({'output': str(args.output.resolve()), 'factor_decisions': decisions,
                      'accepted_candidate_removals': accepted, 'combined_pnl_improvement_usdt': combined_delta,
                      'compact_candidate_comparison': compact_result,
                      'validation20_30bps': [{key: r[key] for key in ['factor_variant', 'closed_trades', 'net_pnl_usdt', 'max_drawdown_pct']} for r in results if r['window'] == 'validation20' and r['cost_bps'] == 30]}, ensure_ascii=False))


if __name__ == '__main__':
    main()
