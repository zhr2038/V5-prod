#!/usr/bin/env python3
"""Run a frozen, offline comparison; never import an exchange or a live runner."""
from __future__ import annotations

import argparse
import contextlib
import csv
import gzip
import hashlib
import io
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.research.rally_reentry_validation import (
    HOUR, SYMBOLS, build_signals, day_block_interval, epoch,
    prepare_market, prepare_records, simulate,
)


def load_json(path: Path) -> dict:
    data = path.read_bytes()
    return json.loads(gzip.decompress(data) if path.suffix == '.gz' else data)


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False) + '\n', encoding='utf-8')


def write_csv(path: Path, rows: list[dict]) -> None:
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with path.open('w', newline='', encoding='utf-8-sig') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value for key, value in row.items()})


def recovery_audit(observed_drawdown: float) -> dict:
    # This original evaluator writes state. A temporary LOCAL directory is the
    # only state path supplied; no production state or equity peak is changed.
    from src.risk.auto_risk_guard import AutoRiskGuard

    rows = []
    with tempfile.TemporaryDirectory(prefix='v5-offline-risk-audit-') as directory, contextlib.redirect_stdout(io.StringIO()):
        for drawdown in sorted(set([observed_drawdown, .12, .119, .08, .079, .049, .03, 0.0]), reverse=True):
            for conversion in [0.0, .6]:
                path = Path(directory) / f'case-{len(rows)}.json'
                path.write_text(json.dumps({'current_level': 'PROTECT'}), encoding='utf-8')
                guard = AutoRiskGuard(state_path=str(path))
                level, _, reason = guard.evaluate(drawdown, conversion, 0, 'flat', 0)
                rows.append({'starting_level': 'PROTECT', 'drawdown_pct': drawdown * 100,
                             'conversion_rate': conversion, 'pnl_trend': 'flat', 'result': level, 'reason': reason})
        path = Path(directory) / 'flat-observation.json'
        path.write_text(json.dumps({'current_level': 'PROTECT'}), encoding='utf-8')
        guard = AutoRiskGuard(state_path=str(path))
        levels = [guard.evaluate(observed_drawdown, 0, 0, 'flat', 0)[0] for _ in range(168)]
    return {'state_write_scope': 'temporary_local_directory_only', 'production_state_changed': False,
            'equity_peak_reset': False, 'cases': rows,
            'repeated_flat_evaluations': len(levels), 'flat_evaluation_levels': sorted(set(levels))}


def admission(results: list[dict], rules: dict) -> list[dict]:
    output = []
    for name, variant in dict.fromkeys((row['policy'], row['execution_variant']) for row in results):
        cases = [row for row in results if row['policy'] == name and row['execution_variant'] == variant and row['window'] == 'validation20' and row['cost_bps'] in (30, 60)]
        failures = []
        if len(cases) != 2:
            failures.append('missing_required_cost_scenarios')
        for row in cases:
            prefix = f"{row['cost_bps']:g}bps:"
            if row['net_pnl_usdt'] <= 0:
                failures.append(prefix + 'nonpositive_net_pnl')
            if row['closed_trades'] < rules['minimum_validation_closed_trades']:
                failures.append(prefix + 'insufficient_trades')
            if row['calendar_days_with_entries'] < rules['minimum_validation_calendar_days_with_trades']:
                failures.append(prefix + 'insufficient_trading_days')
            lower = row['day_block_interval']['lower_95_bps']
            if lower is None or lower <= 0:
                failures.append(prefix + 'positive_expectancy_not_established')
            if row['max_drawdown_pct'] > rules['maximum_trial_drawdown_pct'] * 100:
                failures.append(prefix + 'drawdown_above_budget')
            if row['largest_win_share'] is None or row['largest_win_share'] > .5:
                failures.append(prefix + 'wins_concentrated_or_missing')
        output.append({'policy': name, 'execution_variant': variant, 'historical_screen_pass': not failures, 'failures': failures,
                       'prospective_paper_evidence': 'not_observable', 'live_promotion_allowed': False})
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--history', type=Path, required=True)
    parser.add_argument('--candles', type=Path, required=True)
    parser.add_argument('--protocol', type=Path, default=ROOT / 'docs/rally_validation_protocol.json')
    parser.add_argument('--candidate-profile', type=Path, help='Optional reduced-factor offline profile')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--observed-equity-peak', type=float, required=True)
    args = parser.parse_args()
    protocol = load_json(args.protocol)
    safety = protocol['safety']
    if safety != {'offline_only': True, 'production_changes': False, 'live_order_effect': 'none', 'live_promotion_allowed': False}:
        raise ValueError('Only the frozen offline safety contract is supported')
    history, market = load_json(args.history), load_json(args.candles)
    bars, features = prepare_market(market)
    records, quality = prepare_records(history, bars)
    if history.get('missing') or quality['price_mismatches']:
        raise ValueError('Missing source files or conflicting market prices; resolve before comparing results')
    candidate_profile = None
    if args.candidate_profile:
        from src.research.factor_ablation import ablate_records

        candidate_profile = load_json(args.candidate_profile)
        if candidate_profile.get('mode') != 'offline_research' or candidate_profile.get('live_promotion_allowed') is not False:
            raise ValueError('Only offline candidate profiles are supported')
        protocol_hash = hashlib.sha256(args.protocol.read_bytes()).hexdigest()
        if candidate_profile.get('source_protocol_sha256') != protocol_hash:
            raise ValueError('Candidate profile must bind to the exact frozen source protocol')
        records, quality['factor_removal'] = ablate_records(records, candidate_profile['disabled_factors'], features)
        protocol = {**protocol, 'policies': [candidate_profile['absolute_direction_policy']],
                    'shared_execution': candidate_profile['execution'],
                    'execution_scenarios': [{'name': 'reduced_factor_trend', 'overrides': {}}]}
    start, split, end = (int(epoch(protocol[key])) for key in ['evaluation_start_utc', 'chronological_split_utc', 'evaluation_end_utc'])
    if not start < split < end:
        raise ValueError('Invalid chronological split')
    missing_hours = [stamp for stamp in range(start, end + 1, HOUR) if stamp not in records]
    if missing_hours:
        raise ValueError(f'Missing recorded decision hours: {missing_hours[:5]}')
    args.output.mkdir(parents=True, exist_ok=True)
    for source in [args.history, args.candles, args.protocol]:
        if source.resolve() == (args.output / source.name).resolve():
            raise ValueError('Keep immutable source inputs separate from generated results')
    write_json(args.output / 'data_quality.json', quality)
    windows = {'full60': (start, end), 'development40': (start, split),
               'validation20': (split, end), 'latest72h': (end - 72 * HOUR, end)}
    results, trades, curves, diagnostics = [], [], [], []
    execution = protocol['shared_execution']
    for policy in protocol['policies']:
        for cost in execution['roundtrip_cost_scenarios_bps']:
            signals, counts = build_signals(records, features, policy, execution, cost)
            diagnostics.append({'policy': policy['name'], 'cost_bps': cost,
                                'scope': 'all_source_runs_including_24h_confirmation_warmup',
                                'reason_counts': counts, 'eligible_symbol_decisions': sum(map(len, signals.values()))})
            for variant in protocol.get('execution_scenarios', [{'name': 'fixed_8h', 'overrides': {}}]):
                scenario_execution = {**execution, **variant['overrides']}
                for window, (begin, finish) in windows.items():
                    replay = simulate(signals, bars, records, scenario_execution, begin, finish, cost, features=features)
                    identity = {'policy': policy['name'], 'execution_variant': variant['name'], 'cost_bps': cost, 'window': window}
                    row = {**identity, **replay['metrics'],
                           'eligible_symbol_decisions': sum(len(rows) for stamp, rows in signals.items() if begin <= stamp < finish)}
                    if window == 'validation20':
                        row['day_block_interval'] = day_block_interval(replay['trades'], begin, finish)
                    results.append(row)
                    trades.extend({**identity, **trade} for trade in replay['trades'])
                    curves.extend({**identity, **point} for point in replay['equity_curve'])
    observed_dd = 1 - execution['initial_cash_usdt'] / args.observed_equity_peak
    recovery = recovery_audit(observed_dd)
    market_returns = {name: {symbol: (bars[symbol][finish].close / bars[symbol][begin].close - 1) * 100 for symbol in SYMBOLS}
                      for name, (begin, finish) in windows.items()}
    files = [args.history, args.candles, args.protocol, Path(__file__), ROOT / 'src/research/rally_reentry_validation.py', ROOT / 'src/risk/auto_risk_guard.py']
    if args.candidate_profile:
        files.extend([args.candidate_profile, ROOT / 'src/research/factor_ablation.py'])
    manifest = {'generated_at_utc': datetime.now(timezone.utc).isoformat(), 'safety': safety,
                'sources': [{'path': str(path.resolve()), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()} for path in files],
                'source_audit_files': len(history.get('source_sha256', {})),
                'recorded_actual_fill_rows_context_only': len(history.get('actual_fills', [])),
                'observed_equity_peak_usdt': args.observed_equity_peak,
                'limitations': [protocol['split_warning'],
                    'These are entry-policy comparisons under a common simplified exit model, not a full replay of historical live V5.',
                    'Recorded scores and regimes retain historical code and weights; one config fingerprint does not prove one source-code release.',
                    'Entry decisions use only completed candles and recorded factors; execution is delayed to the following completed hourly close.',
                    'Original negative-expectancy and operational hard blocks are retained conservatively; hypothetical-position trajectories differ from actual state.',
                    'Spread and age are checked at the recorded signal quote; execution-time spread and actual market impact of hypothetical orders are not observable.',
                    'Gross weight is capped for new entries; price appreciation and liquidation costs can cause a small subsequent overshoot.',
                    'The 30/60/120 bps costs are stress assumptions, not observed costs for hypothetical fills.',
                    'Each window starts flat with the same cash and fresh trial budgets; window PnL values are not additive.',
                    'Day-block bootstrap intervals on 20 days are diagnostic, with no correction for trying multiple policies; no profit guarantee or live promotion follows.']}
    snapshot_dir = args.output / 'source_snapshots'
    snapshot_dir.mkdir(exist_ok=True)
    for source in manifest['sources']:
        path = Path(source['path'])
        if path.suffix in ('.py', '.json'):
            snapshot = snapshot_dir / (source['sha256'][:12] + '_' + path.name)
            snapshot.write_bytes(path.read_bytes())
            source['preserved_snapshot'] = str(snapshot.resolve())
    summary = {'schema_version': 'v5.offline_rally_results.v1', 'manifest': manifest, 'data_quality': quality,
               'market_returns_pct': market_returns, 'results': results, 'admission': admission(results, protocol['admission']),
               'recovery_audit': recovery, 'signal_diagnostics': diagnostics}
    write_json(args.output / 'results.json', summary)
    write_json(args.output / 'manifest.json', manifest)
    write_csv(args.output / 'comparison.csv', results)
    write_csv(args.output / 'simulated_trades.csv', trades)
    write_csv(args.output / 'simulated_equity.csv', curves)
    write_csv(args.output / 'recovery_audit.csv', recovery['cases'])
    print(json.dumps({'output': str(args.output.resolve()), 'quality': quality,
                      'validation20_30bps': [{key: row[key] for key in ['policy', 'execution_variant', 'closed_trades', 'net_pnl_usdt', 'max_drawdown_pct', 'mean_trade_net_bps', 'day_block_interval']} for row in results if row['window'] == 'validation20' and row['cost_bps'] == 30],
                      'admission': summary['admission']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
