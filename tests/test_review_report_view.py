import json
import sqlite3

from src.reporting.dashboard_command_center import _review_comparison


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def study(tmp_path):
    root = tmp_path / 'review_comparison'
    directory = root / 'review-test-v2'
    write(root / 'current.json', {'directory': directory.name, 'identity': 'frozen'})
    write(directory / 'manifest.json', {'identity': 'frozen'})
    write(directory / 'latest.json', {'identity': 'frozen', 'latest_observed_at': 1000,
                                      'live_execution_eligible': False, 'automatic_live_scaling': False})
    write(directory / 'worker-status.json', {'ok': True, 'observed_at': 1000})
    with sqlite3.connect(directory / 'comparison.sqlite') as con:
        con.execute('CREATE TABLE meta (key TEXT, value TEXT)')
        con.execute("INSERT INTO meta VALUES ('identity','frozen')")
    return directory


def test_independent_report_requires_explicit_pointer_and_matching_ledger(tmp_path):
    directory = study(tmp_path)
    assert _review_comparison(tmp_path, 1010)['status'] == 'observed'
    assert _review_comparison(tmp_path, 1200)['status'] == 'stale'
    write(directory / 'manifest.json', {'identity': 'other'})
    result = _review_comparison(tmp_path, 1010)
    assert result['status'] == 'invalid' and result['report'] is None


def test_missing_pointer_never_selects_unrelated_latest_or_merges_old_profit(tmp_path):
    directory = study(tmp_path)
    (directory.parent / 'current.json').unlink()
    assert _review_comparison(tmp_path, 1010)['report'] is None


def test_worker_failure_and_future_report_stay_explicit(tmp_path):
    directory = study(tmp_path)
    assert _review_comparison(tmp_path, 900)['status'] == 'future'
    write(directory / 'worker-status.json', {'ok': False, 'detail': 'input rejected'})
    assert _review_comparison(tmp_path, 1010)['status'] == 'worker_failed'


def test_escaping_pointer_does_not_read_other_account(tmp_path):
    root = tmp_path / 'review_comparison'
    write(root / 'current.json', {'directory': '../participation', 'identity': 'frozen'})
    assert _review_comparison(tmp_path, 1010)['status'] == 'invalid'
