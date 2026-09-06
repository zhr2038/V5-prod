import copy
import json
import sqlite3

import pytest

from scripts.migrate_review_continuation import hashes, prepare_continuation


def setup(tmp_path):
    old, new = tmp_path / 'old', tmp_path / 'new'
    old.mkdir()
    identity = {'identity': 'old', 'experiment': {'initial_cash_usdt': 100},
                'source_hashes': {'src/research/review_forward.py': 'old', 'configs/live_prod.yaml': 'frozen'}}
    (old / 'manifest.json').write_text(json.dumps(identity))
    (old / 'accounts/30/original/reports').mkdir(parents=True)
    (old / 'accounts/30/original/reports/queue.json').write_text('{"pending":"must-preserve"}')
    (old / 'hour-input.json').write_bytes(b'{broken-but-retained')
    state = {'last_observed': 1000, 'metrics': {}, 'cash': 89.5, 'peak': 102,
             'pending': ['exit-on-next-quote'], 'position': {'qty': .1, 'cost': 10.5}}
    with sqlite3.connect(old / 'comparison.sqlite') as con:
        con.execute('CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        con.executemany('INSERT INTO meta VALUES(?,?)', [('identity', 'old'), ('checkpoint', json.dumps(state)), ('model_binding', 'frozen')])
        con.execute('CREATE TABLE events(observed_at REAL PRIMARY KEY,input_hash TEXT,frame TEXT,event TEXT)')
        event = {'observed_at': 1000, 'bar_ts': 0, 'hourly_decision': False,
                 'scenarios': {'30': {'cohorts': {'C_hold24_only': {'portfolio': {'gross_exposure_usdt': 10}}}}}}
        con.execute('INSERT INTO events VALUES(1000,?,?,?)', ('input-sha', 'original-frame', json.dumps(event)))
    successor = copy.deepcopy(identity)
    successor['identity'] = 'new'
    successor['source_hashes']['src/research/review_forward.py'] = 'fixed'
    return old, new, successor


def test_migration_preserves_old_bytes_all_accounts_events_and_pending_intents(tmp_path):
    old, new, identity = setup(tmp_path)
    before = hashes(old)
    result = prepare_continuation(old, new, identity)
    assert hashes(old) == before and not result['account_state_reset']
    with sqlite3.connect(old / 'comparison.sqlite') as a, sqlite3.connect(new / 'comparison.sqlite') as b:
        assert a.execute('SELECT * FROM events').fetchall() == b.execute('SELECT * FROM events').fetchall()
        for name in ('checkpoint', 'model_binding'):
            assert a.execute('SELECT value FROM meta WHERE key=?', (name,)).fetchone() == b.execute('SELECT value FROM meta WHERE key=?', (name,)).fetchone()
        assert b.execute("SELECT value FROM meta WHERE key='identity'").fetchone()[0] == 'new'
    assert (old / 'accounts/30/original/reports/queue.json').read_bytes() == (new / 'accounts/30/original/reports/queue.json').read_bytes()


def test_followup_metadata_repair_preserves_policy_metrics_and_the_whole_lineage(tmp_path):
    from src.research.review_integrity import POLICY_HASH
    old, new, identity = setup(tmp_path)
    prepare_continuation(old, new, identity)
    with sqlite3.connect(new / 'comparison.sqlite') as con:
        checkpoint = json.loads(con.execute("SELECT value FROM meta WHERE key='checkpoint'").fetchone()[0])
        checkpoint['metrics']['integrity'] = {'policy_sha256': POLICY_HASH, 'legacy_observations': 1, 'missing_duration_seconds': 60}
        con.execute("UPDATE meta SET value=? WHERE key='checkpoint'", (json.dumps(checkpoint),))
    successor = copy.deepcopy(identity)
    successor.update(identity='metadata-repair')
    successor['source_hashes']['src/research/review_acceptance.py'] = 'legacy-coverage-required'
    result = prepare_continuation(new, tmp_path / 'third', successor)
    assert result['previous_continuation']['predecessor_identity'] == 'old'
    assert result['observation_policy_metrics_preserved']
    with sqlite3.connect(tmp_path / 'third/comparison.sqlite') as con:
        assert json.loads(con.execute("SELECT value FROM meta WHERE key='checkpoint'").fetchone()[0]) == checkpoint
        legacy = json.loads(con.execute("SELECT value FROM meta WHERE key='legacy_observation_integrity'").fetchone()[0])
        assert legacy['valid_quote_observations'] == 1 and legacy['prospective_days'] == 0


@pytest.mark.parametrize('case', ['strategy_change', 'capital_change', 'processing', 'identity_mismatch', 'already_exists'])
def test_migration_refuses_unsafe_source_or_target(tmp_path, case):
    old, new, identity = setup(tmp_path)
    if case == 'strategy_change':
        identity['source_hashes']['src/strategy/participation_policy.py'] = 'changed'
    elif case == 'capital_change':
        identity['experiment']['initial_cash_usdt'] = 200
    elif case == 'already_exists':
        new.mkdir()
    else:
        with sqlite3.connect(old / 'comparison.sqlite') as con:
            if case == 'processing':
                con.execute("INSERT INTO meta VALUES('processing','partial')")
            else:
                con.execute("UPDATE meta SET value='unbound' WHERE key='identity'")
    before = hashes(old)
    with pytest.raises(ValueError):
        prepare_continuation(old, new, identity)
    assert hashes(old) == before
