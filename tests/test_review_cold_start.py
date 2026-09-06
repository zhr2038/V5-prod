import json
from pathlib import Path

from src.research.review_comparison import Comparison
from tests.test_review_comparison import POLICY, config, frame


def test_real_pipeline_cold_start_records_quotes_then_builds_factors_at_shared_cutoff(tmp_path):
    experiment = json.loads((Path(__file__).resolve().parents[1] / 'configs/research/review_experiment_v2.json').read_text())
    runner = Comparison(config(), POLICY, experiment, root=tmp_path / 'accounts')
    hour = 1788652800
    # Real adapter and real snapshot builder: no mocked factor provenance.
    for scenario in runner.scenarios.values():
        path = scenario['adapter'].root / 'reports/okx_spot_instruments.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(frame(hour + 900)['instrument_raw']))
    first = runner.observe(frame(hour + 900))
    assert first['hourly_decision'] is False
    assert all(not s['pending'] and s['audit'] is None for s in runner.scenarios.values())
    assert all(e['decision'] is None for s in first['scenarios'].values() for n, e in s['cohorts'].items() if n != 'A_original_v5')
    restored = Comparison(config(), POLICY, experiment, root=tmp_path / 'accounts', checkpoint=runner.checkpoint())
    before = restored.observe(frame(hour + 3600 + 50))
    assert before['decision_clock']['status'] == 'WAITING_COMMON_CUTOFF'
    decided = restored.observe(frame(hour + 3600 + 410))
    assert decided['hourly_decision'] is True
    assert all(s['audit']['alpha_factor_snapshot'] for s in restored.scenarios.values())
    for scenario in decided['scenarios'].values():
        assert scenario['cohorts']['C_hold24_only']['snapshot_hash'] == scenario['cohorts']['D_reference_only']['snapshot_hash']
