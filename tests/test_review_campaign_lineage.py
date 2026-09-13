import json
from pathlib import Path

import pytest

from scripts.run_review_forward import require_writable_experiment
from src.research.review_comparison import summarize


PROJECT = Path(__file__).resolve().parents[1]


def test_v3_starts_a_new_forward_ledger_with_explicit_campaign_and_count_contracts():
    experiment = json.loads(
        (PROJECT / "configs/research/review_experiment_v3.json").read_text(encoding="utf-8")
    )
    assert experiment["schema_version"] == "v5.review_comparison.v3"
    assert experiment["experiment_id"] == "v5-review-20260913-v3"
    assert experiment["live_execution_eligible"] is False
    assert experiment["automatic_live_scaling"] is False
    assert "new empty v3 ledger" in experiment["forward_start"]
    assert "never recalculated" in experiment["transition_contract"]
    assert experiment["position_lifecycle_contract"]["highest_price_scope"].startswith("current_campaign_only")
    assert experiment["trade_count_contract"]["independent_closed_trade_count"].startswith("completed unique campaign_id")
    report = summarize([], {"metrics": {}}, experiment)
    assert report["position_lifecycle_contract"] == experiment["position_lifecycle_contract"]
    assert report["trade_count_contract"] == experiment["trade_count_contract"]


def test_frozen_experiment_marker_prevents_worker_reuse(tmp_path):
    require_writable_experiment(tmp_path)
    (tmp_path / "FROZEN.json").write_text('{"status":"FROZEN"}', encoding="utf-8")
    with pytest.raises(ValueError, match="frozen_research_experiment_is_read_only"):
        require_writable_experiment(tmp_path)
