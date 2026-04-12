from __future__ import annotations

import pytest

from configs.schema import AppConfig
from src.core.models import MarketSeries
from src.research.walk_forward_optimizer import (
    apply_config_overrides,
    build_parameter_candidates,
    evaluate_walk_forward_candidate,
    score_walk_forward_summary,
)


def _make_market_data(n: int = 180) -> dict[str, MarketSeries]:
    closes = [100.0 + i * 0.5 for i in range(n)]
    return {
        "BTC/USDT": MarketSeries(
            symbol="BTC/USDT",
            timeframe="1h",
            ts=[1_700_000_000_000 + i * 3600 * 1000 for i in range(n)],
            open=closes,
            high=[x * 1.01 for x in closes],
            low=[x * 0.99 for x in closes],
            close=closes,
            volume=[1e7] * n,
        )
    }


def test_build_parameter_candidates_supports_values_and_range_specs() -> None:
    candidates = build_parameter_candidates(
        {
            "alpha.long_top_pct": {"values": [0.2, 0.3]},
            "execution.rank_exit_max_rank": {"start": 3, "stop": 4, "step": 1},
        }
    )

    assert len(candidates) == 4
    assert {"alpha.long_top_pct": 0.2, "execution.rank_exit_max_rank": 3} in candidates
    assert {"alpha.long_top_pct": 0.3, "execution.rank_exit_max_rank": 4} in candidates


def test_apply_config_overrides_updates_nested_fields() -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    updated = apply_config_overrides(
        cfg,
        {
            "alpha.long_top_pct": 0.3,
            "rebalance.deadband_trending": 0.04,
            "execution.rank_exit_max_rank": 4,
        },
    )

    assert updated.alpha.long_top_pct == 0.3
    assert updated.rebalance.deadband_trending == 0.04
    assert updated.execution.rank_exit_max_rank == 4


def test_score_walk_forward_summary_blocks_low_fold_count() -> None:
    summary = {
        "fold_count": 1,
        "metrics": {
            "sharpe": {"mean": 1.5, "std": 0.1},
            "cagr": {"mean": 0.4},
            "max_dd": {"mean": 0.1},
            "turnover": {"mean": 0.2},
        },
    }

    score, components = score_walk_forward_summary(summary, min_fold_count=2)

    assert score == float("-inf")
    assert components["metrics.sharpe.mean"] == 1.5


def test_evaluate_walk_forward_candidate_returns_rankable_payload() -> None:
    cfg = AppConfig(symbols=["BTC/USDT"])
    cfg.execution.collect_ml_training_data = False

    result = evaluate_walk_forward_candidate(
        base_cfg=cfg,
        market_data=_make_market_data(),
        provider_name="cache",
        folds=2,
        overrides={"alpha.long_top_pct": 0.3},
        metric_weights={"metrics.sharpe.mean": 1.0},
        min_fold_count=1,
        candidate_id="cand_0001",
    )

    assert result["candidate_id"] == "cand_0001"
    assert result["overrides"]["alpha.long_top_pct"] == 0.3
    assert "summary" in result
    assert "report" in result
    assert isinstance(result["score"], float)


def test_run_walk_forward_optimizer_task_finalizes_failed_run(monkeypatch, tmp_path: Path) -> None:
    finalized: dict[str, object] = {}

    class FakeRun:
        def __init__(self):
            self.run_dir = tmp_path / "run"
            self.run_dir.mkdir(parents=True, exist_ok=True)
            self.run_id = "run_123"

        def write_json(self, relative_path: str, payload):
            finalized.setdefault("writes", []).append((relative_path, payload))
            return self.run_dir / relative_path

    class FakeRecorder:
        def __init__(self, *args, **kwargs):
            pass

        def start_run(self, **kwargs):
            finalized["task_name"] = kwargs["task_name"]
            return FakeRun()

        def finalize_run(self, run, *, status: str, summary):
            finalized["status"] = status
            finalized["summary"] = summary
            return run.run_dir / "meta.json"

    monkeypatch.setattr("src.research.walk_forward_optimizer.ResearchRecorder", FakeRecorder)
    monkeypatch.setattr(
        "src.research.walk_forward_optimizer._load_market_data_for_task",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("optimizer data load failed")),
    )

    with pytest.raises(RuntimeError, match="optimizer data load failed"):
        from src.research.walk_forward_optimizer import run_walk_forward_optimizer_task

        run_walk_forward_optimizer_task(
            project_root=tmp_path,
            task_config={"task": {"name": "walk_forward_optimizer"}, "paths": {}, "optimizer": {}},
        )

    assert finalized["task_name"] == "walk_forward_optimizer"
    assert finalized["status"] == "failed"
    assert finalized["summary"] == {
        "reason": "walk_forward_optimizer_failed",
        "error_type": "RuntimeError",
        "error": "optimizer data load failed",
    }
