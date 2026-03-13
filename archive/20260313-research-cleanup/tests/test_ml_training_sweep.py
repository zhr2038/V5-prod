from __future__ import annotations

from src.research.ml_training_sweep import (
    apply_dict_overrides,
    build_sweep_candidates,
    is_gpu_task,
    score_training_result,
    select_best_gate_passed_candidate,
)


def test_build_sweep_candidates_random_cap_is_deterministic() -> None:
    grid = {
        "model.xgboost_max_depth": [2, 3],
        "model.xgboost_n_estimators": [64, 128, 256],
        "dataset.selected_feature_count": [5, 7],
    }

    first = build_sweep_candidates(grid, max_candidates=4, sample_strategy="random", random_seed=7)
    second = build_sweep_candidates(grid, max_candidates=4, sample_strategy="random", random_seed=7)

    assert first == second
    assert len(first) == 4


def test_apply_dict_overrides_sets_nested_values() -> None:
    payload = {"model": {"candidates": ["ridge"]}, "parallel": {"cv_workers": 1}}
    updated = apply_dict_overrides(
        payload,
        {
            "model.candidates": ["xgboost"],
            "parallel.cv_workers": 2,
        },
    )

    assert updated["model"]["candidates"] == ["xgboost"]
    assert updated["parallel"]["cv_workers"] == 2


def test_is_gpu_task_detects_xgboost_auto() -> None:
    assert is_gpu_task({"model": {"candidates": ["xgboost"], "xgboost_compute_device": "auto"}}) is True
    assert is_gpu_task({"model": {"candidates": ["xgboost"], "xgboost_compute_device": "cpu"}}) is False
    assert is_gpu_task({"model": {"candidates": ["ridge"]}}) is False


def test_score_training_result_penalizes_instability() -> None:
    score, components = score_training_result(
        {
            "train_ic": 0.60,
            "valid_ic": 0.10,
            "cv_mean_ic": 0.08,
            "cv_std_ic": 0.20,
        }
    )

    assert components["ic_gap"] == 0.50
    assert score < 1.0


def test_select_best_gate_passed_candidate_prefers_highest_scoring_passed_result() -> None:
    best = select_best_gate_passed_candidate(
        [
            {"scenario_name": "cand_001", "gate_passed": False, "score": 9.0},
            {"scenario_name": "cand_002", "gate_passed": True, "score": 0.4},
            {"scenario_name": "cand_003", "gate_passed": True, "score": 0.6},
        ]
    )

    assert best["scenario_name"] == "cand_003"
