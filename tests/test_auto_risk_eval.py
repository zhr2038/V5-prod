from __future__ import annotations

import json
import os
from pathlib import Path

import scripts.auto_risk_eval as auto_risk_eval


def test_resolve_runtime_paths_tracks_runtime_env(monkeypatch, tmp_path):
    monkeypatch.setattr(auto_risk_eval, "PROJECT_ROOT", tmp_path)
    runtime = auto_risk_eval._resolve_runtime_paths(raw_env_path=".env.runtime")
    assert runtime.env_path == (tmp_path / ".env.runtime").resolve()


def test_main_passes_cli_paths_to_evaluate_and_switch(monkeypatch):
    captured = {}

    def _fake_evaluate_and_switch(*, config_path=None, env_path=None):
        captured["config_path"] = config_path
        captured["env_path"] = env_path

    monkeypatch.setattr(auto_risk_eval, "evaluate_and_switch", _fake_evaluate_and_switch)
    auto_risk_eval.main(["--config", "configs/runtime.yaml", "--env", ".env.runtime"])

    assert captured == {"config_path": "configs/runtime.yaml", "env_path": ".env.runtime"}


def test_calculate_metrics_ignores_orders_exit_for_dust_reject_rate() -> None:
    metrics = auto_risk_eval.calculate_metrics(
        [
            {
                "counts": {"selected": 10, "orders_rebalance": 4, "orders_exit": 3},
                "rejects": {"min_notional": 1, "exchange_min_notional": 1},
                "router_decisions": [{"reason": "min_notional"}, {"reason": "exchange_min_notional"}],
            }
        ]
    )

    assert metrics["conversion_rate"] == 0.4
    assert metrics["dust_reject_rate"] == 2 / 12


def test_calculate_metrics_uses_notional_rejects_not_exit_orders() -> None:
    metrics = auto_risk_eval.calculate_metrics(
        [
            {
                "counts": {"selected": 5, "orders_rebalance": 1, "orders_exit": 9},
                "rejects": {"min_notional": 0, "exchange_min_notional": 0},
                "router_decisions": [],
            }
        ]
    )

    assert metrics["conversion_rate"] == 0.2
    assert metrics["dust_reject_rate"] == 0.0


def test_calculate_metrics_uses_reject_counts_when_router_decisions_are_missing() -> None:
    metrics = auto_risk_eval.calculate_metrics(
        [
            {
                "counts": {"selected": 10, "orders_rebalance": 4, "orders_exit": 0},
                "rejects": {"min_notional": 2, "exchange_min_notional": 1},
                "router_decisions": [],
            }
        ]
    )

    assert metrics["conversion_rate"] == 0.4
    assert metrics["dust_reject_rate"] == 3 / 13


def test_calculate_metrics_does_not_double_count_router_and_reject_dust() -> None:
    metrics = auto_risk_eval.calculate_metrics(
        [
            {
                "counts": {"selected": 10, "orders_rebalance": 4, "orders_exit": 0},
                "rejects": {"min_notional": 2, "exchange_min_notional": 1},
                "router_decisions": [
                    {"reason": "min_notional"},
                    {"reason": "exchange_min_notional"},
                    {"reason": "min_notional"},
                ],
            }
        ]
    )

    assert metrics["dust_reject_rate"] == 3 / 13


def test_load_recent_runs_uses_decision_audit_mtime_not_run_dir_mtime(tmp_path: Path) -> None:
    runtime = auto_risk_eval.AutoRiskEvalPaths(
        reports_dir=(tmp_path / "reports").resolve(),
        runs_dir=(tmp_path / "reports" / "runs").resolve(),
        auto_risk_eval_path=(tmp_path / "reports" / "auto_risk_eval.json").resolve(),
        positions_db=(tmp_path / "reports" / "positions.sqlite").resolve(),
        auto_risk_guard_path=(tmp_path / "reports" / "auto_risk_guard.json").resolve(),
        env_path=(tmp_path / ".env").resolve(),
    )
    runtime.runs_dir.mkdir(parents=True, exist_ok=True)

    fresh_run = runtime.runs_dir / "fresh"
    stale_run = runtime.runs_dir / "stale"
    fresh_run.mkdir()
    stale_run.mkdir()

    fresh_audit = fresh_run / "decision_audit.json"
    stale_audit = stale_run / "decision_audit.json"
    fresh_audit.write_text(json.dumps({"counts": {"selected": 1}}), encoding="utf-8")
    stale_audit.write_text(json.dumps({"counts": {"selected": 2}}), encoding="utf-8")

    now = auto_risk_eval.datetime.now().timestamp()
    fresh_audit_ts = now - 1800
    stale_audit_ts = now - 13 * 3600
    fresh_dir_ts = now - 1800
    stale_dir_ts = now
    os.utime(fresh_audit, (fresh_audit_ts, fresh_audit_ts))
    os.utime(stale_audit, (stale_audit_ts, stale_audit_ts))
    os.utime(fresh_run, (fresh_dir_ts, fresh_dir_ts))
    os.utime(stale_run, (stale_dir_ts, stale_dir_ts))

    runs = auto_risk_eval.load_recent_runs(hours=12, runtime_paths=runtime)

    assert len(runs) == 1
    assert runs[0]["_run_id"] == "fresh"
