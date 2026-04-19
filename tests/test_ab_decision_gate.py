from __future__ import annotations
import os
from pathlib import Path

import scripts.ab_decision_gate as ab_decision_gate


def test_resolve_ab_gate_output_path_uses_prefixed_runtime_file(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "shadow_orders.sqlite").write_text("", encoding="utf-8")

    path = ab_decision_gate._resolve_ab_gate_output_path(reports_dir)

    assert path == (reports_dir / "shadow_ab_gate_status.json").resolve()


def test_resolve_ab_gate_output_path_uses_suffixed_runtime_file(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "orders_accelerated.sqlite").write_text("", encoding="utf-8")

    path = ab_decision_gate._resolve_ab_gate_output_path(reports_dir)

    assert path == (reports_dir / "ab_gate_status_accelerated.json").resolve()


def test_resolve_deadband_params_uses_runtime_config(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        ab_decision_gate,
        "_load_active_config",
        lambda project_root: {"rebalance": {"deadband_sideways": 0.07}},
    )

    current_deadband, proposed_deadband = ab_decision_gate._resolve_deadband_params(project_root=tmp_path)

    assert current_deadband == 0.07
    assert proposed_deadband == 0.06


def test_load_active_config_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = tmp_path / "configs" / "missing.yaml"
    monkeypatch.setattr(
        ab_decision_gate,
        "resolve_runtime_config_path",
        lambda project_root=None: str(missing),
    )

    try:
        ab_decision_gate._load_active_config(project_root=tmp_path)
    except FileNotFoundError as exc:
        assert str(missing) in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")


def test_load_runs_prefers_decision_audit_file_mtime(tmp_path: Path) -> None:
    runs_dir = tmp_path / "runs"
    stale_run = runs_dir / "stale"
    fresh_run = runs_dir / "fresh"
    stale_run.mkdir(parents=True, exist_ok=True)
    fresh_run.mkdir(parents=True, exist_ok=True)

    stale_audit = stale_run / "decision_audit.json"
    fresh_audit = fresh_run / "decision_audit.json"
    stale_audit.write_text("{}", encoding="utf-8")
    fresh_audit.write_text("{}", encoding="utf-8")

    os.utime(stale_audit, (100, 100))
    os.utime(fresh_audit, (200, 200))
    os.utime(stale_run, (500, 500))
    os.utime(fresh_run, (50, 50))

    runs = ab_decision_gate.load_runs(runs_dir, limit=2)

    assert [run.name for run in runs] == ["fresh", "stale"]
