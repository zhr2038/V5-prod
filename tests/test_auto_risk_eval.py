from __future__ import annotations

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
