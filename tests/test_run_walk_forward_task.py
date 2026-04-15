from __future__ import annotations

import scripts.run_walk_forward_task as walk_task


def test_resolve_task_config_path_prefers_cli_arg(monkeypatch):
    monkeypatch.setenv("V5_RESEARCH_TASK_CONFIG", "configs/research/from_env.yaml")
    assert walk_task._resolve_task_config_path(["configs/research/from_cli.yaml"]) == "configs/research/from_cli.yaml"


def test_resolve_task_config_path_uses_env_when_cli_missing(monkeypatch):
    monkeypatch.setenv("V5_RESEARCH_TASK_CONFIG", "configs/research/from_env.yaml")
    assert walk_task._resolve_task_config_path([]) == "configs/research/from_env.yaml"


def test_main_passes_cli_task_config_to_loader(monkeypatch, tmp_path):
    captured = {}

    monkeypatch.setattr(
        walk_task,
        "_load_walk_forward_task_config",
        lambda raw_config_path: captured.update({"config_path": raw_config_path}) or {"task": {"name": "walk_forward"}},
    )
    monkeypatch.setattr(
        walk_task,
        "run_walk_forward_task",
        lambda project_root, task_config: {"exit_code": 0, "task_name": task_config["task"]["name"]},
    )

    assert walk_task.main(["configs/research/from_cli.yaml"]) == 0
    assert captured["config_path"] == "configs/research/from_cli.yaml"
