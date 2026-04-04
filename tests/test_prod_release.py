from __future__ import annotations

from pathlib import Path

from deploy.prod_release import PRODUCTION_USER_UNIT_MAPPINGS, iter_production_files, render_unit_text
from deploy.sync_prod_release import _user_bus_wrapped_command


def test_render_unit_text_rewrites_known_roots() -> None:
    source = (
        "WorkingDirectory=/home/admin/clawd/v5-trading-bot\n"
        "ExecStart=/home/admin/clawd/v5-prod/.venv/bin/python main.py\n"
    )
    rendered = render_unit_text(source, "/srv/v5-prod")
    assert "/home/admin/clawd/v5-trading-bot" not in rendered
    assert "/home/admin/clawd/v5-prod" not in rendered
    assert rendered.count("/srv/v5-prod") == 2


def test_render_unit_text_rewrites_shadow_root() -> None:
    source = (
        "WorkingDirectory=/home/admin/clawd/v5-shadow-tuned-xgboost\n"
        "ExecStart=/home/admin/clawd/v5-shadow-tuned-xgboost/scripts/run_shadow_tuned_xgboost_hourly.sh\n"
    )
    rendered = render_unit_text(source, "/srv/v5-shadow-tuned-xgboost")
    assert "/home/admin/clawd/v5-shadow-tuned-xgboost" not in rendered
    assert rendered.count("/srv/v5-shadow-tuned-xgboost") == 2


def test_iter_production_files_excludes_runtime_state(tmp_path: Path) -> None:
    (tmp_path / "main.py").write_text("print('ok')", encoding="utf-8")
    (tmp_path / "reports").mkdir()
    (tmp_path / "reports" / "state.json").write_text("{}", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "run.py").write_text("print('run')", encoding="utf-8")
    (tmp_path / "scripts" / "archive").mkdir(parents=True, exist_ok=True)
    (tmp_path / "scripts" / "archive" / "old.py").write_text("print('old')", encoding="utf-8")

    files = sorted(
        path.relative_to(tmp_path).as_posix()
        for path in iter_production_files(tmp_path, items=("main.py", "reports", "scripts"))
    )

    assert files == ["main.py", "scripts/run.py"]


def test_iter_production_files_includes_explicit_model_file(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "ml_factor_model.pkl").write_bytes(b"binary-model")
    (tmp_path / "models" / "ml_factor_model_active.txt").write_text(
        "models/ml_factor_model",
        encoding="utf-8",
    )
    (tmp_path / "models" / "ml_factor_model_config.json").write_text("{}", encoding="utf-8")
    (tmp_path / "models" / "ml_factor_model_gpu_tuned.json").write_text("{}", encoding="utf-8")
    (tmp_path / "models" / "ml_factor_model_gpu_tuned_config.json").write_text("{}", encoding="utf-8")

    files = list(
        iter_production_files(
            tmp_path,
            items=(
                "models/ml_factor_model.pkl",
                "models/ml_factor_model_active.txt",
                "models/ml_factor_model_config.json",
                "models/ml_factor_model_gpu_tuned.json",
                "models/ml_factor_model_gpu_tuned_config.json",
            ),
        )
    )

    assert [path.relative_to(tmp_path).as_posix() for path in files] == [
        "models/ml_factor_model.pkl",
        "models/ml_factor_model_active.txt",
        "models/ml_factor_model_config.json",
        "models/ml_factor_model_gpu_tuned.json",
        "models/ml_factor_model_gpu_tuned_config.json",
    ]


def test_render_unit_text_rewrites_ubuntu_prod_root() -> None:
    source = (
        "WorkingDirectory=/home/ubuntu/clawd/v5-prod\n"
        "ExecStart=/home/ubuntu/clawd/v5-prod/.venv/bin/python main.py\n"
    )
    rendered = render_unit_text(source, "/srv/v5-prod")
    assert "/home/ubuntu/clawd/v5-prod" not in rendered
    assert rendered.count("/srv/v5-prod") == 2


def test_production_unit_mappings_include_sentiment_collect() -> None:
    mappings = dict(PRODUCTION_USER_UNIT_MAPPINGS)
    assert mappings["v5-sentiment-collect.service"] == "v5-sentiment-collect.service"
    assert mappings["v5-sentiment-collect.timer"] == "v5-sentiment-collect.timer"


def test_user_bus_wrapped_command_exports_user_bus() -> None:
    wrapped = _user_bus_wrapped_command("admin", "systemctl --user daemon-reload")
    assert "id -u admin" in wrapped
    assert "XDG_RUNTIME_DIR=/run/user/$uid" in wrapped
    assert "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$uid/bus" in wrapped
