from __future__ import annotations

import stat
import subprocess
from pathlib import Path

from deploy.prod_release import (
    PRODUCTION_USER_UNIT_MAPPINGS,
    iter_production_files,
    production_snapshot,
    production_sync_relative_paths,
    production_sync_roots,
    render_unit_text,
)
from deploy.sync_prod_release import _prune_remote_files, _user_bus_wrapped_command, _validate_units


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


def test_iter_production_files_includes_models_directory_contents(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "hmm_regime.pkl").write_bytes(b"hmm")
    (tmp_path / "models" / "hmm_regime_info.json").write_text("{}", encoding="utf-8")
    (tmp_path / "models" / "ml_factor_model.txt").write_text("txt", encoding="utf-8")

    files = sorted(path.relative_to(tmp_path).as_posix() for path in iter_production_files(tmp_path, items=("models",)))

    assert files == [
        "models/hmm_regime.pkl",
        "models/hmm_regime_info.json",
        "models/ml_factor_model.txt",
    ]


def test_production_snapshot_uses_head_not_dirty_worktree(tmp_path: Path) -> None:
    (tmp_path / "main.py").write_text("print('head')\n", encoding="utf-8")
    (tmp_path / "deploy").mkdir()
    (tmp_path / "deploy" / "install_systemd.sh").write_text("#!/bin/sh\necho head\n", encoding="utf-8")
    (tmp_path / "scripts").mkdir()

    subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "codex@example.com"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "Codex"], cwd=tmp_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "add", "main.py", "deploy/install_systemd.sh"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    subprocess.run(["git", "commit", "-m", "init"], cwd=tmp_path, check=True, capture_output=True)

    (tmp_path / "deploy" / "install_systemd.sh").write_text("#!/bin/sh\necho dirty\n", encoding="utf-8")
    (tmp_path / "scripts" / "untracked.py").write_text("print('dirty')\n", encoding="utf-8")

    with production_snapshot(tmp_path, items=("main.py", "deploy", "scripts")) as snapshot_root:
        assert (snapshot_root / "main.py").read_text(encoding="utf-8") == "print('head')\n"
        assert (snapshot_root / "deploy" / "install_systemd.sh").read_text(encoding="utf-8") == "#!/bin/sh\necho head\n"
        assert not (snapshot_root / "scripts" / "untracked.py").exists()


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


def test_production_sync_relative_paths_and_roots(tmp_path: Path) -> None:
    (tmp_path / "main.py").write_text("print('ok')", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "run.py").write_text("print('run')", encoding="utf-8")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "PRODUCTION_ONLY_DEPLOYMENT.md").write_text("doc", encoding="utf-8")

    paths = production_sync_relative_paths(
        tmp_path,
        items=("main.py", "scripts", "docs/PRODUCTION_ONLY_DEPLOYMENT.md"),
    )

    assert paths == {
        "main.py",
        "scripts/run.py",
        "docs/PRODUCTION_ONLY_DEPLOYMENT.md",
    }
    assert production_sync_roots(("main.py", "scripts", "docs/PRODUCTION_ONLY_DEPLOYMENT.md")) == (
        "main.py",
        "scripts",
        "docs",
    )


class _FakeAttr:
    def __init__(self, filename: str, *, is_dir: bool, size: int = 0, mtime: int = 0) -> None:
        self.filename = filename
        self.st_mode = (stat.S_IFDIR if is_dir else stat.S_IFREG) | 0o755
        self.st_size = size
        self.st_mtime = mtime


class _FakeSFTP:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = {self._norm(path): content for path, content in files.items()}
        self.removed: list[str] = []

    def _norm(self, path: str) -> str:
        parts = [part for part in path.replace("\\", "/").split("/") if part]
        return "/" + "/".join(parts)

    def _is_dir(self, path: str) -> bool:
        prefix = self._norm(path).rstrip("/") + "/"
        return any(candidate.startswith(prefix) for candidate in self.files)

    def stat(self, path: str):
        normalized = self._norm(path)
        if normalized in self.files:
            return _FakeAttr(normalized.rsplit("/", 1)[-1], is_dir=False, size=len(self.files[normalized]))
        if self._is_dir(normalized):
            return _FakeAttr(normalized.rsplit("/", 1)[-1], is_dir=True)
        raise FileNotFoundError(normalized)

    def listdir_attr(self, path: str):
        normalized = self._norm(path).rstrip("/")
        prefix = normalized + "/"
        if not self._is_dir(normalized):
            raise FileNotFoundError(normalized)
        children: dict[str, _FakeAttr] = {}
        for candidate, content in self.files.items():
            if not candidate.startswith(prefix):
                continue
            remainder = candidate[len(prefix) :]
            child_name = remainder.split("/", 1)[0]
            if child_name in children:
                continue
            child_path = prefix + child_name
            children[child_name] = _FakeAttr(
                child_name,
                is_dir="/" in remainder,
                size=len(content) if "/" not in remainder else 0,
            )
        return list(children.values())

    def remove(self, path: str) -> None:
        normalized = self._norm(path)
        self.removed.append(normalized)
        self.files.pop(normalized, None)


def test_prune_remote_files_removes_stale_production_files_only(tmp_path: Path) -> None:
    (tmp_path / "main.py").write_text("print('ok')", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "run.py").write_text("print('run')", encoding="utf-8")
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "PRODUCTION_ONLY_DEPLOYMENT.md").write_text("doc", encoding="utf-8")

    fake_sftp = _FakeSFTP(
        {
            "/remote/main.py": b"ok",
            "/remote/scripts/run.py": b"run",
            "/remote/scripts/old.py": b"old",
            "/remote/docs/PRODUCTION_ONLY_DEPLOYMENT.md": b"doc",
            "/remote/docs/STALE.md": b"stale",
            "/remote/reports/runtime.json": b"{}",
            "/remote/data/cache.csv": b"cache",
        }
    )

    pruned = _prune_remote_files(fake_sftp, tmp_path, "/remote")

    assert pruned == ["docs/STALE.md", "scripts/old.py"]
    assert fake_sftp.removed == ["/remote/docs/STALE.md", "/remote/scripts/old.py"]
    assert "/remote/reports/runtime.json" in fake_sftp.files
    assert "/remote/data/cache.csv" in fake_sftp.files


def test_validate_units_requires_active_dashboard_and_timers(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_run(_client, command: str):
        captured["command"] = command
        return 0, "ok", ""

    monkeypatch.setattr("deploy.sync_prod_release._run", fake_run)

    assert _validate_units(object(), "ubuntu") == "ok"
    inner = captured["command"]
    assert "is-active v5-web-dashboard.service" in inner
    assert "is-active v5-prod.user.timer" in inner
    assert "is-active v5-event-driven.timer" in inner
