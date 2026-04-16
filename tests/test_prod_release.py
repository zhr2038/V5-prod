from __future__ import annotations

import io
import stat
import subprocess
import tarfile

import pytest
from pathlib import Path

from deploy.prod_release import (
    PRODUCTION_USER_UNIT_MAPPINGS,
    _extract_git_archive,
    iter_production_files,
    production_snapshot,
    production_sync_relative_paths,
    production_sync_roots,
    render_unit_text,
)
from deploy.sync_prod_release import (
    SHADOW_SYNC_ITEMS,
    _prune_remote_files,
    _resolve_remote_root,
    _resolve_service_user,
    _resolve_shadow_root,
    _should_upload,
    _should_restart_web_dashboard,
    _upload_files,
    _user_bus_wrapped_command,
    _validate_units,
)




def test_extract_git_archive_rejects_symlink_member(tmp_path: Path) -> None:
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:") as archive:
        info = tarfile.TarInfo("linked")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        archive.addfile(info)

    with pytest.raises(RuntimeError, match="unsupported archive link member"):
        _extract_git_archive(payload.getvalue(), tmp_path)


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




def test_production_snapshot_git_commands_use_timeout(monkeypatch, tmp_path: Path) -> None:
    (tmp_path / "main.py").write_text("print('head')\n", encoding="utf-8")

    calls: list[dict[str, object]] = []

    def fake_run(cmd, **kwargs):
        calls.append({"cmd": cmd, **kwargs})
        if cmd[3] == "ls-tree":
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=b"main.py\n", stderr=b"")
        if cmd[3] == "archive":
            payload = io.BytesIO()
            with tarfile.open(fileobj=payload, mode="w:") as archive:
                data = b"print('head')\n"
                info = tarfile.TarInfo("main.py")
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=payload.getvalue(), stderr=b"")
        raise AssertionError(cmd)

    monkeypatch.setattr("deploy.prod_release.subprocess.run", fake_run)

    with production_snapshot(tmp_path, items=("main.py",)) as snapshot_root:
        assert (snapshot_root / "main.py").read_text(encoding="utf-8") == "print('head')\n"

    assert len(calls) == 2
    assert all(call["timeout"] == 30 for call in calls)


def test_render_unit_text_rewrites_ubuntu_prod_root() -> None:
    source = (
        "WorkingDirectory=/home/ubuntu/clawd/v5-prod\n"
        "ExecStart=/home/ubuntu/clawd/v5-prod/.venv/bin/python main.py\n"
    )
    rendered = render_unit_text(source, "/srv/v5-prod")
    assert "/home/ubuntu/clawd/v5-prod" not in rendered
    assert rendered.count("/srv/v5-prod") == 2


def test_render_unit_text_drops_user_directive_for_user_units() -> None:
    source = (
        "[Service]\n"
        "User=admin\n"
        "WorkingDirectory=/home/admin/clawd/v5-prod\n"
        "ExecStart=/home/admin/clawd/v5-prod/.venv/bin/python main.py\n"
    )
    rendered = render_unit_text(source, "/srv/v5-prod", drop_user_directive=True)
    assert "User=admin" not in rendered
    assert "WorkingDirectory=/srv/v5-prod" in rendered
    assert "ExecStart=/srv/v5-prod/.venv/bin/python main.py" in rendered


def test_production_unit_mappings_include_sentiment_collect() -> None:
    mappings = dict(PRODUCTION_USER_UNIT_MAPPINGS)
    assert mappings["v5-sentiment-collect.service"] == "v5-sentiment-collect.service"
    assert mappings["v5-sentiment-collect.timer"] == "v5-sentiment-collect.timer"


def test_production_unit_mappings_include_spread_rollup() -> None:
    mappings = dict(PRODUCTION_USER_UNIT_MAPPINGS)
    assert mappings["v5-spread-rollup.user.service"] == "v5-spread-rollup.service"
    assert mappings["v5-spread-rollup.timer"] == "v5-spread-rollup.timer"


def test_shadow_sync_items_cover_shadow_runtime_without_dashboard_payload() -> None:
    assert "main.py" in SHADOW_SYNC_ITEMS
    assert "configs" in SHADOW_SYNC_ITEMS
    assert "models" in SHADOW_SYNC_ITEMS
    assert "src" in SHADOW_SYNC_ITEMS
    assert "scripts/run_shadow_tuned_xgboost.py" in SHADOW_SYNC_ITEMS
    assert "scripts/run_shadow_tuned_xgboost_hourly.sh" in SHADOW_SYNC_ITEMS
    assert "web" not in SHADOW_SYNC_ITEMS
    assert "docs/CURRENT_PRODUCTION_FLOW.md" not in SHADOW_SYNC_ITEMS


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
        self.uploaded: list[str] = []
        self.chmod_calls: list[tuple[str, int]] = []
        self.utime_calls: list[tuple[str, tuple[int, int]]] = []
        self.created_dirs: list[str] = []

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

    def open(self, path: str, mode: str = "r"):
        if mode != "rb":
            raise NotImplementedError(mode)
        normalized = self._norm(path)
        if normalized not in self.files:
            raise FileNotFoundError(normalized)
        return io.BytesIO(self.files[normalized])

    def mkdir(self, path: str) -> None:
        self.created_dirs.append(self._norm(path))

    def put(self, local_path: str, remote_path: str) -> None:
        normalized = self._norm(remote_path)
        self.uploaded.append(normalized)
        self.files[normalized] = Path(local_path).read_bytes()

    def chmod(self, path: str, mode: int) -> None:
        self.chmod_calls.append((self._norm(path), mode))

    def utime(self, path: str, times: tuple[int, int]) -> None:
        self.utime_calls.append((self._norm(path), times))

    def remove(self, path: str) -> None:
        normalized = self._norm(path)
        self.removed.append(normalized)
        self.files.pop(normalized, None)


def test_should_upload_skips_same_content_when_archive_mtime_changed(tmp_path: Path) -> None:
    local = tmp_path / "main.py"
    local.write_bytes(b"print('same')\n")
    fake_sftp = _FakeSFTP({"/remote/main.py": b"print('same')\n"})

    assert _should_upload(fake_sftp, local, "/remote/main.py") is False


def test_should_upload_detects_same_size_content_drift(tmp_path: Path) -> None:
    local = tmp_path / "main.py"
    local.write_bytes(b"print('new')\n")
    fake_sftp = _FakeSFTP({"/remote/main.py": b"print('old')\n"})

    assert _should_upload(fake_sftp, local, "/remote/main.py") is True


def test_should_restart_web_dashboard_only_for_web_runtime_changes() -> None:
    assert _should_restart_web_dashboard(["deploy/sync_prod_release.py"]) is False
    assert _should_restart_web_dashboard(["scripts/web_dashboard.py"]) is True
    assert _should_restart_web_dashboard(["web/static/app.js"]) is True
    assert _should_restart_web_dashboard(["deploy/systemd/v5-web-dashboard.service"]) is True


def test_upload_files_defers_web_dist_html_until_after_assets(tmp_path: Path) -> None:
    (tmp_path / "web" / "dist" / "assets").mkdir(parents=True)
    (tmp_path / "web" / "dist" / "assets" / "index-new.js").write_text("bundle", encoding="utf-8")
    (tmp_path / "web" / "dist" / "index.html").write_text("html", encoding="utf-8")
    (tmp_path / "web" / "static").mkdir(parents=True)
    (tmp_path / "web" / "static" / "app.js").write_text("console.log('app')", encoding="utf-8")

    fake_sftp = _FakeSFTP({})

    uploaded, skipped, rel_paths = _upload_files(
        fake_sftp,
        tmp_path,
        "/remote",
        items=("web",),
    )

    assert uploaded == 3
    assert skipped == 0
    assert rel_paths[-1] == "web/dist/index.html"
    assert fake_sftp.uploaded[-1] == "/remote/web/dist/index.html"
    assert fake_sftp.uploaded[:2] == [
        "/remote/web/dist/assets/index-new.js",
        "/remote/web/static/app.js",
    ]


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


def test_prune_remote_files_honors_shadow_sync_items(tmp_path: Path) -> None:
    (tmp_path / "main.py").write_text("print('ok')", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "run_shadow_tuned_xgboost.py").write_text("print('shadow')", encoding="utf-8")
    (tmp_path / "web").mkdir()
    (tmp_path / "web" / "dashboard.js").write_text("console.log('keep unmanaged')", encoding="utf-8")

    fake_sftp = _FakeSFTP(
        {
            "/remote/main.py": b"ok",
            "/remote/scripts/run_shadow_tuned_xgboost.py": b"shadow",
            "/remote/scripts/old_shadow_helper.py": b"old",
            "/remote/web/dashboard.js": b"dashboard",
        }
    )

    pruned = _prune_remote_files(fake_sftp, tmp_path, "/remote", items=SHADOW_SYNC_ITEMS, exact_items=True)

    assert pruned == []
    assert fake_sftp.removed == []
    assert "/remote/scripts/old_shadow_helper.py" in fake_sftp.files
    assert "/remote/web/dashboard.js" in fake_sftp.files


def test_validate_units_requires_active_dashboard_and_optional_live_timers(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_run(_client, command: str):
        captured["command"] = command
        return 0, "ok", ""

    monkeypatch.setattr("deploy.sync_prod_release._run", fake_run)

    assert (
        _validate_units(
            object(),
            "ubuntu",
            enable_prod_timer=True,
            enable_event_driven_timer=True,
        )
        == "ok"
    )
    inner = captured["command"]
    assert "is-active v5-web-dashboard.service" in inner
    assert "is-enabled v5-trade-monitor.timer" in inner
    assert "is-active v5-trade-monitor.timer" in inner
    assert "is-enabled v5-spread-rollup.timer" in inner
    assert "is-active v5-spread-rollup.timer" in inner
    assert "is-enabled v5-shadow-tuned-xgboost.user.timer" in inner
    assert "is-active v5-shadow-tuned-xgboost.user.timer" in inner
    assert "is-active v5-prod.user.timer" in inner
    assert "is-active v5-event-driven.timer" in inner


def test_validate_units_skips_optional_live_timer_checks_when_not_enabled(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_run(_client, command: str):
        captured["command"] = command
        return 0, "ok", ""

    monkeypatch.setattr("deploy.sync_prod_release._run", fake_run)

    assert (
        _validate_units(
            object(),
            "ubuntu",
            enable_prod_timer=False,
            enable_event_driven_timer=False,
        )
        == "ok"
    )
    inner = captured["command"]
    assert "is-active v5-web-dashboard.service" in inner
    assert "is-active v5-trade-monitor.timer" in inner
    assert "is-active v5-shadow-tuned-xgboost.user.timer" in inner
    assert "is-active v5-prod.user.timer" not in inner
    assert "is-active v5-event-driven.timer" not in inner
    assert "show v5-prod.user.timer" not in inner
    assert "show v5-event-driven.timer" not in inner


def test_sync_prod_release_defaults_follow_ssh_user() -> None:
    assert _resolve_remote_root("", "ubuntu") == "/home/ubuntu/clawd/v5-prod"
    assert _resolve_shadow_root("", "/home/ubuntu/clawd/v5-prod") == "/home/ubuntu/clawd/v5-shadow-tuned-xgboost"
    assert _resolve_service_user("", "ubuntu") == "ubuntu"


def test_sync_prod_release_respects_explicit_overrides() -> None:
    assert _resolve_remote_root("/srv/custom", "ubuntu") == "/srv/custom"
    assert _resolve_shadow_root("/srv/shadow", "/srv/custom") == "/srv/shadow"
    assert _resolve_service_user("admin", "ubuntu") == "admin"
