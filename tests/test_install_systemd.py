from __future__ import annotations

import os
from pathlib import Path
import shlex
import shutil
import subprocess

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SRC = REPO_ROOT / "deploy"


def _bash_bin() -> str:
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("bash is required for systemd install wrapper tests")
    return bash


def _bash_path(path: Path) -> str:
    resolved = str(path.resolve())
    if os.name != "nt":
        return resolved
    quoted = shlex.quote(resolved)
    result = subprocess.run(
        [
            _bash_bin(),
            "-lc",
            f"if command -v wslpath >/dev/null 2>&1; then wslpath -u {quoted}; "
            f"elif command -v cygpath >/dev/null 2>&1; then cygpath -u {quoted}; "
            f"else printf '%s\\n' {quoted}; fi",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _chmod_executable(path: Path) -> None:
    path.chmod(0o755)
    if os.name == "nt":
        subprocess.run([_bash_bin(), "-lc", f"chmod +x {shlex.quote(_bash_path(path))}"], check=True)


def _prepare_install_fixture(project_root: Path) -> Path:
    deploy_dir = project_root / "deploy"
    systemd_dir = deploy_dir / "systemd"
    systemd_dir.parent.mkdir(parents=True, exist_ok=True)
    install_dst = deploy_dir / "install_systemd.sh"
    install_dst.write_text(
        (DEPLOY_SRC / "install_systemd.sh").read_text(encoding="utf-8").replace("\r\n", "\n"),
        encoding="utf-8",
        newline="\n",
    )
    _chmod_executable(install_dst)
    shutil.copy2(DEPLOY_SRC / "render_systemd_units.py", deploy_dir / "render_systemd_units.py")
    shutil.copy2(DEPLOY_SRC / "prod_release.py", deploy_dir / "prod_release.py")
    (deploy_dir / "__init__.py").write_text("", encoding="utf-8")
    shutil.copytree(DEPLOY_SRC / "systemd", systemd_dir, dirs_exist_ok=True)
    return deploy_dir / "install_systemd.sh"


def test_install_systemd_user_production_only_supports_shadow_root_and_required_units(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    script_path = _prepare_install_fixture(project_root)
    prod_service = project_root / "deploy" / "systemd" / "v5-prod.user.service"
    prod_service.write_text(
        prod_service.read_text(encoding="utf-8").replace(
            "[Service]\nType=oneshot\n",
            "[Service]\nType=oneshot\nUser=admin\n",
        ),
        encoding="utf-8",
        newline="\n",
    )

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir(parents=True, exist_ok=True)
    systemctl_log = tmp_path / "systemctl.log"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"\n"
        "exit 0\n",
        encoding="utf-8",
        newline="\n",
    )
    _chmod_executable(fake_systemctl)

    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["PATH"] = f"{_bash_path(fake_bin)}:{env.get('PATH', '')}"
    env["HOME"] = _bash_path(home)
    env["XDG_RUNTIME_DIR"] = _bash_path(runtime_dir)
    env["SYSTEMCTL_LOG"] = _bash_path(systemctl_log)

    command = " ".join(
        [
            "env",
            f"PATH={shlex.quote(_bash_path(fake_bin))}:\"$PATH\"",
            f"HOME={shlex.quote(_bash_path(home))}",
            f"XDG_RUNTIME_DIR={shlex.quote(_bash_path(runtime_dir))}",
            f"SYSTEMCTL_LOG={shlex.quote(_bash_path(systemctl_log))}",
            shlex.quote(_bash_path(script_path)),
            "--user",
            "--production-only",
            "--root",
            shlex.quote(_bash_path(project_root)),
            "--shadow-root",
            "/srv/shadow-runtime",
            "--enable-prod-timer",
            "--enable-event-driven-timer",
        ]
    )
    subprocess.run(
        [_bash_bin(), "-lc", command],
        check=True,
        env=env,
        capture_output=True,
        text=True,
    )

    units_dir = home / ".config" / "systemd" / "user"
    spread_unit = (units_dir / "v5-spread-rollup.service").read_text(encoding="utf-8")
    prod_unit = (units_dir / "v5-prod.user.service").read_text(encoding="utf-8")
    shadow_unit = (units_dir / "v5-shadow-tuned-xgboost.user.service").read_text(encoding="utf-8")
    systemctl_calls = systemctl_log.read_text(encoding="utf-8")

    assert _bash_path(project_root) in spread_unit
    assert "User=admin" not in prod_unit
    assert "/srv/shadow-runtime" in shadow_unit
    assert "--user enable --now v5-spread-rollup.timer" in systemctl_calls
    assert "--user enable --now v5-shadow-tuned-xgboost.user.timer" in systemctl_calls
    assert "--user enable --now v5-prod.user.timer" in systemctl_calls
    assert "--user enable --now v5-event-driven.timer" in systemctl_calls
