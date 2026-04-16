from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_SRC = REPO_ROOT / "deploy"


def _prepare_install_fixture(project_root: Path) -> Path:
    deploy_dir = project_root / "deploy"
    systemd_dir = deploy_dir / "systemd"
    systemd_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(DEPLOY_SRC / "install_systemd.sh", deploy_dir / "install_systemd.sh")
    shutil.copy2(DEPLOY_SRC / "render_systemd_units.py", deploy_dir / "render_systemd_units.py")
    shutil.copy2(DEPLOY_SRC / "prod_release.py", deploy_dir / "prod_release.py")
    (deploy_dir / "__init__.py").write_text("", encoding="utf-8")
    shutil.copytree(DEPLOY_SRC / "systemd", systemd_dir, dirs_exist_ok=True)
    return deploy_dir / "install_systemd.sh"


def test_install_systemd_user_production_only_supports_shadow_root_and_required_units(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    script_path = _prepare_install_fixture(project_root)

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir(parents=True, exist_ok=True)
    systemctl_log = tmp_path / "systemctl.log"
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/usr/bin/env bash\n"
        "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"\n"
        "exit 0\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)

    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env.get('PATH', '')}"
    env["HOME"] = str(home)
    env["XDG_RUNTIME_DIR"] = str(runtime_dir)
    env["SYSTEMCTL_LOG"] = str(systemctl_log)

    subprocess.run(
        [
            "bash",
            str(script_path),
            "--user",
            "--production-only",
            "--root",
            str(project_root),
            "--shadow-root",
            "/srv/shadow-runtime",
            "--enable-prod-timer",
            "--enable-event-driven-timer",
        ],
        check=True,
        env=env,
        capture_output=True,
        text=True,
    )

    units_dir = home / ".config" / "systemd" / "user"
    spread_unit = (units_dir / "v5-spread-rollup.service").read_text(encoding="utf-8")
    shadow_unit = (units_dir / "v5-shadow-tuned-xgboost.user.service").read_text(encoding="utf-8")
    systemctl_calls = systemctl_log.read_text(encoding="utf-8")

    assert str(project_root) in spread_unit
    assert "/srv/shadow-runtime" in shadow_unit
    assert "--user enable --now v5-spread-rollup.timer" in systemctl_calls
    assert "--user enable --now v5-shadow-tuned-xgboost.user.timer" in systemctl_calls
    assert "--user enable --now v5-prod.user.timer" in systemctl_calls
    assert "--user enable --now v5-event-driven.timer" in systemctl_calls
