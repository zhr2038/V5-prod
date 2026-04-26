from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest


def _bash_bin() -> str:
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("bash is required for shell wrapper tests")
    return bash


def _bash_path(path: Path) -> str:
    resolved = str(path.resolve())
    if os.name != "nt":
        return resolved
    result = subprocess.run(
        [_bash_bin(), "-lc", f"wslpath -u {shlex.quote(resolved)}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _chmod_executable(path: Path) -> None:
    path.chmod(0o755)
    if os.name == "nt":
        subprocess.run([_bash_bin(), "-lc", f"chmod +x {shlex.quote(_bash_path(path))}"], check=True)


def _run_wrapper(script_path: Path, *, fake_bin_dir: Path, env_vars: dict[str, str], check: bool = True):
    assignments = [
        "env",
        f"PATH={shlex.quote(_bash_path(fake_bin_dir))}:\"$PATH\"",
        *(f"{key}={shlex.quote(value)}" for key, value in env_vars.items()),
        shlex.quote(_bash_path(script_path)),
    ]
    return subprocess.run(
        [_bash_bin(), "-lc", " ".join(assignments)],
        capture_output=True,
        text=True,
        check=check,
    )

def _write_fake_date(path: Path) -> None:
    path.write_text(
        "#!/bin/bash\n"
        "if [[ \"$1\" == \"+%Y%m%d_%H\" ]]; then\n"
        "  echo 20260415_23\n"
        "elif [[ \"$1\" == \"+%s\" ]]; then\n"
        "  echo 1713193200\n"
        "else\n"
        "  /bin/date \"$@\"\n"
        "fi\n",
        encoding="utf-8",
        newline="\n",
    )
    _chmod_executable(path)


def _write_fake_python(path: Path, project_root: Path, args_log: Path, env_log: Path) -> None:
    path.write_text(
        "#!/bin/bash\n"
        "if [[ \"$1\" == \"-c\" ]]; then\n"
        "  exit 0\n"
        "fi\n"
        "if [[ \"$1\" == \"-\" ]]; then\n"
        "  script=$(cat)\n"
        "  if [[ \"$script\" == *\"resolve_runtime_config_path\"* ]]; then\n"
        f"    echo \"{_bash_path(project_root / 'configs' / 'runtime.yaml')}\"\n"
        "    exit 0\n"
        "  fi\n"
        "  if [[ \"$script\" == *\"derive_runtime_named_json_path\"* ]]; then\n"
        f"    echo \"{_bash_path(project_root / 'reports' / 'trend_cache.json')}\"\n"
        "    exit 0\n"
        "  fi\n"
        "  if [[ \"$script\" == *\"yaml.safe_load\"* ]]; then\n"
        "    exit 0\n"
        "  fi\n"
        "  exit 0\n"
        "fi\n"
        "printf '%s\\n' \"$@\" >> \"$ARGS_LOG\"\n"
        "printf 'V5_CONFIG=%s\\n' \"$V5_CONFIG\" >> \"$ENV_LOG\"\n"
        "printf 'V5_RUN_ID=%s\\n' \"$V5_RUN_ID\" >> \"$ENV_LOG\"\n"
        "printf 'V5_WINDOW_START_TS=%s\\n' \"$V5_WINDOW_START_TS\" >> \"$ENV_LOG\"\n"
        "printf 'V5_WINDOW_END_TS=%s\\n' \"$V5_WINDOW_END_TS\" >> \"$ENV_LOG\"\n",
        encoding="utf-8",
        newline="\n",
    )
    _chmod_executable(path)


def test_run_hourly_live_window_uses_runtime_config_when_v5_config_unset(tmp_path: Path) -> None:
    project_root = tmp_path
    scripts_dir = project_root / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "configs").mkdir(parents=True, exist_ok=True)
    (project_root / "configs" / "runtime.yaml").write_text("execution: {}\n", encoding="utf-8")

    wrapper_src = Path(__file__).resolve().parents[1] / "scripts" / "run_hourly_live_window.sh"
    wrapper_dst = scripts_dir / "run_hourly_live_window.sh"
    wrapper_dst.write_text(wrapper_src.read_text(encoding="utf-8").replace("\r\n", "\n"), encoding="utf-8", newline="\n")
    wrapper_dst.chmod(0o755)

    fake_bin_dir = project_root / "fake-bin"
    fake_bin_dir.mkdir(parents=True, exist_ok=True)
    args_log = project_root / "args.log"
    env_log = project_root / "env.log"
    fake_python = fake_bin_dir / "python3"
    fake_date = fake_bin_dir / "date"
    _write_fake_python(fake_python, project_root, args_log, env_log)
    _write_fake_date(fake_date)

    _run_wrapper(
        wrapper_dst,
        fake_bin_dir=fake_bin_dir,
        env_vars={
            "V5_PYTHON_BIN": _bash_path(fake_python),
            "ARGS_LOG": _bash_path(args_log),
            "ENV_LOG": _bash_path(env_log),
        },
    )

    args = args_log.read_text(encoding="utf-8").splitlines()
    env_lines = env_log.read_text(encoding="utf-8").splitlines()
    assert args == ["main.py"]
    assert f"V5_CONFIG={_bash_path(project_root / 'configs' / 'runtime.yaml')}" in env_lines


def test_run_hourly_live_window_preserves_explicit_v5_config(tmp_path: Path) -> None:
    project_root = tmp_path
    scripts_dir = project_root / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    wrapper_src = Path(__file__).resolve().parents[1] / "scripts" / "run_hourly_live_window.sh"
    wrapper_dst = scripts_dir / "run_hourly_live_window.sh"
    wrapper_dst.write_text(wrapper_src.read_text(encoding="utf-8").replace("\r\n", "\n"), encoding="utf-8", newline="\n")
    wrapper_dst.chmod(0o755)

    fake_bin_dir = project_root / "fake-bin"
    fake_bin_dir.mkdir(parents=True, exist_ok=True)
    args_log = project_root / "args.log"
    env_log = project_root / "env.log"
    fake_python = fake_bin_dir / "python3"
    fake_date = fake_bin_dir / "date"
    _write_fake_python(fake_python, project_root, args_log, env_log)
    _write_fake_date(fake_date)

    explicit_cfg = project_root / "configs" / "explicit.yaml"
    explicit_cfg.parent.mkdir(parents=True, exist_ok=True)
    explicit_cfg.write_text("execution: {}\n", encoding="utf-8")

    _run_wrapper(
        wrapper_dst,
        fake_bin_dir=fake_bin_dir,
        env_vars={
            "V5_PYTHON_BIN": _bash_path(fake_python),
            "ARGS_LOG": _bash_path(args_log),
            "ENV_LOG": _bash_path(env_log),
            "V5_CONFIG": _bash_path(explicit_cfg),
        },
    )

    env_lines = env_log.read_text(encoding="utf-8").splitlines()
    assert f"V5_CONFIG={_bash_path(explicit_cfg)}" in env_lines


def test_run_hourly_live_window_fails_fast_when_runtime_config_missing(tmp_path: Path) -> None:
    project_root = tmp_path
    scripts_dir = project_root / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    wrapper_src = Path(__file__).resolve().parents[1] / "scripts" / "run_hourly_live_window.sh"
    wrapper_dst = scripts_dir / "run_hourly_live_window.sh"
    wrapper_dst.write_text(wrapper_src.read_text(encoding="utf-8").replace("\r\n", "\n"), encoding="utf-8", newline="\n")
    wrapper_dst.chmod(0o755)

    fake_bin_dir = project_root / "fake-bin"
    fake_bin_dir.mkdir(parents=True, exist_ok=True)
    args_log = project_root / "args.log"
    env_log = project_root / "env.log"
    fake_python = fake_bin_dir / "python3"
    fake_date = fake_bin_dir / "date"
    _write_fake_python(fake_python, project_root, args_log, env_log)
    _write_fake_date(fake_date)

    missing_cfg = project_root / "configs" / "missing.yaml"
    proc = _run_wrapper(
        wrapper_dst,
        fake_bin_dir=fake_bin_dir,
        env_vars={
            "V5_PYTHON_BIN": _bash_path(fake_python),
            "ARGS_LOG": _bash_path(args_log),
            "ENV_LOG": _bash_path(env_log),
            "V5_CONFIG": _bash_path(missing_cfg),
        },
        check=False,
    )

    assert proc.returncode != 0
    assert "runtime config not found" in proc.stderr
