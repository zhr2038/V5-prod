from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


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
    )
    path.chmod(0o755)


def _write_fake_python(path: Path, project_root: Path, args_log: Path, env_log: Path, real_python: str) -> None:
    path.write_text(
        "#!/bin/bash\n"
        "if [[ \"$1\" == \"-c\" ]]; then\n"
        "  exit 0\n"
        "fi\n"
        "if [[ \"$1\" == \"-\" ]]; then\n"
        "  script=$(cat)\n"
        "  if [[ \"$script\" == *\"resolve_runtime_config_path\"* ]]; then\n"
        "    if [[ -n \"$V5_CONFIG\" ]]; then\n"
        "      echo \"$V5_CONFIG\"\n"
        "    else\n"
        f"      echo \"{(project_root / 'configs' / 'runtime.yaml').resolve()}\"\n"
        "    fi\n"
        "    exit 0\n"
        "  fi\n"
        "  if [[ \"$script\" == *\"runtime config not found\"* ]]; then\n"
        f"    cfg=\"${{V5_CONFIG:-{(project_root / 'configs' / 'runtime.yaml').resolve()}}}\"\n"
        "    if [[ ! -f \"$cfg\" ]]; then\n"
        "      echo \"runtime config not found: $cfg\" >&2\n"
        "      exit 1\n"
        "    fi\n"
        "    if ! grep -q '^execution:' \"$cfg\"; then\n"
        "      echo \"runtime config missing execution section: $cfg\" >&2\n"
        "      exit 1\n"
        "    fi\n"
        "    exit 0\n"
        "  fi\n"
        "  if [[ \"$script\" == *\"derive_runtime_reports_dir\"* ]]; then\n"
        f"    echo \"{(project_root / 'reports' / 'shadow_runtime').resolve()}\"\n"
        "    exit 0\n"
        "  fi\n"
        "  exit 0\n"
        "fi\n"
        "printf '%s\\n' \"$@\" >> \"$ARGS_LOG\"\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def test_run_hourly_window_skips_compare_when_v4_reports_missing(tmp_path: Path) -> None:
    project_root = tmp_path
    scripts_dir = project_root / "scripts"
    reports_dir = project_root / "reports" / "shadow_runtime" / "runs" / "20260415_23"
    (project_root / "configs").mkdir(parents=True, exist_ok=True)
    (project_root / "configs" / "runtime.yaml").write_text("execution: {}\n", encoding="utf-8")
    scripts_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    wrapper_src = Path(__file__).resolve().parents[1] / "scripts" / "run_hourly_window.sh"
    wrapper_dst = scripts_dir / "run_hourly_window.sh"
    wrapper_dst.write_text(wrapper_src.read_text(encoding="utf-8"), encoding="utf-8")
    wrapper_dst.chmod(0o755)

    (reports_dir / "summary.json").write_text("{}", encoding="utf-8")

    fake_bin_dir = project_root / "fake-bin"
    fake_bin_dir.mkdir(parents=True, exist_ok=True)
    args_log = project_root / "args.log"
    env_log = project_root / "env.log"
    fake_python = fake_bin_dir / "python3"
    fake_date = fake_bin_dir / "date"
    _write_fake_python(fake_python, project_root, args_log, env_log, sys.executable)
    _write_fake_date(fake_date)

    env = {
        **os.environ,
        "PATH": f"{fake_bin_dir}:{os.environ.get('PATH', '')}",
        "V5_PYTHON_BIN": str(fake_python),
        "ARGS_LOG": str(args_log),
        "ENV_LOG": str(env_log),
    }

    result = subprocess.run(
        ["/bin/bash", str(wrapper_dst)],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    args = args_log.read_text(encoding="utf-8").splitlines()
    assert args == ["main.py"]
    assert "skip compare_runs: V4 reports dir unavailable" in result.stdout


def test_run_hourly_window_uses_runtime_v4_reports_dir_when_present(tmp_path: Path) -> None:
    project_root = tmp_path
    scripts_dir = project_root / "scripts"
    reports_dir = project_root / "reports" / "shadow_runtime" / "runs" / "20260415_23"
    v4_dir = project_root / "v4_export"
    runtime_reports_dir = project_root / "reports" / "shadow_runtime"
    (project_root / "configs").mkdir(parents=True, exist_ok=True)
    (project_root / "configs" / "runtime.yaml").write_text(
        "execution:\n  order_store_path: reports/shadow_runtime/orders.sqlite\n",
        encoding="utf-8",
    )
    scripts_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    runtime_reports_dir.mkdir(parents=True, exist_ok=True)
    v4_dir.mkdir(parents=True, exist_ok=True)

    wrapper_src = Path(__file__).resolve().parents[1] / "scripts" / "run_hourly_window.sh"
    wrapper_dst = scripts_dir / "run_hourly_window.sh"
    wrapper_dst.write_text(wrapper_src.read_text(encoding="utf-8"), encoding="utf-8")
    wrapper_dst.chmod(0o755)

    (reports_dir / "summary.json").write_text("{}", encoding="utf-8")

    fake_bin_dir = project_root / "fake-bin"
    fake_bin_dir.mkdir(parents=True, exist_ok=True)
    args_log = project_root / "args.log"
    env_log = project_root / "env.log"
    fake_python = fake_bin_dir / "python3"
    fake_date = fake_bin_dir / "date"
    _write_fake_python(fake_python, project_root, args_log, env_log, sys.executable)
    _write_fake_date(fake_date)

    env = {
        **os.environ,
        "PATH": f"{fake_bin_dir}:{os.environ.get('PATH', '')}",
        "V5_PYTHON_BIN": str(fake_python),
        "V4_REPORTS_DIR": str(v4_dir),
        "ARGS_LOG": str(args_log),
        "ENV_LOG": str(env_log),
    }

    subprocess.run(
        ["/bin/bash", str(wrapper_dst)],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    args = args_log.read_text(encoding="utf-8").splitlines()
    assert args[0] == "main.py"
    assert args[1] == "scripts/compare_runs.py"
    assert args[2] == "--v4_reports_dir"
    assert args[3] == str(v4_dir)
    assert args[4] == "--v5_summary"
    assert args[5] == str((runtime_reports_dir / "runs" / "20260415_23" / "summary.json").resolve())
    assert args[6] == "--out"
    assert args[7] == str((runtime_reports_dir / "compare" / "hourly" / "compare_20260415_23.md").resolve())


def test_run_hourly_window_fails_fast_when_runtime_config_missing(tmp_path: Path) -> None:
    project_root = tmp_path
    scripts_dir = project_root / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    wrapper_src = Path(__file__).resolve().parents[1] / "scripts" / "run_hourly_window.sh"
    wrapper_dst = scripts_dir / "run_hourly_window.sh"
    wrapper_dst.write_text(wrapper_src.read_text(encoding="utf-8"), encoding="utf-8")
    wrapper_dst.chmod(0o755)

    fake_bin_dir = project_root / "fake-bin"
    fake_bin_dir.mkdir(parents=True, exist_ok=True)
    args_log = project_root / "args.log"
    env_log = project_root / "env.log"
    fake_python = fake_bin_dir / "python3"
    fake_date = fake_bin_dir / "date"
    _write_fake_python(fake_python, project_root, args_log, env_log, sys.executable)
    _write_fake_date(fake_date)

    env = {
        **os.environ,
        "PATH": f"{fake_bin_dir}:{os.environ.get('PATH', '')}",
        "V5_PYTHON_BIN": str(fake_python),
        "V5_CONFIG": str((project_root / "configs" / "missing.yaml").resolve()),
        "ARGS_LOG": str(args_log),
        "ENV_LOG": str(env_log),
    }

    result = subprocess.run(
        ["/bin/bash", str(wrapper_dst)],
        cwd=project_root,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "runtime config not found" in result.stderr
    assert not args_log.exists()
