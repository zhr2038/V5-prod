from __future__ import annotations

import os
import subprocess
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


def _write_fake_python(path: Path, args_log: Path) -> None:
    path.write_text(
        "#!/bin/bash\n"
        "if [[ \"$1\" == \"-c\" ]]; then\n"
        "  exit 0\n"
        "fi\n"
        "printf '%s\\n' \"$@\" >> \"$ARGS_LOG\"\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def test_run_hourly_window_skips_compare_when_v4_reports_missing(tmp_path: Path) -> None:
    project_root = tmp_path
    scripts_dir = project_root / "scripts"
    reports_dir = project_root / "reports" / "runs" / "20260415_23"
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
    fake_python = fake_bin_dir / "python3"
    fake_date = fake_bin_dir / "date"
    _write_fake_python(fake_python, args_log)
    _write_fake_date(fake_date)

    env = {
        **os.environ,
        "PATH": f"{fake_bin_dir}:{os.environ.get('PATH', '')}",
        "V5_PYTHON_BIN": str(fake_python),
        "ARGS_LOG": str(args_log),
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
    reports_dir = project_root / "reports" / "runs" / "20260415_23"
    v4_dir = project_root / "v4_export"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    v4_dir.mkdir(parents=True, exist_ok=True)

    wrapper_src = Path(__file__).resolve().parents[1] / "scripts" / "run_hourly_window.sh"
    wrapper_dst = scripts_dir / "run_hourly_window.sh"
    wrapper_dst.write_text(wrapper_src.read_text(encoding="utf-8"), encoding="utf-8")
    wrapper_dst.chmod(0o755)

    (reports_dir / "summary.json").write_text("{}", encoding="utf-8")

    fake_bin_dir = project_root / "fake-bin"
    fake_bin_dir.mkdir(parents=True, exist_ok=True)
    args_log = project_root / "args.log"
    fake_python = fake_bin_dir / "python3"
    fake_date = fake_bin_dir / "date"
    _write_fake_python(fake_python, args_log)
    _write_fake_date(fake_date)

    env = {
        **os.environ,
        "PATH": f"{fake_bin_dir}:{os.environ.get('PATH', '')}",
        "V5_PYTHON_BIN": str(fake_python),
        "V4_REPORTS_DIR": str(v4_dir),
        "ARGS_LOG": str(args_log),
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
