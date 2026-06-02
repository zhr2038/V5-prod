import subprocess
import sys
from pathlib import Path


def test_v5_bundle_export_script_avoids_shell_subprocess() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "scripts" / "generate_v5_bundle_remote.sh").read_text(encoding="utf-8")

    assert "shell=True" not in script


def test_v5_bundle_export_script_writes_lightweight_report_index() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "scripts" / "generate_v5_bundle_remote.sh").read_text(encoding="utf-8")

    assert "def write_static_report_index(" in script
    assert 'write_text("reports/index.json"' in script
    assert 'write_text("reports/index.html"' in script
    assert 'write_text("raw/large/.noindex"' in script


def test_v5_bundle_export_script_regression() -> None:
    root = Path(__file__).resolve().parents[1]
    proc = subprocess.run(
        [sys.executable, "scripts/test_v5_bundle_export.py"],
        cwd=root,
        text=True,
        capture_output=True,
        timeout=180,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
