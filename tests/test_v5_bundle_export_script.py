import subprocess
import sys
from pathlib import Path


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

