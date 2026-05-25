from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


def test_archive_research_cleanup_regression() -> None:
    project_root = Path(__file__).resolve().parents[1]
    archive_tests = project_root / "archive" / "20260313-research-cleanup" / "tests"
    if not archive_tests.exists():
        pytest.skip("archived research tests are not present in this checkout")

    proc = subprocess.run(
        [sys.executable, "-m", "pytest", str(archive_tests), "-q"],
        cwd=project_root,
        text=True,
        capture_output=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
