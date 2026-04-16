from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_configs_loader_import_tolerates_missing_dotenv() -> None:
    code = """
import sys
sys.modules['dotenv'] = None
import configs.loader
print('loader_import_ok')
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT)
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert "loader_import_ok" in proc.stdout
