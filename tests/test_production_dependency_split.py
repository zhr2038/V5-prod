from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_production_pipeline_starts_without_research_ml_imports(tmp_path: Path) -> None:
    code = f"""
import builtins
from pathlib import Path
original_import = builtins.__import__
def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == 'xgboost' or name.startswith('xgboost.') or name == 'sklearn' or name.startswith('sklearn.'):
        raise AssertionError(f'production path imported research dependency: {{name}}')
    return original_import(name, globals, locals, fromlist, level)
builtins.__import__ = guarded_import
from configs.schema import AppConfig
from src.core.pipeline import V5Pipeline
cfg = AppConfig(symbols=['BTC/USDT'])
cfg.alpha.ml_factor.enabled = False
cfg.execution.collect_ml_training_data = False
cfg.execution.order_store_path = str(Path({str(tmp_path)!r}) / 'orders.sqlite')
pipe = V5Pipeline(cfg)
assert pipe.data_collector is None
print('ok')
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "ok" in result.stdout
