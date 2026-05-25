from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType


ARCHIVE_ROOT = Path(__file__).resolve().parents[1]


def load_archive_module(relative_path: str) -> ModuleType:
    module_path = ARCHIVE_ROOT / relative_path
    module_name = "archive_20260313_" + "_".join(module_path.with_suffix("").parts[-3:])
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load archive module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

