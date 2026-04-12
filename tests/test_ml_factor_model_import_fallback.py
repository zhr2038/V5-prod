from __future__ import annotations

import builtins
import importlib
import sys


def test_ml_factor_model_optional_native_import_errors_degrade_to_unavailable(monkeypatch) -> None:
    saved_modules = {
        name: sys.modules.get(name)
        for name in ("src.execution.ml_factor_model", "lightgbm", "xgboost")
    }
    for name in saved_modules:
        sys.modules.pop(name, None)

    original_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {"lightgbm", "xgboost"}:
            raise OSError("missing native dependency")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    module = importlib.import_module("src.execution.ml_factor_model")

    assert module.LIGHTGBM_AVAILABLE is False
    assert module.XGBOOST_AVAILABLE is False
    assert module.lgb is None
    assert module.xgb is None

    sys.modules.pop("src.execution.ml_factor_model", None)
    for name, mod in saved_modules.items():
        if mod is not None:
            sys.modules[name] = mod
        else:
            sys.modules.pop(name, None)
