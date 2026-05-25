from __future__ import annotations

import builtins
import json
import importlib
import sys

import pytest


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


def test_ridge_pickle_model_load_requires_matching_sha256(tmp_path, monkeypatch) -> None:
    from src.execution.ml_factor_model import MLFactorConfig, MLFactorModel

    model = MLFactorModel(MLFactorConfig(model_type="ridge"))
    model.model = {"kind": "test-model"}
    model.scaler = {"kind": "test-scaler"}
    model.feature_names = ["f1"]
    model.is_trained = True
    base_path = tmp_path / "ml_factor_model"

    model.save_model(str(base_path))
    config_path = tmp_path / "ml_factor_model_config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    assert config["pickle_artifact_sha256"]

    loaded = MLFactorModel()
    loaded.load_model(str(base_path))
    assert loaded.model == {"kind": "test-model"}

    with (tmp_path / "ml_factor_model.pkl").open("ab") as handle:
        handle.write(b"tamper")

    monkeypatch.delenv("V5_ALLOW_LEGACY_PICKLE_MODEL_LOAD", raising=False)
    with pytest.raises(RuntimeError, match="sha256 mismatch"):
        MLFactorModel().load_model(str(base_path))


def test_ridge_pickle_model_load_rejects_missing_sha256(tmp_path, monkeypatch) -> None:
    from src.execution.ml_factor_model import MLFactorConfig, MLFactorModel

    model = MLFactorModel(MLFactorConfig(model_type="ridge"))
    model.model = {"kind": "legacy-model"}
    model.scaler = None
    model.feature_names = ["f1"]
    model.is_trained = True
    base_path = tmp_path / "legacy_model"
    model.save_model(str(base_path))

    config_path = tmp_path / "legacy_model_config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config.pop("pickle_artifact_sha256", None)
    config_path.write_text(json.dumps(config), encoding="utf-8")
    monkeypatch.delenv("V5_ALLOW_LEGACY_PICKLE_MODEL_LOAD", raising=False)

    with pytest.raises(RuntimeError, match="sha256 missing"):
        MLFactorModel().load_model(str(base_path))
