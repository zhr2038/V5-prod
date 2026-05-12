from __future__ import annotations

import json
from pathlib import Path

from configs.schema import AppConfig, QuantLabConfig
from src.quant_lab_client.mode import (
    QuantLabMode,
    resolve_quant_lab_mode,
    write_quant_lab_mode_override,
)


def test_invalid_quant_lab_mode_raises() -> None:
    try:
        QuantLabConfig(mode="unsafe")
    except ValueError as exc:
        assert "quant_lab.mode" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("invalid mode should raise")


def test_runtime_override_overrides_config_mode(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.runtime_override_path = str(tmp_path / "quant_lab_mode.json")

    write_quant_lab_mode_override(
        mode="local_only",
        reason="operator_manual_override",
        path=cfg.quant_lab.runtime_override_path,
    )

    resolution = resolve_quant_lab_mode(cfg)
    assert resolution.mode == QuantLabMode.LOCAL_ONLY
    assert resolution.mode_source == "runtime_override"
    assert resolution.override_reason == "operator_manual_override"


def test_invalid_runtime_override_falls_back_to_config(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.runtime_override_path = str(tmp_path / "quant_lab_mode.json")
    Path(cfg.quant_lab.runtime_override_path).write_text(json.dumps({"mode": "bad"}), encoding="utf-8")

    resolution = resolve_quant_lab_mode(cfg)
    assert resolution.mode == QuantLabMode.SHADOW
    assert resolution.mode_source == "config_invalid_override"
    assert resolution.warning


def test_enforce_runtime_override_rejected_with_allow_local_fallback(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.fail_policy = "allow_local_fallback"
    cfg.quant_lab.allow_local_fallback_in_enforce = False
    cfg.quant_lab.runtime_override_path = str(tmp_path / "quant_lab_mode.json")
    write_quant_lab_mode_override(
        mode="enforce",
        reason="operator_manual_override",
        path=cfg.quant_lab.runtime_override_path,
    )

    resolution = resolve_quant_lab_mode(cfg)

    assert resolution.mode == QuantLabMode.SHADOW
    assert resolution.mode_source == "config_unsafe_override"
    assert "allow_local_fallback" in str(resolution.warning)


def test_enforce_runtime_override_accepts_confirmed_allow_local_fallback(tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.fail_policy = "allow_local_fallback"
    cfg.quant_lab.allow_local_fallback_in_enforce = False
    cfg.quant_lab.runtime_override_path = str(tmp_path / "quant_lab_mode.json")
    write_quant_lab_mode_override(
        mode="enforce",
        reason="operator_manual_override",
        path=cfg.quant_lab.runtime_override_path,
        confirm_unsafe_fallback=True,
    )

    resolution = resolve_quant_lab_mode(cfg)

    assert resolution.mode == QuantLabMode.ENFORCE
    assert resolution.mode_source == "runtime_override"
    assert resolution.warning
