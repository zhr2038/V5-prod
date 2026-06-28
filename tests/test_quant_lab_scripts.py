from __future__ import annotations

import json
from pathlib import Path

import pytest

from configs.schema import AppConfig
from scripts import quant_lab_mode
from scripts import quant_lab_selfcheck
from src.quant_lab_client.models import CostEstimate


def test_quant_lab_selfcheck_does_not_access_okx(monkeypatch, tmp_path: Path, capsys) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    monkeypatch.setattr(quant_lab_selfcheck, "load_config", lambda _path: cfg)

    class FakeClient:
        api_token = "super-secret-token"
        api_env_path_present = True
        api_env_secure_permissions = True
        api_env_token_loaded = True
        api_env_warning = None
        token_auth_disabled_reason = None

        def __init__(self, *args, **kwargs):
            pass

        @classmethod
        def from_config(cls, *args, **kwargs):
            return cls()

        @staticmethod
        def _validate_health(health, *, endpoint: str, allow_warning: bool):
            assert endpoint in {"/v1/health", "/v1/health/deep"}
            assert allow_warning is (endpoint == "/v1/health/deep")

        def get_json(self, endpoint: str, params=None):
            class Response:
                ok = True
                status_code = 200
                cached = False
                error = None

                def __init__(self, data):
                    self.data = data

            if endpoint == "/v1/health":
                return Response({"status": "ok", "service": "quant-lab", "mode": "read-only"})
            if endpoint == "/v1/health/deep":
                return Response(
                    {
                        "status": "warning",
                        "service": "quant-lab",
                        "mode": "read-only",
                        "warnings": ["cost_health_warning"],
                        "cost_health": {"status": "warning"},
                        "data_health": {"status": "ok"},
                        "risk_permission_dependency_meta": {"status": "ok"},
                    }
                )
            if endpoint == "/v1/risk/live-permission":
                return Response(
                    {
                        "strategy": params["strategy"],
                        "version": params["version"],
                        "permission": "SELL_ONLY",
                        "reasons": ["required_alpha_gate_quarantine"],
                    }
                )
            if endpoint == "/v1/strategy-opportunity-advisory/v5-compact":
                return Response({"items": [{"strategy_id": "SOL_USDT_F3_DOMINANT_ENTRY_PAPER_V1"}]})
            raise AssertionError(endpoint)

        def estimate_cost(self, **kwargs):
            return CostEstimate(symbol="BTC-USDT", regime="normal", total_cost_bps=1.2, source="public_spread_proxy")

    monkeypatch.setattr(quant_lab_selfcheck, "QuantLabClient", FakeClient)
    out = tmp_path / "selfcheck.json"

    rc = quant_lab_selfcheck.main(["--config", "unused.yaml", "--out", str(out)])

    assert rc == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["permission"] == "SELL_ONLY"
    assert payload["deep_health"]["status"] == "warning"
    assert payload["deep_health"]["warnings"] == ["cost_health_warning"]
    assert payload["deep_health"]["cost_health"] == {"status": "warning"}
    assert payload["safe_for_new_risk"] is False
    assert payload["api_token_loaded"] is True
    assert payload["api_env_token_loaded"] is True
    assert payload["endpoint_checks"]["/v1/health/deep"]["status_code"] == 200
    assert payload["endpoint_checks"]["/v1/risk/live-permission"]["permission"] == "SELL_ONLY"
    advisory_check = payload["endpoint_checks"]["/v1/strategy-opportunity-advisory/v5-compact"]
    assert advisory_check["status_code"] == 200
    assert advisory_check["item_count"] == 1
    assert "P@ssw0rd" not in capsys.readouterr().out


def test_quant_lab_mode_script_set_and_show(monkeypatch, tmp_path: Path, capsys) -> None:
    override = tmp_path / "quant_lab_mode.json"
    rc = quant_lab_mode.main(["set", "--mode", "local_only", "--reason", "test", "--path", str(override)])
    assert rc == 0
    payload = json.loads(override.read_text(encoding="utf-8"))
    assert payload["mode"] == "local_only"
    assert payload["confirmed"] is False

    cfg = AppConfig()
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.runtime_override_path = str(override)
    monkeypatch.setattr(quant_lab_mode, "load_config", lambda _path: cfg)

    rc = quant_lab_mode.main(["show", "--config", "unused.yaml"])
    assert rc == 0
    output = capsys.readouterr().out
    assert '"mode": "local_only"' in output
    assert "P@ssw0rd" not in output


def test_quant_lab_mode_script_shadow_does_not_require_confirmation(tmp_path: Path) -> None:
    override = tmp_path / "quant_lab_mode.json"

    rc = quant_lab_mode.main(["set", "--mode", "shadow", "--reason", "test", "--path", str(override)])

    assert rc == 0
    payload = json.loads(override.read_text(encoding="utf-8"))
    assert payload["mode"] == "shadow"
    assert payload["confirmed"] is False
    assert "confirmation_method" not in payload


def test_quant_lab_mode_script_rejects_enforce_without_confirmation(monkeypatch, tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.fail_policy = "sell_only"
    monkeypatch.setattr(quant_lab_mode, "load_config", lambda _path: cfg)

    with pytest.raises(SystemExit) as exc_info:
        quant_lab_mode.main(
            [
                "set",
                "--mode",
                "enforce",
                "--reason",
                "test",
                "--path",
                str(tmp_path / "quant_lab_mode.json"),
            ]
        )

    assert exc_info.value.code == 2
    assert not (tmp_path / "quant_lab_mode.json").exists()


def test_quant_lab_mode_script_accepts_enforce_confirmation(monkeypatch, tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.fail_policy = "sell_only"
    monkeypatch.setattr(quant_lab_mode, "load_config", lambda _path: cfg)
    override = tmp_path / "quant_lab_mode.json"

    rc = quant_lab_mode.main(
        [
            "set",
            "--mode",
            "enforce",
            "--reason",
            "test",
            "--path",
            str(override),
            "--confirm-enforce",
            "YES",
        ]
    )

    assert rc == 0
    payload = json.loads(override.read_text(encoding="utf-8"))
    assert payload["mode"] == "enforce"
    assert payload["confirmed"] is True
    assert payload["confirmation_method"] == "cli:confirm_enforce"


def test_quant_lab_mode_script_accepts_enforce_env_confirmation(monkeypatch, tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.fail_policy = "sell_only"
    monkeypatch.setattr(quant_lab_mode, "load_config", lambda _path: cfg)
    monkeypatch.setenv("V5_QUANT_LAB_CONFIRM_ENFORCE", "YES")
    override = tmp_path / "quant_lab_mode.json"

    rc = quant_lab_mode.main(["set", "--mode", "enforce", "--reason", "test", "--path", str(override)])

    assert rc == 0
    payload = json.loads(override.read_text(encoding="utf-8"))
    assert payload["confirmed"] is True
    assert payload["confirmation_method"] == "env:V5_QUANT_LAB_CONFIRM_ENFORCE"


def test_quant_lab_mode_script_gated_modes_require_confirmation(monkeypatch, tmp_path: Path) -> None:
    cfg = AppConfig()
    monkeypatch.setattr(quant_lab_mode, "load_config", lambda _path: cfg)

    with pytest.raises(SystemExit):
        quant_lab_mode.main(
            [
                "set",
                "--mode",
                "cost_only",
                "--reason",
                "test",
                "--path",
                str(tmp_path / "cost_only.json"),
            ]
        )

    override = tmp_path / "permission_only.json"
    rc = quant_lab_mode.main(
        [
            "set",
            "--mode",
            "permission_only",
            "--reason",
            "test",
            "--path",
            str(override),
            "--confirm-gated-mode",
            "YES",
        ]
    )

    assert rc == 0
    payload = json.loads(override.read_text(encoding="utf-8"))
    assert payload["mode"] == "permission_only"
    assert payload["confirmed"] is True
    assert payload["confirmation_method"] == "cli:confirm_gated_mode"


def test_quant_lab_mode_script_rejects_enforce_with_unsafe_fallback(monkeypatch, tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.fail_policy = "allow_local_fallback"
    cfg.quant_lab.allow_local_fallback_in_enforce = False
    monkeypatch.setattr(quant_lab_mode, "load_config", lambda _path: cfg)

    with pytest.raises(SystemExit) as exc_info:
        quant_lab_mode.main(
            [
                "set",
                "--mode",
                "enforce",
                "--reason",
                "test",
                "--path",
                str(tmp_path / "quant_lab_mode.json"),
                "--confirm-enforce",
                "YES",
            ]
        )
    assert exc_info.value.code == 2
    assert not (tmp_path / "quant_lab_mode.json").exists()


def test_quant_lab_mode_script_accepts_confirmed_enforce_fallback(monkeypatch, tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.fail_policy = "allow_local_fallback"
    cfg.quant_lab.allow_local_fallback_in_enforce = False
    monkeypatch.setattr(quant_lab_mode, "load_config", lambda _path: cfg)
    override = tmp_path / "quant_lab_mode.json"

    rc = quant_lab_mode.main(
        [
            "set",
            "--mode",
            "enforce",
            "--reason",
            "test",
            "--path",
            str(override),
            "--confirm-enforce",
            "YES",
            "--confirm-unsafe-fallback",
        ]
    )

    assert rc == 0
    payload = json.loads(override.read_text(encoding="utf-8"))
    assert payload["mode"] == "enforce"
    assert payload["confirmed"] is True
    assert payload["confirmation_method"] == "cli:confirm_enforce"
    assert payload["confirm_unsafe_fallback"] is True


def test_quant_lab_selfcheck_local_only_skips_client(monkeypatch, tmp_path: Path) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    cfg.quant_lab.mode = "local_only"
    cfg.quant_lab.runtime_override_path = str(tmp_path / "missing_override.json")
    monkeypatch.setattr(quant_lab_selfcheck, "load_config", lambda _path: cfg)

    class FailingClient:
        @classmethod
        def from_config(cls, *args, **kwargs):
            raise AssertionError("quant-lab client must not be built in local_only")

    monkeypatch.setattr(quant_lab_selfcheck, "QuantLabClient", FailingClient)
    out = tmp_path / "selfcheck.json"

    rc = quant_lab_selfcheck.main(["--config", "unused.yaml", "--out", str(out)])

    assert rc == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["mode"] == "local_only"
    assert payload["called_api"] is False
    assert payload["permission"] == "ALLOW_LOCAL"
