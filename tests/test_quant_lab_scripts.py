from __future__ import annotations

import json
from pathlib import Path

from configs.schema import AppConfig
from scripts import quant_lab_mode
from scripts import quant_lab_selfcheck
from src.quant_lab_client.models import CostEstimate, QuantLabHealth, RiskPermission


def test_quant_lab_selfcheck_does_not_access_okx(monkeypatch, tmp_path: Path, capsys) -> None:
    cfg = AppConfig()
    cfg.quant_lab.enabled = True
    monkeypatch.setattr(quant_lab_selfcheck, "load_config", lambda _path: cfg)

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        @classmethod
        def from_config(cls, *args, **kwargs):
            return cls()

        def get_health(self):
            return QuantLabHealth(status="ok", service="quant-lab", mode="read-only")

        def get_live_permission(self, *, strategy: str, version: str):
            return RiskPermission(strategy=strategy, version=version, permission="SELL_ONLY", reasons=["required_alpha_gate_quarantine"])

        def estimate_cost(self, **kwargs):
            return CostEstimate(symbol="BTC-USDT", regime="normal", total_cost_bps=1.2, source="public_spread_proxy")

    monkeypatch.setattr(quant_lab_selfcheck, "QuantLabClient", FakeClient)
    out = tmp_path / "selfcheck.json"

    rc = quant_lab_selfcheck.main(["--config", "unused.yaml", "--out", str(out)])

    assert rc == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["permission"] == "SELL_ONLY"
    assert payload["safe_for_new_risk"] is False
    assert "P@ssw0rd" not in capsys.readouterr().out


def test_quant_lab_mode_script_set_and_show(monkeypatch, tmp_path: Path, capsys) -> None:
    override = tmp_path / "quant_lab_mode.json"
    rc = quant_lab_mode.main(["set", "--mode", "local_only", "--reason", "test", "--path", str(override)])
    assert rc == 0
    payload = json.loads(override.read_text(encoding="utf-8"))
    assert payload["mode"] == "local_only"

    cfg = AppConfig()
    cfg.quant_lab.mode = "shadow"
    cfg.quant_lab.runtime_override_path = str(override)
    monkeypatch.setattr(quant_lab_mode, "load_config", lambda _path: cfg)

    rc = quant_lab_mode.main(["show", "--config", "unused.yaml"])
    assert rc == 0
    output = capsys.readouterr().out
    assert '"mode": "local_only"' in output
    assert "P@ssw0rd" not in output


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
