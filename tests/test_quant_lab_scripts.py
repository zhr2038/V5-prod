from __future__ import annotations

import json
from pathlib import Path

from configs.schema import AppConfig
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
