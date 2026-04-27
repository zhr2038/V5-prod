from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROD_SYSTEMD_UNITS = (
    "v5-auto-risk-eval.service",
    "v5-cost-rollup-real.user.service",
    "v5-daily-ml-training.service",
    "v5-event-driven.service",
    "v5-ledger.service",
    "v5-model-promotion-gate.service",
    "v5-prod.user.service",
    "v5-reconcile.service",
    "v5-sentiment-collect.service",
    "v5-spread-rollup.service",
    "v5-trade-monitor.service",
    "v5-web-dashboard.service",
)


def test_prod_systemd_units_match_ubuntu_workspace_path() -> None:
    for unit in PROD_SYSTEMD_UNITS:
        path = PROJECT_ROOT / "deploy" / "systemd" / unit
        text = path.read_text(encoding="utf-8")

        assert "/home/ubuntu/clawd/v5-prod" in text, unit
        assert "/home/admin" not in text, unit
        assert "v5-trading-bot" not in text, unit
        assert "\nUser=admin" not in text, unit
        assert "\nGroup=admin" not in text, unit
