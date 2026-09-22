import json
from types import SimpleNamespace

from src.reporting.dashboard_command_center import _live_cost_evidence


def test_cost_evidence_is_runtime_scoped_and_staleness_does_not_disappear(tmp_path):
    paths = SimpleNamespace(reports_dir=tmp_path, orders_db=tmp_path / "shadow_orders.sqlite")
    report = {"schema_version": "v5.live_cost_evidence.v1", "status": "NO_FILLS",
              "generated_at_utc": "2026-09-22T00:00:00Z", "fill_count": 0,
              "live_order_effect": "none", "updates_live_cost_model": False}
    (tmp_path / "live_cost_evidence.json").write_text(json.dumps(report), encoding="utf-8")
    assert _live_cost_evidence(paths, 1790035200)["status"] == "missing"
    target = tmp_path / "shadow_live_cost_evidence.json"
    target.write_text(json.dumps(report), encoding="utf-8")
    current = _live_cost_evidence(paths, 1790035200)
    assert current["status"] == "observed" and current["report"]["fill_count"] == 0
    assert _live_cost_evidence(paths, 1790037001)["status"] == "stale"
    report.update(status="UNAVAILABLE", reason="database missing")
    target.write_text(json.dumps(report), encoding="utf-8")
    assert _live_cost_evidence(paths, 1790035200)["status"] == "unavailable"
    report["updates_live_cost_model"] = True
    target.write_text(json.dumps(report), encoding="utf-8")
    assert _live_cost_evidence(paths, 1790035200)["status"] == "invalid"
