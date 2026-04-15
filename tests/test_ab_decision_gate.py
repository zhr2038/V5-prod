from __future__ import annotations

from pathlib import Path

import scripts.ab_decision_gate as ab_decision_gate


def test_resolve_ab_gate_output_path_uses_prefixed_runtime_file(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "shadow_orders.sqlite").write_text("", encoding="utf-8")

    path = ab_decision_gate._resolve_ab_gate_output_path(reports_dir)

    assert path == (reports_dir / "shadow_ab_gate_status.json").resolve()


def test_resolve_ab_gate_output_path_uses_suffixed_runtime_file(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "orders_accelerated.sqlite").write_text("", encoding="utf-8")

    path = ab_decision_gate._resolve_ab_gate_output_path(reports_dir)

    assert path == (reports_dir / "ab_gate_status_accelerated.json").resolve()
