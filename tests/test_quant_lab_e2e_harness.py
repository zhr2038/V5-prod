from __future__ import annotations

import json
import tarfile
from pathlib import Path

from scripts.quant_lab_e2e_harness import run_harness


def test_quant_lab_e2e_harness_generates_ingestable_bundle_fixture(tmp_path: Path) -> None:
    report = run_harness(tmp_path / "e2e")
    bundle = Path(report["bundle_path"])
    report_path = Path(report["report_path"])

    assert report["passed"] is True
    assert report_path.exists()
    assert json.loads(report_path.read_text(encoding="utf-8"))["passed"] is True
    assert bundle.exists()
    assert Path(str(bundle) + ".sha256").exists()

    with tarfile.open(bundle, "r:gz") as tf:
        names = set(tf.getnames())
        manifest = json.loads(tf.extractfile("manifest.json").read().decode("utf-8"))  # type: ignore[union-attr]

    assert "raw/quant_lab/quant_lab_usage.jsonl" in names
    assert "raw/quant_lab/quant_lab_requests.jsonl" in names
    assert "summaries/quant_lab_cost_usage.csv" in names
    assert "summaries/quant_lab_permission_audit.csv" in names
    assert "summaries/quant_lab_fallbacks.csv" in names
    assert "summaries/trade_metrics.csv" in names
    assert manifest["contract_version"] == "v5.quant_lab.telemetry.v2"
    assert manifest["schema_version"] == "1.0.0"
