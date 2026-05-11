from __future__ import annotations

import hashlib
import json
import tarfile
from pathlib import Path

from src.reporting.v5_bundle_exporter import export_v5_bundle


def test_bundle_export_contains_quant_lab_files_and_sha(tmp_path: Path) -> None:
    root = tmp_path / "root"
    reports = root / "reports"
    out = tmp_path / "bundles"
    reports.mkdir(parents=True)
    (reports / "quant_lab_usage.jsonl").write_text(
        json.dumps(
            {
                "ts": "2026-05-11T13:00:00Z",
                "run_id": "r1",
                "event_type": "cost_estimate",
                "symbol": "BTC/USDT",
                "regime": "normal",
                "notional_usdt": 200,
                "quantile": "p75",
                "total_cost_bps": 1.0,
                "effective_total_cost_bps": 5.0,
                "source": "public_spread_proxy",
                "passed": True,
                "filtered": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (reports / "quant_lab_requests.jsonl").write_text(
        json.dumps({"ts": "2026-05-11T13:00:00Z", "run_id": "r1", "method": "GET", "endpoint_path": "/v1/costs/estimate", "success": True}) + "\n",
        encoding="utf-8",
    )

    bundle = export_v5_bundle(reports_dir=reports, out_dir=out, window_hours=72)
    sha_path = Path(str(bundle) + ".sha256")

    assert bundle.exists()
    assert sha_path.exists()
    assert hashlib.sha256(bundle.read_bytes()).hexdigest() in sha_path.read_text(encoding="utf-8")
    with tarfile.open(bundle, "r:gz") as tf:
        names = tf.getnames()
        assert "raw/quant_lab/quant_lab_usage.jsonl" in names
        assert "raw/quant_lab/quant_lab_requests.jsonl" in names
        assert "summaries/quant_lab_compliance.csv" in names
        assert "summaries/quant_lab_cost_usage.csv" in names
        assert "summaries/quant_lab_fallbacks.csv" in names
        assert not any(Path(name).name == ".env" for name in names)
