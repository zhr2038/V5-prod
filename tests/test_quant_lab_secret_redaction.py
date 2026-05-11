from __future__ import annotations

import tarfile
from pathlib import Path

from src.reporting.v5_bundle_exporter import export_v5_bundle


def test_bundle_redacts_secret_values(tmp_path: Path) -> None:
    root = tmp_path / "root"
    reports = root / "reports"
    configs = root / "configs"
    reports.mkdir(parents=True)
    configs.mkdir(parents=True)
    (reports / "quant_lab_usage.jsonl").write_text("", encoding="utf-8")
    (reports / "quant_lab_requests.jsonl").write_text("", encoding="utf-8")
    (configs / "config.yaml").write_text(
        "exchange:\n  api_key: REALKEY\n  api_secret: REALSECRET\n  passphrase: REALPASS\nquant_lab:\n  api_token_env: QUANT_LAB_API_TOKEN\n",
        encoding="utf-8",
    )

    bundle = export_v5_bundle(reports_dir=reports, out_dir=tmp_path / "out")

    with tarfile.open(bundle, "r:gz") as tf:
        payload = b"".join(tf.extractfile(name).read() for name in tf.getnames() if tf.extractfile(name) is not None)
    text = payload.decode("utf-8", errors="ignore")
    assert "REALKEY" not in text
    assert "REALSECRET" not in text
    assert "REALPASS" not in text
    assert "QUANT_LAB_API_TOKEN" not in text
    assert "<REDACTED>" in text
