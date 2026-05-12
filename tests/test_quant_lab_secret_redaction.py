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
        "\n".join(
            [
                "exchange:",
                "  api_key: REALKEY",
                "  api_secret: REALSECRET",
                "  passphrase: REALPASS",
                "quant_lab:",
                "  api_token_env: QUANT_LAB_API_TOKEN",
                "  api_env_path: /home/ubuntu/.quant-lab/api.env",
                "  allow_insecure_http_with_token: true",
                "  allow_local_fallback_in_enforce: false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    bundle = export_v5_bundle(reports_dir=reports, out_dir=tmp_path / "out")

    with tarfile.open(bundle, "r:gz") as tf:
        payload = b"".join(tf.extractfile(name).read() for name in tf.getnames() if tf.extractfile(name) is not None)
    text = payload.decode("utf-8", errors="ignore")
    assert "REALKEY" not in text
    assert "REALSECRET" not in text
    assert "REALPASS" not in text
    assert "api_token_env: QUANT_LAB_API_TOKEN" in text
    assert "api_env_path: /home/ubuntu/.quant-lab/api.env" in text
    assert "allow_insecure_http_with_token: true" in text
    assert "allow_local_fallback_in_enforce: false" in text
    assert "<REDACTED>" in text
