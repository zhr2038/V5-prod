from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def isolate_default_runtime_artifacts(monkeypatch, tmp_path):
    import src.core.pipeline as pipeline_module
    import src.data.okx_instruments as okx_instruments

    runtime_root = tmp_path / "runtime_repo"
    reports_dir = runtime_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(pipeline_module, "REPORTS_DIR", reports_dir)

    original_init = okx_instruments.OKXSpotInstrumentsCache.__init__

    def _isolated_init(self, *args, **kwargs):
        kwargs.setdefault("cache_path", str(reports_dir / "okx_spot_instruments.json"))
        return original_init(self, *args, **kwargs)

    monkeypatch.setattr(okx_instruments.OKXSpotInstrumentsCache, "__init__", _isolated_init)
