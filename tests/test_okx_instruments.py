from __future__ import annotations

import tempfile
from pathlib import Path

from src.data.okx_instruments import OKXSpotInstrumentsCache


def test_okx_spot_instruments_cache_uses_temp_path_under_pytest(monkeypatch):
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "tests/test_okx_instruments.py::test")

    cache = OKXSpotInstrumentsCache()

    assert cache.cache_path == Path(tempfile.gettempdir()) / "v5-test-cache" / "okx_spot_instruments.json"


def test_okx_spot_instruments_cache_keeps_explicit_path(monkeypatch, tmp_path):
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "tests/test_okx_instruments.py::test")

    cache = OKXSpotInstrumentsCache(cache_path=str(tmp_path / "custom.json"))

    assert cache.cache_path == tmp_path / "custom.json"


def test_okx_spot_instruments_cache_reads_repo_seed_without_fetch_under_pytest(monkeypatch):
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "tests/test_okx_instruments.py::test")

    cache = OKXSpotInstrumentsCache()

    def _fail_fetch():
        raise AssertionError("pytest should use bundled seed cache before fetching")

    monkeypatch.setattr(cache, "_fetch", _fail_fetch)

    spec = cache.get_spec("BTC-USDT")

    assert spec is not None
    assert spec.inst_id == "BTC-USDT"
