import importlib.util
import json
from pathlib import Path

import pytest


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_collect_funding_sentiment_uses_repo_relative_cache_dir():
    module = _load_module(
        Path("scripts/collect_funding_sentiment.py"),
        "collect_funding_sentiment_test",
    )

    expected_root = Path(module.__file__).resolve().parents[1]
    assert module.PROJECT_ROOT == expected_root
    assert module.get_cache_dir() == expected_root / "data" / "sentiment_cache"


def test_collect_rss_sentiment_uses_repo_relative_cache_dir():
    module = _load_module(
        Path("scripts/collect_rss_sentiment.py"),
        "collect_rss_sentiment_test",
    )

    expected_root = Path(module.__file__).resolve().parents[1]
    assert module.PROJECT_ROOT == expected_root
    assert module.get_cache_dir() == expected_root / "data" / "sentiment_cache"


def test_collect_funding_sentiment_writes_weighted_composite(monkeypatch, tmp_path):
    module = _load_module(
        Path("scripts/collect_funding_sentiment.py"),
        "collect_funding_sentiment_weighted_test",
    )

    monkeypatch.setattr(module, "get_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(
        module,
        "get_all_symbols",
        lambda: {
            "AAA-USDT": {"tier": "large", "tier_weight": 0.5, "weight_in_tier": 0.9, "total_weight": 0.45},
            "BBB-USDT": {"tier": "large", "tier_weight": 0.5, "weight_in_tier": 0.1, "total_weight": 0.05},
        },
    )
    rate_map = {
        "AAA-USDT-SWAP": {"funding_rate": 0.0002},
        "BBB-USDT-SWAP": {"funding_rate": -0.0001},
    }
    monkeypatch.setattr(module, "get_okx_funding_rate", lambda inst_id: rate_map[inst_id])

    module.collect_funding_sentiment()

    composite_files = list(tmp_path.glob("funding_COMPOSITE_*.json"))
    assert len(composite_files) == 1
    payload = json.loads(composite_files[0].read_text(encoding="utf-8"))

    assert payload["f6_sentiment"] == pytest.approx(0.17)
    assert payload["tier_breakdown"]["large"]["weighted_avg"] == pytest.approx(0.17)
    assert payload["positive_weight_share"] == pytest.approx(0.9)
    assert payload["negative_weight_share"] == pytest.approx(0.1)
