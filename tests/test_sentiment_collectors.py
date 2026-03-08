import importlib.util
from pathlib import Path


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
