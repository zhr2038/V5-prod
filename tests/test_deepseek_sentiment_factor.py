from __future__ import annotations

from src.factors.deepseek_sentiment_factor import DeepSeekSentimentFactor


def test_deepseek_sentiment_factor_uses_runtime_env_path(monkeypatch, tmp_path):
    (tmp_path / ".env").write_text("DEEPSEEK_API_KEY=root-key\n", encoding="utf-8")
    (tmp_path / ".env.runtime").write_text("DEEPSEEK_API_KEY=runtime-key\n", encoding="utf-8")
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    factor = DeepSeekSentimentFactor(
        cache_dir=str(tmp_path / "cache"),
        env_path=".env.runtime",
        project_root=tmp_path,
    )

    assert factor.api_key == "runtime-key"


def test_deepseek_sentiment_factor_explicit_api_key_wins(monkeypatch, tmp_path):
    (tmp_path / ".env.runtime").write_text("DEEPSEEK_API_KEY=runtime-key\n", encoding="utf-8")
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    factor = DeepSeekSentimentFactor(
        cache_dir=str(tmp_path / "cache"),
        api_key="explicit-key",
        env_path=".env.runtime",
        project_root=tmp_path,
    )

    assert factor.api_key == "explicit-key"


def test_deepseek_sentiment_factor_resolves_relative_cache_dir_from_project_root(monkeypatch, tmp_path):
    (tmp_path / ".env.runtime").write_text("DEEPSEEK_API_KEY=runtime-key\n", encoding="utf-8")
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    factor = DeepSeekSentimentFactor(
        cache_dir="data/custom_sentiment_cache",
        env_path=".env.runtime",
        project_root=tmp_path,
    )

    assert factor.cache_dir == (tmp_path / "data" / "custom_sentiment_cache").resolve()
