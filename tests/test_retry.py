from __future__ import annotations

from src.utils import retry as retry_module


def test_sleep_with_jitter_uses_bounded_secret_jitter(monkeypatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(retry_module.secrets, "randbits", lambda bits: 1 << (bits - 1))
    monkeypatch.setattr(retry_module.time, "sleep", sleeps.append)

    retry_module._sleep_with_jitter(delay=2.0, jitter_frac=0.25)

    assert sleeps == [2.25]
