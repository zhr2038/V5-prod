from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import scripts.smart_alert_check as smart_alert_check


def test_load_telegram_settings_reads_repo_root_env(tmp_path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "TELEGRAM_BOT_TOKEN=test-bot\nTELEGRAM_CHAT_ID=123456\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    bot_token, chat_id = smart_alert_check.load_telegram_settings(tmp_path)

    assert bot_token == "test-bot"
    assert chat_id == "123456"


def test_send_telegram_alert_uses_repo_root_env(tmp_path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "TELEGRAM_BOT_TOKEN=test-bot\nTELEGRAM_CHAT_ID=654321\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    calls: list[dict[str, object]] = []

    def _fake_post(url, json, timeout):
        calls.append({"url": url, "json": json, "timeout": timeout})
        return SimpleNamespace(status_code=200, text="ok")

    monkeypatch.setattr(smart_alert_check.requests, "post", _fake_post)

    ok = smart_alert_check.send_telegram_alert(
        {
            "level": "high",
            "title": "Alert title",
            "message": "Alert message",
            "suggestion": "Do something",
            "time": "2026-04-05 12:00:00",
        },
        project_root=tmp_path,
    )

    assert ok is True
    assert calls == [
        {
            "url": "https://api.telegram.org/bottest-bot/sendMessage",
            "json": {
                "chat_id": "654321",
                "text": smart_alert_check.format_alert_message(
                    {
                        "level": "high",
                        "title": "Alert title",
                        "message": "Alert message",
                        "suggestion": "Do something",
                        "time": "2026-04-05 12:00:00",
                    }
                ),
                "parse_mode": "HTML",
            },
            "timeout": 10,
        }
    ]


def test_main_uses_repo_root_workspace(monkeypatch) -> None:
    captured: dict[str, Path] = {}

    class _FakeEngine:
        def __init__(self, workspace: Path) -> None:
            captured["workspace"] = workspace

        def run_all_checks(self):
            return []

    monkeypatch.setattr(smart_alert_check, "SmartAlertEngine", _FakeEngine)

    assert smart_alert_check.main() == 0
    assert captured["workspace"] == smart_alert_check.PROJECT_ROOT
