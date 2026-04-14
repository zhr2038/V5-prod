from __future__ import annotations

import json
from pathlib import Path

from configs import loader


def test_load_blacklist_merges_runtime_auto_blacklist(monkeypatch, tmp_path: Path) -> None:
    configs_dir = tmp_path / "configs"
    reports_dir = tmp_path / "reports"
    configs_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    static_path = configs_dir / "blacklist.json"
    static_path.write_text(json.dumps({"symbols": ["BTC/USDT"]}, ensure_ascii=False), encoding="utf-8")
    (reports_dir / "shadow_auto_blacklist.json").write_text(
        json.dumps({"symbols": ["PEPE/USDT"]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (reports_dir / "auto_blacklist.json").write_text(
        json.dumps({"symbols": ["ROOT/USDT"]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(loader, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        loader,
        "resolve_auto_blacklist_path",
        lambda path="reports/auto_blacklist.json", project_root=tmp_path: (reports_dir / "shadow_auto_blacklist.json").resolve(),
    )

    payload = loader.load_blacklist("configs/blacklist.json")

    assert payload["symbols"] == ["BTC/USDT", "PEPE/USDT"]
