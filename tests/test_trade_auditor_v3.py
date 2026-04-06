from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import scripts.trade_auditor_v3 as trade_auditor_v3


def test_build_paths_anchor_trade_auditor_to_repo_root(tmp_path) -> None:
    paths = trade_auditor_v3.build_paths(tmp_path)

    assert paths.workspace == tmp_path.resolve()
    assert paths.reports_dir == tmp_path / "reports"
    assert paths.orders_db == tmp_path / "reports" / "orders.sqlite"
    assert paths.env_path == tmp_path / ".env"


def test_load_exchange_credentials_reads_repo_root_env(tmp_path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "OKX_API_KEY=test-key",
                "OKX_API_SECRET=test-secret",
                "OKX_API_PASSPHRASE=test-passphrase",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)
    monkeypatch.delenv("OKX_API_KEY", raising=False)
    monkeypatch.delenv("OKX_API_SECRET", raising=False)
    monkeypatch.delenv("OKX_API_PASSPHRASE", raising=False)

    creds = trade_auditor_v3.load_exchange_credentials(trade_auditor_v3.build_paths(tmp_path))

    assert creds == ("test-key", "test-secret", "test-passphrase")


def test_get_recent_orders_reads_workspace_orders_db(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    db_path = reports_dir / "orders.sqlite"

    now_ms = int(datetime.now().timestamp() * 1000)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE orders (
            inst_id TEXT,
            side TEXT,
            state TEXT,
            created_ts INTEGER
        )
        """
    )
    conn.execute(
        "INSERT INTO orders(inst_id, side, state, created_ts) VALUES (?, ?, ?, ?)",
        ("BTC-USDT", "buy", "FILLED", now_ms),
    )
    conn.commit()
    conn.close()

    auditor = trade_auditor_v3.TradeAuditorV3(workspace=tmp_path)

    assert auditor.get_recent_orders(hours=2) == [("BTC-USDT", "buy", "FILLED", now_ms)]


def test_get_recent_orders_uses_updated_ts_for_recent_fills(tmp_path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    db_path = reports_dir / "orders.sqlite"

    now_ms = int(datetime.now().timestamp() * 1000)
    stale_created_ts = now_ms - 3 * 60 * 60 * 1000
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE orders (
            inst_id TEXT,
            side TEXT,
            state TEXT,
            created_ts INTEGER,
            updated_ts INTEGER
        )
        """
    )
    conn.execute(
        "INSERT INTO orders(inst_id, side, state, created_ts, updated_ts) VALUES (?, ?, ?, ?, ?)",
        ("BTC-USDT", "buy", "FILLED", stale_created_ts, now_ms),
    )
    conn.commit()
    conn.close()

    auditor = trade_auditor_v3.TradeAuditorV3(workspace=tmp_path)

    assert auditor.get_recent_orders(hours=2) == [("BTC-USDT", "buy", "FILLED", now_ms)]


def test_get_market_state_uses_workspace_runs_dir(tmp_path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260405_230000"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "decision_audit.json").write_text(
        '{"regime":"Risk-Off","regime_details":{"position_multiplier":0.25}}',
        encoding="utf-8",
    )

    auditor = trade_auditor_v3.TradeAuditorV3(workspace=tmp_path)

    assert auditor.get_market_state() == {"state": "Risk-Off", "multiplier": 0.25}


def test_get_okx_balance_uses_runtime_env_credentials(tmp_path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "EXCHANGE_API_KEY=live-key",
                "EXCHANGE_API_SECRET=live-secret",
                "EXCHANGE_PASSPHRASE=live-passphrase",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("EXCHANGE_API_KEY", raising=False)
    monkeypatch.delenv("EXCHANGE_API_SECRET", raising=False)
    monkeypatch.delenv("EXCHANGE_PASSPHRASE", raising=False)

    calls: list[dict[str, object]] = []

    def _fake_get(url, headers, timeout):
        calls.append({"url": url, "headers": headers, "timeout": timeout})
        return SimpleNamespace(
            json=lambda: {
                "code": "0",
                "data": [{"details": [{"ccy": "USDT", "eq": "123.45"}, {"ccy": "BTC", "eq": "0.8"}]}],
            }
        )

    monkeypatch.setattr(trade_auditor_v3.requests, "get", _fake_get)

    auditor = trade_auditor_v3.TradeAuditorV3(workspace=tmp_path)
    result = auditor.get_okx_balance()

    assert result == {"usdt": 123.45, "positions": ["BTC: 0.80"]}
    assert calls and calls[0]["url"] == "https://www.okx.com/api/v5/account/balance"
