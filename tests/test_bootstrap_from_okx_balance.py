from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import scripts.bootstrap_from_okx_balance as bootstrap_from_okx_balance


def test_bootstrap_script_help_runs_outside_repo(tmp_path: Path) -> None:
    script_path = Path(bootstrap_from_okx_balance.__file__).resolve()

    result = subprocess.run(
        [sys.executable, str(script_path), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--positions-db" in result.stdout


def test_bootstrap_uses_runtime_positions_db_from_order_store_path(monkeypatch) -> None:
    captured = {"upserts": []}
    workspace = Path(bootstrap_from_okx_balance.__file__).resolve().parents[1]

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        execution=SimpleNamespace(order_store_path="reports/shadow_runtime/orders.sqlite"),
    )

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            captured["client_exchange"] = kwargs.get("exchange")

        def get_balance(self, ccy=None):
            return SimpleNamespace(
                data={
                    "data": [
                        {
                            "details": [
                                {"ccy": "USDT", "cashBal": "100.0"},
                                {"ccy": "BTC", "cashBal": "0.5"},
                            ]
                        }
                    ]
                }
            )

        def close(self) -> None:
            captured["client_closed"] = True

    class DummyPositionStore:
        def __init__(self, path: str) -> None:
            captured["positions_db_path"] = Path(path).resolve()

        def list(self):
            return []

        def upsert_position(self, pos) -> None:
            captured["upserts"].append(pos)

    class DummyAccountStore:
        def __init__(self, path: str) -> None:
            captured["account_db_path"] = Path(path).resolve()

        def get(self):
            return SimpleNamespace(cash_usdt=0.0)

        def set(self, state) -> None:
            captured["cash_usdt"] = state.cash_usdt

    class DummySpreadStore:
        def __init__(self, base_dir) -> None:
            captured["spread_snapshots_dir"] = Path(base_dir).resolve()

        def get_latest_before(self, *, symbol: str, ts_ms: int):
            captured["spread_lookup"] = {"symbol": symbol, "ts_ms": ts_ms}
            return SimpleNamespace(mid=123.0)

    class DummyHighestPriceTracker:
        def __init__(self, state_path: str) -> None:
            captured["highest_state_path"] = Path(state_path).resolve()

        def update(self, symbol: str, highest_px: float, entry_px: float, source: str = "trade") -> None:
            captured.setdefault("tracker_updates", []).append((symbol, highest_px, entry_px, source))

        def get_highest_px(self, symbol: str, default: float = 0.0) -> float:
            return float(default)

    monkeypatch.setattr(bootstrap_from_okx_balance, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(bootstrap_from_okx_balance, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(bootstrap_from_okx_balance, "PositionStore", DummyPositionStore)
    monkeypatch.setattr(bootstrap_from_okx_balance, "AccountStore", DummyAccountStore)
    monkeypatch.setattr(bootstrap_from_okx_balance, "SpreadSnapshotStore", DummySpreadStore)
    monkeypatch.setattr(bootstrap_from_okx_balance, "HighestPriceTracker", DummyHighestPriceTracker)
    monkeypatch.setattr(sys, "argv", ["bootstrap_from_okx_balance.py"])

    bootstrap_from_okx_balance.main()

    assert captured["client_exchange"] == cfg.exchange
    assert captured["client_closed"] is True
    assert captured["positions_db_path"] == (workspace / "reports" / "shadow_runtime" / "positions.sqlite").resolve()
    assert captured["account_db_path"] == (workspace / "reports" / "shadow_runtime" / "positions.sqlite").resolve()
    assert captured["spread_snapshots_dir"] == (workspace / "reports" / "shadow_runtime" / "spread_snapshots").resolve()
    assert captured["highest_state_path"] == (workspace / "reports" / "shadow_runtime" / "highest_px_state.json").resolve()
    assert captured["cash_usdt"] == 100.0
    assert len(captured["upserts"]) == 1
    assert captured["upserts"][0].symbol == "BTC/USDT"
    assert captured["upserts"][0].avg_px == 123.0


def test_bootstrap_respects_explicit_positions_db_override(monkeypatch, tmp_path: Path) -> None:
    captured = {}

    cfg = SimpleNamespace(
        exchange=SimpleNamespace(),
        execution=SimpleNamespace(order_store_path="reports/shadow_runtime/orders.sqlite"),
    )
    explicit_positions_db = tmp_path / "custom" / "positions.sqlite"

    class DummyClient:
        def __init__(self, **kwargs) -> None:
            pass

        def get_balance(self, ccy=None):
            return SimpleNamespace(data={"data": [{"details": [{"ccy": "USDT", "cashBal": "10.0"}]}]})

        def close(self) -> None:
            pass

    class DummyPositionStore:
        def __init__(self, path: str) -> None:
            captured["positions_db_path"] = Path(path).resolve()

        def list(self):
            return []

        def upsert_position(self, pos) -> None:
            pass

    class DummyAccountStore:
        def __init__(self, path: str) -> None:
            captured["account_db_path"] = Path(path).resolve()

        def get(self):
            return SimpleNamespace(cash_usdt=0.0)

        def set(self, state) -> None:
            pass

    class DummySpreadStore:
        def __init__(self, base_dir) -> None:
            captured["spread_snapshots_dir"] = Path(base_dir).resolve()

        def get_latest_before(self, *, symbol: str, ts_ms: int):
            return None

    class DummyHighestPriceTracker:
        def __init__(self, state_path: str) -> None:
            captured["highest_state_path"] = Path(state_path).resolve()

        def update(self, symbol: str, highest_px: float, entry_px: float, source: str = "trade") -> None:
            pass

        def get_highest_px(self, symbol: str, default: float = 0.0) -> float:
            return float(default)

    monkeypatch.setattr(bootstrap_from_okx_balance, "load_config", lambda *args, **kwargs: cfg)
    monkeypatch.setattr(bootstrap_from_okx_balance, "OKXPrivateClient", DummyClient)
    monkeypatch.setattr(bootstrap_from_okx_balance, "PositionStore", DummyPositionStore)
    monkeypatch.setattr(bootstrap_from_okx_balance, "AccountStore", DummyAccountStore)
    monkeypatch.setattr(bootstrap_from_okx_balance, "SpreadSnapshotStore", DummySpreadStore)
    monkeypatch.setattr(bootstrap_from_okx_balance, "HighestPriceTracker", DummyHighestPriceTracker)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "bootstrap_from_okx_balance.py",
            "--positions-db",
            str(explicit_positions_db),
        ],
    )

    bootstrap_from_okx_balance.main()

    assert captured["positions_db_path"] == explicit_positions_db.resolve()
    assert captured["account_db_path"] == explicit_positions_db.resolve()
    assert captured["spread_snapshots_dir"] == (explicit_positions_db.parent / "spread_snapshots").resolve()
    assert captured["highest_state_path"] == (explicit_positions_db.parent / "highest_px_state.json").resolve()
