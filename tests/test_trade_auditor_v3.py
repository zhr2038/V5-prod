from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.trade_auditor_v3 as auditor_mod


@pytest.fixture(autouse=True)
def _runtime_config(monkeypatch, tmp_path: Path) -> Path:
    config_path = tmp_path / "configs" / "live_prod.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        "execution:\n  order_store_path: reports/orders.sqlite\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        auditor_mod,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(config_path),
    )
    return config_path


def test_get_okx_balance_prefers_total_equity_and_eq_usd(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        auditor_mod,
        "load_exchange_credentials",
        lambda _paths=None: ("k", "s", "p"),
    )

    payload = {
        "code": "0",
        "data": [
            {
                "totalEq": "123.45",
                "details": [
                    {"ccy": "USDT", "eq": "90.12", "eqUsd": "90.12"},
                    {"ccy": "BTC", "eq": "0.001", "eqUsd": "75.5"},
                    {"ccy": "DOGE", "eq": "10", "eqUsd": "0.9"},
                ],
            }
        ],
    }

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return payload

    monkeypatch.setattr(auditor_mod.requests, "get", lambda *args, **kwargs: _Resp())

    auditor = auditor_mod.TradeAuditorV3(workspace=tmp_path)
    result = auditor.get_okx_balance()

    assert result == {
        "usdt": 90.12,
        "total_eq_usdt": 123.45,
        "positions": ["BTC: 0.00 ($75.50)"],
    }


def test_get_okx_balance_sanitizes_request_errors(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        auditor_mod,
        "load_exchange_credentials",
        lambda _paths=None: ("k", "s", "p"),
    )
    monkeypatch.setattr(
        auditor_mod.requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("/home/ubuntu/clawd/v5-prod/.env missing")),
    )

    auditor = auditor_mod.TradeAuditorV3(workspace=tmp_path)
    result = auditor.get_okx_balance()

    assert result == {"error": "api unavailable", "detail": "RuntimeError"}


def test_trade_auditor_v3_main_passes_cli_paths(monkeypatch, tmp_path: Path) -> None:
    expected_cfg = (tmp_path / "configs" / "auditor.yaml").resolve()
    expected_env = (tmp_path / "configs" / "auditor.env").resolve()
    seen: dict[str, str] = {}

    monkeypatch.setattr(auditor_mod, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(
        auditor_mod,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(expected_cfg),
    )
    monkeypatch.setattr(
        auditor_mod,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str(expected_env),
    )

    original_init = auditor_mod.TradeAuditorV3.__init__

    def fake_init(self, workspace=None, *, config_path=None, env_path=None):
        seen["config"] = config_path
        seen["env"] = env_path
        self.paths = SimpleNamespace()
        self.issues = []
        self.warnings = []
        self.info = []

    monkeypatch.setattr(auditor_mod.TradeAuditorV3, "__init__", fake_init)
    monkeypatch.setattr(auditor_mod.TradeAuditorV3, "run", lambda self: "ok")

    try:
        auditor_mod.main(["--config", "configs/x.yaml", "--env", "configs/x.env"])
    finally:
        monkeypatch.setattr(auditor_mod.TradeAuditorV3, "__init__", original_init)

    assert seen == {"config": "configs/x.yaml", "env": "configs/x.env"}


def test_build_paths_uses_runtime_entry_helpers(monkeypatch, tmp_path: Path) -> None:
    expected_cfg = (tmp_path / "configs" / "auditor.yaml").resolve()
    expected_env = (tmp_path / "configs" / "auditor.env").resolve()
    expected_cfg.parent.mkdir(parents=True, exist_ok=True)
    expected_cfg.write_text("execution:\n  order_store_path: reports/orders.sqlite\n", encoding="utf-8")

    monkeypatch.setattr(
        auditor_mod,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(expected_cfg),
    )
    monkeypatch.setattr(
        auditor_mod,
        "resolve_runtime_env_path",
        lambda raw_env_path=None, project_root=None: str(expected_env),
    )

    paths = auditor_mod.build_paths(tmp_path, config_path="configs/x.yaml", env_path="configs/x.env")

    assert paths.env_path == expected_env


def test_load_active_config_fails_fast_when_runtime_config_is_missing(monkeypatch, tmp_path: Path) -> None:
    missing = (tmp_path / "configs" / "missing.yaml").resolve()
    monkeypatch.setattr(
        auditor_mod,
        "resolve_runtime_config_path",
        lambda raw_config_path=None, project_root=None: str(missing),
    )

    try:
        auditor_mod._load_active_config(project_root=tmp_path)
    except FileNotFoundError as exc:
        assert str(missing) in str(exc)
    else:
        raise AssertionError("expected FileNotFoundError")


def test_trade_auditor_v3_report_includes_negative_expectancy_counts(tmp_path: Path) -> None:
    run_dir = tmp_path / "reports" / "runs" / "20260417_01"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "decision_audit.json").write_text(
        json.dumps(
            {
                "regime": "TRENDING",
                "counts": {
                    "negative_expectancy_score_penalty": 2,
                    "negative_expectancy_cooldown": 3,
                    "negative_expectancy_open_block": 4,
                    "negative_expectancy_fast_fail_open_block": 5,
                },
            }
        ),
        encoding="utf-8",
    )

    auditor = auditor_mod.TradeAuditorV3(workspace=tmp_path)
    report = auditor.generate_report(
        {
            "okx": {"total_eq_usdt": 100.0, "usdt": 80.0, "positions": []},
            "orders": {"buy": 1, "sell": 0, "rejected": 0, "total": 1},
            "market": auditor.get_market_state(),
        }
    )

    assert "Negative expectancy: penalty=2 cooldown=3 open_block=4 fast_fail_open_block=5" in report


def test_trade_auditor_v3_load_latest_decision_audit_prefers_audit_file_mtime(tmp_path: Path) -> None:
    stale_run = tmp_path / "reports" / "runs" / "stale"
    fresh_run = tmp_path / "reports" / "runs" / "fresh"
    stale_run.mkdir(parents=True, exist_ok=True)
    fresh_run.mkdir(parents=True, exist_ok=True)
    stale_audit = stale_run / "decision_audit.json"
    fresh_audit = fresh_run / "decision_audit.json"
    stale_audit.write_text(json.dumps({"run_id": "stale"}), encoding="utf-8")
    fresh_audit.write_text(json.dumps({"run_id": "fresh"}), encoding="utf-8")

    import os
    stale_audit_ts = 1_710_000_000
    fresh_audit_ts = 1_710_000_100
    os.utime(stale_audit, (stale_audit_ts, stale_audit_ts))
    os.utime(fresh_audit, (fresh_audit_ts, fresh_audit_ts))
    os.utime(stale_run, (fresh_audit_ts + 500, fresh_audit_ts + 500))
    os.utime(fresh_run, (stale_audit_ts, stale_audit_ts))

    auditor = auditor_mod.TradeAuditorV3(workspace=tmp_path)

    assert auditor._load_latest_decision_audit()["run_id"] == "fresh"


def test_trade_auditor_v3_load_latest_decision_audit_prefers_run_id_epoch_when_file_mtime_is_misleading(tmp_path: Path) -> None:
    older_run = tmp_path / "reports" / "runs" / "20260408_01"
    newer_run = tmp_path / "reports" / "runs" / "20260408_02"
    older_run.mkdir(parents=True, exist_ok=True)
    newer_run.mkdir(parents=True, exist_ok=True)
    older_audit = older_run / "decision_audit.json"
    newer_audit = newer_run / "decision_audit.json"
    older_audit.write_text(json.dumps({"run_id": "20260408_01"}), encoding="utf-8")
    newer_audit.write_text(json.dumps({"run_id": "20260408_02"}), encoding="utf-8")

    import os
    os.utime(older_audit, (200, 200))
    os.utime(newer_audit, (100, 100))

    auditor = auditor_mod.TradeAuditorV3(workspace=tmp_path)

    assert auditor._load_latest_decision_audit()["run_id"] == "20260408_02"


def test_trade_auditor_v3_load_latest_decision_audit_limits_audit_file_reads_before_parsing(tmp_path: Path, monkeypatch) -> None:
    runs_dir = tmp_path / "reports" / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    for hour in range(20):
        run_dir = runs_dir / f"20260408_{hour:02d}"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "decision_audit.json").write_text(
            json.dumps({"run_id": f"20260408_{hour:02d}"}),
            encoding="utf-8",
        )

    original_loads = auditor_mod.json.loads
    reads = {"decision_audit": 0}

    def counting_loads(text: str, *args, **kwargs):
        reads["decision_audit"] += 1
        return original_loads(text, *args, **kwargs)

    monkeypatch.setattr(auditor_mod.json, "loads", counting_loads)

    auditor = auditor_mod.TradeAuditorV3(workspace=tmp_path)

    data = auditor._load_latest_decision_audit()

    assert data["run_id"] == "20260408_19"
    assert reads["decision_audit"] <= 2
