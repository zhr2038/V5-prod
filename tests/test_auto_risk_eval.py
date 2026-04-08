import importlib.util
import sqlite3
import types
import uuid
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "auto_risk_eval.py"


def load_auto_risk_eval_module():
    name = f"auto_risk_eval_test_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FakeGuard:
    def __init__(self):
        self.current_level = "DEFENSE"
        self.history = [{"from": "NEUTRAL", "to": "DEFENSE"}]

    def get_current_config(self):
        return {"name": "DEFENSE", "max_positions": 3}


def test_evaluate_and_switch_writes_snapshot_when_sample_is_insufficient(monkeypatch, tmp_path):
    module = load_auto_risk_eval_module()
    reports_dir = tmp_path / "reports"
    runs_dir = reports_dir / "runs"
    eval_path = reports_dir / "auto_risk_eval.json"
    reports_dir.mkdir()
    runs_dir.mkdir()

    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "RUNS_DIR", runs_dir)
    monkeypatch.setattr(module, "AUTO_RISK_EVAL_PATH", eval_path)
    monkeypatch.setattr(
        module,
        "_resolve_runtime_paths",
        lambda: module.AutoRiskEvalPaths(
            reports_dir=reports_dir,
            runs_dir=runs_dir,
            auto_risk_eval_path=eval_path,
            positions_db=reports_dir / "positions.sqlite",
            auto_risk_guard_path=reports_dir / "auto_risk_guard.json",
        ),
    )
    monkeypatch.setattr(module, "get_auto_risk_guard", lambda *args, **kwargs: FakeGuard())

    module.evaluate_and_switch()

    payload = module.json.loads(eval_path.read_text(encoding="utf-8"))
    assert payload["current_level"] == "DEFENSE"
    assert payload["config"]["max_positions"] == 3
    assert payload["metrics"]["sample_size"] == 0
    assert payload["reason"] == "样本不足 (0轮)，维持当前档位"


def test_calculate_metrics_sanitizes_corrupted_low_peak(monkeypatch, tmp_path):
    module = load_auto_risk_eval_module()
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(
        module,
        "_resolve_runtime_paths",
        lambda: module.AutoRiskEvalPaths(
            reports_dir=reports_dir,
            runs_dir=reports_dir / "runs",
            auto_risk_eval_path=reports_dir / "auto_risk_eval.json",
            positions_db=reports_dir / "positions.sqlite",
            auto_risk_guard_path=reports_dir / "auto_risk_guard.json",
        ),
    )

    db_path = reports_dir / "positions.sqlite"
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE account_state (
          k TEXT PRIMARY KEY,
          cash_usdt REAL NOT NULL,
          equity_peak_usdt REAL NOT NULL,
          scale_basis_usdt REAL DEFAULT 0.0
        )
        """
    )
    cur.execute(
        "INSERT INTO account_state(k, cash_usdt, equity_peak_usdt, scale_basis_usdt) VALUES ('default', 10.0, 10.0, 0.0)"
    )
    con.commit()
    con.close()

    fake_live_equity_fetcher = types.SimpleNamespace(get_live_equity_from_okx=lambda: 100.0)
    monkeypatch.setitem(module.sys.modules, "src.risk.live_equity_fetcher", fake_live_equity_fetcher)

    metrics = module.calculate_metrics([{"counts": {}}])

    assert metrics["dd_pct"] == 1.0 - (100.0 / 120.0)


def test_evaluate_and_switch_uses_runtime_reports_dir_from_active_config(monkeypatch, tmp_path):
    module = load_auto_risk_eval_module()
    fake_root = tmp_path / "repo"
    reports_dir = fake_root / "reports"
    runtime_dir = reports_dir / "shadow_runtime"
    runtime_runs = runtime_dir / "runs"
    configs_dir = fake_root / "configs"
    runtime_runs.mkdir(parents=True, exist_ok=True)
    configs_dir.mkdir(parents=True, exist_ok=True)

    (configs_dir / "live_prod.yaml").write_text(
        "\n".join(
            [
                "execution:",
                "  order_store_path: reports/shadow_runtime/orders.sqlite",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(module, "PROJECT_ROOT", fake_root)
    monkeypatch.setattr(module, "REPORTS_DIR", reports_dir)
    monkeypatch.setattr(module, "RUNS_DIR", reports_dir / "runs")
    monkeypatch.setattr(module, "AUTO_RISK_EVAL_PATH", reports_dir / "auto_risk_eval.json")
    monkeypatch.setattr(module, "get_auto_risk_guard", lambda *args, **kwargs: FakeGuard())

    module.evaluate_and_switch()

    assert (runtime_dir / "auto_risk_eval.json").exists()
    assert not (reports_dir / "auto_risk_eval.json").exists()
