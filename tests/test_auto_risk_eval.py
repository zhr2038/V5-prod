import importlib.util
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
    monkeypatch.setattr(module, "get_auto_risk_guard", lambda: FakeGuard())

    module.evaluate_and_switch()

    payload = module.json.loads(eval_path.read_text(encoding="utf-8"))
    assert payload["current_level"] == "DEFENSE"
    assert payload["config"]["max_positions"] == 3
    assert payload["metrics"]["sample_size"] == 0
    assert payload["reason"] == "样本不足 (0轮)，维持当前档位"
