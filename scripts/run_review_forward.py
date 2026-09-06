"""One public-data observation of the isolated V5 review experiment."""
import argparse
import json
import time
from pathlib import Path

from configs.loader import load_config
from src.reporting.participation_runtime import _save_report
from src.research.review_forward import collect, process, source_identity


def main():
    import fcntl

    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--experiment", default="configs/research/review_experiment_v2.json")
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    root = Path(args.output).resolve()
    if root == project or not root.is_relative_to((project / "reports/review_comparison").resolve()):
        raise ValueError("independent reports/review_comparison experiment directory required")
    root.mkdir(parents=True, exist_ok=True)
    with (root / "worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            config_path = project / "configs/live_prod.yaml"
            cfg = load_config(str(config_path), env_path=None)
            experiment_path = (project / args.experiment).resolve()
            if not experiment_path.is_relative_to(project / "configs/research"):
                raise ValueError("frozen repository research configuration required")
            experiment = json.loads(experiment_path.read_text(encoding="utf-8"))
            if experiment.get("schema_version") != "v5.review_comparison.v2" or not experiment.get("reference_contract"):
                raise ValueError("new clock and valuation require a new v2 experiment; v1 ledgers remain frozen")
            policy = json.loads((project / "configs/research/participation_policy_v1.json").read_text(encoding="utf-8"))
            identity = source_identity(project, experiment, config_path)
            frame = collect(symbols=policy["symbols"], root=root, reference_path=project / "reports/decision_reference/receipts.sqlite",
                            entry_minimum_notional_usdt=experiment["entry_minimum_notional_usdt"])
            report = process(cfg=cfg, policy=policy, experiment=experiment, project=project, root=root, frame=frame, identity=identity)
            _save_report(root / "worker-status.json", {"ok": True, "identity": identity["identity"], "observed_at": frame["observed_at"], "status": report["status"], "live_order_effect": "none"})
        except Exception as exc:
            _save_report(root / "worker-status.json", {"ok": False, "observed_at": time.time(), "error": type(exc).__name__, "detail": str(exc)[:1000], "live_order_effect": "none"})
            raise


if __name__ == "__main__":
    main()
