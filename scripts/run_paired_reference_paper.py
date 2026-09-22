"""One public-data observation of the separately funded paired paper accounts."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from configs.loader import load_config
from src.reporting.participation_runtime import _save_report
from src.research.paired_reference_forward import collect, process, source_identity
from src.research.paired_reference_paper import validate_experiment


def main():
    import fcntl

    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--experiment", default="configs/research/paired_reference_paper_v1.json")
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    root = Path(args.output).resolve()
    expected_parent = (project / "reports/paired_reference_paper").resolve()
    if root.parent != expected_parent:
        raise ValueError("direct_independent_paired_paper_experiment_directory_required")
    path = (project / args.experiment).resolve()
    if path.parent != (project / "configs/research").resolve():
        raise ValueError("repository_frozen_research_config_required")
    experiment = json.loads(path.read_text(encoding="utf-8"))
    validate_experiment(experiment)
    if root.name != experiment["experiment_id"]:
        raise ValueError("experiment_directory_identity_mismatch")
    root.mkdir(parents=True, exist_ok=True)
    with (root / "worker.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            if (root / "FROZEN.json").exists():
                raise ValueError("frozen_paper_experiment_is_read_only")
            config_path = project / "configs/live_prod.yaml"
            cfg = load_config(str(config_path), env_path=None)
            identity = source_identity(project, experiment, config_path, cfg)
            manifest_path = root / "manifest.json"
            if manifest_path.exists() and json.loads(manifest_path.read_text(encoding="utf-8")).get("identity") != identity["identity"]:
                raise ValueError("frozen_strategy_or_config_changed_requires_new_experiment")
            frame = collect(experiment=experiment, root=root, reference_path=project / "reports/decision_reference/receipts.sqlite")
            report = process(cfg=cfg, experiment=experiment, root=root, frame=frame, identity=identity)
            _save_report(root / "worker-status.json", {"ok": True, "identity": identity["identity"],
                                                       "observed_at": frame["observed_at"], "status": report["status"],
                                                       "paper_only": True, "live_order_effect": "none"})
        except Exception as exc:
            _save_report(root / "worker-status.json", {"ok": False, "observed_at": time.time(), "error": type(exc).__name__,
                                                       "detail": str(exc)[:1000], "paper_only": True, "live_order_effect": "none"})
            raise


if __name__ == "__main__":
    main()
