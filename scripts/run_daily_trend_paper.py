"""Run one public-data observation of the frozen daily-trend paper experiment."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from src.reporting.participation_runtime import _save_report
from src.research.daily_trend_paper import DailyTrendConfig, collect_snapshot, digest, process_snapshot, source_identity


def main() -> None:
    import fcntl

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/research/daily_trend_paper_v1.json")
    parser.add_argument("--output", required=True)
    parser.add_argument("--observe-only", action="store_true")
    args = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    config_path = (project / args.config).resolve()
    config_root = (project / "configs/research").resolve()
    if not config_path.is_relative_to(config_root):
        raise ValueError("repository research config required")
    config = DailyTrendConfig.load(config_path)
    root = Path(args.output).resolve()
    reports_root = (project / "reports/daily_trend_paper").resolve()
    if root.parent != reports_root or root.name != config.experiment_id:
        raise ValueError("versioned independent daily trend output directory required")
    root.mkdir(parents=True, exist_ok=True)
    with (root / "worker.lock").open("a", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            if (root / "FROZEN.json").exists():
                raise ValueError("frozen daily trend experiment is read-only")
            identity = source_identity(project, config_path)
            manifest = {
                "schema_version": config.schema_version,
                "experiment_id": config.experiment_id,
                "identity": identity,
                "config_sha256": digest(config.__dict__),
                "paper_only": True,
                "live_order_effect": "none",
                "live_execution_eligible": False,
                "automatic_live_scaling": False,
            }
            manifest_path = root / "manifest.json"
            if manifest_path.exists() and json.loads(manifest_path.read_text(encoding="utf-8")) != manifest:
                raise ValueError("daily trend manifest changed; new experiment directory required")
            _save_report(manifest_path, manifest)
            snapshot = collect_snapshot(config)
            raw_path = root / "inputs" / f"{snapshot['input_sha256']}.json"
            _save_report(raw_path, snapshot)
            if args.observe_only:
                report = {
                    "ok": True,
                    "status": "OBSERVE_ONLY",
                    "experiment_id": config.experiment_id,
                    "observed_at": snapshot["observed_at"],
                    "completed_bar_ts": snapshot["completed_bar_ts"],
                    "input_sha256": snapshot["input_sha256"],
                    "paper_only": True,
                    "live_order_effect": "none",
                }
            else:
                report = process_snapshot(
                    root / "ledger.sqlite",
                    config,
                    snapshot,
                    source_identity=identity,
                )
                _save_report(root / "latest.json", report)
                _save_report(
                    root.parent / "current.json",
                    {
                        "schema_version": "v5.daily_trend_pointer.v1",
                        "directory": root.name,
                        "experiment_id": config.experiment_id,
                        "identity": identity,
                        "published_at": time.time(),
                        "paper_only": True,
                        "live_order_effect": "none",
                    },
                )
            status = {
                "ok": True,
                "status": report["status"],
                "identity": identity,
                "observed_at": snapshot["observed_at"],
                "input_sha256": snapshot["input_sha256"],
                "paper_only": True,
                "live_order_effect": "none",
            }
            _save_report(root / "worker-status.json", status)
            print(json.dumps(status, sort_keys=True))
        except Exception as exc:
            _save_report(
                root / "worker-status.json",
                {
                    "ok": False,
                    "observed_at": time.time(),
                    "error": type(exc).__name__,
                    "detail": str(exc)[:1000],
                    "identity": locals().get("identity"),
                    "paper_only": True,
                    "live_order_effect": "none",
                },
            )
            raise


if __name__ == "__main__":
    main()
