from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_default(value: Any) -> Any:
    try:
        import numpy as np

        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            return float(value)
        if isinstance(value, np.ndarray):
            return value.tolist()
    except Exception:
        pass
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


@dataclass
class ResearchRun:
    run_id: str
    task_name: str
    run_dir: Path
    started_at: str

    def write_json(self, relative_path: str, payload: Any) -> Path:
        path = self.run_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
        return path

    def write_text(self, relative_path: str, content: str) -> Path:
        path = self.run_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def append_jsonl(self, relative_path: str, payload: Any) -> Path:
        path = self.run_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, default=_json_default) + "\n")
        return path

    def artifact_base(self, name: str) -> Path:
        path = self.run_dir / "artifacts" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        return path


class ResearchRecorder:
    def __init__(self, base_dir: str | Path = "reports/runs"):
        self.base_dir = Path(base_dir)

    def start_run(self, *, task_name: str, task_config: dict[str, Any]) -> ResearchRun:
        started_at = _now_utc_iso()
        suffix = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        run_id = f"research_{task_name}_{suffix}"
        run_dir = self.base_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        run = ResearchRun(run_id=run_id, task_name=task_name, run_dir=run_dir, started_at=started_at)
        run.write_json(
            "meta.json",
            {
                "run_id": run_id,
                "task_name": task_name,
                "status": "running",
                "started_at": started_at,
            },
        )
        run.write_json("task.json", task_config)
        return run

    def finalize_run(
        self,
        run: ResearchRun,
        *,
        status: str,
        summary: dict[str, Any] | None = None,
    ) -> Path:
        meta_path = run.run_dir / "meta.json"
        meta: dict[str, Any] = {}
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                meta = {}
        meta.update(
            {
                "run_id": run.run_id,
                "task_name": run.task_name,
                "status": status,
                "started_at": run.started_at,
                "ended_at": _now_utc_iso(),
                "summary": summary or {},
            }
        )
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
        return meta_path


def find_latest_task_run(task_name: str, *, base_dir: str | Path = "reports/runs") -> Path | None:
    root = Path(base_dir)
    if not root.exists():
        return None

    candidates: list[tuple[float, Path]] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        meta_path = child / "meta.json"
        if not meta_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(meta.get("task_name")) != str(task_name):
            continue
        ended_at = str(meta.get("ended_at") or meta.get("started_at") or "")
        sort_key = child.stat().st_mtime
        if ended_at:
            try:
                sort_key = datetime.fromisoformat(ended_at.replace("Z", "+00:00")).timestamp()
            except Exception:
                pass
        else:
            run_id = str(meta.get("run_id") or child.name or "")
            match = re.search(r"(20\d{6}_\d{6}_\d{6})$", run_id)
            if match:
                try:
                    sort_key = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S_%f").timestamp()
                except Exception:
                    pass
        candidates.append((sort_key, child))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def load_latest_task_record(
    task_name: str,
    relative_path: str,
    *,
    base_dir: str | Path = "reports/runs",
) -> dict[str, Any] | None:
    run_dir = find_latest_task_run(task_name, base_dir=base_dir)
    if run_dir is None:
        return None
    target = run_dir / relative_path
    if not target.exists():
        return None
    try:
        obj = json.loads(target.read_text(encoding="utf-8"))
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None
