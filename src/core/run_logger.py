from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class RunLogger:
    def __init__(self, run_dir: str):
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / "equity.jsonl").touch(exist_ok=True)
        (self.run_dir / "positions.jsonl").touch(exist_ok=True)

    def log_equity(self, obj: Dict[str, Any]) -> None:
        with (self.run_dir / "equity.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    def log_position(self, obj: Dict[str, Any]) -> None:
        with (self.run_dir / "positions.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
