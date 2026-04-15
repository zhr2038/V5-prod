from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_run_dir(run_dir: str | Path) -> Path:
    resolved = Path(run_dir)
    if not resolved.is_absolute():
        resolved = (PROJECT_ROOT / resolved).resolve()
    return resolved


class RunLogger:
    """运行日志记录器
    
    记录每次运行的权益和持仓数据到JSONL文件
    """
    
    def __init__(self, run_dir: str):
        """初始化日志记录器
        
        Args:
            run_dir: 运行日志目录路径
        """
        self.run_dir = _resolve_run_dir(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / "equity.jsonl").touch(exist_ok=True)
        (self.run_dir / "positions.jsonl").touch(exist_ok=True)

    def log_equity(self, obj: Dict[str, Any]) -> None:
        """记录权益数据
        
        Args:
            obj: 权益数据字典
        """
        with (self.run_dir / "equity.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    def log_position(self, obj: Dict[str, Any]) -> None:
        """记录持仓数据
        
        Args:
            obj: 持仓数据字典
        """
        with (self.run_dir / "positions.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
