#!/usr/bin/env python3
"""
Detect abnormal moves in recent equity curves for the active V5 workspace.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import load_runtime_config, resolve_runtime_path
from src.execution.fill_store import derive_runtime_reports_dir, derive_runtime_runs_dir


@dataclass(frozen=True)
class DetectorPaths:
    workspace: Path
    reports_dir: Path
    runs_dir: Path
    orders_db: Path


def _derive_anomaly_report_path(order_store_path: Path, timestamp: str) -> Path:
    path = Path(order_store_path)
    if path.name == "orders.sqlite":
        return path.with_name(f"equity_anomaly_{timestamp}.json")
    if "orders" in path.stem:
        report_name = path.stem.replace("orders", "equity_anomaly", 1)
        return path.with_name(f"{report_name}_{timestamp}.json")
    return path.with_name(f"equity_anomaly_{timestamp}.json")


def build_paths(workspace: Path | None = None) -> DetectorPaths:
    root = (workspace or PROJECT_ROOT).resolve()
    try:
        cfg = load_runtime_config(project_root=root)
        execution_cfg = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
        orders_db = Path(
            resolve_runtime_path(
                execution_cfg.get("order_store_path") if isinstance(execution_cfg, dict) else None,
                default="reports/orders.sqlite",
                project_root=root,
            )
        ).resolve()
        reports_dir = derive_runtime_reports_dir(orders_db).resolve()
        runs_dir = derive_runtime_runs_dir(orders_db).resolve()
    except Exception:
        reports_dir = (root / "reports").resolve()
        runs_dir = (reports_dir / "runs").resolve()
        orders_db = (reports_dir / "orders.sqlite").resolve()

    return DetectorPaths(
        workspace=root,
        reports_dir=reports_dir,
        runs_dir=runs_dir,
        orders_db=orders_db,
    )


def _parse_equity_timestamp(raw: str) -> datetime | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00").replace("+00:00", ""))
    except Exception:
        return None


class EquityAnomalyDetector:
    def __init__(self, workspace: Path | None = None) -> None:
        self.paths = build_paths(workspace)
        self.anomalies: list[dict[str, Any]] = []
        self.stats = {"total_points": 0, "anomalies": 0}

    def log(self, msg: str) -> None:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def load_equity_data(self, days: int = 7) -> list[dict[str, Any]]:
        points: list[dict[str, Any]] = []
        cutoff = datetime.now() - timedelta(days=days)

        if self.paths.runs_dir.exists():
            for run_dir in self.paths.runs_dir.iterdir():
                if not run_dir.is_dir():
                    continue
                equity_file = run_dir / "equity.jsonl"
                if not equity_file.exists():
                    continue
                try:
                    for line in equity_file.read_text(encoding="utf-8", errors="ignore").splitlines():
                        try:
                            data = json.loads(line)
                        except Exception:
                            continue
                        ts = _parse_equity_timestamp(data.get("ts", ""))
                        if ts is None or ts <= cutoff:
                            continue
                        points.append(
                            {
                                "ts": ts,
                                "equity": float(data.get("equity", 0.0)),
                                "cash": float(data.get("cash", 0.0)),
                                "positions_value": float(data.get("positions_value", 0.0)),
                            }
                        )
                except Exception:
                    continue

        points.sort(key=lambda item: item["ts"])
        seen: set[str] = set()
        unique: list[dict[str, Any]] = []
        for point in points:
            key = point["ts"].strftime("%Y-%m-%d %H:%M")
            if key in seen:
                continue
            seen.add(key)
            unique.append(point)
        return unique

    def detect_jumps(self, points: list[dict[str, Any]], threshold: float = 0.1) -> list[dict[str, Any]]:
        anomalies: list[dict[str, Any]] = []
        for idx in range(1, len(points)):
            prev = points[idx - 1]
            curr = points[idx]
            if prev["equity"] <= 0:
                continue
            change_pct = abs(curr["equity"] - prev["equity"]) / prev["equity"]
            if change_pct > threshold:
                anomalies.append(
                    {
                        "type": "jump",
                        "time": curr["ts"],
                        "prev_equity": prev["equity"],
                        "curr_equity": curr["equity"],
                        "change_pct": change_pct,
                        "description": f"权益跳变 {change_pct:.1%}（无交易）",
                    }
                )
        return anomalies

    def detect_volatility(self, points: list[dict[str, Any]], window: int = 24) -> list[dict[str, Any]]:
        if len(points) < window:
            return []

        anomalies: list[dict[str, Any]] = []
        equities = [point["equity"] for point in points]
        for idx in range(window, len(points)):
            window_data = equities[idx - window : idx]
            mean = float(np.mean(window_data))
            std = float(np.std(window_data))
            if std <= 0:
                continue
            curr = equities[idx]
            z_score = abs(curr - mean) / std
            if z_score > 3:
                anomalies.append(
                    {
                        "type": "volatility",
                        "time": points[idx]["ts"],
                        "equity": curr,
                        "z_score": z_score,
                        "description": f"异常波动 (Z-score: {z_score:.1f})",
                    }
                )
        return anomalies

    def detect_stale_data(self, points: list[dict[str, Any]], max_gap_hours: float = 2) -> list[dict[str, Any]]:
        anomalies: list[dict[str, Any]] = []
        for idx in range(1, len(points)):
            prev = points[idx - 1]
            curr = points[idx]
            gap = (curr["ts"] - prev["ts"]).total_seconds() / 3600
            if gap > max_gap_hours:
                anomalies.append(
                    {
                        "type": "stale",
                        "time": prev["ts"],
                        "gap_hours": gap,
                        "description": f"数据中断 {gap:.1f} 小时",
                    }
                )
        return anomalies

    def run_detection(self, days: int = 7) -> list[dict[str, Any]]:
        self.log("=" * 60)
        self.log("权益曲线异常检测")
        self.log("=" * 60)

        points = self.load_equity_data(days=days)
        self.stats["total_points"] = len(points)
        if len(points) < 2:
            self.log("数据点不足")
            return []

        self.log(f"加载 {len(points)} 个数据点")
        jumps = self.detect_jumps(points)
        volatility = self.detect_volatility(points)
        stale = self.detect_stale_data(points)

        all_anomalies = sorted(jumps + volatility + stale, key=lambda item: item["time"])
        self.stats["anomalies"] = len(all_anomalies)

        if all_anomalies:
            self.log(f"\n发现 {len(all_anomalies)} 个异常")
            for anomaly in all_anomalies[:10]:
                self.log(
                    f"  [{anomaly['type'].upper()}] "
                    f"{anomaly['time'].strftime('%Y-%m-%d %H:%M')} - {anomaly['description']}"
                )
            if len(all_anomalies) > 10:
                self.log(f"  ... 还有 {len(all_anomalies) - 10} 个异常")
        else:
            self.log("\n未发现异常")

        equities = [point["equity"] for point in points]
        self.log("\n数据统计:")
        self.log(f"  数据点: {len(points)}")
        self.log(f"  起始权益: ${equities[0]:.2f}")
        self.log(f"  结束权益: ${equities[-1]:.2f}")
        self.log(f"  最大值: ${max(equities):.2f}")
        self.log(f"  最小值: ${min(equities):.2f}")
        self.log(f"  平均值: ${float(np.mean(equities)):.2f}")
        self.log(f"  标准差: ${float(np.std(equities)):.2f}")
        return all_anomalies

    def save_report(self, anomalies: list[dict[str, Any]]) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = _derive_anomaly_report_path(self.paths.orders_db, timestamp)
        report = {
            "timestamp": datetime.now().isoformat(),
            "stats": self.stats,
            "anomalies": anomalies,
        }
        report_file.parent.mkdir(parents=True, exist_ok=True)
        report_file.write_text(json.dumps(report, indent=2, default=str, ensure_ascii=False), encoding="utf-8")
        self.log(f"\n报告已保存: {report_file}")
        return report_file


def main() -> int:
    parser = argparse.ArgumentParser(description="V5 权益曲线异常检测")
    parser.add_argument("--days", type=int, default=7, help="检查天数")
    args = parser.parse_args()

    detector = EquityAnomalyDetector()
    anomalies = detector.run_detection(days=args.days)
    detector.save_report(anomalies)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
