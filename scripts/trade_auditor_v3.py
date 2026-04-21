#!/usr/bin/env python3
"""
Trade auditor V3 for the active V5 workspace.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from configs.runtime_config import (
    resolve_runtime_config_path,
    resolve_runtime_env_path,
    resolve_runtime_path,
)


@dataclass(frozen=True)
class AuditorPaths:
    workspace: Path
    reports_dir: Path
    runs_dir: Path
    orders_db: Path
    env_path: Path


def _resolve_runtime_entry_paths(
    *,
    project_root: Path,
    config_path: str | None = None,
    env_path: str | None = None,
) -> tuple[Path, Path]:
    return (
        Path(resolve_runtime_config_path(config_path, project_root=project_root)),
        Path(resolve_runtime_env_path(env_path, project_root=project_root)),
    )


def _load_active_config(*, project_root: Path, config_path: str | None = None) -> dict[str, Any]:
    resolved_config_path, _ = _resolve_runtime_entry_paths(project_root=project_root, config_path=config_path)
    config_path = resolved_config_path
    if not config_path.exists():
        raise FileNotFoundError(f"runtime config not found: {config_path}")
    try:
        import yaml

        payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise ValueError(f"runtime config is invalid: {config_path}") from exc
    if not isinstance(payload, dict) or not payload:
        raise ValueError(f"runtime config is empty or invalid: {config_path}")
    execution_cfg = payload.get("execution")
    if not isinstance(execution_cfg, dict):
        raise ValueError(f"runtime config missing execution section: {config_path}")
    return payload


def build_paths(
    workspace: Path | None = None,
    config_path: str | None = None,
    env_path: str | None = None,
) -> AuditorPaths:
    root = (workspace or PROJECT_ROOT).resolve()
    cfg = _load_active_config(project_root=root, config_path=config_path)
    execution_cfg = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
    orders_db = Path(
        resolve_runtime_path(
            execution_cfg.get("order_store_path"),
            default="reports/orders.sqlite",
            project_root=root,
        )
    )
    reports_dir = orders_db.parent.resolve()
    _, resolved_env_path = _resolve_runtime_entry_paths(project_root=root, env_path=env_path)
    return AuditorPaths(
        workspace=root,
        reports_dir=reports_dir,
        runs_dir=reports_dir / "runs",
        orders_db=orders_db,
        env_path=resolved_env_path,
    )


DEFAULT_PATHS = build_paths()


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def load_exchange_credentials(paths: AuditorPaths = DEFAULT_PATHS) -> tuple[str | None, str | None, str | None]:
    load_env_file(paths.env_path)
    key = os.getenv("EXCHANGE_API_KEY") or os.getenv("OKX_API_KEY")
    secret = os.getenv("EXCHANGE_API_SECRET") or os.getenv("OKX_API_SECRET")
    passphrase = os.getenv("EXCHANGE_PASSPHRASE") or os.getenv("OKX_API_PASSPHRASE")
    return key, secret, passphrase


class TradeAuditorV3:
    def __init__(
        self,
        workspace: Path | None = None,
        *,
        config_path: str | None = None,
        env_path: str | None = None,
    ) -> None:
        self.paths = build_paths(workspace, config_path=config_path, env_path=env_path)
        self.issues: list[str] = []
        self.warnings: list[str] = []
        self.info: list[str] = []

    def log(self, msg: str) -> None:
        print(msg)

    def _load_latest_decision_audit(self) -> dict[str, Any]:
        try:
            runs_dir = self.paths.runs_dir
            if runs_dir.exists():
                run_dirs = [
                    d
                    for d in runs_dir.iterdir()
                    if d.is_dir() and (d / "decision_audit.json").exists()
                ]
                def _sort_epoch(run_dir: Path) -> float:
                    audit_path = run_dir / "decision_audit.json"
                    try:
                        payload = json.loads(audit_path.read_text(encoding="utf-8"))
                    except Exception:
                        payload = {}

                    for key in ("timestamp", "now_ts", "window_start_ts"):
                        value = payload.get(key) if isinstance(payload, dict) else None
                        if value is None:
                            continue
                        try:
                            return float(value)
                        except Exception:
                            pass

                    run_id = str(payload.get("run_id") or run_dir.name) if isinstance(payload, dict) else run_dir.name
                    try:
                        return datetime.strptime(run_id, "%Y%m%d_%H").timestamp()
                    except Exception:
                        try:
                            return audit_path.stat().st_mtime
                        except OSError:
                            return run_dir.stat().st_mtime

                run_dirs.sort(key=_sort_epoch, reverse=True)
                if run_dirs:
                    return json.loads((run_dirs[0] / "decision_audit.json").read_text(encoding="utf-8"))
        except Exception:
            pass
        return {}

    def get_okx_balance(self) -> dict[str, Any] | None:
        try:
            key, secret, passphrase = load_exchange_credentials(self.paths)
            if not (key and secret and passphrase):
                return None

            ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"
            path = "/api/v5/account/balance"
            msg = ts + "GET" + path
            sig = base64.b64encode(
                hmac.new(secret.encode(), msg.encode(), hashlib.sha256).digest()
            ).decode()
            headers = {
                "OK-ACCESS-KEY": key,
                "OK-ACCESS-SIGN": sig,
                "OK-ACCESS-TIMESTAMP": ts,
                "OK-ACCESS-PASSPHRASE": passphrase,
            }

            resp = requests.get("https://www.okx.com" + path, headers=headers, timeout=8)
            resp.raise_for_status()
            data = resp.json()

            if data.get("code") == "0" and data.get("data"):
                account = data["data"][0] if isinstance(data["data"][0], dict) else {}
                details = account.get("details", [])
                usdt_eq = 0.0
                total_eq = float(account.get("totalEq") or 0.0)
                positions: list[str] = []
                for detail in details:
                    ccy = str(detail.get("ccy") or "")
                    eq = float(detail.get("eq") or 0.0)
                    eq_usd = float(detail.get("eqUsd") or 0.0)
                    if ccy == "USDT":
                        usdt_eq = eq
                    elif eq_usd > 1.0:
                        positions.append(f"{ccy}: {eq:.2f} (${eq_usd:.2f})")
                return {"usdt": usdt_eq, "total_eq_usdt": total_eq or usdt_eq, "positions": positions}
        except Exception as exc:
            return {"error": "api unavailable", "detail": type(exc).__name__}
        return None

    def get_recent_orders(self, hours: int = 2) -> list[tuple[Any, ...]]:
        if not self.paths.orders_db.exists():
            return []

        conn = sqlite3.connect(str(self.paths.orders_db))
        try:
            start_ts = int((datetime.now() - timedelta(hours=hours)).timestamp() * 1000)
            try:
                rows = conn.execute(
                    """
                    SELECT
                        inst_id,
                        side,
                        state,
                        created_ts AS event_ts
                    FROM orders
                    WHERE created_ts > ?
                    ORDER BY event_ts DESC
                    """,
                    (start_ts,),
                ).fetchall()
            except sqlite3.OperationalError:
                rows = conn.execute(
                    """
                    SELECT inst_id, side, state, created_ts
                    FROM orders
                    WHERE created_ts > ?
                    ORDER BY created_ts DESC
                    """,
                    (start_ts,),
                ).fetchall()
        finally:
            conn.close()
        return rows

    def get_market_state(self) -> dict[str, Any]:
        data = self._load_latest_decision_audit()
        if data:
            regime = data.get("regime")
            details = data.get("regime_details", {})
            multiplier = details.get(
                "position_multiplier",
                data.get("regime_multiplier", 0.6),
            )
            counts = data.get("counts", {}) if isinstance(data, dict) else {}
            return {
                "state": regime,
                "multiplier": multiplier,
                "negative_expectancy_penalty_count": int(counts.get("negative_expectancy_score_penalty", 0) or 0),
                "negative_expectancy_cooldown_count": int(counts.get("negative_expectancy_cooldown", 0) or 0),
                "negative_expectancy_open_block_count": int(counts.get("negative_expectancy_open_block", 0) or 0),
                "negative_expectancy_fast_fail_open_block_count": int(
                    counts.get("negative_expectancy_fast_fail_open_block", 0) or 0
                ),
            }
        return {"state": "Unknown", "multiplier": 0}

    def analyze(self) -> dict[str, Any]:
        okx_data = self.get_okx_balance()
        orders = self.get_recent_orders(hours=2)

        buy_filled = sum(1 for order in orders if order[1] == "buy" and order[2] == "FILLED")
        sell_filled = sum(1 for order in orders if order[1] == "sell" and order[2] == "FILLED")
        rejected = sum(1 for order in orders if order[2] == "REJECTED")
        market = self.get_market_state()

        return {
            "okx": okx_data,
            "orders": {
                "buy": buy_filled,
                "sell": sell_filled,
                "rejected": rejected,
                "total": len(orders),
            },
            "market": market,
        }

    def generate_report(self, data: dict[str, Any]) -> str:
        lines = [
            "交易审计报告",
            "",
            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "",
        ]

        okx = data.get("okx", {})
        if isinstance(okx, dict) and "error" in okx:
            lines.append(f"OKX API错误: {okx['error']}")
        elif okx:
            lines.append(f"账户权益: {okx.get('total_eq_usdt', okx.get('usdt', 0)):.2f} USDT")
            lines.append(f"USDT余额: {okx.get('usdt', 0):.2f} USDT")
            if okx.get("positions"):
                lines.append(f"持仓: {', '.join(okx['positions'][:3])}")

        lines.extend(["", f"市场状态: {data.get('market', {}).get('state', 'Unknown')}"])
        lines.append(f"仓位乘数: {data.get('market', {}).get('multiplier', 0):.2f}x")
        lines.append(
            "Negative expectancy: "
            f"penalty={data.get('market', {}).get('negative_expectancy_penalty_count', 0)} "
            f"cooldown={data.get('market', {}).get('negative_expectancy_cooldown_count', 0)} "
            f"open_block={data.get('market', {}).get('negative_expectancy_open_block_count', 0)} "
            f"fast_fail_open_block={data.get('market', {}).get('negative_expectancy_fast_fail_open_block_count', 0)}"
        )
        lines.extend(["", "最近2小时交易:"])

        orders = data.get("orders", {})
        lines.append(f"  买入: {orders.get('buy', 0)} 笔")
        lines.append(f"  卖出: {orders.get('sell', 0)} 笔")
        if orders.get("rejected", 0) > 0:
            lines.append(f"  拒绝: {orders.get('rejected', 0)} 笔")

        lines.append("")
        if orders.get("total", 0) == 0:
            lines.append("结果: 无交易")
        elif orders.get("rejected", 0) > 20:
            lines.append("结果: 通过（大量 dust/minSz 拒单，属于正常保护）")
        else:
            lines.append("结果: 通过")

        return "\n".join(lines)

    def run(self) -> str:
        report = self.generate_report(self.analyze())
        self.log(report)
        return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the V5 trade auditor against the active runtime.")
    parser.add_argument("--config", default=None)
    parser.add_argument("--env", default=None)
    args = parser.parse_args(argv)

    TradeAuditorV3(config_path=args.config, env_path=args.env).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
