#!/usr/bin/env python3
"""
V5 旧风控恢复只读兼容入口

展示历史回撤；风险档位写入统一交给 scripts/auto_risk_eval.py。
"""

import json
import sys
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import resolve_runtime_path
from src.execution.fill_store import (
    derive_runtime_auto_risk_guard_path,
    derive_runtime_named_json_path,
    derive_runtime_reports_dir,
    derive_runtime_runs_dir,
)

CANONICAL_ENTRYPOINT = "scripts/auto_risk_eval.py"
DEPRECATED_RECOVERY_REASON = (
    "旧独立恢复写入口已停用；风险档位只能由统一评估器按真实回撤和成交证据决定。"
    "请运行 python scripts/auto_risk_eval.py --config configs/live_prod.yaml --env .env。"
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


class RiskAutoRecovery:
    """风控自动恢复管理器"""
    
    def __init__(self, workspace: Path = PROJECT_ROOT):
        self.workspace = Path(workspace).resolve()
        cfg = self._load_active_runtime_config()
        execution_cfg = cfg.get('execution', {}) if isinstance(cfg, dict) else {}
        order_store_path = Path(
            resolve_runtime_path(
                execution_cfg.get('order_store_path') if isinstance(execution_cfg, dict) else None,
                default='reports/orders.sqlite',
                project_root=self.workspace,
            )
        ).resolve()
        self.reports_dir = derive_runtime_reports_dir(order_store_path).resolve()
        self.runs_dir = derive_runtime_runs_dir(order_store_path).resolve()
        self.risk_state_file = derive_runtime_auto_risk_guard_path(order_store_path).resolve()
        self.config_file = derive_runtime_named_json_path(order_store_path, 'risk_recovery_config').resolve()
        self.config = self.load_config()

    def _load_active_runtime_config(self):
        config_path = (self.workspace / "configs" / "live_prod.yaml").resolve()
        if not config_path.exists():
            raise FileNotFoundError(f"runtime config not found: {config_path}")
        try:
            payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            raise ValueError(f"runtime config is invalid: {config_path}: {exc}") from exc
        if not isinstance(payload, dict) or not payload:
            raise ValueError(f"runtime config is empty or invalid: {config_path}")
        execution = payload.get("execution")
        if not isinstance(execution, dict):
            raise ValueError(f"runtime config missing execution section: {config_path}")
        return payload
    
    def load_config(self):
        """加载配置"""
        default_config = {
            'enabled': True,           # 是否启用自动恢复
            'cooldown_hours': 24,      # 档位切换冷却期
            'require_consecutive': 2,   # 需要连续N次检查满足条件才降级
            'min_time_in_level_hours': 4,  # 在档位至少停留4小时
            'manual_override_until': None   # 手动暂停截止时间
        }
        
        if self.config_file.exists():
            try:
                with open(self.config_file) as f:
                    saved = json.load(f)
                    default_config.update(saved)
            except Exception:
                pass
        
        return default_config
    
    def save_config(self):
        """保存配置"""
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)
    
    def get_current_risk_state(self):
        """获取当前风险状态"""
        if self.risk_state_file.exists():
            try:
                with open(self.risk_state_file) as f:
                    state = json.load(f)
                    if isinstance(state, dict):
                        current_level = str(state.get('current_level') or state.get('level') or 'NEUTRAL').upper()
                        latest_history_ts = ''
                        history = state.get('history')
                        if isinstance(history, list):
                            def _history_sort_epoch(item):
                                if not isinstance(item, dict):
                                    return float('-inf')
                                try:
                                    parsed = self._parse_state_datetime(item.get('ts'))
                                    if parsed is not None:
                                        return parsed.timestamp()
                                except Exception:
                                    pass
                                return float('-inf')

                            latest_history = max(history, key=_history_sort_epoch, default=None)
                            if isinstance(latest_history, dict):
                                latest_history_ts = str(latest_history.get('ts') or '').strip()
                        since = str(
                            state.get('since')
                            or state.get('last_update')
                            or latest_history_ts
                            or _utc_now_iso()
                        )
                        state['current_level'] = current_level
                        state['level'] = current_level
                        state['since'] = since
                        return state
            except Exception:
                pass
        now_iso = _utc_now_iso()
        return {'current_level': 'UNKNOWN', 'level': 'UNKNOWN', 'since': now_iso, 'last_update': now_iso}
    
    def get_drawdown_history(self, hours=24):
        """获取回撤历史"""
        try:
            points = []
            cutoff = _utc_now() - timedelta(hours=hours)

            def _candidate_sort_epoch(run_dir: Path) -> float:
                try:
                    # Use end-of-hour as a lightweight upper bound for cutoff filtering.
                    return datetime.strptime(run_dir.name, "%Y%m%d_%H").replace(tzinfo=timezone.utc).timestamp() + 3600.0
                except Exception:
                    equity_file = run_dir / 'equity.jsonl'
                    try:
                        return equity_file.stat().st_mtime
                    except OSError:
                        return run_dir.stat().st_mtime

            equity_files = []
            if self.runs_dir.exists():
                run_dirs = sorted(self.runs_dir.iterdir(), key=_candidate_sort_epoch, reverse=True)
                for run_dir in run_dirs:
                    if not run_dir.is_dir():
                        continue
                    equity_file = run_dir / 'equity.jsonl'
                    if not equity_file.exists():
                        continue
                    if datetime.fromtimestamp(_candidate_sort_epoch(run_dir), tz=timezone.utc) <= cutoff:
                        continue
                    equity_files.append(equity_file)
            legacy_equity_file = self.reports_dir / 'equity_history.jsonl'
            if legacy_equity_file.exists():
                equity_files.append(legacy_equity_file)

            if not equity_files:
                return []

            for equity_file in equity_files:
                try:
                    with open(equity_file) as f:
                        for line in f:
                            try:
                                data = json.loads(line)
                                raw_ts = str(data.get('ts', '') or '').strip()
                                if not raw_ts:
                                    continue
                                ts = datetime.fromisoformat(raw_ts.replace('Z', '+00:00'))
                                if ts.tzinfo is None:
                                    ts = ts.replace(tzinfo=timezone.utc)
                                else:
                                    ts = ts.astimezone(timezone.utc)
                                if ts > cutoff:
                                    drawdown = data.get('drawdown')
                                    if drawdown is None:
                                        drawdown = data.get('dd')
                                    points.append({
                                        'ts': ts,
                                        'equity': data.get('equity', 0),
                                        'peak': data.get('peak', 0),
                                        'drawdown': drawdown
                                    })
                            except Exception:
                                continue
                except Exception:
                    continue
            
            points.sort(key=lambda item: item['ts'])
            dedup = {}
            for point in points:
                key = point['ts'].isoformat()
                if key not in dedup:
                    dedup[key] = point
            return list(dedup.values())
        except Exception:
            return []
    
    def calculate_avg_drawdown(self, hours=6):
        """Return observed drawdown only; missing or invalid data stays unknown."""
        drawdowns = []
        for point in self.get_drawdown_history(hours=hours):
            try:
                value = float(point.get('drawdown'))
            except (TypeError, ValueError):
                continue
            if math.isfinite(value) and 0.0 <= value <= 1.0:
                drawdowns.append(value)
        return sum(drawdowns) / len(drawdowns) if drawdowns else None

    @staticmethod
    def _parse_state_datetime(raw_value: str | None) -> datetime | None:
        try:
            text = str(raw_value or "").strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = datetime.fromtimestamp(parsed.timestamp(), tz=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            return parsed
        except Exception:
            return None
    
    def check_recovery_conditions(self, current_level):
        """Compatibility API: this reader cannot authorize a risk-level change."""
        return False, None

    def time_in_current_level(self, state):
        """计算在当前档位停留的时间"""
        try:
            since = self._parse_state_datetime(state.get('since'))
            if since is None:
                raise ValueError("missing since")
            return (_utc_now() - since).total_seconds() / 3600  # 小时
        except Exception:
            return 999  # 如果解析失败，假设已停留很久
    
    def evaluate_recovery(self):
        """Read-only legacy status, with no competing recovery thresholds."""
        avg_dd = self.calculate_avg_drawdown(hours=6)
        return {
            'action': 'canonical_required',
            'reason': DEPRECATED_RECOVERY_REASON,
            'canonical_entrypoint': CANONICAL_ENTRYPOINT,
            'average_drawdown': avg_dd,
            'drawdown_status': 'observed' if avg_dd is not None else 'unavailable',
        }

    def execute_recovery(self, target_level):
        """Reject caller-selected upgrades instead of writing a second risk state."""
        return False, DEPRECATED_RECOVERY_REASON

    def print_report(self):
        """打印评估报告"""
        print("=" * 60)
        print("🛡️  V5 风控自动恢复评估")
        print("=" * 60)
        print(f"时间: {_utc_now().strftime('%Y-%m-%d %H:%M:%S')}")
        print('兼容入口：只读；档位由统一风险评估器决定')
        print()
        
        # 当前状态
        state = self.get_current_risk_state()
        print(f"当前档位: {state.get('level', 'UNKNOWN')}")
        print(f"进入时间: {state.get('since', 'N/A')}")
        hours_in = self.time_in_current_level(state)
        print(f"已停留: {hours_in:.1f} 小时")
        print()
        
        # 回撤情况
        avg_dd = self.calculate_avg_drawdown(hours=6)
        if avg_dd is None:
            print("最近6小时平均回撤: 不可观测")
        else:
            print(f"最近6小时平均回撤: {avg_dd:.1%}")
        print()

        # 评估结果
        result = self.evaluate_recovery()
        print(f"建议操作: {result['action'].upper()}")
        print(f"原因: {result['reason']}")
        
        if result['action'] == 'recover':
            print(f"建议降级至: {result['to_level']}")
        
        print("=" * 60)
        return result


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description='V5 旧风险恢复入口（只读兼容）')
    parser.add_argument('--execute', action='store_true', help='已停用；改用统一风险评估器')
    parser.add_argument('--enable', action='store_true', help='已停用的旧恢复开关')
    parser.add_argument('--disable', action='store_true', help='已停用的旧恢复开关')
    parser.add_argument('--pause-hours', type=int, help='已停用的旧恢复暂停')
    args = parser.parse_args(argv)
    if args.execute or args.enable or args.disable or args.pause_hours is not None:
        print(DEPRECATED_RECOVERY_REASON, file=sys.stderr)
        return 2

    RiskAutoRecovery().print_report()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
