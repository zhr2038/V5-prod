#!/usr/bin/env python3
"""
V5 风控自动恢复机制

功能：
- 监控回撤状态
- 自动降级风险档位（PROTECT → DEFENSE → NEUTRAL）
- 可配置是否启用自动恢复
- 提供手动暂停开关
"""

import json
import sqlite3
import sys
from dataclasses import asdict
from pathlib import Path
from datetime import datetime, timedelta, timezone

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.runtime_config import load_runtime_config, resolve_runtime_path
from src.execution.fill_store import (
    derive_runtime_auto_risk_guard_path,
    derive_runtime_named_json_path,
    derive_runtime_reports_dir,
    derive_runtime_runs_dir,
)
from src.risk.auto_risk_guard import AutoRiskGuard

# 自动恢复阈值配置
RECOVERY_THRESHOLDS = {
    'PROTECT': {
        'drawdown_exit': 0.15,   # 回撤从24%→15%时退出PROTECT
        'target_level': 'DEFENSE'
    },
    'DEFENSE': {
        'drawdown_exit': 0.08,   # 回撤从15%→8%时退出DEFENSE
        'target_level': 'NEUTRAL'
    }
}


class RiskAutoRecovery:
    """风控自动恢复管理器"""
    
    def __init__(self, workspace: Path = PROJECT_ROOT):
        self.workspace = Path(workspace).resolve()
        cfg = load_runtime_config(project_root=self.workspace)
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
            except:
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
                        since = str(
                            state.get('since')
                            or state.get('last_update')
                            or (
                                state.get('history', [])[-1].get('ts')
                                if isinstance(state.get('history'), list) and state.get('history')
                                and isinstance(state.get('history')[-1], dict)
                                else ''
                            )
                            or datetime.now().isoformat()
                        )
                        state['current_level'] = current_level
                        state['level'] = current_level
                        state['since'] = since
                        return state
            except:
                pass
        now_iso = datetime.now().isoformat()
        return {'current_level': 'NEUTRAL', 'level': 'NEUTRAL', 'since': now_iso, 'last_update': now_iso}
    
    def get_drawdown_history(self, hours=24):
        """获取回撤历史"""
        try:
            points = []
            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

            equity_files = []
            legacy_equity_file = self.reports_dir / 'equity_history.jsonl'
            if legacy_equity_file.exists():
                equity_files.append(legacy_equity_file)
            if self.runs_dir.exists():
                equity_files.extend(sorted(run_dir / 'equity.jsonl' for run_dir in self.runs_dir.iterdir() if run_dir.is_dir()))

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
                                        'drawdown': drawdown if drawdown is not None else 0
                                    })
                            except:
                                continue
                except:
                    continue
            
            points.sort(key=lambda item: item['ts'])
            dedup = {}
            for point in points:
                dedup[point['ts'].isoformat()] = point
            return list(dedup.values())
        except:
            return []
    
    def calculate_avg_drawdown(self, hours=6):
        """计算最近N小时平均回撤"""
        history = self.get_drawdown_history(hours=hours)
        if not history:
            return 0
        
        drawdowns = [h['drawdown'] for h in history if h['drawdown'] is not None]
        if not drawdowns:
            return 0
        
        return sum(drawdowns) / len(drawdowns)

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
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
            return parsed
        except Exception:
            return None
    
    def check_recovery_conditions(self, current_level):
        """检查是否满足降级条件"""
        if current_level not in RECOVERY_THRESHOLDS:
            return False, None
        
        threshold = RECOVERY_THRESHOLDS[current_level]
        avg_drawdown = self.calculate_avg_drawdown(hours=6)
        
        # 检查回撤是否低于退出阈值
        if avg_drawdown <= threshold['drawdown_exit']:
            return True, threshold['target_level']
        
        return False, None
    
    def time_in_current_level(self, state):
        """计算在当前档位停留的时间"""
        try:
            since = self._parse_state_datetime(state.get('since'))
            if since is None:
                raise ValueError("missing since")
            return (datetime.now(timezone.utc) - since).total_seconds() / 3600  # 小时
        except:
            return 999  # 如果解析失败，假设已停留很久
    
    def evaluate_recovery(self):
        """评估是否执行自动恢复"""
        # 检查是否被手动暂停
        if self.config.get('manual_override_until'):
            until = self._parse_state_datetime(self.config['manual_override_until'])
            if until is not None and datetime.now(timezone.utc) < until:
                return {'action': 'paused', 'reason': f'手动暂停至 {until}'}
        
        # 检查是否启用
        if not self.config.get('enabled', True):
            return {'action': 'disabled', 'reason': '自动恢复已禁用'}
        
        # 获取当前状态
        state = self.get_current_risk_state()
        current_level = str(state.get('current_level') or state.get('level') or 'NEUTRAL').upper()
        
        # NEUTRAL不需要恢复
        if current_level == 'NEUTRAL':
            return {'action': 'none', 'reason': '已在NEUTRAL档位'}
        
        # 检查停留时间
        hours_in_level = self.time_in_current_level(state)
        if hours_in_level < self.config.get('min_time_in_level_hours', 4):
            return {'action': 'wait', 'reason': f'在当前档位仅{hours_in_level:.1f}小时，需至少{self.config["min_time_in_level_hours"]}小时'}
        
        # 检查降级条件
        should_recover, target_level = self.check_recovery_conditions(current_level)
        
        if should_recover:
            return {
                'action': 'recover',
                'from_level': current_level,
                'to_level': target_level,
                'reason': f'回撤已恢复至阈值以下，建议降级至{target_level}'
            }
        
        avg_dd = self.calculate_avg_drawdown(hours=6)
        return {
            'action': 'hold',
            'reason': f'当前档位{current_level}，最近6小时平均回撤{avg_dd:.1%}，未满足降级条件'
        }
    
    def execute_recovery(self, target_level):
        """执行恢复（修改风险状态文件）"""
        try:
            state = self.get_current_risk_state()
            old_level = str(state.get('current_level') or state.get('level') or 'NEUTRAL').upper()
            now_iso = datetime.now().isoformat()
            metrics = state.get('metrics') if isinstance(state.get('metrics'), dict) else {}
            history = list(state.get('history') or []) if isinstance(state.get('history'), list) else []
            history.append({
                'ts': now_iso,
                'from': old_level,
                'to': target_level,
                'reason': '[AUTO] recovery',
                'metrics': dict(metrics),
            })

            # 更新状态
            state['current_level'] = target_level
            state['current_config'] = asdict(AutoRiskGuard.LEVELS[target_level])
            state['metrics'] = metrics
            state['history'] = history[-50:]
            state['last_update'] = now_iso
            state['level'] = target_level
            state['since'] = now_iso
            state['recovered_from'] = old_level
            state['recovery_reason'] = 'auto'
            
            self.risk_state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.risk_state_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            return True, f"已从{old_level}降级至{target_level}"
        except Exception as e:
            return False, str(e)
    
    def print_report(self):
        """打印评估报告"""
        print("=" * 60)
        print("🛡️  V5 风控自动恢复评估")
        print("=" * 60)
        print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"自动恢复: {'✅ 启用' if self.config.get('enabled') else '❌ 禁用'}")
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
        print(f"最近6小时平均回撤: {avg_dd:.1%}")
        
        if state.get('level') in RECOVERY_THRESHOLDS:
            threshold = RECOVERY_THRESHOLDS[state['level']]
            print(f"降级阈值: {threshold['drawdown_exit']:.1%}")
        print()
        
        # 评估结果
        result = self.evaluate_recovery()
        print(f"建议操作: {result['action'].upper()}")
        print(f"原因: {result['reason']}")
        
        if result['action'] == 'recover':
            print(f"建议降级至: {result['to_level']}")
        
        print("=" * 60)
        return result


def main():
    import argparse
    parser = argparse.ArgumentParser(description='V5 风控自动恢复')
    parser.add_argument('--execute', action='store_true', help='执行恢复（默认仅评估）')
    parser.add_argument('--enable', action='store_true', help='启用自动恢复')
    parser.add_argument('--disable', action='store_true', help='禁用自动恢复')
    parser.add_argument('--pause-hours', type=int, help='暂停自动恢复N小时')
    args = parser.parse_args()
    
    manager = RiskAutoRecovery()
    
    # 处理配置命令
    if args.enable:
        manager.config['enabled'] = True
        manager.save_config()
        print("✅ 已启用自动恢复")
        return
    
    if args.disable:
        manager.config['enabled'] = False
        manager.save_config()
        print("❌ 已禁用自动恢复")
        return
    
    if args.pause_hours:
        until = datetime.now() + timedelta(hours=args.pause_hours)
        manager.config['manual_override_until'] = until.isoformat()
        manager.save_config()
        print(f"⏸️  已暂停自动恢复至 {until.strftime('%Y-%m-%d %H:%M')}")
        return
    
    # 评估并打印报告
    result = manager.print_report()
    
    # 执行恢复
    if args.execute and result['action'] == 'recover':
        print()
        print("🔄 执行恢复...")
        success, msg = manager.execute_recovery(result['to_level'])
        if success:
            print(f"✅ {msg}")
        else:
            print(f"❌ 失败: {msg}")


if __name__ == '__main__':
    main()
