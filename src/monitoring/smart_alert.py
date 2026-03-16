"""
V5 智能告警系统 - Smart Alert System

只报异常，不报平安
"""

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class SmartAlertEngine:
    """智能告警引擎"""
    
    def __init__(self, workspace: Path = PROJECT_ROOT):
        self.workspace = workspace
        self.reports_dir = workspace / 'reports'
        self.alerts_state_file = self.reports_dir / 'alerts_state.json'
        self._load_state()
    
    def _load_state(self):
        """加载告警状态（防止重复告警）"""
        if self.alerts_state_file.exists():
            try:
                with open(self.alerts_state_file, 'r', encoding='utf-8') as f:
                    self.state = json.load(f)
            except:
                self.state = {}
        else:
            self.state = {}
    
    def _save_state(self):
        """保存告警状态"""
        self.alerts_state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.alerts_state_file, 'w', encoding='utf-8') as f:
            json.dump(self.state, f, indent=2)
    
    def _should_alert(self, alert_type: str, cooldown_minutes: int = 60) -> bool:
        """检查是否应该发送告警（冷却期控制）"""
        now = datetime.now().timestamp()
        last_alert = self.state.get(f'last_{alert_type}', 0)
        if now - last_alert > cooldown_minutes * 60:
            self.state[f'last_{alert_type}'] = now
            return True
        return False
    
    def check_signal_no_trade(self) -> Optional[Dict]:
        """检查：有信号但连续无成交"""
        try:
            runs_dir = self.reports_dir / 'runs'
            if not runs_dir.exists():
                return None
            
            run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
            run_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            if len(run_dirs) < 2:
                return None
            
            # 检查最近2轮
            consecutive_no_trade = 0
            for run_dir in run_dirs[:2]:
                with open(run_dir / 'decision_audit.json', 'r') as f:
                    data = json.load(f)
                counts = data.get('counts', {})
                selected = counts.get('selected', 0)
                rebalance = counts.get('orders_rebalance', 0)
                
                if selected > 0 and rebalance == 0:
                    consecutive_no_trade += 1
            
            if consecutive_no_trade >= 2:
                if self._should_alert('signal_no_trade', cooldown_minutes=120):
                    return {
                        'type': 'signal_no_trade',
                        'level': 'high',
                        'title': '⚠️ 有信号无成交',
                        'message': f'连续{consecutive_no_trade}轮有策略信号但未执行交易，可能是deadband或风控过严',
                        'suggestion': '检查决策归因面板，考虑下调 deadband 或放宽风控'
                    }
            return None
        except Exception as e:
            print(f"[SmartAlert] check_signal_no_trade error: {e}")
            return None
    
    def check_no_buy_in_market(self) -> Optional[Dict]:
        """检查：行情好但长时间无买入"""
        try:
            runs_dir = self.reports_dir / 'runs'
            if not runs_dir.exists():
                return None
            
            run_dirs = [d for d in runs_dir.iterdir() if d.is_dir() and (d / 'decision_audit.json').exists()]
            run_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            # 检查最近6轮（约6小时）
            recent_buys = 0
            in_good_market = False
            
            for run_dir in run_dirs[:6]:
                with open(run_dir / 'decision_audit.json', 'r') as f:
                    data = json.load(f)
                
                counts = data.get('counts', {})
                rebalance = counts.get('orders_rebalance', 0)
                if rebalance > 0:
                    recent_buys += 1
                
                regime = data.get('regime', '')
                if regime in ['Sideways', 'Trending']:
                    in_good_market = True
            
            if in_good_market and recent_buys == 0:
                if self._should_alert('no_buy_in_market', cooldown_minutes=360):
                    return {
                        'type': 'no_buy_in_market',
                        'level': 'medium',
                        'title': '📉 行情正常但无买入',
                        'message': '近6小时处于Sideways/Trending状态但无任何买入成交',
                        'suggestion': '检查策略信号强度或考虑放宽执行门槛'
                    }
            return None
        except Exception as e:
            print(f"[SmartAlert] check_no_buy_in_market error: {e}")
            return None
    
    def check_drawdown(self) -> Optional[Dict]:
        """检查：回撤超限"""
        try:
            # 从reconcile_status读取当前回撤
            reconcile_file = self.reports_dir / 'reconcile_status.json'
            if not reconcile_file.exists():
                return None
            
            with open(reconcile_file, 'r') as f:
                data = json.load(f)
            
            drawdown_pct = data.get('local_snapshot', {}).get('drawdown_pct', 0)
            
            if drawdown_pct > 0.10:  # 10%回撤
                if self._should_alert('drawdown', cooldown_minutes=180):
                    return {
                        'type': 'drawdown',
                        'level': 'high',
                        'title': '🔴 回撤超限警告',
                        'message': f'当前回撤 {drawdown_pct*100:.1f}%，超过10%阈值',
                        'suggestion': '建议检查持仓风险，必要时手动干预'
                    }
            return None
        except Exception as e:
            print(f"[SmartAlert] check_drawdown error: {e}")
            return None
    
    def check_ic_degradation(self) -> Optional[Dict]:
        """检查：IC因子失效"""
        try:
            ic_file = self.reports_dir / 'ic_diagnostics_30d_20u.json'
            if not ic_file.exists():
                return None
            
            with open(ic_file, 'r') as f:
                data = json.load(f)
            
            overall_ic = data.get('overall_tradable', {}).get('ic', {}).get('mean', 0)
            if overall_ic is None:
                overall_ic = 0
            
            if overall_ic < 0:
                if self._should_alert('ic_degradation', cooldown_minutes=720):
                    return {
                        'type': 'ic_degradation',
                        'level': 'medium',
                        'title': '📊 IC因子失效',
                        'message': f'整体IC为负({overall_ic:.4f})，策略可能失效',
                        'suggestion': '建议检查因子配置或重新训练模型'
                    }
            return None
        except Exception as e:
            print(f"[SmartAlert] check_ic_degradation error: {e}")
            return None
    
    def check_kill_switch(self) -> Optional[Dict]:
        """检查：Kill Switch触发"""
        try:
            ks_file = self.reports_dir / 'kill_switch.json'
            if not ks_file.exists():
                return None
            
            with open(ks_file, 'r') as f:
                data = json.load(f)
            
            if data.get('enabled') or data.get('active') or data.get('kill_switch'):
                if self._should_alert('kill_switch', cooldown_minutes=30):
                    return {
                        'type': 'kill_switch',
                        'level': 'critical',
                        'title': '🚨 Kill Switch 已触发',
                        'message': '系统安全开关已启动，交易暂停',
                        'suggestion': '立即检查系统状态和日志，确认安全后手动解除'
                    }
            return None
        except Exception as e:
            print(f"[SmartAlert] check_kill_switch error: {e}")
            return None
    
    def run_all_checks(self) -> List[Dict]:
        """运行所有检查，返回需要发送的告警列表"""
        alerts = []
        
        checks = [
            self.check_signal_no_trade,
            self.check_no_buy_in_market,
            self.check_drawdown,
            self.check_ic_degradation,
            self.check_kill_switch
        ]
        
        for check in checks:
            try:
                alert = check()
                if alert:
                    alerts.append(alert)
            except Exception as e:
                print(f"[SmartAlert] Check error: {e}")
        
        if alerts:
            self._save_state()
        
        return alerts


if __name__ == '__main__':
    engine = SmartAlertEngine()
    alerts = engine.run_all_checks()
    print(f"[SmartAlert] Found {len(alerts)} alerts")
    for a in alerts:
        print(f"  - {a['title']}: {a['message']}")
