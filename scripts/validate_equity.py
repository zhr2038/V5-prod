#!/usr/bin/env python3
"""
Equity 数据验证脚本
在每次运行前验证 equity 数据一致性
"""

import json
import sys
from pathlib import Path

def validate_equity_data(cfg, run_id):
    """验证 equity 数据一致性"""
    
    # 检查配置
    cap_eq = getattr(cfg.budget, "live_equity_cap_usdt", None)
    if cap_eq is None:
        return True
    
    cap_eq_f = float(cap_eq)
    
    # 读取或创建 equity.jsonl 的第一行
    eq_file = Path(f"reports/runs/{run_id}/equity.jsonl")
    eq_file.parent.mkdir(parents=True, exist_ok=True)
    
    if eq_file.exists():
        with open(eq_file, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        if lines:
            first_line = json.loads(lines[0])
            equity = first_line.get('equity', 0)
            
            # 检查是否是测试资金
            if abs(equity - cap_eq_f) < 5.0:
                print(f"⚠️  检测到测试资金 ({equity})，需要替换为实际余额")
                return False
    
    return True

def fix_equity_first_line(cfg, run_id, actual_equity):
    """修复 equity.jsonl 的第一行"""
    eq_file = Path(f"reports/runs/{run_id}/equity.jsonl")
    
    if eq_file.exists():
        with open(eq_file, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        if lines:
            first_line = json.loads(lines[0])
            first_line['equity'] = actual_equity
            first_line['cash'] = actual_equity
            
            lines[0] = json.dumps(first_line)
            
            with open(eq_file, 'w') as f:
                for line in lines:
                    f.write(line + '\n')
            
            print(f"✅ 修复 equity 第一行: {actual_equity}")
    
    return True

if __name__ == "__main__":
    # 示例用法
    from configs.loader import load_config
    cfg = load_config("configs/live_20u_real.yaml", env_path=".env")
    run_id = "test_validation"
    
    if not validate_equity_data(cfg, run_id):
        print("需要修复 equity 数据")
