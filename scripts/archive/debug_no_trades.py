#!/usr/bin/env python3
"""
调试为什么没有交易
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[1]))

def debug_trade_generation():
    """调试交易生成逻辑"""
    
    print("🔍 调试交易生成逻辑")
    print("=" * 60)
    
    try:
        from src.core.pipeline import Pipeline
        from configs.loader import load_config
        
        # 加载配置
        cfg = load_config("configs/fixed_test.yaml", env_path=".env")
        
        print("📋 配置检查:")
        print(f"  币种: {cfg.symbols}")
        print(f"  Deadband设置:")
        print(f"    sideways: {cfg.rebalance.deadband_sideways}")
        print(f"    trending: {cfg.rebalance.deadband_trending}")
        print(f"    riskoff: {cfg.rebalance.deadband_riskoff}")
        print(f"  最小交易金额: 需要检查订单生成逻辑")
        
        # 创建Pipeline
        pipeline = Pipeline(cfg)
        
        # 加载数据
        print(f"\n📊 加载数据...")
        
        import sqlite3
        db_path = "reports/alpha_history.db"
        conn = sqlite3.connect(db_path)
        
        # 获取最新数据点
        for symbol in cfg.symbols[:2]:  # 检查前两个币种
            query = f"""
            SELECT timestamp, close 
            FROM market_data_1h 
            WHERE symbol = ? 
            ORDER BY timestamp DESC 
            LIMIT 10
            """
            
            df = pd.read_sql_query(query, conn, params=(symbol,))
            print(f"  {symbol} 最新价格: ${df['close'].iloc[0]:.2f}")
        
        conn.close()
        
        # 测试Pipeline的step方法
        print(f"\n🎯 测试Pipeline.step()...")
        
        try:
            # 模拟一个时间步
            result = pipeline.step(
                current_ts=int(pd.Timestamp.now().timestamp()),
                market_data={},  # 实际需要真实数据
                current_positions={},
                current_equity=100.0,
                audit=None
            )
            
            print(f"  ✅ step()执行成功")
            
            # 检查结果
            if hasattr(result, 'orders'):
                orders = result.orders or []
                print(f"  生成订单: {len(orders)}笔")
                
                if orders:
                    for order in orders:
                        print(f"    {order}")
                else:
                    print(f"  ❌ 没有生成订单")
                    
                    # 检查可能的原因
                    if hasattr(result, 'debug_info'):
                        debug_info = result.debug_info or {}
                        print(f"  调试信息: {debug_info}")
            
        except Exception as e:
            print(f"  ❌ step()执行错误: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"\n💡 交易生成调试完成")
        
    except Exception as e:
        print(f"❌ 调试错误: {e}")
        import traceback
        traceback.print_exc()

def check_order_generation_logic():
    """检查订单生成逻辑"""
    
    print("\n" + "=" * 60)
    print("🔧 检查订单生成逻辑")
    print("=" * 60)
    
    # 查找订单生成相关代码
    order_files = [
        "src/execution/order_generator.py",
        "src/execution/execution_engine.py", 
        "src/core/pipeline.py"
    ]
    
    for file_path in order_files:
        full_path = Path(f"/home/admin/clawd/v5-trading-bot/{file_path}")
        if full_path.exists():
            print(f"\n📄 检查文件: {file_path}")
            
            with open(full_path, 'r') as f:
                content = f.read()
            
            # 查找订单生成相关函数
            if 'def generate_orders' in content:
                print(f"  找到generate_orders函数")
                
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if 'def generate_orders' in line:
                        print(f"    行 {i+1}: {line.strip()}")
                        # 显示函数内容
                        for j in range(i+1, min(i+20, len(lines))):
                            if lines[j].strip() and not lines[j].startswith(' ' * 4):
                                break
                            print(f"    行 {j+1}: {lines[j].rstrip()}")
                        break
            
            # 查找返回订单的地方
            if 'return orders' in content or 'return []' in content:
                print(f"  找到订单返回逻辑")
                
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if 'return orders' in line or 'return []' in line:
                        print(f"    行 {i+1}: {line.strip()}")
                        # 显示上下文
                        for j in range(max(0, i-3), min(len(lines), i+4)):
                            if j != i:
                                print(f"    行 {j+1}: {lines[j].strip()}")
                        print("")
    
    print(f"\n💡 订单生成逻辑检查完成")

def check_deadband_application():
    """检查deadband应用"""
    
    print("\n" + "=" * 60)
    print("🎯 检查deadband应用")
    print("=" * 60)
    
    # 查找deadband相关代码
    deadband_files = [
        "src/portfolio/portfolio_engine.py",
        "src/execution/order_generator.py"
    ]
    
    for file_path in deadband_files:
        full_path = Path(f"/home/admin/clawd/v5-trading-bot/{file_path}")
        if full_path.exists():
            print(f"\n📄 检查文件: {file_path}")
            
            with open(full_path, 'r') as f:
                content = f.read()
            
            if 'deadband' in content.lower():
                print(f"  找到deadband相关代码")
                
                lines = content.split('\n')
                for i, line in enumerate(lines):
                    if 'deadband' in line.lower():
                        print(f"    行 {i+1}: {line.strip()}")
    
    print(f"\n💡 deadband检查完成")

def main():
    """主函数"""
    
    print("🚀 调试无交易问题")
    print("=" * 60)
    
    # 调试交易生成
    debug_trade_generation()
    
    # 检查订单生成逻辑
    check_order_generation_logic()
    
    # 检查deadband应用
    check_deadband_application()
    
    print("\n✅ 调试完成")
    print("=" * 60)
    
    print("\n💡 可能的原因:")
    print("  1. Deadband设置可能仍然太高")
    print("  2. 订单生成逻辑可能有额外限制")
    print("  3. 最小交易金额限制")
    print("  4. 持仓变化太小，不满足交易条件")

if __name__ == "__main__":
    main()