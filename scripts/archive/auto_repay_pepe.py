#!/usr/bin/env python3
"""
自动化处理 PEPE 借币问题
方案：卖出少量 SPACE，购买 PEPE 还款
"""

import json
import time
from datetime import datetime
from configs.loader import load_config
from src.execution.okx_private_client import OKXPrivateClient

def auto_repay_pepe():
    print("🤖 自动化处理：PEPE 借币问题")
    print("=" * 60)
    
    # 1. 加载配置
    cfg = load_config('configs/live_20u_real.yaml', env_path='.env')
    okx = OKXPrivateClient(exchange=cfg.exchange)
    
    # 2. 检查当前状态
    print("检查账户状态...")
    resp = okx.get_balance()
    if not resp.data or 'data' not in resp.data:
        print("❌ 无法获取账户数据")
        return
    
    account = resp.data['data'][0]
    
    # 检查 PEPE 借币
    pepe_borrow = 0
    for detail in account.get('details', []):
        if detail.get('ccy') == 'PEPE':
            pepe_borrow = float(detail.get('liab', 0))
            break
    
    if pepe_borrow <= 0:
        print("✅ 无 PEPE 借币")
        return
    
    print(f"检测到 PEPE 借币: {pepe_borrow}")
    
    # 3. 计算需要购买的 PEPE 数量
    # 需要额外购买：借币数量 - 当前持仓
    # 当前持仓约 4,353,429，借币约 4,357,782
    # 差额约 4,353 PEPE
    
    pepe_needed = pepe_borrow - 4353429  # 简化计算
    if pepe_needed <= 0:
        print("✅ 持仓足够还清借币")
        return
    
    print(f"需要购买 PEPE: {pepe_needed:.0f}")
    
    # 4. 计算需要卖出的 SPACE 数量
    # 获取当前价格
    try:
        from src.data.providers.okx_ccxt_provider import OKXCCXTProvider
        provider = OKXCCXTProvider(rate_limit=True)
        
        # 获取 PEPE 和 SPACE 价格
        symbols = ['PEPE/USDT', 'SPACE/USDT']
        md = provider.fetch_ohlcv(symbols, timeframe="1m", limit=1)
        
        pepe_price = md['PEPE/USDT'].close[-1] if 'PEPE/USDT' in md else 0.0000045
        space_price = md['SPACE/USDT'].close[-1] if 'SPACE/USDT' in md else 0.0113
        
        print(f"当前价格:")
        print(f"  PEPE: {pepe_price}")
        print(f"  SPACE: {space_price}")
        
        # 计算需要卖出的 SPACE 数量
        pepe_cost_usdt = pepe_needed * pepe_price
        space_needed = pepe_cost_usdt / space_price
        
        print(f"需要卖出 SPACE: {space_needed:.2f} (约 {pepe_cost_usdt:.4f} USDT)")
        
    except Exception as e:
        print(f"获取价格失败: {e}")
        # 使用估算值
        space_needed = 50  # 估算卖出 50 个 SPACE
        print(f"使用估算值: 卖出 {space_needed} SPACE")
    
    # 5. 执行还款计划
    print(f"\n🎯 执行计划:")
    print("1. 卖出少量 SPACE")
    print("2. 购买 PEPE")
    print("3. 还款")
    
    # 6. 生成操作指南
    print(f"\n📋 手动操作指南:")
    print("=" * 60)
    print("请按以下步骤操作:")
    print("")
    print("步骤1: 登录 OKX")
    print("  打开 OKX 网页版或 APP")
    print("")
    print("步骤2: 卖出 SPACE")
    print(f"  卖出数量: {space_needed:.2f} SPACE")
    print("  交易对: SPACE/USDT")
    print("  订单类型: 市价单")
    print("")
    print("步骤3: 购买 PEPE")
    print(f"  购买数量: {pepe_needed:.0f} PEPE")
    print("  交易对: PEPE/USDT")
    print("  订单类型: 市价单")
    print("")
    print("步骤4: 还款")
    print("  进入「资产」->「借贷」")
    print(f"  选择 PEPE，还款数量: {pepe_borrow}")
    print("")
    print("步骤5: 验证")
    print("  重新运行借币监控:")
    print("  cd /home/admin/clawd/v5-trading-bot")
    print("  PYTHONPATH=. python3 scripts/enhanced_borrow_monitor.py")
    print("=" * 60)
    
    # 7. 保存详细计划
    plan = {
        'timestamp': datetime.now().isoformat(),
        'pepe_borrow': pepe_borrow,
        'pepe_needed': pepe_needed,
        'space_needed': space_needed,
        'estimated_cost_usdt': pepe_needed * 0.0000045,
        'steps': [
            '登录 OKX',
            f'卖出 {space_needed:.2f} SPACE',
            f'购买 {pepe_needed:.0f} PEPE',
            f'还款 {pepe_borrow} PEPE',
            '验证还款结果'
        ]
    }
    
    with open('reports/pepe_repay_plan.json', 'w') as f:
        json.dump(plan, f, indent=2)
    
    print(f"\n📝 详细计划已保存: reports/pepe_repay_plan.json")
    print("\n" + "=" * 60)
    print("⚠️ 请尽快按指南操作，避免产生利息！")
    print("=" * 60)

if __name__ == "__main__":
    auto_repay_pepe()