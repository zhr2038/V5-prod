#!/usr/bin/env python3
"""
直接充值还款指南
"""

import json
from datetime import datetime

def direct_repay_guide():
    print("💰 直接充值还款指南")
    print("=" * 60)
    
    # 借币详情
    pepe_borrow = 4357782.429
    pepe_needed = 4353  # 需要充值的数量
    
    print(f"借币详情:")
    print(f"  PEPE 借币: {pepe_borrow:,}")
    print(f"  需要充值: {pepe_needed:,} PEPE")
    print(f"  估算价值: {pepe_needed * 0.0000045:.6f} USDT")
    print(f"  (约 ${pepe_needed * 0.0000045 * 7.2:.4f} 人民币)")
    
    print(f"\n🎯 最简单方案：直接充值 PEPE")
    print("=" * 60)
    
    print("步骤1: 获取 PEPE")
    print("  - 从其他交易所购买（如币安、火币）")
    print(f"  - 购买数量: {pepe_needed:,} PEPE")
    print("  - 成本: 约 $0.02 USD")
    
    print(f"\n步骤2: 转账到 OKX")
    print("  - 打开 OKX，进入「资产」")
    print("  - 点击「充值」，选择 PEPE")
    print("  - 复制充值地址")
    print("  - 从其他交易所提现到该地址")
    
    print(f"\n步骤3: 等待到账")
    print("  - PEPE 网络确认时间: 几分钟")
    print("  - 到账后检查余额")
    
    print(f"\n步骤4: 还款")
    print("  - 进入「资产」->「借贷」")
    print(f"  - 选择 PEPE，还款数量: {pepe_borrow}")
    print("  - 确认还款")
    
    print(f"\n步骤5: 验证")
    print("  - 重新运行借币监控")
    print("  - 确认无借币显示")
    
    print(f"\n💡 替代方案：")
    print("1. 使用信用卡购买 PEPE（如果支持）")
    print("2. 向朋友借少量 PEPE")
    print("3. 参加空投获取 PEPE")
    
    # 保存指南
    guide = {
        'timestamp': datetime.now().isoformat(),
        'pepe_borrow': pepe_borrow,
        'pepe_needed': pepe_needed,
        'estimated_cost_usdt': pepe_needed * 0.0000045,
        'estimated_cost_cny': pepe_needed * 0.0000045 * 7.2,
        'steps': [
            '从其他交易所购买 4,353 PEPE',
            '转账到 OKX',
            '等待到账',
            '还款 4,357,782 PEPE',
            '验证还款结果'
        ],
        'notes': [
            '成本极低（约 $0.02）',
            '避免利息产生',
            '处理后 PEPE 将保持黑名单状态'
        ]
    }
    
    with open('reports/direct_repay_guide.json', 'w') as f:
        json.dump(guide, f, indent=2)
    
    print(f"\n📝 指南已保存: reports/direct_repay_guide.json")
    print("\n" + "=" * 60)
    print("✅ 这是最简单的解决方案！")
    print("=" * 60)

if __name__ == "__main__":
    direct_repay_guide()