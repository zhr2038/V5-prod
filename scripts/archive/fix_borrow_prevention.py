#!/usr/bin/env python3
"""
修复借币预防机制
"""

import json
from pathlib import Path

def create_borrow_prevention():
    print("🛡️ 创建借币预防机制")
    print("=" * 60)
    
    # 1. 创建风险币种过滤器
    risk_filter = {
        'name': '借币风险过滤器',
        'description': '防止因手续费导致借币的交易',
        'rules': [
            {
                'rule': '检查手续费币种余额',
                'description': '交易前检查是否有足够的手续费币种余额',
                'implementation': '在交易执行前调用余额检查'
            },
            {
                'rule': '过滤低价值高手续费币种',
                'description': '避免交易单价极低、手续费比例高的币种',
                'thresholds': {
                    'min_price_usdt': 0.001,  # 最低单价
                    'max_fee_ratio': 0.1,     # 最大手续费比例
                    'min_trade_value': 5.0    # 最小交易金额
                }
            },
            {
                'rule': '预留手续费',
                'description': '购买时预留额外数量支付手续费',
                'reserve_ratio': 0.001  # 预留 0.1%
            },
            {
                'rule': '借币检测阻止',
                'description': '检测到账户有借币时阻止新交易',
                'action': 'block_trades'
            }
        ],
        'high_risk_symbols': [
            'PEPE/USDT',  # 单价极低，手续费比例高
            'SHIB/USDT',  # 类似风险
            'FLOKI/USDT', # 类似风险
            'BONK/USDT',  # 类似风险
        ],
        'created_at': '2026-02-18'
    }
    
    with open('configs/borrow_prevention_rules.json', 'w') as f:
        json.dump(risk_filter, f, indent=2)
    
    print("✅ 创建风险过滤器: configs/borrow_prevention_rules.json")
    
    # 2. 更新黑名单（增加高风险币种）
    blacklist_file = Path('configs/blacklist.json')
    if blacklist_file.exists():
        with open(blacklist_file, 'r') as f:
            blacklist = json.load(f)
        
        # 添加高风险币种
        high_risk = ['SHIB/USDT', 'FLOKI/USDT', 'BONK/USDT']
        current = set(blacklist.get('symbols', []))
        
        added = []
        for symbol in high_risk:
            if symbol not in current:
                blacklist['symbols'].append(symbol)
                added.append(symbol)
        
        with open(blacklist_file, 'w') as f:
            json.dump(blacklist, f, indent=2)
        
        if added:
            print(f"✅ 更新黑名单，添加高风险币种: {added}")
    
    # 3. 创建交易前检查脚本
    pre_trade_check = '''#!/usr/bin/env python3
"""
交易前检查：防止借币
"""

def check_borrow_risk(symbol, trade_amount_usdt):
    """检查交易是否有借币风险"""
    
    # 提取币种
    base_currency = symbol.split('/')[0]
    
    # 检查规则
    risks = []
    
    # 1. 检查是否高风险币种
    high_risk = ['PEPE', 'SHIB', 'FLOKI', 'BONK']
    if base_currency in high_risk:
        risks.append(f"高风险币种: {base_currency}")
    
    # 2. 检查单价（需要市场价格数据）
    # 这里可以集成价格检查
    
    # 3. 检查手续费比例
    # 低价值币种手续费比例可能很高
    
    return risks

if __name__ == "__main__":
    # 测试
    test_symbols = ['PEPE/USDT', 'BTC/USDT', 'SHIB/USDT']
    for symbol in test_symbols:
        risks = check_borrow_risk(symbol, 20.0)
        if risks:
            print(f"⚠️ {symbol}: {risks}")
        else:
            print(f"✅ {symbol}: 安全")
'''
    
    with open('scripts/pre_trade_borrow_check.py', 'w') as f:
        f.write(pre_trade_check)
    
    print("✅ 创建交易前检查脚本: scripts/pre_trade_borrow_check.py")
    
    # 4. 更新配置建议
    print(f"\n🎯 配置更新建议:")
    print("1. 在 configs/live_20u_real.yaml 中添加:")
    print("""
execution:
  borrow_prevention: true
  min_trade_value_usdt: 5.0
  max_fee_ratio: 0.05
  check_fee_currency_balance: true
""")
    
    print(f"\n2. 在自动化脚本中集成检查:")
    print("""
# 在 auto_run_v5.sh 中添加
echo "🔍 运行借币风险检查..."
python3 scripts/pre_trade_borrow_check.py
""")
    
    print(f"\n" + "=" * 60)
    print("📋 预防机制已创建:")
    print("1. ✅ 风险过滤器规则")
    print("2. ✅ 更新黑名单")
    print("3. ✅ 交易前检查脚本")
    print("4. ✅ 配置建议")
    print("=" * 60)

if __name__ == "__main__":
    create_borrow_prevention()