#!/usr/bin/env python3
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
