"""
实时权益获取模块 - 直接从OKX API获取，不依赖本地缓存
"""
import os
import ccxt
from pathlib import Path
from typing import Dict, Optional


def get_live_equity_from_okx() -> Optional[float]:
    """
    从OKX交易账户获取实时权益
    
    Returns:
        总权益（USDT），失败返回None
    """
    try:
        # 加载API密钥
        env_path = Path(__file__).resolve().parents[2] / ".env"
        if env_path.exists():
            with env_path.open(encoding="utf-8") as f:
                for line in f:
                    if '=' in line and not line.startswith('#'):
                        key, val = line.strip().split('=', 1)
                        val = val.strip('"').strip("'")
                        os.environ[key] = val
        
        # 连接OKX
        exchange = ccxt.okx({
            'apiKey': os.getenv('EXCHANGE_API_KEY'),
            'secret': os.getenv('EXCHANGE_API_SECRET'),
            'password': os.getenv('EXCHANGE_PASSPHRASE'),
            'enableRateLimit': True
        })
        
        # 获取交易账户余额
        balance = exchange.fetch_balance({'type': 'trade'})
        
        total_equity = 0.0
        
        # 计算USDT价值
        for coin, amount in balance.get('total', {}).items():
            if amount <= 0:
                continue
                
            if coin == 'USDT':
                total_equity += float(amount)
            else:
                # 获取价格并计算价值
                try:
                    ticker = exchange.fetch_ticker(f"{coin}/USDT")
                    price = float(ticker.get('last', 0))
                    total_equity += float(amount) * price
                except:
                    pass  # 忽略价格获取失败的币种
        
        return total_equity
        
    except Exception as e:
        print(f"[EquityFetcher] 获取权益失败: {e}")
        return None


def check_budget_limit(equity_cap: float = 20.0) -> Dict:
    """
    检查是否超过预算限制
    
    Returns:
        {
            'ok': bool,           # 是否在预算内
            'current': float,     # 当前权益
            'cap': float,         # 预算上限
            'utilization': float  # 使用率(%)
        }
    """
    equity = get_live_equity_from_okx()
    
    if equity is None:
        return {
            'ok': False,
            'current': 0,
            'cap': equity_cap,
            'utilization': 0,
            'error': '无法获取权益'
        }
    
    utilization = (equity / equity_cap * 100) if equity_cap > 0 else 0
    
    # 严格预算：不再放宽10%缓冲，超过上限即视为超限。
    ok = equity <= equity_cap
    
    return {
        'ok': ok,
        'current': equity,
        'cap': equity_cap,
        'utilization': utilization
    }


if __name__ == "__main__":
    # 测试
    print("=" * 50)
    print("OKX 实时权益检测")
    print("=" * 50)
    
    equity = get_live_equity_from_okx()
    if equity is not None:
        print(f"\n当前交易账户权益: {equity:.2f} USDT")
        
        result = check_budget_limit(equity_cap=20.0)
        print(f"预算检查: {result}")
    else:
        print("\n❌ 无法获取权益")
