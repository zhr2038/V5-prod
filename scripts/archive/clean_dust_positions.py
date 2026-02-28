#!/usr/bin/env python3
"""
清理灰尘持仓脚本 - 简化版
自动卖出所有黑名单币的小额持仓（灰尘）
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

import os
import json
import time
import hmac
import hashlib
import base64
import requests
from dotenv import load_dotenv

# 加载环境变量
load_dotenv('/home/admin/clawd/v5-trading-bot/.env')

# 黑名单币（需要清理的）
BLACKLIST = {
    'SPACE', 'KITE', 'WLFI', 'MERL', 'J', 'PEPE', 
    'XAUT', 'AGLD', 'USDG', 'PROMPT'
}

# 灰尘阈值
DUST_QTY_THRESHOLD = 0.1
DUST_VALUE_THRESHOLD = 1.0


def get_okx_credentials():
    """获取OKX API凭证"""
    return {
        'api_key': os.getenv('EXCHANGE_API_KEY'),
        'api_secret': os.getenv('EXCHANGE_API_SECRET'),
        'passphrase': os.getenv('EXCHANGE_PASSPHRASE')
    }


def sign_okx(api_secret: str, timestamp: str, method: str, request_path: str, body: str = "") -> str:
    """OKX API签名"""
    message = timestamp + method.upper() + request_path + body
    mac = hmac.new(
        api_secret.encode('utf-8'),
        message.encode('utf-8'),
        hashlib.sha256
    )
    return base64.b64encode(mac.digest()).decode('utf-8')


def okx_request(method: str, path: str, params=None, body=None) -> dict:
    """发送OKX API请求"""
    creds = get_okx_credentials()
    if not all([creds['api_key'], creds['api_secret'], creds['passphrase']]):
        print("[ERROR] 缺少API凭证")
        return None
    
    timestamp = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'
    body_json = json.dumps(body) if body else ""
    
    headers = {
        'OK-ACCESS-KEY': creds['api_key'],
        'OK-ACCESS-SIGN': sign_okx(creds['api_secret'], timestamp, method, path, body_json),
        'OK-ACCESS-TIMESTAMP': timestamp,
        'OK-ACCESS-PASSPHRASE': creds['passphrase'],
        'Content-Type': 'application/json'
    }
    
    url = f"https://www.okx.com{path}"
    try:
        if method.upper() == 'GET':
            resp = requests.get(url, headers=headers, params=params, timeout=10)
        else:
            resp = requests.post(url, headers=headers, data=body_json, timeout=10)
        
        data = resp.json()
        if data.get('code') == '0':
            return data
        else:
            print(f"[API ERROR] {data.get('msg')}")
            return None
    except Exception as e:
        print(f"[REQUEST ERROR] {e}")
        return None


def get_balances() -> list:
    """获取账户余额"""
    data = okx_request('GET', '/api/v5/account/balance')
    if data and data.get('data'):
        return data['data'][0].get('details', [])
    return []


def get_ticker(inst_id: str) -> float:
    """获取最新价格"""
    try:
        resp = requests.get(f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}", timeout=5)
        data = resp.json()
        if data.get('code') == '0' and data.get('data'):
            return float(data['data'][0].get('last', 0))
    except Exception as e:
        print(f"  [WARN] 获取价格失败: {e}")
    return 0


def sell_market(inst_id: str, sz: str) -> bool:
    """市价卖出"""
    body = {
        "instId": inst_id,
        "tdMode": "cash",
        "side": "sell",
        "ordType": "market",
        "sz": sz
    }
    data = okx_request('POST', '/api/v5/trade/order', body=body)
    if data and data.get('data'):
        ord_id = data['data'][0].get('ordId')
        print(f"  [OK] 订单已提交, ID={ord_id}")
        return True
    return False


def main():
    print("="*60)
    print("V5 灰尘持仓清理工具")
    print("="*60)
    
    # 获取余额
    print("\n[1] 获取账户余额...")
    balances = get_balances()
    if not balances:
        print("[ERROR] 无法获取余额")
        return
    
    print(f"[OK] 获取到 {len(balances)} 个币种")
    
    # 筛选灰尘币
    print("\n[2] 筛选黑名单灰尘币...")
    dust_coins = []
    
    for bal in balances:
        ccy = bal.get('ccy', '')
        if ccy not in BLACKLIST or ccy == 'USDT':
            continue
        
        avail = float(bal.get('availBal', 0))
        frozen = float(bal.get('frozenBal', 0))
        total = avail + frozen
        
        if total <= 0:
            continue
        
        # 获取价格
        inst_id = f"{ccy}-USDT"
        price = get_ticker(inst_id)
        value = total * price
        
        is_dust = (total < DUST_QTY_THRESHOLD) or (value < DUST_VALUE_THRESHOLD)
        
        if is_dust:
            dust_coins.append({
                'ccy': ccy,
                'inst_id': inst_id,
                'qty': total,
                'avail': avail,
                'price': price,
                'value': value
            })
            print(f"  {ccy}: 数量={total:.8f}, 价格=${price:.4f}, 价值=${value:.4f} [DUST]")
    
    if not dust_coins:
        print("[INFO] 没有发现需要清理的灰尘币")
        return
    
    print(f"\n[OK] 发现 {len(dust_coins)} 个灰尘币")
    
    # 确认
    print("\n[3] 准备清理...")
    print("即将市价卖出以下币种:")
    for c in dust_coins:
        print(f"  - {c['ccy']}: {c['avail']:.8f} (${c['value']:.4f})")
    
    # 自动确认
    print("\n[自动确认执行...]")
    confirm = 'yes'
    
    # 执行卖出
    print("\n[4] 执行卖出...")
    success = 0
    failed = 0
    
    for c in dust_coins:
        print(f"\n  {c['ccy']}:")
        
        # 尝试获取最小下单量
        try:
            resp = requests.get(f"https://www.okx.com/api/v5/public/instruments?instType=SPOT&instId={c['inst_id']}", timeout=5)
            inst_data = resp.json()
            if inst_data.get('code') == '0' and inst_data.get('data'):
                min_sz = float(inst_data['data'][0].get('minSz', 0))
                lot_sz = float(inst_data['data'][0].get('lotSz', 0))
            else:
                min_sz = 0.0001
                lot_sz = 0.0001
        except:
            min_sz = 0.0001
            lot_sz = 0.0001
        
        # 检查是否可卖
        if c['avail'] < min_sz:
            print(f"    [SKIP] 数量{c['avail']} < 最小下单量{min_sz}")
            failed += 1
            continue
        
        # 计算卖出数量（按lot size取整）
        import math
        sell_qty = math.floor(c['avail'] / lot_sz) * lot_sz
        
        if sell_qty < min_sz:
            print(f"    [SKIP] 调整后数量{sell_qty} < 最小下单量{min_sz}")
            failed += 1
            continue
        
        print(f"    [SELL] 卖出{sell_qty} (最小量{min_sz}, lot{lot_sz})")
        
        if sell_market(c['inst_id'], str(sell_qty)):
            success += 1
            time.sleep(0.5)
        else:
            failed += 1
    
    # 汇总
    print("\n" + "="*60)
    print("清理完成")
    print("="*60)
    print(f"成功: {success}")
    print(f"失败/跳过: {failed}")
    
    if failed > 0:
        print("\n部分灰尘因数量太小无法卖出（低于OKX最小下单量）")
        print("建议：去OKX App使用'小额资产兑换'功能一键清理")


if __name__ == '__main__':
    main()
