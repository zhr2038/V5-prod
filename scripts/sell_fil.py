#!/usr/bin/env python3
"""卖出FIL持仓"""

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

load_dotenv('/home/admin/clawd/v5-trading-bot/.env')

def sign(timestamp, method, path, body=""):
    message = timestamp + method.upper() + path + body
    mac = hmac.new(
        os.getenv('EXCHANGE_API_SECRET').encode('utf-8'),
        message.encode('utf-8'),
        hashlib.sha256
    )
    return base64.b64encode(mac.digest()).decode('utf-8')

def okx_request(method, path, body=None):
    timestamp = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'
    body_json = json.dumps(body) if body else ""
    
    headers = {
        'OK-ACCESS-KEY': os.getenv('EXCHANGE_API_KEY'),
        'OK-ACCESS-SIGN': sign(timestamp, method, path, body_json),
        'OK-ACCESS-TIMESTAMP': timestamp,
        'OK-ACCESS-PASSPHRASE': os.getenv('EXCHANGE_PASSPHRASE'),
        'Content-Type': 'application/json'
    }
    
    url = f"https://www.okx.com{path}"
    if method.upper() == 'GET':
        resp = requests.get(url, headers=headers, timeout=10)
    else:
        resp = requests.post(url, headers=headers, data=body_json, timeout=10)
    
    return resp.json()

# 获取FIL余额
print("[1] 查询FIL余额...")
data = okx_request('GET', '/api/v5/account/balance')

fil_avail = 0
if data.get('code') == '0':
    for detail in data['data'][0].get('details', []):
        if detail.get('ccy') == 'FIL':
            fil_avail = float(detail.get('availBal', 0))
            print(f"  FIL可用: {fil_avail}")
            break

if fil_avail <= 0:
    print("[ERROR] 没有FIL可卖")
    exit(1)

# 下市价卖单
print(f"\n[2] 市价卖出 {fil_avail} FIL...")
order = {
    "instId": "FIL-USDT",
    "tdMode": "cash",
    "side": "sell",
    "ordType": "market",
    "sz": str(fil_avail)
}

result = okx_request('POST', '/api/v5/trade/order', order)

if result.get('code') == '0':
    ord_id = result['data'][0].get('ordId')
    print(f"  [OK] 订单已提交, ID={ord_id}")
else:
    print(f"  [FAIL] {result.get('msg')}")
