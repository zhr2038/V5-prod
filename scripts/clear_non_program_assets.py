#!/usr/bin/env python3
"""
Clear non-program assets from OKX spot account.

Rules:
- Keep allowlist symbols (from config.symbols + common core)
- Sell only assets with positive cashBal and eqUsd >= min_value_usd
- Use market sell in cash mode
"""
from __future__ import print_function

import os
import json
import time
import hmac
import base64
import hashlib
from typing import Dict, List, Set

import requests
from dotenv import load_dotenv

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')
from configs.loader import load_config


def sign(ts: str, method: str, path: str, body: str = "") -> str:
    secret = os.getenv('EXCHANGE_API_SECRET', '')
    msg = f"{ts}{method.upper()}{path}{body}".encode('utf-8')
    mac = hmac.new(secret.encode('utf-8'), msg, hashlib.sha256).digest()
    return base64.b64encode(mac).decode('utf-8')


def okx_request(method: str, path: str, body: Dict = None):
    ts = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + 'Z'
    body_json = json.dumps(body, separators=(',', ':')) if body else ""
    headers = {
        'OK-ACCESS-KEY': os.getenv('EXCHANGE_API_KEY', ''),
        'OK-ACCESS-SIGN': sign(ts, method, path, body_json),
        'OK-ACCESS-TIMESTAMP': ts,
        'OK-ACCESS-PASSPHRASE': os.getenv('EXCHANGE_PASSPHRASE', ''),
        'Content-Type': 'application/json',
    }
    url = f"https://www.okx.com{path}"
    if method.upper() == 'GET':
        r = requests.get(url, headers=headers, timeout=12)
    else:
        r = requests.post(url, headers=headers, data=body_json, timeout=12)
    return r.json()


def build_allowlist() -> Set[str]:
    cfg = load_config('configs/live_20u_real.yaml', env_path='/home/admin/clawd/v5-trading-bot/.env')
    keep = set(getattr(cfg, 'symbols', []) or [])
    keep |= {'BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'DOGE/USDT', 'BNB/USDT'}
    return keep


def main():
    load_dotenv('/home/admin/clawd/v5-trading-bot/.env')
    allowlist = build_allowlist()
    min_value_usd = 0.5

    bal = okx_request('GET', '/api/v5/account/balance')
    if bal.get('code') != '0':
        print(f"[ERROR] balance failed: {bal}")
        return 1

    details = bal.get('data', [{}])[0].get('details', [])
    candidates: List[Dict] = []

    for d in details:
        ccy = str(d.get('ccy') or '').upper()
        if not ccy or ccy == 'USDT':
            continue
        sym = f"{ccy}/USDT"
        if sym in allowlist:
            continue

        try:
            cash_bal = float(d.get('cashBal') or 0)
            eq_usd = float(d.get('eqUsd') or 0)
        except Exception:
            continue

        if cash_bal <= 0:
            continue
        if eq_usd < min_value_usd:
            continue

        candidates.append({
            'ccy': ccy,
            'symbol': sym,
            'cash_bal': cash_bal,
            'eq_usd': eq_usd,
        })

    if not candidates:
        print('[OK] no non-program assets above threshold')
        return 0

    print('[INFO] will clear non-program assets:')
    for c in candidates:
        print(f"  - {c['symbol']} qty={c['cash_bal']:.8f} eqUsd={c['eq_usd']:.4f}")

    for c in candidates:
        inst = f"{c['ccy']}-USDT"
        qty = max(0.0, c['cash_bal'] * 0.999)
        sz = f"{qty:.8f}".rstrip('0').rstrip('.')
        if not sz or float(sz) <= 0:
            continue

        payload = {
            'instId': inst,
            'tdMode': 'cash',
            'side': 'sell',
            'ordType': 'market',
            'sz': sz,
        }
        res = okx_request('POST', '/api/v5/trade/order', payload)
        if res.get('code') == '0':
            ord_id = (res.get('data') or [{}])[0].get('ordId')
            print(f"[SELL] {inst} sz={sz} ordId={ord_id}")
        else:
            print(f"[WARN] sell failed {inst} sz={sz}: {res}")

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
