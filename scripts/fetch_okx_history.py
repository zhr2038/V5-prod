#!/usr/bin/env python3
"""
从OKX获取历史K线数据用于HMM训练

OKX API限制:
- 最多300条/请求
- 1小时K线，300条 = 12.5天
- 需要分页获取1年数据 (~30次请求)
"""

import requests
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3

def get_okx_candles(inst_id: str, bar: str = "1H", limit: int = 300, after: str = None):
    """获取OKX K线数据"""
    url = f"https://www.okx.com/api/v5/market/history-candles"
    params = {
        "instId": inst_id,
        "bar": bar,
        "limit": limit
    }
    if after:
        params["after"] = after  # 获取比这个时间更早的数据
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        if data.get("code") == "0":
            return data.get("data", [])
    except Exception as e:
        print(f"Error fetching {inst_id}: {e}")
    return []

def fetch_year_data(symbol: str = "BTC-USDT"):
    """获取1年数据"""
    print(f"[OKX] 开始获取 {symbol} 1年历史数据...")
    
    all_data = []
    inst_id = symbol.replace("/", "-")
    
    # 从当前时间往前推1年
    end_ts = int(datetime.now().timestamp() * 1000)
    
    # 需要获取约30次（每次300条 ≈ 12.5天）
    for i in range(30):
        print(f"  批次 {i+1}/30...")
        
        candles = get_okx_candles(inst_id, after=str(end_ts) if all_data else None)
        
        if not candles:
            break
        
        all_data.extend(candles)
        
        # 更新end_ts为最早一条数据的时间
        end_ts = int(candles[-1][0])  # 时间戳在第一个字段
        
        # 检查是否已经超过1年
        earliest = datetime.fromtimestamp(int(candles[-1][0]) / 1000)
        if datetime.now() - earliest > timedelta(days=365):
            print(f"  已获取超过1年数据，最早: {earliest}")
            break
        
        time.sleep(0.5)  # 避免 rate limit
    
    print(f"[OKX] 总共获取 {len(all_data)} 条K线数据")
    return all_data

def convert_to_features(candles: list):
    """将K线转换为HMM特征"""
    # candles格式: [ts, open, high, low, close, vol, volCcy]
    features = []
    
    closes = [float(c[4]) for c in candles if c[4]]
    
    for i in range(len(closes)):
        if i < 20:
            continue
        
        # 计算动量特征
        ret_1h = (closes[i] - closes[i-1]) / closes[i-1] if closes[i-1] > 0 else 0
        ret_6h = (closes[i] - closes[max(0,i-6)]) / closes[max(0,i-6)] if closes[max(0,i-6)] > 0 else 0
        
        # 波动率
        window = closes[max(0,i-14):i+1]
        vol = 0
        if len(window) > 1:
            rets = [(window[j] - window[j-1]) / window[j-1] for j in range(1, len(window))]
            vol = sum(r**2 for r in rets) / len(rets)
        
        # RSI
        gains = [closes[j]-closes[j-1] for j in range(max(0,i-14), i+1) if closes[j] > closes[j-1]]
        losses = [closes[j-1]-closes[j] for j in range(max(0,i-14), i+1) if closes[j] < closes[j-1]]
        avg_gain = sum(gains) / len(gains) if gains else 0
        avg_loss = sum(losses) / len(losses) if losses else 0.001
        rsi = 100 - (100 / (1 + avg_gain/avg_loss))
        
        features.append([ret_1h, ret_6h, vol**0.5, rsi])
    
    return features

if __name__ == "__main__":
    import numpy as np
    
    # 获取BTC 1年数据
    candles = fetch_year_data("BTC-USDT")
    
    if len(candles) < 100:
        print("数据不足，无法训练")
        exit(1)
    
    # 转换为特征
    features = convert_to_features(candles)
    X = np.array(features)
    
    print(f"[HMM] 特征矩阵: {X.shape}")
    print(f"  mom_5d范围: [{X[:,0].min():.5f}, {X[:,0].max():.5f}]")
    print(f"  mom_20d范围: [{X[:,1].min():.5f}, {X[:,1].max():.5f}]")
    print(f"  vol范围: [{X[:,2].min():.5f}, {X[:,2].max():.5f}]")
    print(f"  RSI范围: [{X[:,3].min():.1f}, {X[:,3].max():.1f}]")
    
    # 保存特征数据
    np.save("/home/admin/clawd/v5-trading-bot/data/hmm_training_features.npy", X)
    print("[HMM] 特征数据已保存到 data/hmm_training_features.npy")
    
    # 训练HMM
    print("[HMM] 开始训练...")
    from src.regime.hmm_regime_detector import HMMRegimeDetector
    
    detector = HMMRegimeDetector()
    detector.model.fit(X)
    
    print(f"[HMM] 训练完成，收敛: {detector.model.converged}")
    print("\n状态特征:")
    for i in range(3):
        mean = detector.model.means_[i]
        print(f"  State {i}: mom_5d={mean[0]:.5f}, mom_20d={mean[1]:.5f}, vol={mean[2]:.5f}, RSI={mean[3]:.1f}")
    
    # 保存模型
    detector.model.save(detector.model_path)
    print(f"[HMM] 模型已保存到 {detector.model_path}")
