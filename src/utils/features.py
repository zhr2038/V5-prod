"""
公共特征计算函数

用于ML模型和DataCollector的特征计算，避免代码重复
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple, Optional


def calculate_price_features(close: pd.Series) -> Dict[str, float]:
    """
    计算价格相关特征
    
    Args:
        close: 收盘价序列
        
    Returns:
        价格特征字典
    """
    if len(close) < 2:
        return {
            'returns_1h': 0.0,
            'returns_6h': 0.0,
            'returns_24h': 0.0,
            'momentum_5d': 0.0,
            'momentum_20d': 0.0,
        }
    
    # 收益率
    returns_1h = close.pct_change(1).iloc[-1] if len(close) > 1 else 0.0
    returns_6h = close.pct_change(6).iloc[-1] if len(close) > 6 else 0.0
    returns_24h = close.pct_change(24).iloc[-1] if len(close) > 24 else 0.0
    
    # 动量
    if len(close) > 5 * 24:
        momentum_5d = (close.iloc[-1] - close.shift(5*24).iloc[-1]) / close.shift(5*24).iloc[-1]
    else:
        momentum_5d = 0.0
        
    if len(close) > 20 * 24:
        momentum_20d = (close.iloc[-1] - close.shift(20*24).iloc[-1]) / close.shift(20*24).iloc[-1]
    else:
        momentum_20d = 0.0
    
    return {
        'returns_1h': returns_1h,
        'returns_6h': returns_6h,
        'returns_24h': returns_24h,
        'momentum_5d': momentum_5d,
        'momentum_20d': momentum_20d,
    }


def calculate_volatility_features(close: pd.Series) -> Dict[str, float]:
    """
    计算波动率特征
    
    Args:
        close: 收盘价序列
        
    Returns:
        波动率特征字典
    """
    if len(close) < 2:
        return {
            'volatility_6h': 0.0,
            'volatility_24h': 0.0,
            'volatility_ratio': 1.0,
        }
    
    returns_series = close.pct_change()
    
    volatility_6h = returns_series.rolling(6).std().iloc[-1] if len(close) > 6 else 0.0
    volatility_24h = returns_series.rolling(24).std().iloc[-1] if len(close) > 24 else 0.0
    
    # 避免除以零
    if volatility_24h > 0 and not pd.isna(volatility_24h):
        volatility_ratio = volatility_6h / volatility_24h
    else:
        volatility_ratio = 1.0
    
    return {
        'volatility_6h': volatility_6h,
        'volatility_24h': volatility_24h,
        'volatility_ratio': volatility_ratio,
    }


def calculate_volume_features(close: pd.Series, volume: pd.Series) -> Dict[str, float]:
    """
    计算成交量特征
    
    Args:
        close: 收盘价序列
        volume: 成交量序列
        
    Returns:
        成交量特征字典
    """
    if len(volume) < 24 or len(close) < 2:
        return {
            'volume_ratio': 1.0,
            'obv': 0.0,
        }
    
    # 成交量比率
    volume_sma = volume.rolling(24).mean()
    volume_sma_val = volume_sma.iloc[-1] if not volume_sma.empty else 0.0
    
    if volume_sma_val > 0 and not pd.isna(volume_sma_val):
        volume_ratio = volume.iloc[-1] / volume_sma_val
    else:
        volume_ratio = 1.0
    
    # OBV (On Balance Volume)
    returns_series = close.pct_change()
    obv_series = (np.sign(returns_series) * volume).cumsum()
    obv = obv_series.iloc[-1] if not obv_series.empty and not pd.isna(obv_series.iloc[-1]) else 0.0
    
    return {
        'volume_ratio': volume_ratio,
        'obv': obv,
    }


def calculate_rsi(close: pd.Series, window: int = 14) -> float:
    """
    计算RSI指标
    
    Args:
        close: 收盘价序列
        window: RSI计算窗口
        
    Returns:
        RSI值 (0-100)，如果无法计算返回50.0（中性）
    """
    if len(close) < window + 1:
        return 50.0
    
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window).mean()
    
    loss_val = loss.iloc[-1]
    gain_val = gain.iloc[-1]
    
    # 避免除以零
    if pd.isna(loss_val) or loss_val == 0:
        return 50.0
    
    rs_val = gain_val / loss_val
    if pd.isna(rs_val):
        return 50.0
    
    rsi = 100 - (100 / (1 + rs_val))
    if pd.isna(rsi):
        return 50.0
    
    return float(rsi)


def calculate_macd(close: pd.Series) -> Tuple[float, float]:
    """
    计算MACD指标
    
    Args:
        close: 收盘价序列
        
    Returns:
        (macd, macd_signal)，如果无法计算返回(0.0, 0.0)
    """
    if len(close) < 26:
        return 0.0, 0.0
    
    exp1 = close.ewm(span=12).mean()
    exp2 = close.ewm(span=26).mean()
    macd_line = exp1 - exp2
    macd_signal_line = macd_line.ewm(span=9).mean()
    
    macd = macd_line.iloc[-1] if not pd.isna(macd_line.iloc[-1]) else 0.0
    macd_signal = macd_signal_line.iloc[-1] if not pd.isna(macd_signal_line.iloc[-1]) else 0.0
    
    return float(macd), float(macd_signal)


def calculate_bollinger_position(close: pd.Series, window: int = 20) -> float:
    """
    计算布林带位置
    
    Args:
        close: 收盘价序列
        window: 布林带窗口
        
    Returns:
        布林带位置 (-1 to 1)，如果无法计算返回0.0（中间位置）
    """
    if len(close) < window:
        return 0.0
    
    bb_middle = close.rolling(window).mean()
    bb_std = close.rolling(window).std()
    
    bb_middle_val = bb_middle.iloc[-1]
    bb_std_val = bb_std.iloc[-1]
    
    if pd.isna(bb_middle_val) or pd.isna(bb_std_val) or bb_std_val <= 0:
        return 0.0
    
    bb_position = (close.iloc[-1] - bb_middle_val) / (2 * bb_std_val)
    if pd.isna(bb_position):
        return 0.0
    
    # 限制在合理范围内
    return float(np.clip(bb_position, -1.0, 1.0))


def calculate_price_position(close: pd.Series, high: pd.Series, low: pd.Series, window: int = 20*24) -> float:
    """
    计算价格位置（在高低点区间中的位置）
    
    Args:
        close: 收盘价序列
        high: 最高价序列
        low: 最低价序列
        window: 窗口大小
        
    Returns:
        价格位置 (0-1)，0=低点，1=高点，如果无法计算返回0.5
    """
    if len(close) < window or len(high) < window or len(low) < window:
        return 0.5
    
    high_window = high.rolling(window).max()
    low_window = low.rolling(window).min()
    
    high_val = high_window.iloc[-1]
    low_val = low_window.iloc[-1]
    
    if pd.isna(high_val) or pd.isna(low_val) or (high_val - low_val) <= 0:
        return 0.5
    
    position = (close.iloc[-1] - low_val) / (high_val - low_val)
    if pd.isna(position):
        return 0.5
    
    # 限制在0-1范围内
    return float(np.clip(position, 0.0, 1.0))


def calculate_all_features(
    close: pd.Series,
    volume: Optional[pd.Series] = None,
    high: Optional[pd.Series] = None,
    low: Optional[pd.Series] = None
) -> Dict[str, float]:
    """
    计算所有特征（统一接口）
    
    Args:
        close: 收盘价序列（必需）
        volume: 成交量序列（可选）
        high: 最高价序列（可选，默认使用close）
        low: 最低价序列（可选，默认使用close）
        
    Returns:
        所有特征的字典
    """
    if volume is None:
        volume = pd.Series([0] * len(close))
    if high is None:
        high = close
    if low is None:
        low = close
    
    features = {}
    
    # 价格特征
    features.update(calculate_price_features(close))
    
    # 波动率特征
    features.update(calculate_volatility_features(close))
    
    # 成交量特征
    features.update(calculate_volume_features(close, volume))
    
    # 技术指标
    features['rsi'] = calculate_rsi(close)
    features['macd'], features['macd_signal'] = calculate_macd(close)
    features['bb_position'] = calculate_bollinger_position(close)
    features['price_position'] = calculate_price_position(close, high, low)
    
    return features
