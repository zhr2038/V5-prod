#!/usr/bin/env python3
"""
Highest Price Tracker - 峰值价格持久化

防止 highest_px 在账本重建时被重置
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional


@dataclass
class HighestPriceRecord:
    """峰值价格记录"""
    symbol: str
    highest_px: float
    entry_px: float
    updated_at: str
    source: str = "trade"  # trade | bootstrap | manual


class HighestPriceTracker:
    """
    峰值价格持久化追踪器
    
    功能:
    1. 独立于 positions.sqlite 存储 highest_px
    2. 账本重建时自动恢复
    3. 支持合并（取最大）
    """
    
    def __init__(self, state_path: str = "reports/highest_px_state.json"):
        self.state_path = Path(state_path)
        self.records: Dict[str, HighestPriceRecord] = {}
        self._load()
    
    def _load(self):
        """加载状态"""
        if self.state_path.exists():
            try:
                with open(self.state_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for sym, rec in data.items():
                        self.records[sym] = HighestPriceRecord(**rec)
            except Exception as e:
                print(f"[HighestPriceTracker] 加载失败: {e}")
    
    def _save(self):
        """保存状态"""
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            data = {sym: asdict(rec) for sym, rec in self.records.items()}
            with open(self.state_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"[HighestPriceTracker] 保存失败: {e}")
    
    def update(self, symbol: str, highest_px: float, entry_px: float, source: str = "trade"):
        """更新峰值价格（取最大）"""
        symbol = str(symbol)
        existing = self.records.get(symbol)
        
        if existing:
            # 保留更大的值
            new_high = max(float(highest_px), float(existing.highest_px))
        else:
            new_high = float(highest_px)
        
        self.records[symbol] = HighestPriceRecord(
            symbol=symbol,
            highest_px=new_high,
            entry_px=float(entry_px),
            updated_at=datetime.now().isoformat(),
            source=source
        )
        self._save()
    
    def get(self, symbol: str) -> Optional[HighestPriceRecord]:
        """获取峰值价格记录"""
        return self.records.get(str(symbol))
    
    def get_highest_px(self, symbol: str, default: float = 0.0) -> float:
        """获取峰值价格（带默认值）"""
        rec = self.records.get(str(symbol))
        return float(rec.highest_px) if rec else float(default)
    
    def merge_from_positions(self, positions: list):
        """从 positions 列表合并峰值价格"""
        for p in positions:
            try:
                sym = getattr(p, 'symbol', None)
                high = getattr(p, 'highest_px', 0.0)
                entry = getattr(p, 'avg_px', 0.0)
                if sym and high:
                    self.update(sym, high, entry, source="positions_import")
            except Exception:
                continue
        self._save()
    
    def apply_to_position(self, position) -> bool:
        """将持久化的 highest_px 应用到 position 对象"""
        try:
            sym = getattr(position, 'symbol', None)
            if not sym:
                return False
            
            rec = self.records.get(sym)
            if rec and rec.highest_px > 0:
                current_high = float(getattr(position, 'highest_px', 0.0) or 0.0)
                # 使用更大的值
                if rec.highest_px > current_high:
                    position.highest_px = rec.highest_px
                    return True
        except Exception:
            pass
        return False
    
    def clear_symbol(self, symbol: str):
        """清除指定币种的记录（清仓后调用）"""
        if str(symbol) in self.records:
            del self.records[str(symbol)]
            self._save()
    
    def list_all(self) -> Dict[str, HighestPriceRecord]:
        """获取所有记录"""
        return dict(self.records)


# 全局实例
_tracker_instance: Optional[HighestPriceTracker] = None

def get_highest_price_tracker() -> HighestPriceTracker:
    """获取全局追踪器实例"""
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = HighestPriceTracker()
    return _tracker_instance


if __name__ == '__main__':
    # 测试
    tracker = HighestPriceTracker()
    tracker.update("BTC/USDT", 70000.0, 65000.0)
    tracker.update("BTC/USDT", 69000.0, 65000.0)  # 应该保留70000
    
    print(f"BTC highest: {tracker.get_highest_px('BTC/USDT')}")  # 应该输出 70000.0
    print(f"All records: {tracker.list_all()}")
