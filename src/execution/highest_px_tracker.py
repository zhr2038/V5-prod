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

PROJECT_ROOT = Path(__file__).resolve().parents[2]


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
        self.state_path = self._resolve_state_path(state_path)
        self.records: Dict[str, HighestPriceRecord] = {}
        self._load()

    @staticmethod
    def _resolve_state_path(state_path: str | Path) -> Path:
        path = Path(state_path)
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
        return path
    
    def _load(self):
        """加载状态"""
        self.records = {}
        if self.state_path.exists():
            try:
                with open(self.state_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if not isinstance(data, dict):
                        return
                    for sym, rec in data.items():
                        if not isinstance(rec, dict):
                            continue
                        try:
                            self.records[str(sym)] = HighestPriceRecord(**rec)
                        except Exception:
                            continue
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
        """更新峰值价格（默认取最大；new_position 时强制重置为入场价附近）"""
        symbol = str(symbol)
        existing = self.records.get(symbol)

        # 根治：新开仓必须重置峰值，避免继承历史污染导致atr_trailing误触发
        if source in {"new_position", "reopen_position", "reset"}:
            new_high = float(highest_px)
        elif existing:
            # 常规路径保留更大的值
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
        self._load()
        symbol = str(symbol)
        if symbol in self.records:
            del self.records[symbol]
            self._save()
    
    def list_all(self) -> Dict[str, HighestPriceRecord]:
        """获取所有记录"""
        return dict(self.records)


def derive_tracker_state_path(position_store_path: str | Path) -> Path:
    """Derive a tracker file path from a positions DB path.

    Keep the legacy default path for the primary positions DB, but isolate any
    alternate stores such as shadow or test DBs so they do not share tracker
    state accidentally.
    """
    db_path = Path(position_store_path)
    stem = db_path.stem
    if stem == "positions":
        return db_path.with_name("highest_px_state.json")
    return db_path.with_name(f"{stem}_highest_px_state.json")


# 全局实例（按 state_path 隔离，避免 shadow/test 仓位库串到同一个 tracker）
_tracker_instances: Dict[str, HighestPriceTracker] = {}


def get_highest_price_tracker(state_path: str | Path = "reports/highest_px_state.json") -> HighestPriceTracker:
    """获取全局追踪器实例"""
    normalized = str(Path(state_path))
    tracker = _tracker_instances.get(normalized)
    if tracker is None:
        tracker = HighestPriceTracker(normalized)
        _tracker_instances[normalized] = tracker
    return tracker


if __name__ == '__main__':
    # 测试
    tracker = HighestPriceTracker()
    tracker.update("BTC/USDT", 70000.0, 65000.0)
    tracker.update("BTC/USDT", 69000.0, 65000.0)  # 应该保留70000
    
    print(f"BTC highest: {tracker.get_highest_px('BTC/USDT')}")  # 应该输出 70000.0
    print(f"All records: {tracker.list_all()}")
