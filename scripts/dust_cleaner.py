#!/usr/bin/env python3
"""
V5 粉尘持仓批量清理工具

功能：
- 批量标记粉尘持仓为"不可交易"
- 从仓位计算中排除粉尘
- 生成清理报告
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime

REPORTS_DIR = Path('/home/admin/clawd/v5-trading-bot/reports')
POSITIONS_DB = REPORTS_DIR / 'positions.sqlite'
ORDERS_DB = REPORTS_DIR / 'orders.sqlite'

# 粉尘定义
DUST_CRITERIA = {
    'max_qty': 0.1,        # 数量小于0.1
    'max_value_usdt': 0.5,  # 价值小于$0.5
    'max_price': 0.1        # 单价小于$0.1（针对低价币）
}

DUST_SYMBOLS = {'PROMPT', 'SPACE', 'KITE', 'WLFI', 'MERL', 'J', 'PEPE', 'XAUT'}


class DustCleaner:
    """粉尘清理器"""
    
    def __init__(self):
        self.stats = {'marked': 0, 'already_excluded': 0, 'errors': 0}
        self.dust_list = []
    
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    
    def get_positions(self):
        """获取所有持仓"""
        if not POSITIONS_DB.exists():
            return []
        
        conn = sqlite3.connect(str(POSITIONS_DB))
        cursor = conn.cursor()
        
        cursor.execute("SELECT symbol, qty, avg_px, last_mark_px FROM positions")
        positions = []
        for row in cursor.fetchall():
            symbol = row[0]
            qty = float(row[1] or 0)
            avg_px = float(row[2] or 0)
            last_px = float(row[3] or 0)
            value = qty * last_px if last_px > 0 else qty * avg_px
            
            positions.append({
                'symbol': symbol,
                'qty': qty,
                'avg_px': avg_px,
                'last_px': last_px,
                'value': value
            })
        
        conn.close()
        return positions
    
    def is_dust(self, position):
        """判断是否为粉尘"""
        symbol = position['symbol']
        qty = position['qty']
        value = position['value']
        price = position['last_px'] or position['avg_px']
        
        # 检查是否在粉尘币种列表
        base_symbol = symbol.split('/')[0] if '/' in symbol else symbol.split('-')[0]
        if base_symbol in DUST_SYMBOLS:
            return True, f"在粉尘币种列表中"
        
        # 检查价值
        if value < DUST_CRITERIA['max_value_usdt']:
            return True, f"价值${value:.4f} < ${DUST_CRITERIA['max_value_usdt']}"
        
        # 检查数量
        if qty < DUST_CRITERIA['max_qty'] and price < DUST_CRITERIA['max_price']:
            return True, f"数量{qty:.6f} < {DUST_CRITERIA['max_qty']} 且单价${price:.4f} < ${DUST_CRITERIA['max_price']}"
        
        return False, None
    
    def add_dust_tags(self):
        """为粉尘持仓添加标签"""
        if not POSITIONS_DB.exists():
            self.log("❌ positions.sqlite不存在")
            return
        
        conn = sqlite3.connect(str(POSITIONS_DB))
        cursor = conn.cursor()
        
        # 检查是否有tags_json列
        cursor.execute("PRAGMA table_info(positions)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'tags_json' not in columns:
            self.log("添加tags_json列...")
            cursor.execute("ALTER TABLE positions ADD COLUMN tags_json TEXT DEFAULT '{}'")
            conn.commit()
        
        # 获取所有持仓
        positions = self.get_positions()
        
        self.log("=" * 60)
        self.log("🔍 扫描粉尘持仓...")
        self.log("=" * 60)
        
        for pos in positions:
            is_dust, reason = self.is_dust(pos)
            
            if is_dust:
                self.dust_list.append({
                    'symbol': pos['symbol'],
                    'qty': pos['qty'],
                    'value': pos['value'],
                    'reason': reason
                })
                
                # 更新tags_json
                tags = {'dust': True, 'dust_reason': reason, 'dust_marked_at': datetime.now().isoformat()}
                cursor.execute(
                    "UPDATE positions SET tags_json = ? WHERE symbol = ?",
                    (json.dumps(tags), pos['symbol'])
                )
                self.stats['marked'] += 1
                self.log(f"🏷️  {pos['symbol']}: {reason}")
        
        conn.commit()
        conn.close()
        
        self.log("=" * 60)
        self.log(f"✅ 已标记 {self.stats['marked']} 个粉尘持仓")
        self.log("=" * 60)
    
    def update_reconcile_config(self):
        """更新对账配置，排除粉尘币种"""
        config_file = REPORTS_DIR / 'dust_config.json'
        
        config = {
            'dust_symbols': list(DUST_SYMBOLS),
            'dust_criteria': DUST_CRITERIA,
            'excluded_from_equity': True,
            'excluded_from_rebalance': True,
            'excluded_from_borrow_check': True,
            'updated_at': datetime.now().isoformat()
        }
        
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        self.log(f"💾 粉尘配置已保存: {config_file}")
    
    def generate_report(self):
        """生成清理报告"""
        report_file = REPORTS_DIR / f'dust_cleanup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'dust_criteria': DUST_CRITERIA,
            'dust_symbols': list(DUST_SYMBOLS),
            'marked_positions': self.dust_list,
            'stats': self.stats
        }
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        self.log(f"📄 报告已保存: {report_file}")
        return report
    
    def print_summary(self):
        """打印摘要"""
        print("\n" + "=" * 60)
        print("🧹 粉尘清理摘要")
        print("=" * 60)
        print(f"粉尘标准:")
        print(f"  - 价值 < ${DUST_CRITERIA['max_value_usdt']}")
        print(f"  - 或数量 < {DUST_CRITERIA['max_qty']} 且单价 < ${DUST_CRITERIA['max_price']}")
        print(f"  - 或在粉尘币种列表: {', '.join(DUST_SYMBOLS)}")
        print()
        print(f"已标记持仓: {self.stats['marked']} 个")
        
        if self.dust_list:
            print("\n粉尘列表:")
            for d in self.dust_list:
                print(f"  {d['symbol']:12} {d['qty']:12.6f} ${d['value']:8.4f} - {d['reason']}")
        
        print("=" * 60)
    
    def run(self):
        """运行清理流程"""
        self.log("🚀 粉尘清理开始")
        
        # 1. 添加粉尘标签
        self.add_dust_tags()
        
        # 2. 更新配置
        self.update_reconcile_config()
        
        # 3. 生成报告
        self.generate_report()
        
        # 4. 打印摘要
        self.print_summary()
        
        self.log("✅ 粉尘清理完成")
        self.log("⚠️  注意：这不会删除粉尘，只是标记为不可交易")
        self.log("   如需彻底清理，需要手动在OKX卖出或忽略")


def main():
    cleaner = DustCleaner()
    cleaner.run()


if __name__ == '__main__':
    main()
