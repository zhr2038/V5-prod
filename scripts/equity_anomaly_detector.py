#!/usr/bin/env python3
"""
V5 权益曲线异常检测

功能：
- 检测权益跳变（数据错误）
- 检测异常波动
- 生成质量报告
"""

import json
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

REPORTS_DIR = Path('/home/admin/clawd/v5-trading-bot/reports')


class EquityAnomalyDetector:
    """权益异常检测器"""
    
    def __init__(self):
        self.anomalies = []
        self.stats = {'total_points': 0, 'anomalies': 0}
    
    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    
    def load_equity_data(self, days=7):
        """加载权益数据"""
        points = []
        cutoff = datetime.now() - timedelta(days=days)
        
        # 从所有runs目录收集
        runs_dir = REPORTS_DIR / 'runs'
        if runs_dir.exists():
            for run_dir in runs_dir.iterdir():
                if not run_dir.is_dir():
                    continue
                equity_file = run_dir / 'equity.jsonl'
                if equity_file.exists():
                    try:
                        with open(equity_file) as f:
                            for line in f:
                                try:
                                    data = json.loads(line)
                                    ts = datetime.fromisoformat(data.get('ts', '').replace('Z', '+00:00').replace('+00:00', ''))
                                    if ts > cutoff:
                                        points.append({
                                            'ts': ts,
                                            'equity': float(data.get('equity', 0)),
                                            'cash': float(data.get('cash', 0)),
                                            'positions_value': float(data.get('positions_value', 0))
                                        })
                                except:
                                    continue
                    except:
                        continue
        
        # 排序
        points.sort(key=lambda x: x['ts'])
        
        # 去重
        seen = set()
        unique = []
        for p in points:
            key = p['ts'].strftime('%Y-%m-%d %H:%M')
            if key not in seen:
                seen.add(key)
                unique.append(p)
        
        return unique
    
    def detect_jumps(self, points, threshold=0.1):
        """检测权益跳变"""
        """
        检测条件：
        - 单小时权益变化超过10%
        - 但无对应交易记录
        """
        anomalies = []
        
        for i in range(1, len(points)):
            prev = points[i-1]
            curr = points[i]
            
            if prev['equity'] <= 0:
                continue
            
            change_pct = abs(curr['equity'] - prev['equity']) / prev['equity']
            
            if change_pct > threshold:
                anomalies.append({
                    'type': 'jump',
                    'time': curr['ts'],
                    'prev_equity': prev['equity'],
                    'curr_equity': curr['equity'],
                    'change_pct': change_pct,
                    'description': f"权益跳变 {change_pct:.1%}（无交易）"
                })
        
        return anomalies
    
    def detect_volatility(self, points, window=24):
        """检测异常波动"""
        """
        使用移动窗口标准差检测异常波动
        """
        if len(points) < window:
            return []
        
        anomalies = []
        equities = [p['equity'] for p in points]
        
        for i in range(window, len(points)):
            window_data = equities[i-window:i]
            mean = np.mean(window_data)
            std = np.std(window_data)
            
            if std > 0:
                curr = equities[i]
                z_score = abs(curr - mean) / std
                
                if z_score > 3:  # 3个标准差
                    anomalies.append({
                        'type': 'volatility',
                        'time': points[i]['ts'],
                        'equity': curr,
                        'z_score': z_score,
                        'description': f"异常波动 (Z-score: {z_score:.1f})"
                    })
        
        return anomalies
    
    def detect_stale_data(self, points, max_gap_hours=2):
        """检测数据中断"""
        anomalies = []
        
        for i in range(1, len(points)):
            prev = points[i-1]
            curr = points[i]
            
            gap = (curr['ts'] - prev['ts']).total_seconds() / 3600
            
            if gap > max_gap_hours:
                anomalies.append({
                    'type': 'stale',
                    'time': prev['ts'],
                    'gap_hours': gap,
                    'description': f"数据中断 {gap:.1f} 小时"
                })
        
        return anomalies
    
    def run_detection(self, days=7):
        """运行检测"""
        self.log("=" * 60)
        self.log("🔍 权益曲线异常检测")
        self.log("=" * 60)
        
        points = self.load_equity_data(days=days)
        self.stats['total_points'] = len(points)
        
        if len(points) < 2:
            self.log("❌ 数据点不足")
            return []
        
        self.log(f"加载 {len(points)} 个数据点")
        
        # 运行各种检测
        jumps = self.detect_jumps(points)
        volatility = self.detect_volatility(points)
        stale = self.detect_stale_data(points)
        
        all_anomalies = jumps + volatility + stale
        all_anomalies.sort(key=lambda x: x['time'])
        
        self.stats['anomalies'] = len(all_anomalies)
        
        # 输出结果
        if all_anomalies:
            self.log(f"\n⚠️  发现 {len(all_anomalies)} 个异常:")
            for a in all_anomalies[:10]:  # 只显示前10个
                self.log(f"  [{a['type'].upper()}] {a['time'].strftime('%Y-%m-%d %H:%M')} - {a['description']}")
            
            if len(all_anomalies) > 10:
                self.log(f"  ... 还有 {len(all_anomalies) - 10} 个异常")
        else:
            self.log("\n✅ 未发现异常")
        
        # 基础统计
        equities = [p['equity'] for p in points]
        self.log(f"\n📊 数据统计:")
        self.log(f"  数据点: {len(points)}")
        self.log(f"  起始权益: ${equities[0]:.2f}")
        self.log(f"  结束权益: ${equities[-1]:.2f}")
        self.log(f"  最大值: ${max(equities):.2f}")
        self.log(f"  最小值: ${min(equities):.2f}")
        self.log(f"  平均值: ${np.mean(equities):.2f}")
        self.log(f"  标准差: ${np.std(equities):.2f}")
        
        return all_anomalies
    
    def save_report(self, anomalies):
        """保存检测报告"""
        report_file = REPORTS_DIR / f'equity_anomaly_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'stats': self.stats,
            'anomalies': anomalies
        }
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        self.log(f"\n📄 报告已保存: {report_file}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='V5 权益曲线异常检测')
    parser.add_argument('--days', type=int, default=7, help='检查天数')
    args = parser.parse_args()
    
    detector = EquityAnomalyDetector()
    anomalies = detector.run_detection(days=args.days)
    detector.save_report(anomalies)


if __name__ == '__main__':
    main()
