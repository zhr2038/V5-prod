#!/usr/bin/env python3
"""
Alpha评分方法回测验证
对比：
  A. 当前方法：纯截面Z-Score标准化
  B. 改进方法：Z-Score + 绝对动量保底
"""

import sys
sys.path.insert(0, '/home/admin/clawd/v5-trading-bot')

import json
import math
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import urllib.request
import urllib.error


class AlphaBacktest:
    """Alpha评分方法回测器"""
    
    def __init__(self, symbols: List[str], start_date: str, end_date: str):
        self.symbols = symbols
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.cache = {}
        self.data_limit = 300  # 默认获取300小时数据
        
    def fetch_klines(self, symbol: str, bar: str = '1H', limit: int = None) -> List[dict]:
        """获取K线数据（带缓存）"""
        if limit is None:
            limit = self.data_limit
        cache_key = f"{symbol}_{bar}_{limit}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        try:
            # 使用candles接口（最新数据）而不是history-candles
            url = f"https://www.okx.com/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                candles = data.get('data', [])
                # 转换为列表，从旧到新
                result = []
                for c in reversed(candles):
                    result.append({
                        'ts': c[0],
                        'o': float(c[1]),
                        'h': float(c[2]),
                        'l': float(c[3]),
                        'c': float(c[4]),
                        'vol': float(c[5])
                    })
                self.cache[cache_key] = result
                return result
        except Exception as e:
            print(f"  Error fetching {symbol}: {e}")
            return []
    
    def compute_factors(self, klines: List[dict]) -> Dict[str, float]:
        """计算原始因子"""
        if len(klines) < 121:  # 需要至少5天+的数据
            return {}
        
        closes = [k['c'] for k in klines]
        volumes = [k['vol'] for k in klines]
        
        # f1: 5日动量 (120小时)
        mom_5d = (closes[-1] - closes[-121]) / closes[-121]
        
        # f2: 20日动量 (480小时，但只有200小时数据，用20小时*10)
        # 简化：用最近20小时代表短期趋势
        mom_20d = (closes[-1] - closes[-21]) / closes[-21] if len(closes) >= 21 else mom_5d
        
        # f3: 波动调整收益 (使用20小时数据)
        if len(closes) >= 21:
            rets = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(-20, 0)]
            vol = math.sqrt(sum(r**2 for r in rets) / len(rets)) if rets else 0.001
            vol_adj = mom_20d / vol if vol > 0 else 0
        else:
            vol_adj = 0
        
        return {
            'f1_mom_5d': mom_5d,
            'f2_mom_20d': mom_20d,
            'f3_vol_adj_ret': vol_adj,
        }
    
    def zscore_cross_section(self, values: Dict[str, float]) -> Dict[str, float]:
        """截面Z-Score标准化（当前方法）"""
        if not values:
            return {}
        
        xs = list(values.values())
        mean_val = sum(xs) / len(xs)
        variance = sum((x - mean_val) ** 2 for x in xs) / len(xs)
        std_val = math.sqrt(variance) if variance > 0 else 1e-6
        
        return {k: (v - mean_val) / std_val for k, v in values.items()}
    
    def compute_score_method_a(self, factors: Dict[str, Dict[str, float]]) -> Dict[str, float]:
        """方法A：纯截面Z-Score（当前方法）"""
        if not factors:
            return {}
        
        # 对每个因子做Z-Score
        z_f1 = self.zscore_cross_section({k: v['f1_mom_5d'] for k, v in factors.items()})
        z_f2 = self.zscore_cross_section({k: v['f2_mom_20d'] for k, v in factors.items()})
        
        # 等权合成
        scores = {}
        for sym in factors:
            scores[sym] = 0.5 * z_f1.get(sym, 0) + 0.5 * z_f2.get(sym, 0)
        return scores
    
    def compute_score_method_b(self, factors: Dict[str, Dict[str, float]]) -> Dict[str, float]:
        """方法B：Z-Score + 绝对动量保底（改进方法）"""
        if not factors:
            return {}
        
        # 先计算Z-Score
        z_f1 = self.zscore_cross_section({k: v['f1_mom_5d'] for k, v in factors.items()})
        z_f2 = self.zscore_cross_section({k: v['f2_mom_20d'] for k, v in factors.items()})
        
        scores = {}
        for sym, f in factors.items():
            raw_mom_5d = f['f1_mom_5d']
            raw_mom_20d = f['f2_mom_20d']
            z_score = 0.5 * z_f1.get(sym, 0) + 0.5 * z_f2.get(sym, 0)
            
            # 改进逻辑：正动量保底
            if raw_mom_5d > 0 and raw_mom_20d > 0:
                # 双重正动量，保底0.1
                scores[sym] = max(z_score, 0.1)
            elif raw_mom_5d > 0 or raw_mom_20d > 0:
                # 单重正动量，保底0.05
                scores[sym] = max(z_score, 0.05)
            else:
                # 负动量，保持原Z-Score（可能为负）
                scores[sym] = z_score
        
        return scores
    
    def simulate_portfolio(self, scores: Dict[str, float], next_returns: Dict[str, float], 
                           top_pct: float = 0.15, min_threshold: float = 0.05) -> Tuple[float, int]:
        """
        模拟组合表现
        返回: (组合收益, 选中币数)
        """
        if not scores:
            return 0.0, 0
        
        # 排序并选择
        sorted_items = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        k = max(1, int(len(sorted_items) * top_pct))
        
        # 应用最低阈值
        selected = [s for s, score in sorted_items[:k] if score >= min_threshold]
        
        if not selected:
            return 0.0, 0  # 空仓
        
        # 等权组合收益
        portfolio_return = sum(next_returns.get(s, 0) for s in selected) / len(selected)
        return portfolio_return, len(selected)
    
    def run_backtest(self) -> dict:
        """执行回测"""
        print("="*70)
        print("Alpha评分方法回测对比")
        print("="*70)
        print(f"币池: {len(self.symbols)} 个币")
        print(f"回测期间: {self.start_date.date()} ~ {self.end_date.date()}")
        print()
        
        # 获取数据
        print("正在获取历史数据...")
        all_data = {}
        for sym in self.symbols:
            klines = self.fetch_klines(sym)
            if klines:
                all_data[sym] = klines
            else:
                print(f"  ⚠️  {sym} 无数据")
        
        if len(all_data) < 5:
            print("❌ 数据不足，无法回测")
            return {}
        
        print(f"✅ 成功获取 {len(all_data)} 个币的数据")
        print()
        
        # 模拟每日调仓（使用每24小时的数据点）
        returns_a = []  # 方法A收益
        returns_b = []  # 方法B收益
        positions_a = []  # 方法A持仓数
        positions_b = []  # 方法B持仓数
        
        # 使用最近的200个数据点（约8天）
        max_len = min(len(klines) for klines in all_data.values())
        test_points = max_len - 25  # 留出25小时预测未来
        
        print(f"回测数据点: {test_points} 个")
        print(f"数据长度检查: max_len={max_len}")
        
        # 调试：打印第一个币的数据长度
        first_sym = list(all_data.keys())[0]
        print(f"示例 {first_sym} 数据长度: {len(all_data[first_sym])}")
        print()
        
        for i in range(121, test_points, 24):  # 从121开始（5天数据），每天调仓
            # 计算因子（使用前i个数据点）
            factors = {}
            future_returns = {}
            
            for sym, klines in all_data.items():
                if i + 24 < len(klines):
                    hist = klines[:i]
                    future = klines[i:i+24]  # 未来24小时
                    
                    f = self.compute_factors(hist)
                    if f:
                        factors[sym] = f
                        # 未来24小时收益
                        future_ret = (future[-1]['c'] - future[0]['o']) / future[0]['o']
                        future_returns[sym] = future_ret
            
            if len(factors) < 3:
                if i == 121:  # 只打印一次调试信息
                    print(f"  调试: i={i}, factors={len(factors)}, 跳过")
                continue
            
            if i == 121:
                print(f"  调试: i={i}, factors={len(factors)}, 开始计算")
            
            # 方法A评分
            scores_a = self.compute_score_method_a(factors)
            ret_a, pos_a = self.simulate_portfolio(scores_a, future_returns)
            
            # 方法B评分
            scores_b = self.compute_score_method_b(factors)
            ret_b, pos_b = self.simulate_portfolio(scores_b, future_returns)
            
            # 详细对比输出（第一轮）
            if i == 121:
                print("\n--- 详细对比（第一轮调仓）---")
                print(f"{'币种':<12} {'f1_5d':>8} {'f2_20d':>8} {'A评分':>10} {'B评分':>10} {'选中A':>6} {'选中B':>6}")
                print("-" * 70)
                
                # 排序显示
                sorted_syms = sorted(scores_a.keys(), key=lambda x: scores_a[x], reverse=True)[:10]
                for sym in sorted_syms:
                    f = factors[sym]
                    s_a = scores_a[sym]
                    s_b = scores_b[sym]
                    in_a = sym in [s for s, _ in sorted(scores_a.items(), key=lambda x: x[1], reverse=True)[:max(1, int(len(scores_a)*0.15))]]
                    in_b = sym in [s for s, _ in sorted(scores_b.items(), key=lambda x: x[1], reverse=True)[:max(1, int(len(scores_b)*0.15))]]
                    print(f"{sym:<12} {f['f1_mom_5d']*100:>+7.2f}% {f['f2_mom_20d']*100:>+7.2f}% {s_a:>+10.3f} {s_b:>+10.3f} {'Y' if in_a else '':>6} {'Y' if in_b else '':>6}")
                
                print(f"\n方法A选中: {pos_a} 个币, 方法B选中: {pos_b} 个币")
                print(f"未来24h组合收益: A={ret_a*100:+.2f}%, B={ret_b*100:+.2f}%")
                print()
            
            returns_a.append(ret_a)
            returns_b.append(ret_b)
            positions_a.append(pos_a)
            positions_b.append(pos_b)
            
        print(f"实际回测轮数: {len(returns_a)}")
        print()
        
        # 计算累计收益
        cum_ret_a = 1.0
        cum_ret_b = 1.0
        for r in returns_a:
            cum_ret_a *= (1 + r)
        for r in returns_b:
            cum_ret_b *= (1 + r)
        
        # 统计
        print("-"*70)
        print("回测结果对比")
        print("-"*70)
        
        if not returns_a:
            print("⚠️  无有效回测数据（可能是数据时间范围问题）")
            return {}
        
        print(f"{'指标':<30} {'方法A(当前)':>15} {'方法B(改进)':>15}")
        print("-"*70)
        print(f"{'累计收益':<30} {cum_ret_a-1:>+14.2%} {cum_ret_b-1:>+14.2%}")
        print(f"{'平均持仓数':<30} {sum(positions_a)/len(positions_a):>15.1f} {sum(positions_b)/len(positions_b):>15.1f}")
        print(f"{'空仓次数':<30} {positions_a.count(0):>15} {positions_b.count(0):>15}")
        print(f"{'交易天数':<30} {len(returns_a):>15} {len(returns_b):>15}")
        
        if returns_a:
            avg_ret_a = sum(returns_a) / len(returns_a)
            avg_ret_b = sum(returns_b) / len(returns_b)
            print(f"{'日均收益':<30} {avg_ret_a:>+15.3%} {avg_ret_b:>+15.3%}")
        
        print("-"*70)
        
        # 结论
        print()
        print("="*70)
        print("结论")
        print("="*70)
        
        if cum_ret_b > cum_ret_a:
            improvement = (cum_ret_b - cum_ret_a) / abs(cum_ret_a) * 100 if cum_ret_a != 0 else float('inf')
            print(f"✅ 方法B（改进）更优，收益提升 {improvement:.1f}%")
            print(f"   原因：方法B避免了错过正动量币的机会")
        elif cum_ret_a > cum_ret_b:
            print(f"⚠️  方法A（当前）更优")
            print(f"   原因：截面标准化筛选了相对强弱，避免了'涨得不够'的陷阱")
        else:
            print("➡️  两种方法差异不大")
        
        print()
        print("观察要点：")
        print(f"  • 方法A平均持仓 {sum(positions_a)/len(positions_a):.1f} 个币")
        print(f"  • 方法B平均持仓 {sum(positions_b)/len(positions_b):.1f} 个币")
        print(f"  • 方法B{'更多' if sum(positions_b) > sum(positions_a) else '更少'}交易机会")
        
        return {
            'method_a_return': cum_ret_a - 1,
            'method_b_return': cum_ret_b - 1,
            'positions_a_avg': sum(positions_a) / len(positions_a) if positions_a else 0,
            'positions_b_avg': sum(positions_b) / len(positions_b) if positions_b else 0,
        }


if __name__ == '__main__':
    # 币池（与实盘一致）
    symbols = [
        'BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'BNB-USDT', 'ADA-USDT',
        'AVAX-USDT', 'DOT-USDT', 'UNI-USDT', 'LINK-USDT', 'LTC-USDT',
        'DOGE-USDT', 'XRP-USDT', 'SUI-USDT', 'NEAR-USDT', 'ETC-USDT',
        'POL-USDT', 'AAVE-USDT', 'ATOM-USDT', 'TRX-USDT', 'HYPE-USDT'
    ]
    
    # 回测最近30天
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # 增加数据限制以获取更多历史
    backtest = AlphaBacktest(
        symbols=symbols,
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    )
    
    # 修改获取数据的限制
    backtest.data_limit = 300  # 获取300小时数据
    
    results = backtest.run_backtest()
