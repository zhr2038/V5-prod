"""
IC-based动态权重配置

核心原则: 权重 ∝ |IC| (IC绝对值)

IC解释:
- IC = 因子与下期收益的相关系数
- IC > 0: 因子正向预测 (因子高→收益高)
- IC < 0: 因子反向预测 (因子高→收益低)
- |IC| 越大: 预测能力越强 → 权重越高
"""

import json
import numpy as np
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class ICBasedWeightCalculator:
    """
    基于IC的动态权重计算器
    """
    
    def __init__(self, ic_file_path: str = 'reports/ic_diagnostics_30d_20u.json'):
        self.ic_file = self._resolve_ic_file_path(ic_file_path)
        self.factors = {}
        self.load_ic_data()

    @staticmethod
    def _resolve_ic_file_path(ic_file_path: str | Path) -> Path:
        path = Path(ic_file_path)
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
        return path
    
    def load_ic_data(self):
        """加载IC诊断数据"""
        if not self.ic_file.exists():
            print("[IC-Weight] IC诊断文件不存在，使用默认权重")
            return
        
        try:
            with open(self.ic_file, 'r') as f:
                data = json.load(f)
            
            ic_data = data.get('overall_tradable', {}).get('ic', {})
            
            for name, values in ic_data.items():
                ic = values.get('mean', 0)
                # 计算IR (信息比率)
                p25 = values.get('p25', 0)
                p75 = values.get('p75', 0)
                std = (p75 - p25) / 1.35 if p75 != p25 else 0.1
                ir = ic / std if std > 0 else 0
                
                self.factors[name] = {
                    'ic': ic,
                    'ir': ir,
                    'std': std,
                    'count': values.get('count', 0)
                }
            
            print(f"[IC-Weight] 加载了 {len(self.factors)} 个因子的IC数据")
            
        except Exception as e:
            print(f"[IC-Weight] 加载IC数据失败: {e}")
    
    def calculate_weights(self, method: str = 'ic_abs') -> Dict[str, float]:
        """
        计算IC-based权重
        
        Args:
            method: 'ic_abs' | 'ic_squared' | 'ir_based'
        
        Returns:
            各因子权重 dict
        """
        if not self.factors:
            # 无IC数据时使用默认等权重
            return self._default_weights()
        
        weights = {}
        
        if method == 'ic_abs':
            # 方法1: 权重 ∝ |IC|
            for name, data in self.factors.items():
                weights[name] = abs(data['ic'])
        
        elif method == 'ic_squared':
            # 方法2: 权重 ∝ IC² (强调强因子)
            for name, data in self.factors.items():
                weights[name] = data['ic'] ** 2
        
        elif method == 'ir_based':
            # 方法3: 权重 ∝ |IR| (风险调整后)
            for name, data in self.factors.items():
                weights[name] = abs(data['ir'])
        
        # 归一化到总和=1
        total = sum(weights.values())
        if total > 0:
            weights = {k: v/total for k, v in weights.items()}
        
        return weights
    
    def get_factor_direction(self) -> Dict[str, int]:
        """
        获取因子方向 (正向=1, 反向=-1)
        
        IC < 0 的因子需要反向使用
        """
        directions = {}
        for name, data in self.factors.items():
            ic = data['ic']
            if ic >= 0:
                directions[name] = 1  # 正向使用
            else:
                directions[name] = -1  # 反向使用
        return directions
    
    def apply_weights_with_direction(self, factor_values: Dict[str, float]) -> float:
        """
        应用IC-based权重计算综合得分
        
        Args:
            factor_values: {factor_name: factor_value}
        
        Returns:
            加权综合得分
        """
        weights = self.calculate_weights()
        directions = self.get_factor_direction()
        
        score = 0
        total_weight = 0
        
        for name, value in factor_values.items():
            if name in weights:
                # 权重 × 方向 × 因子值
                adjusted_value = directions.get(name, 1) * value
                score += weights[name] * adjusted_value
                total_weight += weights[name]
        
        # 归一化
        if total_weight > 0:
            score /= total_weight
        
        return score
    
    def generate_config(self) -> Dict:
        """
        生成IC-based权重配置
        """
        weights = self.calculate_weights()
        directions = self.get_factor_direction()
        
        config = {
            'scheme': 'ic_based',
            'last_updated': str(self.ic_file.stat().st_mtime) if self.ic_file.exists() else None,
            'factors': {}
        }
        
        for name, weight in weights.items():
            ic = self.factors[name]['ic']
            ir = self.factors[name]['ir']
            
            # 评价
            if abs(ic) > 0.03:
                quality = 'strong'
            elif abs(ic) > 0.01:
                quality = 'medium'
            else:
                quality = 'weak'
            
            config['factors'][name] = {
                'weight': round(weight, 4),
                'ic': round(ic, 4),
                'ir': round(ir, 4),
                'direction': directions[name],
                'direction_label': '正向' if directions[name] == 1 else '反向',
                'quality': quality,
                'note': self._get_factor_note(name, ic, quality)
            }
        
        return config
    
    def _get_factor_note(self, name: str, ic: float, quality: str) -> str:
        """生成因子说明"""
        notes = {
            'f1_mom_5d': '短期动量',
            'f2_mom_20d': '中期动量',
            'f3_vol_adj_ret_20d': '波动率调整收益',
            'f4_volume_expansion': '成交量扩张',
            'f5_rsi_trend_confirm': 'RSI趋势确认',
            'f6_sentiment': '情绪分析'
        }
        
        base_note = notes.get(name, name)
        
        if quality == 'strong':
            if ic > 0:
                return f"{base_note} - 强预测力(正向)"
            else:
                return f"{base_note} - 强预测力(需反向)"
        elif quality == 'medium':
            if ic > 0:
                return f"{base_note} - 中等预测力"
            else:
                return f"{base_note} - 中等预测力(反向)"
        else:
            return f"{base_note} - 预测力弱"
    
    def _default_weights(self) -> Dict[str, float]:
        """默认等权重"""
        return {
            'f1_mom_5d': 0.15,
            'f2_mom_20d': 0.25,
            'f3_vol_adj_ret_20d': 0.15,
            'f4_volume_expansion': 0.15,
            'f5_rsi_trend_confirm': 0.15,
            'f6_sentiment': 0.15
        }
    
    def print_report(self):
        """打印IC权重报告"""
        print("="*70)
        print("IC-based动态权重配置")
        print("="*70)
        
        config = self.generate_config()
        
        print(f"\n配置方案: {config['scheme']}")
        print(f"更新时间: {config['last_updated']}")
        print()
        
        # 按权重排序
        sorted_factors = sorted(
            config['factors'].items(),
            key=lambda x: x[1]['weight'],
            reverse=True
        )
        
        print(f"{'因子':<25} {'权重':<10} {'IC':<10} {'方向':<10} {'评价':<15}")
        print("-"*70)
        
        for name, data in sorted_factors:
            print(f"{name:<25} {data['weight']:>8.2%} {data['ic']:>+8.4f} "
                  f"{data['direction_label']:<10} {data['quality']:<15}")
        
        print("\n" + "="*70)
        print("使用说明:")
        print("  1. 权重基于|IC|自动计算")
        print("  2. IC<0的因子自动反向使用")
        print("  3. 建议每周更新一次IC数据")
        print("  4. 熊市期间IC普遍较低，属于正常现象")
        print("="*70)


# ============================================================
# 与V5集成
# ============================================================

def integrate_with_v5_alpha_calculator():
    """
    集成到V5的AlphaCalculator
    
    修改: src/strategy/alpha_calculator.py
    """
    
    code = '''
# 在AlphaCalculator中添加IC-based权重支持

from src.factors.ic_weight_calculator import ICBasedWeightCalculator

class AlphaCalculator:
    def __init__(self, config):
        ...
        # 初始化IC权重计算器
        self.ic_weights = ICBasedWeightCalculator()
        self.use_ic_weights = config.get('use_ic_weights', True)
    
    def calculate_alpha(self, symbol_data):
        """计算单个币种的Alpha得分"""
        
        # 获取各因子值
        factor_values = {
            'f1_mom_5d': symbol_data.get('mom_5d', 0),
            'f2_mom_20d': symbol_data.get('mom_20d', 0),
            'f3_vol_adj_ret_20d': symbol_data.get('vol_adj_ret', 0),
            'f4_volume_expansion': symbol_data.get('volume_exp', 0),
            'f5_rsi_trend_confirm': symbol_data.get('rsi_confirm', 0),
            'f6_sentiment': symbol_data.get('sentiment', 0),
        }
        
        if self.use_ic_weights:
            # 使用IC-based权重
            alpha = self.ic_weights.apply_weights_with_direction(factor_values)
        else:
            # 使用固定权重
            alpha = self._apply_fixed_weights(factor_values)
        
        return alpha
    
    def get_factor_weights(self) -> dict:
        """获取当前使用的权重"""
        if self.use_ic_weights:
            return self.ic_weights.calculate_weights()
        else:
            return self._get_fixed_weights()
'''
    return code


# ============================================================
# 运行示例
# ============================================================

if __name__ == "__main__":
    calculator = ICBasedWeightCalculator()
    calculator.print_report()
    
    # 生成配置
    config = calculator.generate_config()
    print("\n生成配置JSON:")
    print(json.dumps(config, indent=2, ensure_ascii=False))
