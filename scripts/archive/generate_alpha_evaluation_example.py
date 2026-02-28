#!/usr/bin/env python3
"""
生成 alpha 评估报告样例（模拟数据）
用于展示评估框架的输出格式和关键指标
"""

from __future__ import annotations

import json
import numpy as np
from datetime import datetime
from pathlib import Path


def generate_example_report() -> dict:
    """生成模拟评估报告"""
    
    # 模拟数据：基于典型 crypto alpha 特征
    # IC 通常很小（0.01-0.05），衰减快，成本敏感
    
    report = {
        "metadata": {
            "generated_at": datetime.utcnow().isoformat(),
            "data_range": "2026-01-01 to 2026-02-17",
            "num_snapshots": 150,
            "num_symbols_avg": 22,
            "universe_coverage": "21 major crypto pairs",
            "survivorship_bias_warning": "WARNING: Only includes currently active pairs",
            "time_alignment_check": "PASSED: snapshot_time < label_window_start",
            "purge_gap_hours": 24
        },
        
        "ic_analysis": {
            "method": "Spearman rank correlation (Rank IC)",
            "confidence_interval_method": "Block bootstrap (1000 samples, block_size=5)",
            "by_horizon": {
                "1": {
                    "ic_mean": 0.042,
                    "ic_std": 0.031,
                    "ic_ir": 1.35,
                    "count": 150,
                    "ci_95_lower": 0.028,
                    "ci_95_upper": 0.056,
                    "p_value": 0.003,
                    "significant": True
                },
                "4": {
                    "ic_mean": 0.038,
                    "ic_std": 0.035,
                    "ic_ir": 1.09,
                    "count": 148,
                    "ci_95_lower": 0.022,
                    "ci_95_upper": 0.054,
                    "p_value": 0.008,
                    "significant": True
                },
                "12": {
                    "ic_mean": 0.025,
                    "ic_std": 0.042,
                    "ic_ir": 0.60,
                    "count": 145,
                    "ci_95_lower": 0.005,
                    "ci_95_upper": 0.045,
                    "p_value": 0.042,
                    "significant": True
                },
                "24": {
                    "ic_mean": 0.012,
                    "ic_std": 0.048,
                    "ic_ir": 0.25,
                    "count": 142,
                    "ci_95_lower": -0.008,
                    "ci_95_upper": 0.032,
                    "p_value": 0.231,
                    "significant": False
                },
                "72": {
                    "ic_mean": -0.005,
                    "ic_std": 0.055,
                    "ic_ir": -0.09,
                    "count": 135,
                    "ci_95_lower": -0.028,
                    "ci_95_upper": 0.018,
                    "p_value": 0.672,
                    "significant": False
                }
            },
            "decay_summary": "IC peaks at 1h horizon (0.042), decays to near-zero by 24h",
            "best_horizon": 1,
            "ic_annualized": 0.665  # sqrt(252) * IC_daily equivalent
        },
        
        "quantile_analysis": {
            "horizon_hours": 1,
            "n_quantiles": 5,
            "monotonic": True,
            "spread_q5_q1": 0.0082,  # Q5 mean return - Q1 mean return
            "spread_significant": True,
            "by_quantile": {
                "1": {  # Lowest score (weakest)
                    "mean_return": -0.0012,
                    "annualized_return": -0.302,
                    "win_rate": 0.48,
                    "vol": 0.023,
                    "sharpe": -0.131,
                    "count": 660
                },
                "2": {
                    "mean_return": -0.0003,
                    "annualized_return": -0.076,
                    "win_rate": 0.51,
                    "vol": 0.022,
                    "sharpe": -0.035,
                    "count": 660
                },
                "3": {
                    "mean_return": 0.0005,
                    "annualized_return": 0.126,
                    "win_rate": 0.53,
                    "vol": 0.021,
                    "sharpe": 0.060,
                    "count": 660
                },
                "4": {
                    "mean_return": 0.0018,
                    "annualized_return": 0.453,
                    "win_rate": 0.56,
                    "vol": 0.022,
                    "sharpe": 0.206,
                    "count": 660
                },
                "5": {  # Highest score (strongest)
                    "mean_return": 0.0030,
                    "annualized_return": 0.755,
                    "win_rate": 0.59,
                    "vol": 0.024,
                    "sharpe": 0.315,
                    "count": 660
                }
            }
        },
        
        "factor_contributions": {
            "total_score_ic": 0.042,
            "by_factor": {
                "f1_mom_5d": {
                    "weight": 0.25,
                    "ic_mean": 0.028,
                    "ic_ir": 1.12,
                    "marginal_contribution": 0.0070,
                    "rank_ic_stability": "moderate"
                },
                "f2_mom_20d": {
                    "weight": 0.25,
                    "ic_mean": 0.015,
                    "ic_ir": 0.60,
                    "marginal_contribution": 0.0038,
                    "rank_ic_stability": "low"
                },
                "f3_vol_adj_ret_20d": {
                    "weight": 0.20,
                    "ic_mean": 0.022,
                    "ic_ir": 0.88,
                    "marginal_contribution": 0.0044,
                    "rank_ic_stability": "moderate"
                },
                "f4_volume_expansion": {
                    "weight": 0.15,
                    "ic_mean": 0.010,
                    "ic_ir": 0.40,
                    "marginal_contribution": 0.0015,
                    "rank_ic_stability": "low"
                },
                "f5_rsi_trend_confirm": {
                    "weight": 0.15,
                    "ic_mean": 0.018,
                    "ic_ir": 0.72,
                    "marginal_contribution": 0.0027,
                    "rank_ic_stability": "moderate"
                }
            },
            "factor_correlation_matrix": {
                "f1_f2": 0.65,
                "f1_f3": 0.42,
                "f1_f4": 0.18,
                "f1_f5": 0.31,
                "f2_f3": 0.58,
                "f2_f4": 0.22,
                "f2_f5": 0.35,
                "f3_f4": 0.15,
                "f3_f5": 0.28,
                "f4_f5": 0.12
            }
        },
        
        "cost_sensitivity": {
            "turnover_analysis": {
                "annual_turnover_pct": 2150.0,  # 非常高的换手率
                "avg_holding_period_days": 0.17,  # ~4小时
                "trades_per_day": 12.3,
                "effective_n": 3.2  # 权重集中度：1/∑w²
            },
            "cost_components": {
                "fee_structure": "taker 0.06%, maker 0.04%",
                "assumed_fee_bps": 6.0,
                "bid_ask_spread_bps": 5.0,
                "slippage_bps": 3.0,
                "total_cost_bps_per_trade": 14.0
            },
            "breakeven_analysis": {
                "gross_alpha_annualized": 0.755,
                "cost_drag_annualized": 0.301,  # turnover * cost_per_trade
                "net_alpha_annualized": 0.454,
                "breakeven_ic": 0.018,
                "current_ic_vs_breakeven": "2.33x"  # 0.042 / 0.018
            },
            "scenario_analysis": {
                "baseline": {
                    "fee_bps": 6.0,
                    "spread_bps": 5.0,
                    "net_alpha": 0.454
                },
                "high_liquidity": {
                    "fee_bps": 4.0,
                    "spread_bps": 2.0,
                    "net_alpha": 0.598
                },
                "low_liquidity": {
                    "fee_bps": 8.0,
                    "spread_bps": 10.0,
                    "net_alpha": 0.227
                },
                "market_stress": {
                    "fee_bps": 10.0,
                    "spread_bps": 20.0,
                    "net_alpha": -0.045
                }
            }
        },
        
        "regime_analysis": {
            "by_regime": {
                "trending": {
                    "frequency": 0.35,
                    "ic_mean": 0.051,
                    "net_alpha": 0.612,
                    "recommended_multiplier": 1.0
                },
                "sideways": {
                    "frequency": 0.45,
                    "ic_mean": 0.038,
                    "net_alpha": 0.423,
                    "recommended_multiplier": 0.6
                },
                "risk_off": {
                    "frequency": 0.20,
                    "ic_mean": 0.015,
                    "net_alpha": 0.128,
                    "recommended_multiplier": 0.3
                }
            },
            "regime_aware_improvement": "IC improves 28% when regime-filtered"
        },
        
        "robustness_checks": {
            "time_period_stability": {
                "first_half_ic": 0.045,
                "second_half_ic": 0.039,
                "stability_ratio": 0.87
            },
            "deflated_sharpe_ratio": {
                "gross_sharpe": 1.85,
                "deflated_sharpe": 1.42,
                "probability_sharpe_is_real": 0.89
            },
            "probability_backtest_overfitting": {
                "pbo": 0.23,
                "cscv_confidence": 0.77
            },
            "multiple_testing_adjustment": {
                "tests_performed": 8,
                "bonferroni_ic_threshold": 0.015,
                "current_ic_significant": True
            }
        },
        
        "recommendations": {
            "signal_strength": "MODERATE",
            "summary": "Alpha shows statistically significant predictive power at short horizons (1-4h), but high turnover makes cost management critical.",
            "priority_actions": [
                "1. Reduce turnover: Increase minimum holding period to 6-12h",
                "2. Improve cost model: Implement maker/taker logic, spread-aware execution",
                "3. Factor optimization: Reduce weight on f2_mom_20d (low IC stability)",
                "4. Regime adaptation: Use lower multipliers in Risk-Off regimes",
                "5. Temperature tuning: Increase softmax temperature to reduce concentration (effective_n from 3.2 to ~5)"
            ],
            "expected_improvements": {
                "with_holding_period_6h": {
                    "turnover_reduction": "-60%",
                    "net_alpha_improvement": "+0.15"
                },
                "with_cost_optimization": {
                    "cost_reduction": "-40%",
                    "net_alpha_improvement": "+0.12"
                },
                "with_factor_reweight": {
                    "ic_improvement": "+0.005",
                    "net_alpha_improvement": "+0.08"
                }
            }
        }
    }
    
    return report


def main() -> None:
    """生成并保存样例报告"""
    report = generate_example_report()
    
    output_dir = Path("reports/alpha_evaluation_examples")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"alpha_eval_example_{timestamp}.json"
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"Example alpha evaluation report saved to: {json_path}")
    print("\n=== KEY INSIGHTS FROM EXAMPLE REPORT ===")
    print()
    print("1. SIGNAL STRENGTH: IC = 0.042 at 1h horizon (significant)")
    print("   - Decays to near-zero by 24h → short-term alpha")
    print("   - Best for 1-4h holding periods")
    print()
    print("2. COST SENSITIVITY: High turnover (2150% annual) kills 40% of gross alpha")
    print("   - Current: Gross 0.755 → Net 0.454 after costs")
    print("   - Breakeven IC = 0.018 (we're at 0.042, 2.33x above)")
    print()
    print("3. FACTOR CONTRIBUTIONS:")
    print("   - f1_mom_5d: Strongest (IC=0.028, weight=0.25)")
    print("   - f2_mom_20d: Weakest (IC=0.015, consider reducing weight)")
    print("   - f4_volume_expansion: Low IC stability")
    print()
    print("4. REGIME AWARENESS: IC varies significantly:")
    print("   - Trending: IC=0.051, Net=0.612")
    print("   - Risk-Off: IC=0.015, Net=0.128")
    print("   - → Should use regime-dependent multipliers")
    print()
    print("5. ROBUSTNESS: Passes basic checks but PBO=0.23 (23% overfit risk)")
    print("   - Deflated Sharpe = 1.42 (89% probability real)")
    print("   - Time stability ratio = 0.87 (good)")
    print()
    print("6. RECOMMENDED ACTIONS:")
    print("   - Increase holding period to 6-12h (reduce turnover)")
    print("   - Implement spread-aware execution")
    print("   - Reduce f2_mom_20d weight, increase f3_vol_adj_ret")
    print("   - Increase softmax temperature (reduce concentration)")
    print()
    print("=== NEXT STEPS ===")
    print("1. Collect real data (running V5)")
    print("2. Run update_forward_returns.py (needs price data)")
    print("3. Generate real evaluation report")
    print("4. Compare with this example to identify gaps")


if __name__ == "__main__":
    main()