# 脚本清理计划

## 保留的核心脚本（正在使用或重要）

### 定时任务相关
- daily_ml_training.py
- compute_dynamic_alpha_weights.py
- rollup_costs.py
- auto_risk_eval.py
- train_hmm_regime.py
- smart_alert_check.py
- trade_auditor_v2.py
- reconcile_guard_once.py
- reflection_agent.py
- collect_funding_sentiment.py
- collect_rss_sentiment.py

### Web面板
- web_dashboard.py

### 执行脚本
- run_hourly_live_window.sh
- run_hourly_window.sh

### 工具脚本
- bills_sync.py
- fill_sync.py
- orders_repair_once.py
- orders_gc_once.py
- live_preflight_once.py
- okx_private_selfcheck.py
- reconcile_once.py
- ledger_once.py
- sell_fil.py

### 数据脚本
- collect_alpha_history.py
- collect_market_data.py
- generate_30day_alpha_history.py

### 回测相关
- quick_backtest.py
- run_walk_forward.py
- full_backtest.py
- run_profitability_backtest.py
- backtest_*.py (保留主要回测)

### IC/分析
- ic_diagnostics.py
- run_ic_research.py
- deep_ic_analysis.py
- compute_dynamic_alpha_weights_by_regime.py

### 演示
- reflection_demo.py
- multi_strategy_demo.py

## 建议删除的脚本（调试/一次性/过时）

### 调试脚本（已修复问题，可删）
- debug_*.py (大量调试脚本)
- fix_*.py (修复脚本，问题已解决)
- test_*.py (测试脚本)
- analyze_*.py (分析脚本，一次性)
- validate_*.py (验证脚本)
- final_*.py (最终修复脚本)
- investigate_*.py (调查脚本)
- zscore_fix.py
- consistency_continuity_optimization.py
- continuity_optimization.py
- actual_fix_continuity.py

### 自动还款/紧急处理（已处理完毕）
- auto_repay_pepe.py
- emergency_*.py
- instant_*.py
- manual_*.py
- fix_*_liability.py
- fix_merl_*.py
- fix_sol_*.py
- fix_pepe_*.py

### 数据回填（已完成）
- auto_backfill.py
- backfill_*.py
- quick_backfill.py
- mini_backfill.py
- simple_backfill.py
- final_backfill.py
- fixed_backfill.py

### 监控（过时）
- data_monitor.py
- monitor_*.py
- borrow_monitor.py
- enhanced_borrow_monitor.py
- plan_b_monitor.py

### 其他
- export_*.py (导出脚本)
- bootstrap_from_okx_balance.py (一次性)
- fix_position_sync.py (已修复)
- reset_equity_peak.py (一次性)
- separate_real_cost_data.py (一次性)
- cleanup_cost_events.py (一次性)
- update_forward_returns*.py (已使用)

## 删除方法

```bash
# 创建备份分支
git checkout -b backup-scripts-before-cleanup

# 删除调试脚本
git rm scripts/debug_*.py
git rm scripts/fix_*.py
git rm scripts/test_*.py
...

# 提交并推送
git commit -m "chore: remove debug and one-time scripts"
git push origin main
```
