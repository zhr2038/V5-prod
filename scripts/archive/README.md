# Scripts Archive / 脚本归档

此目录包含110+个已归档的脚本，仅在本地保留，**不会上传到GitHub**。

## 归档原因

这些脚本主要是：
- **调试脚本** (debug_*.py): 问题已修复，不再需要
- **修复脚本** (fix_*.py): 一次性修复脚本
- **回填脚本** (backfill_*.py): 历史数据回填完成
- **紧急处理** (emergency_*.py): 已处理完毕
- **测试脚本** (test_*.py): 开发和测试使用

## 如何使用

如需使用归档中的脚本，直接调用即可：
```bash
cd /home/admin/clawd/v5-trading-bot
python3 scripts/archive/debug_xxx.py
```

## 核心脚本位置

正在使用的核心脚本仍在 `scripts/` 根目录：
- 定时任务: daily_ml_training.py, trade_auditor_v2.py
- Web面板: web_dashboard.py
- 执行脚本: run_hourly_live_window.sh
- 工具脚本: bills_sync.py, fill_sync.py

## 归档时间

2026-02-27
