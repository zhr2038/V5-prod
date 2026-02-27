# V5 交易机器人

> 专业级量化交易系统 | OKX现货 | 多策略融合 | 机器学习驱动

---

## 📖 项目简介

V5是一个完整的横截面趋势轮动量化交易系统，专为OKX现货市场设计。系统采用模块化架构，支持多策略并行、动态风险管理、机器学习因子和实时Web监控。

**核心设计理念**：用户看到什么，系统就显示什么。

---

## 🏗️ 系统架构

### 六层流水线架构

```
┌─────────────────────────────────────────────────────────────┐
│  数据层 (Data)                                                │
│  ├─ OKX行情数据获取                                          │
│  ├─ 资金费率数据                                             │
│  └─ RSS新闻情绪数据                                          │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│  因子层 (Alpha)                                               │
│  ├─ 5因子动态加权 (动量/波动率/成交量/反转/情绪)              │
│  ├─ Ridge回归机器学习因子                                    │
│  └─ 多策略信号融合                                           │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│  市场状态层 (Regime)                                          │
│  └─ Ensemble三方法投票：HMM + 资金费率 + RSS情绪              │
│     ├─ HMM模型 (35%权重)：隐马尔可夫趋势识别                 │
│     ├─ 资金费率 (40%权重)：10币种分层加权                     │
│     └─ RSS情绪 (25%权重)：新闻情绪分析                        │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│  组合层 (Portfolio)                                           │
│  ├─ 横截面评分排序                                           │
│  ├─ 动态仓位分配                                             │
│  └─ Rebalance死区控制                                        │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│  风控层 (Risk)                                                │
│  ├─ AutoRiskGuard：PROTECT/DEFENSE/NEUTRAL三档               │
│  ├─ 固定止损 (Fixed Stop Loss)                               │
│  ├─ 分阶段止盈 (Profit Taking)                               │
│  │   ├─ 盈利5%+：保本止损                                    │
│  │   ├─ 盈利10%+：保本+5%                                    │
│  │   └─ 盈利15%+：追踪止损（保护80%利润）                    │
│  └─ 回撤控制 (Drawdown Control)                              │
└───────────────────────┬─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│  执行层 (Execution)                                           │
│  ├─ OKX私有接口下单/查单/撤单                                │
│  ├─ PositionBuilder分批建仓                                  │
│  └─ 粉尘订单过滤 (Dust Skip)                                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 快速开始

### 环境配置

```bash
# 克隆仓库
git clone git@github.com:zhr2038/v5-trading-bot.git
cd v5-trading-bot

# 创建虚拟环境
python3 -m venv .venv
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 配置文件

```bash
# 复制环境变量模板
cp .env.example .env

# 编辑.env文件，填写OKX API密钥
# EXCHANGE_API_KEY=your_key
# EXCHANGE_API_SECRET=your_secret
# EXCHANGE_PASSPHRASE=your_passphrase
# DEEPSEEK_API_KEY=your_key (可选，用于情绪分析)
```

### 运行模式

#### 1. Dry-Run模式（模拟交易，默认）
```bash
python3 main.py --config configs/live_20u_real.yaml
```

#### 2. Live模式（实盘交易）
```bash
# 必须设置ARM环境变量
export V5_LIVE_ARM=YES
python3 main.py --config configs/live_20u_real.yaml
```

> ⚠️ **安全提示**：即使配置写了`mode: live`，也必须显式设置`V5_LIVE_ARM=YES`才会真正下单。

---

## 📊 核心功能详解

### 1. 多策略并行系统

系统同时运行多个策略，通过信号融合生成最终决策：

| 策略 | 类型 | 权重 | 核心逻辑 |
|------|------|------|----------|
| **TrendFollowing** | 趋势跟踪 | 15% | 双均线交叉 + ADX确认 |
| **MeanReversion** | 均值回归 | 35% | RSI超买超卖 + 布林带 |
| **Alpha6Factor** | 综合因子 | 50% | 5因子动态加权 + 机器学习 |

**信号融合规则**：
- 同向信号：加权平均
- 反向信号：冲突解决（趋势优先）
- 无信号：保持现状

### 2. Ensemble市场状态判断

三种方法投票决定市场状态：

```yaml
hmm_weight: 0.35      # HMM模型：基于价格序列的隐状态识别
funding_weight: 0.40  # 资金费率：10币种分层加权综合
rss_weight: 0.25      # RSS情绪：新闻情绪分析
```

**输出状态**：
- `TrendingUp`：趋势向上，满仓操作
- `Sideways`：震荡行情，80%仓位
- `TrendingDown`：趋势向下，60%仓位
- `Risk-Off`：风控模式，只减仓不开新仓

### 3. 三层风控体系

#### 3.1 AutoRiskGuard（自动风险档位）

| 档位 | 触发条件 | 行为 |
|------|----------|------|
| **NEUTRAL** | 正常状态 | 正常交易 |
| **DEFENSE** | 回撤5-10% | 限制新仓，最多3个持仓 |
| **PROTECT** | 回撤>10% | 只减仓不开新仓 |

#### 3.2 固定止损

- 每币种独立止损线
- 触发后自动市价卖出
- 防止单边暴跌风险

#### 3.3 分阶段止盈

```python
# 盈利阶梯
if pnl >= 15%:  # 追踪止损，保护80%利润
    stop = highest * 0.8
elif pnl >= 10%:  # 保本+5%
    stop = cost * 1.05
elif pnl >= 5%:   # 保本
    stop = cost
```

### 4. 机器学习因子模型

**模型**：Ridge回归（L2正则化线性模型）

选用Ridge而非树模型的原因：
- 小数据集下树模型（LightGBM）严重过拟合
- 线性模型泛化能力强，可解释性好
- Ridge的正则化有效控制过拟合

**特征工程**（11个精选特征）：
- 动量类：returns_24h, momentum_5d, momentum_20d
- 波动率：volatility_24h
- 成交量：volume_ratio, obv
- 技术指标：rsi, macd, macd_signal, bb_position, price_position

> 已移除高相关性/泄露特征：returns_1h/6h, volatility_ratio

**训练流程**：
1. 每小时收集特征快照
2. 6小时后回填标签（未来收益率）
3. 每天00:30自动训练（需100+条记录）
4. 计算IC（信息系数），验证IC>0时保存模型
5. 生成特征系数报告

**模型性能**（当前）：
```
Train IC: 0.83
Valid IC: 0.04
```

**启用定时任务**：
```bash
systemctl --user enable v5-daily-ml-training.timer
systemctl --user start v5-daily-ml-training.timer
```

---

## 🖥️ Web监控面板

### 启动面板

```bash
python3 scripts/web_dashboard.py
# 访问 http://localhost:5000
```

### 核心页面

#### 1. 监控首页 (/monitor)
- **账户概览**：总权益、现金、持仓市值、持仓数量
- **风险状态**：当前档位、回撤比例、最大回撤
- **市场状态**：Ensemble三方法投票结果
- **持仓明细**：成本价、现价、盈亏（与交易所同步）
- **最近交易**：成交时间、币种、方向、金额

#### 2. 策略信号 (/signals)
- 各策略信号数量
- 融合后选中币种
- 信号时间戳（与run_id同步）

#### 3. 决策归因 (/decision_audit)
- "为什么没买"透明化分析
- 策略层、风控层、执行层详细数据
- 阻塞归因：deadband拦截、漂移值等

### API端点

| 端点 | 功能 |
|------|------|
| `/api/account` | 账户信息 |
| `/api/positions` | 持仓明细（含盈亏） |
| `/api/trades` | 交易记录 |
| `/api/market_state` | 市场状态判断详情 |
| `/api/scores` | 币种评分排名 |
| `/api/decision_audit` | 决策审计 |

---

## ⏰ 系统定时任务

| Timer | 频率 | 功能 | 脚本 |
|-------|------|------|------|
| **v5-live-20u** | 每小时 | 实盘交易执行 | `run_hourly_live_window.sh` |
| **v5-reconcile** | 每5分钟 | 对账状态刷新 | `reconcile_guard_once.py` |
| **v5-daily-ml-training** | 每天00:30 | ML模型训练 | `daily_ml_training.py` |
| **v5-reflection-agent** | 每天21:00 | 交易后分析 | `reflection_agent.py` |
| **v5-trade-auditor** | 每小时 | 交易审计 | `trade_auditor_v2.py` |
| **v5-smart-alert** | 每30分钟 | 智能异常检测 | `smart_alert_check.py` |
| **v5-sentiment-collect** | 每小时 | 情绪数据收集 | `collect_funding_sentiment.py` + `collect_rss_sentiment.py` |
| **v5-cost-rollup** | 每天08:20 | 成本统计汇总 | `rollup_costs.py` |
| **v5-hmm-retrain** | 每周 | HMM模型重训练 | `train_hmm_regime.py` |

**启用所有定时任务**：
```bash
# 安装user-level systemd服务
bash deploy/install_systemd.sh --user

# 启用linger（用户不登录也运行）
sudo loginctl enable-linger admin

# 启动所有timer
systemctl --user start v5-live-20u.user.timer
systemctl --user start v5-reconcile.user.timer
# ... 其他timer
```

---

## 📁 项目目录结构

```
v5-trading-bot/
├── configs/                    # 配置文件
│   ├── live_20u_real.yaml     # 20U实盘配置
│   ├── multi_strategy.yaml    # 多策略配置
│   └── backtest_*.yaml        # 回测配置
│
├── src/                        # 源代码
│   ├── core/                  # 核心流水线
│   │   └── pipeline.py        # 主流程编排
│   ├── alpha/                 # Alpha引擎
│   │   └── alpha_engine.py    # 因子计算
│   ├── strategy/              # 策略系统
│   │   └── multi_strategy_system.py  # 多策略融合
│   ├── regime/                # 市场状态
│   │   ├── ensemble_regime_engine.py # Ensemble判断
│   │   ├── hmm_regime_detector.py    # HMM模型
│   │   └── regime_engine.py          # 传统判断
│   ├── portfolio/             # 组合管理
│   │   └── portfolio_engine.py
│   ├── risk/                  # 风险管理
│   │   ├── risk_engine.py           # 主风控
│   │   ├── auto_risk_guard.py       # 自动档位
│   │   ├── fixed_stop_loss.py       # 固定止损
│   │   └── profit_taking.py         # 分阶段止盈
│   ├── execution/             # 执行层
│   │   ├── live_execution_engine.py # 实盘执行
│   │   ├── position_builder.py      # 分批建仓
│   │   ├── order_store.py           # 订单存储
│   │   ├── fill_store.py            # 成交存储
│   │   ├── reflection_agent.py      # 反思Agent
│   │   └── ml_factor_model.py       # ML因子模型
│   ├── data/                  # 数据层
│   │   ├── market_data_provider.py
│   │   └── okx_ccxt_provider.py
│   ├── factors/               # 因子实现
│   │   ├── sentiment_factor.py
│   │   └── deepseek_sentiment_factor.py
│   ├── backtest/              # 回测系统
│   │   └── backtest_engine.py
│   └── reporting/             # 报告生成
│       ├── decision_audit.py
│       └── summary_writer.py
│
├── scripts/                    # 工具脚本（核心56个）
│   ├── daily_ml_training.py   # ML训练
│   ├── trade_auditor_v2.py    # 交易审计
│   ├── smart_alert_check.py   # 智能告警
│   ├── web_dashboard.py       # Web面板
│   ├── run_hourly_live_window.sh  # 主执行脚本
│   └── ...
│
├── scripts/archive/           # 归档脚本（本地保留）
│   # 110+个调试/修复/一次性脚本
│   # 被.gitignore忽略，不上传GitHub
│
├── web/                       # Web前端
│   └── templates/
│       └── monitor_v2.html    # 监控页面
│
├── reports/                   # 报告输出（运行时生成）
│   ├── runs/                  # 每次运行结果
│   ├── cost_stats_real/       # 成本统计
│   └── spread_stats/          # 价差统计
│
├── data/                      # 数据存储（运行时生成）
│   ├── cache/                 # 行情缓存
│   └── sentiment_cache/       # 情绪缓存
│
├── models/                    # 模型文件
│   ├── hmm_regime.pkl         # HMM模型
│   └── ml_model.pkl           # ML因子模型
│
├── deploy/                    # 部署配置
│   └── systemd/               # systemd服务文件
│
├── docs/                      # 文档
├── tests/                     # 测试用例
├── requirements.txt           # 依赖列表
├── README.md                  # 本文件
└── .gitignore                 # 忽略规则
```

---

## ⚙️ 核心配置说明

### 实盘配置示例 (`configs/live_20u_real.yaml`)

```yaml
# 账户配置
account:
  live_equity_cap_usdt: 20.0    # 硬上限20USDT
  prevent_borrow: true          # 禁止借贷

# 执行模式
execution:
  mode: live                    # live或dry_run
  preflight_enabled: true       # 预检开启
  preflight_bootstrap_patch_enabled: true  # 自动状态对齐

# 市场状态
regime:
  engine: ensemble              # ensemble或traditional
  hmm_weight: 0.35
  funding_weight: 0.40
  rss_weight: 0.25

# 风控参数
risk:
  drawdown_trigger: 0.50        # 回撤触发线50%
  drawdown_delever: 1.00        # 降杠杆比例
  deadband_sideways: 0.03       # 震荡死区3%
  
# 多策略
multi_strategy:
  enabled: true
  allocations:
    TrendFollowing: 0.15
    MeanReversion: 0.35
    Alpha6Factor: 0.50

# ML因子
ml_factor:
  enabled: true
  min_training_samples: 100
  ic_threshold: 0.02
```

---

## 🔍 故障排查

### 常见问题

#### 1. 持仓显示不正确
```bash
# 检查OKX API是否正常
curl -s http://localhost:5000/api/positions

# 手动同步持仓
python3 scripts/sync_positions_from_okx.py
```

#### 2. 定时任务未运行
```bash
# 检查timer状态
systemctl --user list-timers | grep v5

# 查看日志
journalctl --user -u v5-live-20u.user.service -n 50
```

#### 3. ML训练失败
```bash
# 检查样本数量
python3 scripts/daily_ml_training.py --dry-run

# 手动训练
python3 scripts/daily_ml_training.py
```

#### 4. Web面板无法访问
```bash
# 检查服务状态
systemctl --user status v5-web-dashboard.service

# 重启服务
systemctl --user restart v5-web-dashboard.service
```

---

## 📝 更新日志

### 2026-02-28
- ✅ **ML模型修复**：LightGBM严重过拟合 → 改用Ridge回归
  - 修复前：Train IC 0.95, Valid IC -0.09（过拟合）
  - 修复后：Train IC 0.83, Valid IC 0.04（有效）
- ✅ **特征清理**：移除泄露特征（id列、returns_1h/6h、volatility_ratio）
- ✅ **数据导出修复**：排除自增id列，避免伪相关
- ✅ **训练脚本统一**：两个训练路径（直接传入/market_data）都使用安全特征

#### 代码审查修复（26个问题）
**Critical (3)**
- ✅ ML数据泄露风险 - 添加缓存文件时间戳验证
- ✅ 订单精度丢失 - 全程使用Decimal计算
- ✅ 多策略信号合并 - 加权平均替代简单取最大

**High (8)**
- ✅ Pipeline类型检查 - 严谨的positions验证
- ✅ 时间戳处理 - 明确阈值判断替代相对接近度
- ✅ 配置验证 - AlphaWeights总和=1.0, RiskConfig逻辑校验
- ✅ 价格无效告警 - 记录并汇总无效价格symbols
- ✅ NaN传播修复 - 改进RSI/MACD/布林带计算
- ✅ 硬编码路径 - 7个脚本改为动态路径检测

**Medium (15)**
- ✅ 数据库连接池 - MLDataCollector连接管理
- ✅ 异常处理细化 - 区分可恢复/致命错误
- ✅ 硬编码魔数 - 灰尘阈值提取到配置
- ✅ 资源管理 - LiveExecutionEngine.close()和上下文管理器
- ✅ 日志格式统一 - print改为logging
- ✅ httpx.Client上下文管理器支持
- ✅ 类型注解完善 - MLDataCollector方法
- ✅ 代码重复消除 - 提取src/utils/features.py

**Low (性能优化)**
- ✅ 批量数据库更新 - executemany替代逐条更新
- ✅ 代码风格修复 - 添加文档字符串(36→25), 行长度检查通过

**提交**: `16a44db`, `26ec009`, `185f5a8`, `d8b17a9`, `9fd4b23`, `0489e8a`, `a505a6e`, `4537b51`, `3a5242b`, `14b3725`, `ad974a9`

### 2026-02-27
- ✅ Web面板：持仓盈亏显示修复，与交易所同步
- ✅ 实时价格：优先OKX API，缓存15分钟过期
- ✅ 持仓同步：OKX返回空时不回退缓存
- ✅ 策略信号：时间戳使用文件修改时间
- ✅ 数据库：修复sz字段同步问题
- ✅ 脚本清理：110+个脚本归档到scripts/archive/

### 2026-02-26
- ✅ 粉尘过滤：小于$1的持仓自动过滤
- ✅ 退出加速：收紧close-only死区
- ✅ 分阶段止盈：新增保本/部分止盈/追踪止损
- ✅ 重启连续性：自动注册现有持仓

---

## 🤝 贡献指南

1. Fork本仓库
2. 创建feature分支：`git checkout -b feature/xxx`
3. 提交更改：`git commit -m "feat: xxx"`
4. 推送分支：`git push origin feature/xxx`
5. 创建Pull Request

---

## 📄 许可证

MIT License

---

## ⚠️ 免责声明

本系统仅供学习和研究使用，不构成投资建议。加密货币交易风险极高，请根据自身情况谨慎决策。

**使用本系统进行交易，风险自负。**

---

*V5 Trading Bot - 专业级量化交易系统*
