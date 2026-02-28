# 📊 V5 Trading Bot 深度监控报告

📅 2026-02-26 17:12 (Asia/Shanghai)

---

## 🎯 一、市场状态判断逻辑

当前市场状态由 `regime_engine.py` 判定，采用双保险机制：

### 1. 主逻辑：MA+ATR 方法
- 计算 BTC 的 MA20 和 MA60
- 计算 14 周期 ATR（波动率）
- 判断规则：
  - MA20 > MA60 且 ATR > 2% → **TRENDING（趋势市）**，仓位系数 1.2x
  - ATR < 0.8% → **SIDEWAYS（震荡市）**，仓位系数 0.6x
  - 其他情况 → **RISK_OFF（风险规避）**，仓位系数 0.3x

### 2. 情绪修正机制
- 读取 BTC/ETH/SOL/BNB 的市场情绪（-1~1）
- 当情绪 > 0.3 且 MA 差距 < 阈值时，可将 RISK_OFF 放宽至 SIDEWAYS
- 当情绪 < -0.5 时，强制进入 RISK_OFF

### 3. HMM 隐藏马尔可夫模型（备用）
- 使用 4 维特征（1h/6h 收益、波动率、RSI）
- 自动识别 TrendingUp/TrendingDown/Sideways 三种状态

---

## 📈 二、买入决策代码逻辑

### 买入类型 1：OPEN_LONG（开新仓）
代码路径：`alpha_engine.py` → `compute_scores()`

选股逻辑（6 因子模型）：
- **f1_mom_5d** (25%)：5 天动量，涨得快的加分
- **f2_mom_20d** (25%)：20 天动量，中期趋势强的加分
- **f3_vol_adj_ret** (20%)：波动率调整收益，风险收益比高的加分
- **f4_volume_expansion** (15%)：成交量放大，资金流入的加分
- **f5_rsi_trend_confirm** (15%)：RSI 趋势确认，RSI>50 bullish

流程：
1. 对每个币种计算 5 个原始因子
2. 做截面 z-score 标准化（去除极端值）
3. 加权求和得到最终得分
4. 选取得分最高的前 20% 币种
5. 按目标权重分配资金

### 实际买入记录：
- UNI-USDT: $2.04 (Alpha信号，排名靠前)
- AVAX-USDT: $1.11 (Alpha信号)
- LTC-USDT: $1.14 (Alpha信号)
- NEAR-USDT: $0.92 + $0.90 (Alpha信号，分两次买入)
- FIL-USDT: $1.62 (Alpha信号)

### 买入类型 2：REBALANCE（再平衡）
代码逻辑：当持仓偏离目标权重超过 deadband 时触发

实际记录：
- TRX-USDT: $3.60 + $3.36 (仓位偏低，补足目标权重)
- DOT-USDT: $1.70 (再平衡)
- POL-USDT: $1.62 (再平衡)
- SOL-USDT: $1.84 (再平衡)

**为什么买这些？**
→ Alpha 引擎算出来它们在动量、趋势、量价配合上综合得分高，且符合当前市场状态下的仓位限制。

---

## 📉 三、卖出决策代码逻辑

### 卖出类型 1：regime_exit（市场状态退出）
代码逻辑：`portfolio_engine.py` 中的持仓清理

触发条件：
- 币种不在 Alpha 选中的前 20% 列表中
- 或市场状态变化导致该币种不符合持仓条件

实际记录（7 笔）：
- BNB-USDT: $0.0004
- BTC-USDT: $0.0002
- SOL-USDT: ~$0
- ETC-USDT: ~$0
- DOT-USDT: ~$0
- POL-USDT: $0.0008
- TRX-USDT: ~$0

**为什么卖？**
→ 这些币种已不在策略选中的优质列表里，属于"清仓处理"，金额极小说明是剩余 dust 仓位。

### 卖出类型 2：atr_trailing（ATR 跟踪止损）
代码逻辑：`risk_engine.py` 中的 ATR trailing stop

触发条件：
- 价格从最高点回落超过 ATR × 倍数
- 保护盈利、截断亏损

实际记录（3 笔）：
- ETC-USDT × 2: ~$0
- ETH-USDT: $0.003

**为什么卖？**
→ 这些持仓触发了移动止损线，属于风控机制正常运作。

### 卖出类型 3：REBALANCE（再平衡卖出）
实际记录：4 笔（日志中未显示具体币种）
→ 部分持仓超过目标权重，自动减仓。

---

## 🛑 四、未交易原因分析

### 核心问题：Kill Switch 触发

错误记录（13:00-17:00，每小时一次）：
```
live preflight failed (ABORT LIVE): borrow_detected
```

### 代码逻辑解析：
`live_execution_engine.py` 第 165-200 行的 STRICT NO-BORROW ENFORCEMENT：

1. **卖出前检查**：
   - 检查本地 position_store 是否有足够持仓
   - 如果没有或数量为 0，直接拒绝卖出（防止借贷）
   - 双重检查 OKX 余额，如为负数或不足，拒绝交易

2. **预检失败原因**：
   - `borrow_detected` 表示系统检测到了借贷行为或余额异常
   - 这是安全机制，宁可不交易也不冒险借贷

### 为什么从 13:00 开始每小时都失败？
→ 可能原因：
1. 账户存在未结清的借贷余额
2. 本地 position_store 与交易所实际持仓不同步
3. 某些币种的余额检测出现异常

### 当前状态：
- 交易模式：dry_run = true（模拟模式）
- 实际资金风险：无（未使用真实资金）
- 但预检逻辑仍在运行，检测到异常后阻止交易

---

## ✅ 五、合理性评估

| 维度 | 评估 | 说明 |
|------|------|------|
| 买入逻辑 | ✅ 合理 | Alpha 6因子选股，分散投资，金额适中 |
| 卖出逻辑 | ✅ 合理 | 清仓+止损，风控机制正常 |
| 再平衡 | ✅ 合理 | 仓位调整符合目标权重 |
| Kill Switch | ⚠️ 需关注 | 风控介入，但 dry_run 模式无实际损失 |
| 代码质量 | ✅ 良好 | 有完善的防借贷检查、idempotency、订单持久化 |

### 亮点：
1. 严格的 NO-BORROW 机制，避免杠杆风险
2. 订单幂等性设计（clOrdId + OrderStore）
3. 多层级风控（Kill Switch + 预检 + 执行检查）
4. 详细的日志和审计追踪

---

## ⚠️ 六、风险提示与建议

### 🔴 需立即关注：
1. **borrow_detected 错误** - 已连续 5 小时触发
   - 建议：检查 OKX 账户是否有未结清借贷
   - 建议：同步 position_store 与交易所持仓

### 🟡 建议优化：
1. 当前配置为 dry_run 模式，如需实盘需修改：
   ```yaml
   execution:
     mode: live
     dry_run: false
   ```

2. 考虑降低仓位系数（当前 TRENDING 1.2x 偏高）

3. 监控 ATR 阈值是否适应当前市场波动

### 🟢 整体评价：
策略逻辑清晰，风控严格，代码健壮。Kill Switch 的触发说明安全机制在正常工作，但需要排查借贷检测的误报或真实借贷情况。

---

## 📌 总结

- **买入**：Alpha 信号触发，选出了 UNI/AVAX/LTC/NEAR/FIL 等币种
- **卖出**：清仓+止损，处理 dust 仓位
- **未交易**：Kill Switch 保护机制触发（borrow_detected）
- **建议**：排查账户借贷状态，确认后可恢复正常交易

报告生成时间：2026-02-26 17:12:30
