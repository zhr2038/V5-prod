export function finite(value: unknown): number | null {
  if (typeof value !== 'number' && (typeof value !== 'string' || !value.trim())) return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

export function number(value: unknown, digits = 2): string {
  const n = finite(value);
  return n === null ? '—' : n.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

export function signed(value: unknown, digits = 2): string {
  const n = finite(value);
  return n === null ? '—' : `${n > 0 ? '+' : ''}${number(n, digits)}`;
}

export function ratio(value: unknown, digits = 1): string {
  const n = finite(value);
  return n === null ? '—' : `${number(n * 100, digits)}%`;
}

export function epoch(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value < 1e11 ? value * 1000 : value;
  if (typeof value !== 'string' || !value.trim()) return null;
  // A timezone-free server label is not silently reinterpreted in the viewer's zone.
  if (!/(Z|[+-]\d{2}:?\d{2})$/i.test(value)) return null;
  const stamp = Date.parse(value);
  return Number.isFinite(stamp) ? stamp : null;
}

export function dateTime(value: unknown, full = false): string {
  const stamp = epoch(value);
  if (stamp === null) return typeof value === 'string' && value ? value.replace('T', ' ').slice(0, 19) : '—';
  return new Date(stamp).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai',
    ...(full ? { year: 'numeric' } as const : {}), month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hour12: false });
}

// Use only for API fields explicitly documented as Beijing time when timezone-free.
export function beijingEpoch(value: unknown): number | null {
  const explicit = epoch(value);
  if (explicit !== null) return explicit;
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$/.test(value)) return null;
  const stamp = Date.parse(value.replace(' ', 'T') + '+08:00');
  return Number.isFinite(stamp) ? stamp : null;
}

export function observationState(status: string | undefined, stamp: number | null, now: number, maxAgeMs: number): string {
  if (status && !['observed', 'healthy', 'fresh', 'ok', 'complete'].includes(status)) return status;
  if (stamp === null) return 'unavailable';
  if (stamp > now + 5000) return 'future';
  if (now - stamp > maxAgeMs) return 'stale';
  return 'observed';
}

export function countQualifier(status: string | undefined): string {
  return status === 'observed' || status === 'complete' ? '' : status === 'partial' ? '部分观测' : statusLabels[status || 'unknown'] || status || '待确认';
}

export function age(value: unknown, now: number): string {
  const stamp = epoch(value);
  if (stamp === null) return '时间待确认';
  const secs = Math.floor((now - stamp) / 1000);
  if (secs < 0) return '时间异常';
  if (secs < 60) return '刚刚';
  if (secs < 3600) return `${Math.floor(secs / 60)} 分钟前`;
  if (secs < 86400) return `${Math.floor(secs / 3600)} 小时前`;
  return `${Math.floor(secs / 86400)} 天前`;
}

const reasons: Record<string, string> = {
  protect_entry_rsi_confirm_too_weak: 'RSI 确认不足',
  protect_entry_trend_only: '仅趋势确认，缺少 Alpha 买入',
  protect_entry_alpha6_score_too_low: 'Alpha 分数未达门槛',
  protect_entry_no_alpha6_confirmation: '缺少 Alpha 买入确认',
  protect_entry_volume_confirm_negative: '成交量确认不足',
  btc_leadership_probe_no_alpha6_buy: 'BTC 探针缺少 Alpha 买入',
  btc_leadership_probe_score_too_low: 'BTC 探针分数不足',
  btc_leadership_probe_alpha6_score_too_low: 'BTC 探针 Alpha 分数不足',
  btc_leadership_probe_not_flat: 'BTC 探针要求空仓',
  price_below_ema20: '价格低于 EMA20',
  ema20_slope_not_positive: 'EMA20 趋势尚未转强',
  momentum_below_cost_buffer: '动量不足以覆盖成本储备',
  absolute_direction_then_relative_rank: '趋势与预算通过，等待后续报价',
  no_eligible_candidate: '暂无满足条件的候选',
  position_open: '持仓跟踪中',
  pending_intent: '等待后续报价确认',
  ema20_trend_exit: '趋势失效，准备退出',
  hard_stop: '触发价格止损',
  risk_off: '市场转入避险',
  risk_off_or_unknown: '市场避险或状态待确认',
  same_symbol_cooldown: '该币种仍在冷却期',
  stale_quote: '盘口报价已过期',
  spread_too_wide: '买卖价差过大',
  zero_target_close: '目标仓位归零',
  negative_expectancy_cooldown: '历史负期望冷却中',
  missing_forward_observation: '缺少连续观测，取消入场',
  entry_intent_expired: '入场意图已过期',
  entry_price_premium: '价格偏离信号过多',
  minimum_order_exceeds_risk_budget: '最小订单超出风险预算',
  no_orders: '本轮没有生成订单',
  invalid_candidate: '候选未通过有效性筛选',
};

export function reasonLabel(reason: unknown): string {
  const raw = typeof reason === 'string' ? reason : '';
  if (!raw) return '等待决策证据';
  if (reasons[raw]) return reasons[raw];
  if (raw.includes('negative_expectancy')) return '历史收益条件未通过';
  if (raw.includes('kill_switch')) return '停止开关或状态阻断';
  if (raw.includes('reconcile')) return '对账状态未通过';
  if (raw.includes('ledger')) return '账本状态未通过';
  if (raw.includes('min_notional') || raw.includes('minimum_order')) return '未满足最小可执行金额';
  return raw;
}

export const statusLabels: Record<string, string> = {
  observed: '已观测', healthy: '正常', ok: '正常', fresh: '新鲜', active: '运行中',
  stale: '数据过期', future: '时间异常', unavailable: '不可观测', unknown: '待确认',
  missing: '等待首轮观测', disabled: '未启用', partial: '部分数据可用',
  identity_mismatch: '策略版本不匹配', failed: '异常', critical: '异常', warning: '需关注',
  invalid: '数据无效', complete: '完整', incomplete: '不完整',
  flat_cash: '现金估值', observed_quote: '观测报价估值', stale_quote: '估值报价过期',
};
export const regimeLabels: Record<string, string> = { Trending: '趋势行情', Sideways: '震荡行情', 'Risk-Off': '避险行情', RiskOff: '避险行情' };
export const riskLabels: Record<string, string> = { PROTECT: '保护模式', DEFENSE: '防御模式', NEUTRAL: '中性模式', ATTACK: '进攻模式' };
