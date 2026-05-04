export const fmtNum = (value: unknown, digits = 2) =>
  Number.isFinite(Number(value)) ? Number(value).toFixed(digits) : '--';

export const fmtUsd = (value: unknown) =>
  Number.isFinite(Number(value)) ? `$${Number(value).toFixed(2)}` : '--';

export const fmtUsdt = (value: unknown) =>
  Number.isFinite(Number(value)) ? `${Number(value).toFixed(2)} USDT` : '--';

export const pctVal = (value: unknown) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.abs(num) <= 1 ? num * 100 : num;
};

export const fmtPct = (value: unknown, digits = 2) => {
  const pct = pctVal(value);
  if (pct === null) return '--';
  const sign = pct > 0 ? '+' : '';
  return `${sign}${pct.toFixed(digits)}%`;
};

export const stateLabels: Record<string, string> = {
  TRENDING: '趋势',
  SIDEWAYS: '震荡',
  RISK_OFF: '避险',
};

export const riskLabels: Record<string, string> = {
  ATTACK: '进攻',
  NEUTRAL: '中性',
  DEFENSE: '防守',
  PROTECT: '保护',
};

export const statusLabels: Record<string, string> = {
  healthy: '健康',
  warning: '告警',
  critical: '严重',
  fresh: '新鲜',
  stale: '过期',
  missing: '缺失',
  active: '运行中',
  armed: '待触发',
  idle: '空闲',
  ready: '就绪',
  running: '运行中',
  error: '异常',
  ok: '正常',
};

export const modeLabels: Record<string, string> = {
  live: '实盘',
  dry_run: '演练',
  paper: '模拟',
  unknown: '未知',
};

export const sideLabels: Record<string, string> = {
  buy: '买入',
  sell: '卖出',
};

export const orderStateLabels: Record<string, string> = {
  filled: '已成交',
  rejected: '已拒绝',
  open: '挂单',
  partial: '部分成交',
  partially_filled: '部分成交',
  live: '挂单中',
  canceled: '已撤单',
  cancelled: '已撤单',
  pending: '待处理',
  failed: '失败',
};
