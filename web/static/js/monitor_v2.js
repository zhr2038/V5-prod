const REFRESH_MS = 30000;
const MOBILE_QUERY = "(max-width: 760px)";
const POSITION_KLINE_LIMITS = { "1h": 96, "4h": 120, "1d": 90 };
const POSITION_KLINE_LABELS = { "1h": "1小时", "4h": "4小时", "1d": "1日" };

const stateLabels = { TRENDING: "趋势", SIDEWAYS: "震荡", RISK_OFF: "避险" };
const stateClasses = { TRENDING: "state-trending", SIDEWAYS: "state-sideways", RISK_OFF: "state-riskoff" };
const riskClasses = { ATTACK: "risk-attack", NEUTRAL: "risk-neutral", DEFENSE: "risk-defense", PROTECT: "risk-protect" };
const riskLabels = { ATTACK: "进攻", NEUTRAL: "中性", DEFENSE: "防守", PROTECT: "保护" };
const healthTone = { healthy: "tone-positive", warning: "tone-warn", critical: "tone-negative" };
const statusLabels = {
  healthy: "健康",
  warning: "告警",
  critical: "严重",
  fresh: "新鲜",
  stale: "过期",
  missing: "缺失",
  active: "运行中",
  armed: "待触发",
  idle: "空闲",
  ready: "就绪",
  running: "运行中",
  error: "异常",
  ok: "正常",
};
const modeLabels = { live: "实盘", dry_run: "演练", paper: "模拟", unknown: "未知" };
const cacheLabels = { fresh: "新鲜", stale: "过期", missing: "缺失" };
const sideLabels = { buy: "买入", sell: "卖出" };
const orderStateLabels = {
  filled: "已成交",
  rejected: "已拒绝",
  open: "挂单",
  partial: "部分成交",
  partially_filled: "部分成交",
  live: "挂单中",
  canceled: "已撤单",
  cancelled: "已撤单",
  pending: "待处理",
  failed: "失败",
};
const methodLabels = {
  ensemble: "综合投票",
  decision_audit: "运行快照",
  hmm: "HMM",
  hmm_only: "仅 HMM",
  ensemble_regime: "综合投票",
};
const messageLabels = {
  stale_or_missing: "信号过期或缺失",
  funding_signal_stale_or_missing: "资金费率缺失",
  rss_signal_stale_or_missing: "RSS 缺失",
  min_notional: "低于最小金额",
  min_size: "低于最小下单量",
  min_qty: "低于最小下单量",
  sell_only: "仅允许卖出",
  kill_switch_enabled: "熔断开启",
  reconcile_not_ok: "对账未通过",
  preflight_blocked: "预检拦截",
  insufficient_balance: "余额不足",
  duplicate_clordid: "重复委托",
  network_error: "网络异常",
  auth_failed: "鉴权失败",
  no_actionable_events: "无可执行事件",
};
const messageContains = [
  ["systemctl is not available", "systemctl 不可用"],
  ["api key", "API 密钥异常"],
  ["timeout", "请求超时"],
  ["rate limit", "触发频率限制"],
  ["insufficient", "余额不足"],
  ["duplicate", "重复委托"],
  ["min_notional", "低于最小金额"],
  ["min size", "低于最小下单量"],
  ["min_size", "低于最小下单量"],
  ["sell_only", "仅允许卖出"],
  ["kill_switch", "熔断触发"],
  ["reconcile", "对账未通过"],
  ["preflight", "预检未通过"],
  ["network", "网络异常"],
  ["auth", "鉴权失败"],
  ["cooldown", "冷却中"],
];

const esc = (value) => String(value ?? "")
  .replace(/&/g, "&amp;")
  .replace(/</g, "&lt;")
  .replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;")
  .replace(/'/g, "&#39;");

const fmtNum = (value, digits = 2) => Number.isFinite(Number(value)) ? Number(value).toFixed(digits) : "--";
const fmtUsd = (value) => Number.isFinite(Number(value)) ? `$${Number(value).toFixed(2)}` : "--";
const fmtUsdt = (value) => Number.isFinite(Number(value)) ? `${Number(value).toFixed(2)} USDT` : "--";
const pctVal = (value) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  return Math.abs(num) <= 1 ? num * 100 : num;
};
const fmtPct = (value, digits = 2) => {
  const pct = pctVal(value);
  if (pct === null) return "--";
  const sign = pct > 0 ? "+" : "";
  const klass = pct > 0 ? "text-green" : (pct < 0 ? "text-red" : "");
  return `<span class="${klass}">${sign}${pct.toFixed(digits)}%</span>`;
};
const stateZh = (value) => stateLabels[String(value || "").toUpperCase()] || value || "--";
const riskZh = (value) => riskLabels[String(value || "").toUpperCase()] || String(value || "--").toUpperCase();
const statusZh = (value) => statusLabels[String(value || "").toLowerCase()] || value || "--";
const modeZh = (value) => modeLabels[String(value || "").toLowerCase()] || value || "--";
const cacheZh = (value) => cacheLabels[String(value || "").toLowerCase()] || value || "--";
const sideZh = (value) => sideLabels[String(value || "").toLowerCase()] || value || "--";
const orderStateZh = (value) => orderStateLabels[String(value || "").toLowerCase()] || value || "--";
const methodZh = (value) => methodLabels[String(value || "").toLowerCase()] || value || "--";
const statePill = (value) => `<span class="pill ${stateClasses[String(value || "").toUpperCase()] || "state-sideways"}">${esc(stateZh(value))}</span>`;
const riskPill = (value) => `<span class="pill ${riskClasses[String(value || "").toUpperCase()] || "risk-neutral"}">${esc(riskZh(value))}</span>`;

const setHtml = (id, html) => {
  const el = document.getElementById(id);
  if (el) el.innerHTML = html;
};
const setText = (id, text) => {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
};

function messageZh(value) {
  const raw = String(value ?? "");
  const key = raw.toLowerCase();
  if (messageLabels[key]) return messageLabels[key];
  if (statusLabels[key]) return statusLabels[key];
  for (const [needle, label] of messageContains) {
    if (key.includes(needle)) return label;
  }
  return raw || "--";
}

function isMobileViewport() {
  return window.matchMedia(MOBILE_QUERY).matches;
}

let activeController = null;
let refreshTimer = null;
let secondaryTimer = null;
let secondaryIdleHandle = null;
let dashboardCache = null;
let riskCache = { risk: null, account: null };
let marketCache = null;
let decisionCache = null;
let healthCache = null;
let shadowMlCache = null;
let lastMobileState = isMobileViewport();
let latestPositions = [];
let latestAccount = {};
let positionSpotlightState = {
  symbol: "",
  timeframe: "1h",
  requestId: 0,
  cache: new Map(),
};

async function fetchJson(url, signal) {
  try {
    const res = await fetch(`${url}${url.includes("?") ? "&" : "?"}_=${Date.now()}`, {
      cache: "no-store",
      signal,
      headers: { Accept: "application/json" },
    });
    if (typeof res.ok !== "undefined" && !res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    if (err?.name !== "AbortError") console.error("fetch failed", url, err);
    return null;
  }
}

function renderProbRows(rows) {
  if (!rows.some(([, value]) => value > 0)) return "";
  return `<div class="prob-list">${rows.map(([label, value]) => `
    <div class="prob-row">
      <div class="prob-label">${esc(label)}</div>
      <div class="prob-track"><span style="width:${Math.max(0, Math.min(100, value * 100))}%"></span></div>
      <div class="prob-value">${fmtNum(value * 100, 1)}%</div>
    </div>
  `).join("")}</div>`;
}

function renderHmmProbRows(vote) {
  const probs = vote?.probs || {};
  const rows = [
    ["上涨", Number(probs.TrendingUp || 0)],
    ["震荡", Number(probs.Sideways || 0)],
    ["避险", Number(probs.TrendingDown || 0)],
  ];
  return renderProbRows(rows);
}

function renderDerivedVoteRows(vote) {
  const state = String(vote?.state || "").toUpperCase();
  const rawSentiment = Number(vote?.sentiment);
  const sentiment = Number.isFinite(rawSentiment) ? Math.max(-1, Math.min(1, rawSentiment)) : null;
  const fallbackConf = Math.max(0, Math.min(1, Number(vote?.confidence || 0)));
  let rows;

  if (sentiment !== null) {
    rows = [
      ["上涨", Math.max(0, sentiment)],
      ["震荡", Math.max(0, 1 - Math.abs(sentiment))],
      ["避险", Math.max(0, -sentiment)],
    ];
  } else if (state === "TRENDING") {
    rows = [["上涨", fallbackConf], ["震荡", Math.max(0, 1 - fallbackConf)], ["避险", 0]];
  } else if (state === "RISK_OFF") {
    rows = [["上涨", 0], ["震荡", Math.max(0, 1 - fallbackConf)], ["避险", fallbackConf]];
  } else {
    const sideways = Math.max(fallbackConf, 0.5);
    const edge = Math.max(0, (1 - sideways) / 2);
    rows = [["上涨", edge], ["震荡", sideways], ["避险", edge]];
  }

  const total = rows.reduce((sum, [, value]) => sum + Math.max(0, Number(value) || 0), 0);
  if (total > 1e-9 && total > 1) {
    rows = rows.map(([label, value]) => [label, Math.max(0, Number(value) || 0) / total]);
  }
  return renderProbRows(rows);
}

function historyTipHtml(cell) {
  const score = cell.dataset.score;
  const sentiment = cell.dataset.sentiment;
  const rows = [
    ["轨迹", cell.dataset.seriesLabel || "--"],
    ["时间", cell.dataset.time || "--"],
    ["状态", cell.dataset.state || "--"],
    ["置信度", cell.dataset.confidence || "--"],
  ];
  if (score) rows.push(["综合分", score]);
  if (sentiment) rows.push(["情绪值", sentiment]);
  return `<div class="hover-tip-title">${esc(cell.dataset.seriesLabel || "轨迹详情")}</div>
    <div class="hover-tip-grid">${rows.map(([k, v]) => `<div class="hover-tip-row"><span>${esc(k)}</span><strong>${esc(v)}</strong></div>`).join("")}</div>`;
}

function moveHistoryTooltip(evt) {
  const tip = document.getElementById("history-tooltip");
  if (!tip || tip.hidden) return;
  const offset = 16;
  const width = tip.offsetWidth || 220;
  const height = tip.offsetHeight || 120;
  const x = Math.min(window.innerWidth - width - 12, evt.clientX + offset);
  const y = Math.min(window.innerHeight - height - 12, evt.clientY + offset);
  tip.style.left = `${Math.max(12, x)}px`;
  tip.style.top = `${Math.max(12, y)}px`;
}

function showHistoryTooltip(cell, evt) {
  const tip = document.getElementById("history-tooltip");
  if (!tip) return;
  tip.innerHTML = historyTipHtml(cell);
  tip.hidden = false;
  moveHistoryTooltip(evt);
}

function hideHistoryTooltip() {
  const tip = document.getElementById("history-tooltip");
  if (!tip) return;
  tip.hidden = true;
}

document.addEventListener("mouseover", (evt) => {
  const cell = evt.target.closest(".history-cell");
  if (!cell) {
    hideHistoryTooltip();
    return;
  }
  showHistoryTooltip(cell, evt);
});
document.addEventListener("mousemove", (evt) => {
  if (evt.target.closest(".history-cell")) moveHistoryTooltip(evt);
});
document.addEventListener("mouseout", (evt) => {
  if (evt.target.closest(".history-cell")) hideHistoryTooltip();
});
window.addEventListener("scroll", hideHistoryTooltip, true);

function renderVoteTrack(history, key, label) {
  return `<div class="history-row">
    <div class="history-label">${esc(label)}</div>
    <div class="history-track">${history.map((point) => {
      const node = key === "final" ? (point.final || {}) : ((point.votes || {})[key] || {});
      const state = String(node.state || "SIDEWAYS").toUpperCase();
      const rawConf = Number(node.confidence || 0);
      const conf = Math.max(0.2, Math.min(1, rawConf || 0.2));
      const score = key === "final" && point.final?.score != null ? fmtNum(point.final.score, 3) : "";
      const sentiment = node?.sentiment != null ? fmtNum(node.sentiment, 3) : "";
      const title = `${label} ${point.label || "--"} ${stateZh(state)} / ${(rawConf * 100).toFixed(1)}%`;
      return `<span class="history-cell ${stateClasses[state] || "state-sideways"}"
        style="opacity:${conf}"
        data-series-label="${esc(label)}"
        data-time="${esc(point.label || "--")}"
        data-state="${esc(stateZh(state))}"
        data-confidence="${esc(`${(rawConf * 100).toFixed(1)}%`)}"
        ${score ? `data-score="${esc(score)}"` : ""}
        ${sentiment ? `data-sentiment="${esc(sentiment)}"` : ""}
        title="${esc(title)}"
        aria-label="${esc(title)}"></span>`;
    }).join("")}</div>
  </div>`;
}

function renderVoteHistory(history) {
  const items = Array.isArray(history) ? history : [];
  if (!items.length) return '<div class="empty">暂无 24 小时投票轨迹。</div>';
  const range = `${items[0].label || "--"} → ${items[items.length - 1].label || "--"}`;
  return `<div class="history-title">最近 24 小时 · ${esc(range)}</div>
    <div class="history-grid">
      ${renderVoteTrack(items, "final", "综合")}
      ${renderVoteTrack(items, "hmm", "HMM")}
      ${renderVoteTrack(items, "funding", "费率")}
      ${renderVoteTrack(items, "rss", "RSS")}
    </div>`;
}

function shortSymbol(symbol) {
  return String(symbol || "--").replace("/USDT", "").replace("-USDT", "");
}

function normalizeInstrumentSymbol(symbol) {
  const raw = String(symbol || "").trim().toUpperCase().replace(/_/g, "/").replace(/-/g, "/");
  if (!raw) return "";
  if (raw.endsWith("/USDT")) return raw;
  if (raw.endsWith("USDT") && !raw.includes("/")) return `${raw.slice(0, -4)}/USDT`;
  if (raw.includes("/")) return `${raw.split("/")[0]}/USDT`;
  return `${raw}/USDT`;
}

function baseSymbol(symbol) {
  return normalizeInstrumentSymbol(symbol).split("/")[0] || shortSymbol(symbol);
}

function fmtCompactUsd(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  if (Math.abs(num) >= 1000000) return `$${(num / 1000000).toFixed(2)}M`;
  if (Math.abs(num) >= 1000) return `$${(num / 1000).toFixed(1)}K`;
  return fmtUsd(num);
}

function getActivePosition(positions) {
  const items = Array.isArray(positions) ? positions : [];
  if (!items.length) return null;
  if (!positionSpotlightState.symbol) {
    positionSpotlightState.symbol = baseSymbol(items[0].symbol);
    return items[0];
  }
  return items.find((item) => baseSymbol(item.symbol) === positionSpotlightState.symbol) || items[0];
}

function updatePositionTimeframeButtons() {
  const root = document.getElementById("position-kline-timeframes");
  if (!root) return;
  root.querySelectorAll("[data-timeframe]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.timeframe === positionSpotlightState.timeframe);
  });
}

function renderPositionKlineSymbolButtons(positions) {
  const root = document.getElementById("position-kline-symbols");
  if (!root) return;
  const items = Array.isArray(positions) ? positions : [];
  if (!items.length) {
    root.innerHTML = "";
    return;
  }

  root.innerHTML = items.slice(0, 10).map((position) => {
    const symbolKey = baseSymbol(position.symbol);
    const activeClass = symbolKey === positionSpotlightState.symbol ? " is-active" : "";
    const pnlValue = Number(position.pnl ?? position.pnl_value ?? 0);
    const pnlClass = pnlValue >= 0 ? "text-green" : "text-red";
    return `<button type="button" class="symbol-chip${activeClass}" data-position-symbol="${esc(symbolKey)}">
      <strong>${esc(symbolKey)}</strong>
      <span>${fmtCompactUsd(position.value)}</span>
      <span class="${pnlClass}">${fmtPct(position.pnlPercent ?? position.pnl_pct ?? 0, 1)}</span>
    </button>`;
  }).join("");
}

function renderPositionSpotlightSummary(position, account, payload) {
  const weight = Number(account?.totalEquity) > 0 ? Number(position.value || 0) / Number(account.totalEquity) : null;
  const lastClose = Number(payload?.summary?.close ?? position.currentPrice ?? position.price);
  const avgPrice = Number(position.avgPrice ?? position.avg_px);
  const pnlValue = Number(position.pnl ?? position.pnl_value ?? 0);
  const pnlPct = Number(position.pnlPercent ?? position.pnl_pct ?? 0);
  const rangePct = payload?.summary?.high > 0
    ? (Number(payload.summary.high) - Number(payload.summary.low || 0)) / Number(payload.summary.low || payload.summary.high)
    : null;
  const cards = [
    ["持仓市值", fmtUsd(position.value)],
    ["仓位占比", weight == null ? "--" : `${fmtNum(weight * 100, 1)}%`],
    ["成本价", avgPrice > 0 ? fmtUsd(avgPrice) : "--"],
    ["最新价", Number.isFinite(lastClose) ? fmtUsd(lastClose) : "--"],
    ["未实现盈亏", `${fmtUsd(pnlValue)} / ${fmtPct(pnlPct, 2)}`],
    ["区间振幅", rangePct == null ? "--" : `${fmtNum(rangePct * 100, 2)}%`],
  ];
  setHtml("position-kline-summary", cards.map(([label, value]) => `<div class="stat-card">
    <div class="mini-label">${esc(label)}</div>
    <div class="stat-card-value">${value}</div>
  </div>`).join(""));
}

function renderPositionSpotlightHeader(position, account, payload) {
  if (!position) {
    setText("position-spotlight-symbol", "暂无持仓");
    setText("position-spotlight-copy", "默认展示当前仓位中市值最高的币种，并支持切换时间周期。");
    setText("position-kline-source", "数据源 --");
    setHtml("position-kline-summary", `<div class="stat-card"><div class="mini-label">持仓市值</div><div class="stat-card-value">--</div></div>`);
    return;
  }

  const symbolText = normalizeInstrumentSymbol(position.symbol) || `${baseSymbol(position.symbol)}/USDT`;
  const weight = Number(account?.totalEquity) > 0 ? Number(position.value || 0) / Number(account.totalEquity) : null;
  const lastTime = payload?.summary?.last_time ? ` · 截止 ${payload.summary.last_time}` : "";
  const sourceText = payload?.source ? ` · ${String(payload.source).toUpperCase()}` : "";
  setText("position-spotlight-symbol", symbolText);
  setText(
    "position-spotlight-copy",
    `数量 ${fmtNum(position.qty ?? position.quantity, 4)} · 占权益 ${weight == null ? "--" : `${fmtNum(weight * 100, 1)}%`} · ${POSITION_KLINE_LABELS[positionSpotlightState.timeframe]} K 线${sourceText}${lastTime}`,
  );
  setText(
    "position-kline-source",
    payload?.summary?.bars
      ? `数据 ${String(payload.source || "--").toUpperCase()} · ${POSITION_KLINE_LABELS[payload.timeframe] || payload.timeframe} · ${payload.summary.bars} 根`
      : "数据源 --",
  );
}

function formatChartAxisPrice(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  if (Math.abs(num) >= 1000) return num.toFixed(0);
  if (Math.abs(num) >= 100) return num.toFixed(2);
  if (Math.abs(num) >= 1) return num.toFixed(3);
  return num.toFixed(5);
}

function formatChartTimeLabel(value) {
  const raw = String(value || "");
  if (!raw) return "--";
  const [datePart, timePart = ""] = raw.split(" ");
  if (positionSpotlightState.timeframe === "1d") return datePart.slice(5);
  return timePart.slice(0, 5) || datePart.slice(5);
}

function buildCandlestickSvg(candles, avgPrice) {
  const width = 920;
  const height = 360;
  const margin = { top: 18, right: 64, bottom: 34, left: 16 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const highs = candles.map((item) => Number(item.high)).filter(Number.isFinite);
  const lows = candles.map((item) => Number(item.low)).filter(Number.isFinite);
  if (!highs.length || !lows.length) return "";

  let minPrice = Math.min(...lows);
  let maxPrice = Math.max(...highs);
  if (Number.isFinite(avgPrice) && avgPrice > 0) {
    minPrice = Math.min(minPrice, avgPrice);
    maxPrice = Math.max(maxPrice, avgPrice);
  }
  const padding = Math.max((maxPrice - minPrice) * 0.12, maxPrice * 0.01, 0.0001);
  minPrice -= padding;
  maxPrice += padding;

  const yFor = (price) => {
    const pct = (Number(price) - minPrice) / Math.max(maxPrice - minPrice, 1e-9);
    return margin.top + plotHeight - (pct * plotHeight);
  };

  const step = plotWidth / Math.max(candles.length, 1);
  const bodyWidth = Math.max(3, Math.min(step * 0.62, 10));
  const gridValues = Array.from({ length: 5 }, (_, index) => maxPrice - ((maxPrice - minPrice) / 4) * index);
  const grid = gridValues.map((value) => {
    const y = yFor(value);
    return `<g>
      <line x1="${margin.left}" y1="${y.toFixed(2)}" x2="${width - margin.right + 8}" y2="${y.toFixed(2)}" stroke="rgba(147,175,198,.12)" stroke-dasharray="4 6"></line>
      <text x="${width - margin.right + 12}" y="${(y + 4).toFixed(2)}" fill="#8ea1b8" font-size="11" font-family="JetBrains Mono,monospace">${esc(formatChartAxisPrice(value))}</text>
    </g>`;
  }).join("");

  const candlesSvg = candles.map((item, index) => {
    const x = margin.left + (step * index) + step / 2;
    const open = Number(item.open);
    const close = Number(item.close);
    const high = Number(item.high);
    const low = Number(item.low);
    const rising = close >= open;
    const bodyTop = Math.min(yFor(open), yFor(close));
    const bodyBottom = Math.max(yFor(open), yFor(close));
    const bodyHeight = Math.max(bodyBottom - bodyTop, 1.6);
    const color = rising ? "#5edac7" : "#ff879d";
    const fill = rising ? "rgba(94,218,199,.28)" : "rgba(255,135,157,.24)";
    return `<g>
      <line x1="${x.toFixed(2)}" y1="${yFor(high).toFixed(2)}" x2="${x.toFixed(2)}" y2="${yFor(low).toFixed(2)}" stroke="${color}" stroke-width="1.4" stroke-linecap="round"></line>
      <rect x="${(x - bodyWidth / 2).toFixed(2)}" y="${bodyTop.toFixed(2)}" width="${bodyWidth.toFixed(2)}" height="${bodyHeight.toFixed(2)}" rx="1.8" fill="${fill}" stroke="${color}" stroke-width="1.1"></rect>
    </g>`;
  }).join("");

  const avgLine = Number.isFinite(avgPrice) && avgPrice > 0
    ? `<g>
      <line x1="${margin.left}" y1="${yFor(avgPrice).toFixed(2)}" x2="${width - margin.right}" y2="${yFor(avgPrice).toFixed(2)}" stroke="rgba(241,197,108,.92)" stroke-width="1.4" stroke-dasharray="8 6"></line>
      <text x="${margin.left + 6}" y="${(yFor(avgPrice) - 8).toFixed(2)}" fill="#f1c56c" font-size="11" font-family="JetBrains Mono,monospace">AVG ${esc(formatChartAxisPrice(avgPrice))}</text>
    </g>`
    : "";

  const lastClose = Number(candles[candles.length - 1]?.close);
  const lastLine = Number.isFinite(lastClose)
    ? `<g>
      <line x1="${margin.left}" y1="${yFor(lastClose).toFixed(2)}" x2="${width - margin.right}" y2="${yFor(lastClose).toFixed(2)}" stroke="rgba(131,208,255,.88)" stroke-width="1.1" stroke-dasharray="4 8"></line>
      <text x="${width - margin.right - 4}" y="${(yFor(lastClose) - 8).toFixed(2)}" text-anchor="end" fill="#83d0ff" font-size="11" font-family="JetBrains Mono,monospace">LAST ${esc(formatChartAxisPrice(lastClose))}</text>
    </g>`
    : "";

  const labelIndexes = Array.from(new Set([
    0,
    Math.floor((candles.length - 1) * 0.33),
    Math.floor((candles.length - 1) * 0.66),
    candles.length - 1,
  ])).filter((index) => index >= 0 && index < candles.length);
  const labels = labelIndexes.map((index) => {
    const x = margin.left + (step * index) + step / 2;
    return `<text x="${x.toFixed(2)}" y="${height - 10}" text-anchor="middle" fill="#8ea1b8" font-size="11" font-family="JetBrains Mono,monospace">${esc(formatChartTimeLabel(candles[index].time))}</text>`;
  }).join("");

  return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="持仓 K 线图">
    <defs>
      <linearGradient id="candle-bg" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="rgba(131,208,255,.08)"></stop>
        <stop offset="100%" stop-color="rgba(6,12,21,0)"></stop>
      </linearGradient>
    </defs>
    <rect x="0" y="0" width="${width}" height="${height}" fill="url(#candle-bg)"></rect>
    ${grid}
    ${avgLine}
    ${lastLine}
    ${candlesSvg}
    ${labels}
  </svg>`;
}

function renderPositionKlineState(message, tone = "empty") {
  const klass = tone === "error" ? "chart-stage-empty error" : "chart-stage-empty";
  setHtml("position-kline-chart", `<div class="${klass}">${esc(message)}</div>`);
}

function renderPositionKline(payload, position, account) {
  const candles = Array.isArray(payload?.candles) ? payload.candles : [];
  renderPositionSpotlightHeader(position, account, payload);
  renderPositionSpotlightSummary(position, account, payload);
  if (!candles.length) {
    renderPositionKlineState("当前没有可展示的 K 线数据。");
    return;
  }
  const avgPrice = Number(position.avgPrice ?? position.avg_px);
  const svg = buildCandlestickSvg(candles, avgPrice);
  if (!svg) {
    renderPositionKlineState("K 线数据不完整，暂时无法渲染图表。", "error");
    return;
  }
  setHtml("position-kline-chart", svg);
}

async function fetchPositionKlinePayload(symbol, timeframe) {
  const limit = POSITION_KLINE_LIMITS[timeframe] || POSITION_KLINE_LIMITS["1h"];
  return fetchJson(`/api/position_kline?symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(timeframe)}&limit=${limit}`);
}

async function loadPositionSpotlight(position, account, { force = false } = {}) {
  if (!position) {
    renderPositionSpotlightHeader(null, null, null);
    renderPositionKlineState("暂无持仓，K 线图将在有仓位后显示。");
    return;
  }

  const symbolKey = baseSymbol(position.symbol);
  const cacheKey = `${symbolKey}|${positionSpotlightState.timeframe}`;
  const cached = positionSpotlightState.cache.get(cacheKey);
  renderPositionSpotlightHeader(position, account, cached?.payload || null);
  renderPositionSpotlightSummary(position, account, cached?.payload || null);

  if (!force && cached && (Date.now() - cached.fetchedAt) < REFRESH_MS) {
    renderPositionKline(cached.payload, position, account);
    return;
  }

  renderPositionKlineState("K 线加载中...");
  const requestId = ++positionSpotlightState.requestId;
  const payload = await fetchPositionKlinePayload(symbolKey, positionSpotlightState.timeframe);
  if (requestId !== positionSpotlightState.requestId) return;

  if (!payload || payload.error || !Array.isArray(payload.candles) || !payload.candles.length) {
    renderPositionKlineState(payload?.error || "K 线数据获取失败。", "error");
    return;
  }

  positionSpotlightState.cache.set(cacheKey, { payload, fetchedAt: Date.now() });
  renderPositionKline(payload, position, account);
}

function syncPositionSpotlight(positions, account, { force = false } = {}) {
  const items = Array.isArray(positions) ? positions.slice().sort((a, b) => Number(b.value || 0) - Number(a.value || 0)) : [];
  const active = getActivePosition(items);
  if (active) {
    positionSpotlightState.symbol = baseSymbol(active.symbol);
  }
  updatePositionTimeframeButtons();
  renderPositionKlineSymbolButtons(items);
  void loadPositionSpotlight(active, account, { force });
}

document.addEventListener("click", (evt) => {
  const timeframeButton = evt.target.closest("#position-kline-timeframes [data-timeframe]");
  if (timeframeButton) {
    const timeframe = timeframeButton.dataset.timeframe;
    if (timeframe && timeframe !== positionSpotlightState.timeframe) {
      positionSpotlightState.timeframe = timeframe;
      updatePositionTimeframeButtons();
      syncPositionSpotlight(latestPositions, latestAccount, { force: false });
    }
    return;
  }

  const symbolButton = evt.target.closest("[data-position-symbol]");
  if (symbolButton) {
    const symbol = symbolButton.dataset.positionSymbol;
    if (symbol && symbol !== positionSpotlightState.symbol) {
      positionSpotlightState.symbol = symbol;
      renderPositionKlineSymbolButtons(latestPositions);
      syncPositionSpotlight(latestPositions, latestAccount, { force: false });
    }
  }
});

function renderMlSignalCard(ml) {
  return renderMlImpactCardClean(ml);
}
/*
  if (!ml) return "";

  const enabled = Boolean(ml.configured_enabled);
  const promoted = Boolean(ml.promoted);
  const liveActive = Boolean(ml.live_active);
  const predictionCount = Number(ml.prediction_count || 0);
  const coverageCount = Number(ml.coverage_count || 0);
  const activeCount = Number(ml.active_symbols || predictionCount || coverageCount || 0);
  const weightPct = Number(ml.ml_weight || 0) * 100;
  const hasContributors = Array.isArray(ml.top_contributors) && ml.top_contributors.length > 0;
  const promotedRows = Array.isArray(ml.top_promoted) ? ml.top_promoted : [];
  const suppressedRows = Array.isArray(ml.top_suppressed) ? ml.top_suppressed : [];
  const lastStep = ml.last_step || {};
  const rolling = ml.rolling_24h || {};
  const impactStatus = String(ml.impact_status || "");

  if (!enabled && !promoted && !liveActive && !coverageCount && !hasContributors) {
    return "";
  }

  let value = "未启用";
  if (liveActive) {
    value = `本轮参与 ${activeCount || 0} 个币种`;
  } else if (promoted) {
    value = "已晋升，等待加载";
  } else if (enabled) {
    value = "本轮未参与";
  }

  const details = [];
  if (impactStatus && impactStatus !== "insufficient") {
    const tone = impactStatus === "positive" ? "姝ｉ潰" : (impactStatus === "negative" ? "璐熼潰" : "涓€?");
    value = `${value} 路 ${tone}`;
  }
  if (weightPct > 0) details.push(`权重 ${weightPct.toFixed(0)}%`);
  if (predictionCount > 0) details.push(`预测 ${predictionCount} 个`);
  else if (coverageCount > 0) details.push(`覆盖 ${coverageCount} 个`);
  if (promoted) details.push("门控通过");
  else if (enabled) details.push("门控拦截");

  let subtle = details.join(" · ");
  if (lastStep?.delta_bps != null) {
    details.push(`涓婅疆 Top${lastStep.top_n || 3} ${Number(lastStep.delta_bps) >= 0 ? "+" : ""}${fmtNum(lastStep.delta_bps, 1)}bps`);
  }
  if (rolling?.topn_delta_mean_bps != null) {
    details.push(`24h ${Number(rolling.topn_delta_mean_bps) >= 0 ? "+" : ""}${fmtNum(rolling.topn_delta_mean_bps, 1)}bps`);
  }
  let subtle = details.join(" 路 ");
  if (hasContributors) {
    const topEffects = ml.top_contributors.slice(0, 3).map((item) => {
      const zscore = Number(item.ml_zscore || 0);
      const sign = zscore > 0 ? "+" : "";
      return `${shortSymbol(item.symbol)} ${sign}${fmtNum(zscore, 2)}`;
    }).join("、");
    subtle = `${subtle}${subtle ? "；" : ""}当前影响：${topEffects}`;
  } else if (ml.reason) {
    subtle = `${subtle}${subtle ? "；" : ""}${messageZh(ml.reason)}`;
  }

  return `<div class="signal">
    <div class="label">机器学习叠加</div>
    <div class="value">${esc(value)}</div>
    <div class="subtle">${esc(subtle || "等待机器学习决策快照...")}</div>
  </div>`;
}

*/
function renderMlImpactCard(ml) {
  if (!ml) return "";

  const enabled = Boolean(ml.configured_enabled);
  const promoted = Boolean(ml.promoted);
  const liveActive = Boolean(ml.live_active);
  const predictionCount = Number(ml.prediction_count || 0);
  const coverageCount = Number(ml.coverage_count || 0);
  const activeCount = Number(ml.active_symbols || predictionCount || coverageCount || 0);
  const weightPct = Number(ml.ml_weight || 0) * 100;
  const promotedRows = Array.isArray(ml.top_promoted) ? ml.top_promoted : [];
  const suppressedRows = Array.isArray(ml.top_suppressed) ? ml.top_suppressed : [];
  const contributors = Array.isArray(ml.top_contributors) ? ml.top_contributors : [];
  const lastStep = ml.last_step || {};
  const rolling = ml.rolling_24h || {};
  const impactStatus = String(ml.impact_status || "");

  if (!enabled && !promoted && !liveActive && !coverageCount && !contributors.length) {
    return "";
  }

  let title = "ML 鍙犲姞";
  if (impactStatus === "positive") title = "ML 鍙犲姞 路 姝ｉ潰";
  else if (impactStatus === "negative") title = "ML 鍙犲姞 路 璐熼潰";
  else if (impactStatus === "mixed") title = "ML 鍙犲姞 路 涓€?";

  let value = "鏈惎鐢?";
  if (liveActive) value = `鏈疆 ${activeCount || 0} 涓竵`;
  else if (promoted) value = "宸查€氳繃闂ㄦ帶";
  else if (enabled) value = "鏈疆鏈弬涓?";

  const meta = [];
  if (weightPct > 0) meta.push(`鏉冮噸 ${weightPct.toFixed(0)}%`);
  if (predictionCount > 0) meta.push(`棰勬祴 ${predictionCount}`);
  else if (coverageCount > 0) meta.push(`瑕嗙洊 ${coverageCount}`);
  if (lastStep?.delta_bps != null) {
    meta.push(`涓婅疆 Top${lastStep.top_n || 3} ${Number(lastStep.delta_bps) >= 0 ? "+" : ""}${fmtNum(lastStep.delta_bps, 1)}bps`);
  }
  if (rolling?.topn_delta_mean_bps != null) {
    meta.push(`24h ${Number(rolling.topn_delta_mean_bps) >= 0 ? "+" : ""}${fmtNum(rolling.topn_delta_mean_bps, 1)}bps`);
  }

  const moveParts = [];
  if (promotedRows.length) {
    moveParts.push(`鎶崌 ${promotedRows.slice(0, 2).map((item) => `${shortSymbol(item.symbol)} ${item.base_rank}->${item.final_rank}`).join(" / ")}`);
  }
  if (suppressedRows.length) {
    moveParts.push(`鍘嬩綆 ${suppressedRows.slice(0, 2).map((item) => `${shortSymbol(item.symbol)} ${item.base_rank}->${item.final_rank}`).join(" / ")}`);
  }
  if (!moveParts.length && contributors.length) {
    moveParts.push(`褰撳墠 ${contributors.slice(0, 3).map((item) => {
      const delta = Number(item.score_delta || 0);
      const sign = delta > 0 ? "+" : "";
      return `${shortSymbol(item.symbol)} ${sign}${fmtNum(delta, 2)}`;
    }).join(" / ")}`);
  }
  if (!moveParts.length && ml.reason) {
    moveParts.push(messageZh(ml.reason));
  }

  return `<div class="signal">
    <div class="label">${esc(title)}</div>
    <div class="value">${esc(value)}</div>
    <div class="subtle">${esc(meta.join(" 路 ") || "绛夊緟 ML 褰卞搷鏁版嵁...")}</div>
    ${moveParts.length ? `<div class="subtle" style="margin-top:6px">${esc(moveParts.join(" 锛?"))}</div>` : ""}
  </div>`;
}

function renderMlImpactCardClean(ml) {
  if (!ml) return "";

  const enabled = Boolean(ml.configured_enabled);
  const promoted = Boolean(ml.promoted);
  const liveActive = Boolean(ml.live_active);
  const predictionCount = Number(ml.prediction_count || 0);
  const coverageCount = Number(ml.coverage_count || 0);
  const activeCount = Number(ml.active_symbols || predictionCount || coverageCount || 0);
  const configuredWeightPct = Number(ml.configured_ml_weight ?? ml.ml_weight ?? 0) * 100;
  const effectiveWeightPct = Number(ml.effective_ml_weight ?? ml.ml_weight ?? 0) * 100;
  const overlayMode = String(ml.overlay_mode || "");
  const controlReason = String(ml.online_control_reason || "");
  const promotedRows = Array.isArray(ml.top_promoted) ? ml.top_promoted : [];
  const suppressedRows = Array.isArray(ml.top_suppressed) ? ml.top_suppressed : [];
  const contributors = Array.isArray(ml.top_contributors) ? ml.top_contributors : [];
  const lastStep = ml.last_step || {};
  const rolling = ml.rolling_24h || {};
  const rolling48 = ml.rolling_48h || {};
  const impactStatus = String(ml.impact_status || "");

  if (!enabled && !promoted && !liveActive && !coverageCount && !contributors.length) {
    return "";
  }

  const modeLabelMap = {
    live: "实盘",
    downweighted: "降权",
    observe: "观察",
    shadow: "旁路观察",
  };
  const reasonLabelMap = {
    healthy_online_attribution: "24小时归因稳定",
    insufficient_24h_history: "归因样本不足",
    rolling_24h_negative: "24小时归因为负",
    rolling_48h_negative: "48小时归因为负",
    online_control_disabled: "控制关闭",
  };

  let title = "机器学习叠加";
  const modeLabel = modeLabelMap[overlayMode] || "";
  if (modeLabel) title = `机器学习叠加 / ${modeLabel}`;
  else if (impactStatus === "positive") title = "机器学习叠加 / 正面";
  else if (impactStatus === "negative") title = "机器学习叠加 / 负面";
  else if (impactStatus === "mixed") title = "机器学习叠加 / 中性";

  let value = "未启用";
  if (overlayMode === "shadow") value = `旁路观察 ${activeCount || 0} 个币`;
  else if (overlayMode === "downweighted") value = `降权参与 ${activeCount || 0} 个币`;
  else if (overlayMode === "observe") value = `观察中 ${activeCount || 0} 个币`;
  else if (liveActive) value = `实盘参与 ${activeCount || 0} 个币`;
  else if (promoted) value = "已过门控";
  else if (enabled) value = "本轮未参与";

  const meta = [];
  if (configuredWeightPct > 0 && Math.abs(configuredWeightPct - effectiveWeightPct) > 0.01) {
    meta.push(`权重 ${configuredWeightPct.toFixed(0)}%→${effectiveWeightPct.toFixed(0)}%`);
  } else if (effectiveWeightPct > 0) {
    meta.push(`权重 ${effectiveWeightPct.toFixed(0)}%`);
  }
  if (predictionCount > 0) meta.push(`预测 ${predictionCount}`);
  else if (coverageCount > 0) meta.push(`覆盖 ${coverageCount}`);
  if (lastStep?.delta_bps != null) {
    meta.push(`上轮前${lastStep.top_n || 3} ${Number(lastStep.delta_bps) >= 0 ? "+" : ""}${fmtNum(lastStep.delta_bps, 1)}基点`);
  }
  if (rolling?.topn_delta_mean_bps != null) {
    meta.push(`24小时 ${Number(rolling.topn_delta_mean_bps) >= 0 ? "+" : ""}${fmtNum(rolling.topn_delta_mean_bps, 1)}基点`);
  }
  if (rolling48?.topn_delta_mean_bps != null) {
    meta.push(`48小时 ${Number(rolling48.topn_delta_mean_bps) >= 0 ? "+" : ""}${fmtNum(rolling48.topn_delta_mean_bps, 1)}基点`);
  }

  const moveParts = [];
  if (promotedRows.length) {
    moveParts.push(`抬升 ${promotedRows.slice(0, 2).map((item) => `${shortSymbol(item.symbol)} ${item.base_rank}->${item.final_rank}`).join(" / ")}`);
  }
  if (suppressedRows.length) {
    moveParts.push(`压低 ${suppressedRows.slice(0, 2).map((item) => `${shortSymbol(item.symbol)} ${item.base_rank}->${item.final_rank}`).join(" / ")}`);
  }
  if (!moveParts.length && contributors.length) {
    moveParts.push(`当前 ${contributors.slice(0, 3).map((item) => {
      const delta = Number(item.score_delta || 0);
      const sign = delta > 0 ? "+" : "";
      return `${shortSymbol(item.symbol)} ${sign}${fmtNum(delta, 2)}`;
    }).join(" / ")}`);
  }
  if (!moveParts.length && controlReason && reasonLabelMap[controlReason]) {
    moveParts.push(`状态 ${reasonLabelMap[controlReason]}`);
  }
  if (!moveParts.length && ml.reason) {
    moveParts.push(messageZh(ml.reason));
  }

  return `<div class="signal">
    <div class="label">${esc(title)}</div>
    <div class="value">${esc(value)}</div>
    <div class="subtle">${esc(meta.join(" / ") || "等待机器学习影响数据...")}</div>
    ${moveParts.length ? `<div class="subtle" style="margin-top:6px">${esc(moveParts.join(" / "))}</div>` : ""}
  </div>`;
}

function fmtSigned(value, digits = 1, suffix = "") {
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  const sign = num > 0 ? "+" : "";
  return `${sign}${num.toFixed(digits)}${suffix}`;
}

function shadowMlPhaseText(ml) {
  if (ml?.live_active) return "本轮旁路模型已参与";
  if (ml?.promoted) return "旁路模型已通过门控";
  if (ml?.configured_enabled) return "旁路模型已接入，等待更多样本";
  return "旁路模型未接入";
}

function renderShadowMlHeadline(payload) {
  if (!payload || !payload.available) {
    setText("ml-impact-headline", "旁路模型未就绪");
    setText("ml-impact-subtitle", payload?.error || "未找到旁路调优版 XGBoost 工作区");
    return;
  }

  const ml = payload.ml_signal_overview || {};
  const rolling = ml.rolling_24h || {};
  const lastStep = ml.last_step || {};
  let headline = shadowMlPhaseText(ml);

  if (rolling?.topn_delta_mean_bps != null) {
    headline = `${fmtSigned(rolling.topn_delta_mean_bps, 1, "基点")} / 24小时`;
  } else if (lastStep?.delta_bps != null) {
    headline = `${fmtSigned(lastStep.delta_bps, 1, "基点")} / 本轮`;
  } else if (Number(ml.overlay_score_max_abs || 0) > 0) {
    headline = `最大叠加强度 ${fmtNum(ml.overlay_score_max_abs, 2)}`;
  }

  const meta = [];
  if (payload.run_id) meta.push(`轮次 ${payload.run_id}`);
  if (ml.prediction_count != null) meta.push(`预测 ${Number(ml.prediction_count || 0)}`);
  if (ml.ml_weight != null) meta.push(`权重 ${fmtNum(Number(ml.ml_weight || 0) * 100, 0)}%`);
  if (ml.last_update) meta.push(`更新 ${ml.last_update}`);
  setText("ml-impact-headline", headline);
  setText("ml-impact-subtitle", meta.join(" / ") || "等待旁路调优版 XGBoost 归因数据...");
}

function renderShadowMlMoverList(title, items, emptyText) {
  const rows = Array.isArray(items) ? items.slice(0, 3) : [];
  if (!rows.length) {
    return `<div class="info">
      <div class="label">${esc(title)}</div>
      <div class="subtle" style="margin-top:10px">${esc(emptyText)}</div>
    </div>`;
  }
  return `<div class="info">
    <div class="label">${esc(title)}</div>
    <div style="display:grid;gap:10px;margin-top:10px">
      ${rows.map((item) => {
        const rankDelta = Number(item.rank_delta || 0);
        const scoreDelta = Number(item.score_delta || 0);
        const scoreClass = scoreDelta >= 0 ? "text-green" : "text-red";
        return `<div class="row">
          <div>
            <div><strong>${esc(shortSymbol(item.symbol))}</strong></div>
            <div class="subtle">排名 ${esc(`${item.base_rank || "--"} -> ${item.final_rank || "--"}`)}</div>
          </div>
          <div class="text-right">
            <div class="${scoreClass}">${fmtSigned(scoreDelta, 3)}</div>
            <div class="subtle">${rankDelta >= 0 ? "+" : ""}${rankDelta}</div>
          </div>
        </div>`;
      }).join("")}
    </div>
  </div>`;
}

function renderShadowMlPanel(payload) {
  shadowMlCache = payload;
  renderShadowMlHeadline(payload);

  if (!payload || !payload.available) {
    setHtml("ml-impact-content", `<div class="empty">${esc(payload?.error || "未找到旁路调优版 XGBoost 工作区")}</div>`);
    return;
  }

  const ml = payload.ml_signal_overview || {};
  const contributors = Array.isArray(ml.top_contributors) ? ml.top_contributors : [];
  const promotedRows = Array.isArray(ml.top_promoted) ? ml.top_promoted : [];
  const suppressedRows = Array.isArray(ml.top_suppressed) ? ml.top_suppressed : [];
  const rolling = ml.rolling_24h || {};
  const lastStep = ml.last_step || {};
  const lifted = Number(ml.lifted_into_top3 || 0);
  const pushed = Number(ml.pushed_out_of_top3 || 0);
  const overlayMaxAbs = Number(ml.overlay_score_max_abs || 0);
  const coverageCount = Number(ml.coverage_count || ml.active_symbols || ml.prediction_count || 0);

  const summaryCards = `<div class="signal-grid" style="margin-bottom:12px">
    <div class="info">
      <div class="label">状态</div>
      <div class="value">${esc(shadowMlPhaseText(ml))}</div>
      <div class="subtle">${esc(({ positive: "正面", negative: "负面", mixed: "中性", insufficient: "样本不足" })[String(ml.impact_status || "insufficient")] || String(ml.impact_status || "样本不足"))}</div>
    </div>
    <div class="info">
      <div class="label">24小时前N影响</div>
      <div class="value">${rolling?.topn_delta_mean_bps != null ? fmtSigned(rolling.topn_delta_mean_bps, 1, "基点") : "--"}</div>
      <div class="subtle">${rolling?.points != null ? `${rolling.points} 个点` : "等待更多样本"}</div>
    </div>
    <div class="info">
      <div class="label">本轮</div>
      <div class="value">${lastStep?.delta_bps != null ? fmtSigned(lastStep.delta_bps, 1, "基点") : "--"}</div>
      <div class="subtle">覆盖 ${coverageCount} 个币 / 最大叠加强度 ${fmtNum(overlayMaxAbs, 2)}</div>
    </div>
    <div class="info">
      <div class="label">前三影响</div>
      <div class="value">${lifted || pushed ? `+${lifted} / -${pushed}` : `${promotedRows.length} / ${suppressedRows.length}`}</div>
      <div class="subtle">抬升 / 压低</div>
    </div>
  </div>`;

  const contributorRows = contributors.map((item) => {
    const scoreDelta = Number(item.score_delta || 0);
    const scoreClass = scoreDelta >= 0 ? "text-green" : "text-red";
    const rankDelta = Number(item.rank_delta || 0);
    const rankText = item.base_rank && item.final_rank ? `${item.base_rank} -> ${item.final_rank}` : "--";
    return `<tr>
      <td><strong>${esc(shortSymbol(item.symbol))}</strong></td>
      <td class="text-right mono">${fmtNum(item.ml_zscore, 3)}</td>
      <td class="text-right mono">${fmtNum(item.ml_overlay_score, 3)}</td>
      <td class="text-right ${scoreClass}">${fmtSigned(scoreDelta, 3)}</td>
      <td class="text-right mono">${esc(rankText)}${rankDelta ? ` (${rankDelta > 0 ? "+" : ""}${rankDelta})` : ""}</td>
    </tr>`;
  }).join("");

  const contributorTable = contributors.length ? `<div class="table-shell" style="margin-bottom:12px">
    <table>
      <thead>
        <tr><th>币种</th><th class="text-right">Z分</th><th class="text-right">叠加分</th><th class="text-right">分数变化</th><th class="text-right">排名</th></tr>
      </thead>
      <tbody>${contributorRows}</tbody>
    </table>
  </div>` : '<div class="empty" style="margin-bottom:12px">旁路模型归因明细还没出来。</div>';

  const movers = `<div class="grid2">
    ${renderShadowMlMoverList("抬升最多", promotedRows, "这一轮没有明显被抬升的币。")}
    ${renderShadowMlMoverList("压低最多", suppressedRows, "这一轮没有明显被压低的币。")}
  </div>`;

  const footer = `<div class="subtle" style="margin-top:12px">
    ${esc(`工作区 ${payload.workspace || "--"} / 轮次 ${payload.run_id || "--"} / ${ml.reason ? `原因 ${ml.reason}` : "旁路调优版 XGBoost"}`)}
  </div>`;

  setHtml("ml-impact-content", `${summaryCards}${contributorTable}${movers}${footer}`);
}

function voteCard(label, vote, cache, opts = {}) {
  const conf = Number(vote?.confidence || 0);
  const state = vote?.state || "--";
  const stateKey = String(state || "").toUpperCase();
  const voteToneClass = stateKey === "TRENDING"
    ? "vote-trending"
    : (stateKey === "SIDEWAYS" ? "vote-sideways" : (stateKey === "RISK_OFF" ? "vote-riskoff" : ""));
  const freshness = cache?.status || "missing";
  const tone = cache ? (freshness === "fresh" ? "tone-positive" : (freshness === "stale" ? "tone-warn" : "tone-negative")) : "tone-positive";
  const status = vote?.error ? statusZh(vote.error) : `${stateZh(state)} / ${(conf * 100).toFixed(1)}%`;
  const extras = [
    vote?.weight != null ? `权重 ${fmtNum(vote.weight, 2)}` : null,
    vote?.trigger ? `触发 ${vote.trigger}` : null,
    vote?.breadth != null ? `广度 ${fmtNum(pctVal(vote.breadth), 0)}%` : null,
    vote?.positive_weight_share != null ? `多头 ${fmtNum(pctVal(vote.positive_weight_share), 0)}%` : null,
    cache?.age_minutes != null ? `${fmtNum(cache.age_minutes, 1)} 分钟前` : null,
  ].filter(Boolean).slice(0, 3);
  const rows = opts.showHmmProbs ? renderHmmProbRows(vote) : (opts.showStateBars ? renderDerivedVoteRows(vote) : "");
  const summary = opts.showSummary ? (vote?.summary_short || vote?.summary || "") : "";
  return `<div class="vote ${voteToneClass}">
    <div class="vote-head">
      <div class="vote-title">${esc(label)}</div>
      ${statePill(state)}
    </div>
    <div class="vote-value">${esc(status)}</div>
    <div class="vote-meta">
      ${cache ? `<span class="tiny ${tone}">缓存 ${esc(cacheZh(freshness))}</span>` : ""}
      ${extras.map((item) => `<span class="tiny">${esc(item)}</span>`).join("")}
    </div>
    ${rows}
    ${summary ? `<div class="vote-summary">${esc(summary)}</div>` : ""}
  </div>`;
}

function renderPositions(positions) {
  const items = Array.isArray(positions) ? positions.slice().sort((a, b) => Number(b.value || 0) - Number(a.value || 0)) : [];
  setText("positions-count-top", `${items.length} 仓`);
  setText("positions-subtitle", items.length ? `${items.length} 仓 · 按市值排序` : "当前无有效持仓");

  if (!items.length) {
    setHtml("positions-content", '<div class="empty">当前没有有效持仓。</div>');
    return;
  }

  if (isMobileViewport()) {
    setHtml("positions-content", `<div class="position-grid">${items.map((p) => {
      const pnlValueRaw = p.pnl ?? p.pnl_value;
      const pnlPctRaw = p.pnlPercent ?? p.pnl_pct;
      const pnlValue = Number(pnlValueRaw ?? 0);
      const pnlPct = Number(pnlPctRaw ?? 0);
      const qty = p.qty ?? p.quantity;
      const currentPrice = p.currentPrice ?? p.price;
      const pnlClass = pnlValue >= 0 ? "text-green" : "text-red";
      return `<div class="mobile-item">
        <div class="position-card-top">
          <div>
            <strong>${esc(p.symbol || "--")}</strong>
            <div class="subtle">${fmtUsd(p.value)} 市值</div>
          </div>
          <div class="${pnlClass}">${fmtPct(pnlPct, 2)}</div>
        </div>
        <div class="position-card-kpi">
          <div><span class="mini-label">数量</span><span class="mini-value mono">${fmtNum(qty, 4)}</span></div>
          <div><span class="mini-label">现价</span><span class="mini-value mono">${fmtUsd(currentPrice)}</span></div>
          <div><span class="mini-label">浮盈亏</span><span class="mini-value ${pnlClass}">${fmtUsd(pnlValue)}</span></div>
          <div><span class="mini-label">方向</span><span class="mini-value">${pnlValue >= 0 ? "盈利" : "回撤"}</span></div>
        </div>
      </div>`;
    }).join("")}</div>`);
    return;
  }

  const rows = items.map((p) => {
    const pnlValueRaw = p.pnl ?? p.pnl_value;
    const pnlPctRaw = p.pnlPercent ?? p.pnl_pct;
    const pnlValue = Number(pnlValueRaw ?? 0);
    const pnlPct = Number(pnlPctRaw ?? 0);
    const qty = p.qty ?? p.quantity;
    const currentPrice = p.currentPrice ?? p.price;
    const pnlClass = pnlValue >= 0 ? "text-green" : "text-red";
    return `<tr>
      <td><strong>${esc(p.symbol || "--")}</strong></td>
      <td class="text-right mono">${fmtNum(qty, 4)}</td>
      <td class="text-right mono">${fmtUsd(currentPrice)}</td>
      <td class="text-right mono">${fmtUsd(p.value)}</td>
      <td class="text-right">
        <div class="${pnlClass}">${fmtUsd(pnlValue)}</div>
        <div class="subtle">${fmtPct(pnlPct, 2)}</div>
      </td>
    </tr>`;
  }).join("");
  setHtml("positions-content", `<div class="table-shell">
    <table>
      <thead>
        <tr><th>标的</th><th class="text-right">数量</th><th class="text-right">现价</th><th class="text-right">市值</th><th class="text-right">浮盈亏 / 收益率</th></tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`);
}

function renderTrades(trades) {
  const items = Array.isArray(trades) ? trades.slice(0, isMobileViewport() ? 4 : 6) : [];
  if (!items.length) {
    setHtml("trades-content", '<div class="empty">最近没有成交。</div>');
    return;
  }
  setHtml("trades-content", items.map((trade) => {
    const buy = String(trade.side || "").toLowerCase() === "buy";
    return `<div class="trade">
      <div class="row">
        <div>
          <div><strong>${esc(trade.symbol || "--")}</strong></div>
          <div class="subtle">${esc(trade.timestamp || trade.time || "--")}</div>
        </div>
        <div class="text-right">
          <div class="${buy ? "text-green" : "text-red"}"><strong>${buy ? "买入" : "卖出"}</strong></div>
          <div class="mono">${fmtUsd(trade.value || trade.amount)}</div>
        </div>
      </div>
    </div>`;
  }).join(""));
}

function renderAlpha(alphaScores) {
  const items = Array.isArray(alphaScores)
    ? alphaScores.slice()
      .sort((a, b) => {
        const aRank = Number(a?.rank || 0);
        const bRank = Number(b?.rank || 0);
        if (aRank > 0 && bRank > 0 && aRank !== bRank) return aRank - bRank;
        const aScore = Number(a?.display_score ?? a?.score ?? 0);
        const bScore = Number(b?.display_score ?? b?.score ?? 0);
        return bScore - aScore;
      })
      .slice(0, isMobileViewport() ? 4 : 6)
    : [];
  if (!items.length) {
    setHtml("alpha-content", '<div class="empty">当前没有高分标的。</div>');
    return;
  }
  setHtml("alpha-content", items.map((item, index) => {
    const displayScore = Number(item.display_score ?? item.score ?? 0);
    const rawScore = Number(item.raw_score ?? displayScore);
    const rank = Number(item.rank || index + 1);
    const absScore = Math.abs(displayScore);
    const width = absScore > 0 ? Math.max(4, Math.min(50, absScore * 50)) : 0;
    const fill = width <= 0
      ? ""
      : (displayScore >= 0
        ? `<span class="bar-fill positive" style="left:50%;width:${width}%"></span>`
        : `<span class="bar-fill negative" style="right:50%;width:${width}%"></span>`);
    return `<div class="alpha">
      <div class="row"><strong>${esc(`${rank ? `#${rank} ` : ""}${item.symbol || "--"}`)}</strong><span class="mono ${displayScore >= 0 ? "text-green" : "text-red"}">${fmtNum(displayScore, 3)}</span></div>
      <div class="bar bidirectional">${fill}</div>
      <div class="subtle">原始 ${fmtNum(rawScore, 3)}</div>
    </div>`;
  }).join(""));
}

function renderSystem(status, timers, cost, ic, ml, reflection) {
  const errs = Array.isArray(status.errors) ? status.errors : [];
  const prioritized = ["v5-prod.user.timer", "v5-event-driven.timer", "v5-reconcile.timer", "v5-daily-ml-training.timer", "v5-model-promotion-gate.timer"];
  const sortedTimers = (Array.isArray(timers) ? timers.slice() : [])
    .sort((a, b) => {
      const ai = prioritized.indexOf(a.name);
      const bi = prioritized.indexOf(b.name);
      return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
    })
    .slice(0, 5);

  const cards = [
    `<div class="ops">
      <div class="label">系统状态</div>
      <div class="value">${status.isRunning ? "运行中" : "未运行"}</div>
      <div class="subtle">${modeZh(status.mode || "--")}${errs.length ? ` · ${errs.map(messageZh).slice(0, 2).join(" / ")}` : ""}</div>
    </div>`,
    ...sortedTimers.map((timer) => `<div class="ops">
      <div class="label">${esc(timer.desc || timer.name || "定时器")}</div>
      <div class="value">${statusZh(timer.active ? "active" : (timer.enabled ? "armed" : "idle"))}</div>
      <div class="subtle">${esc(timer.next_run || "暂无下一次")}</div>
    </div>`),
    `<div class="ops">
      <div class="label">研究链路</div>
      <div class="value">${esc(statusZh(ml.status || "--"))}</div>
      <div class="subtle">成本 ${esc(statusZh(cost.status || "--"))} · 因子诊断 ${esc(statusZh(ic.status || "--"))} · 复盘 ${(Array.isArray(reflection.reports) ? reflection.reports.length : 0)} 份</div>
    </div>`,
  ];

  setHtml("system-content", cards.join(""));
}

function renderDashboard(payload) {
  dashboardCache = payload;
  const account = payload.account || {};
  const positions = Array.isArray(payload.positions) ? payload.positions : [];
  const trades = Array.isArray(payload.trades) ? payload.trades : [];
  const alpha = Array.isArray(payload.alphaScores) ? payload.alphaScores : [];
  const status = payload.systemStatus || {};
  const timers = Array.isArray(payload.timers?.timers) ? payload.timers.timers : [];
  const cost = payload.costCalibration || {};
  const ic = payload.icDiagnostics || {};
  const ml = payload.mlTraining || {};
  const reflection = payload.reflectionReports || {};
  latestAccount = account;
  latestPositions = positions.slice().sort((a, b) => Number(b.value || 0) - Number(a.value || 0));

  setText("update-time", `最近刷新 ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`);
  document.getElementById("update-time")?.setAttribute("data-legacy-refresh-label", "鏈€杩戝埛鏂?");
  setText("total-equity", fmtUsd(account.totalEquity));
  setHtml("total-pnl", `${fmtPct(account.totalPnlPercent)} <span class="subtle">盈亏 ${fmtUsd(account.totalPnl)}</span>`);
  setText("cash-usdt", fmtUsdt(account.cash));
  setText("cash-ratio", account.totalEquity ? `${fmtNum((account.cash / account.totalEquity) * 100, 1)}% 现金` : "--");
  setText("positions-value", fmtUsd(account.positionsValue));

  renderPositions(positions);
  syncPositionSpotlight(latestPositions, latestAccount, { force: false });
  renderTrades(trades);
  renderAlpha(alpha);
  renderSystem(status, timers, cost, ic, ml, reflection);
}

function renderRiskPanels(risk, account) {
  const level = String(risk?.current_level || "NEUTRAL").toUpperCase();
  const metrics = risk?.metrics || {};
  const dd = account?.drawdown_pct??metrics.dd_pct??metrics.last_dd_pct??null;
  const conversionRate = metrics.conversion_rate??metrics.last_conversion_rate??null;
  setHtml("risk-level", riskPill(level));
  setText("risk-detail", `回撤 ${dd == null ? "--" : `${fmtNum(pctVal(dd), 1)}%`} · 上限 ${riskZh(level)}`);
  setHtml("current-drawdown", dd == null ? "--" : fmtPct(dd, 1));
  setText("conversion-rate", conversionRate == null ? "--" : `${fmtNum(pctVal(conversionRate), 0)}%`);
}

function renderMarket(payload) {
  marketCache = payload;
  const state = String(payload.state || "SIDEWAYS").toUpperCase();
  const votes = payload.votes || {};
  const signal = payload.signal_health || {};
  const alerts = Array.isArray(payload.alerts) ? payload.alerts : [];
  const history = Array.isArray(payload.history_24h) ? payload.history_24h : [];
  const alertHtml = alerts.length
    ? `<div style="grid-column:1/-1">${alerts.slice(0, 4).map((item) => `<span class="alert">${esc(messageZh(item))}</span>`).join("")}</div>`
    : "";

  setHtml("market-state-val", statePill(state));
  setText("market-state-detail", `${stateZh(state)} / ${methodZh(payload.method)}`);
  setText("market-detail", `${fmtNum(payload.position_multiplier, 2)}x 仓位 · ${alerts.length ? alerts.slice(0, 2).map(messageZh).join(" / ") : "无额外告警"}`);
  setText("position-multiplier", `${fmtNum(payload.position_multiplier, 2)}x`);

  const html = [
    voteCard("HMM", votes.hmm || {}, null, { showHmmProbs:true }),
    voteCard("资金费率", votes.funding || {}, signal.funding || null, { showStateBars:true }),
    voteCard("RSS", votes.rss || {}, signal.rss || null, { showStateBars:true, showSummary:true }),
  ].join("");

  setHtml("ensemble-votes", html + alertHtml);
  setHtml("vote-history", renderVoteHistory(history));
}

function renderSignals(data) {
  const strategies = Array.isArray(data.strategy_signals) ? data.strategy_signals : [];
  const counts = data.counts || {};
  const mlCard = renderMlImpactCardClean(data.ml_signal_overview || null);
  const sourceMap = {
    decision_audit: "当前轮审计",
    strategy_file: "当前轮信号文件",
    previous_run_decision_audit: "上一轮审计回退",
    previous_run_strategy_file: "上一轮信号文件回退",
    missing: "当前轮缺失",
  };
  const sourceText = sourceMap[String(data.strategy_signal_source || "missing")] || "当前轮缺失";
  setText("signals-time", data.run_id ? `运行 ${data.run_id}` : "等待策略信号...");

  if (!strategies.length && !mlCard) {
    setHtml("signals-content", '<div class="empty">当前没有策略信号。</div>');
    return;
  }

  const summary = `<div class="grid2" style="margin-bottom:12px">
    <div class="info"><div class="label">入池</div><div class="value">${counts.selected || 0}</div><div class="subtle">进入候选池</div></div>
    <div class="info"><div class="label">订单</div><div class="value">${(counts.orders_rebalance || 0) + (counts.orders_exit || 0)}</div><div class="subtle">本轮尝试</div></div>
  </div>`;
  const strategyCards = strategies.map((item) => `<div class="signal">
    <div class="label">${esc(item.strategy || "策略")}</div>
    <div class="value">${item.total_signals || 0} 个信号</div>
    <div class="subtle">买 ${item.buy_signals || 0} / 卖 ${item.sell_signals || 0} / 配置 ${(Number(item.allocation || 0) * 100).toFixed(0)}%</div>
  </div>`);
  const cards = [mlCard, ...strategyCards].filter(Boolean).join("");

  setHtml("signals-content", `${summary}<div class="signal-grid">${cards}</div>`);
}

function renderSignalsV2(data) {
  const strategies = Array.isArray(data.strategy_signals) ? data.strategy_signals : [];
  const counts = data.counts || {};
  const mlCard = renderMlImpactCardClean(data.ml_signal_overview || null);
  const sourceMap = {
    decision_audit: "当前轮审计",
    strategy_file: "当前轮信号文件",
    previous_run_decision_audit: "上一轮审计回退",
    previous_run_strategy_file: "上一轮信号文件回退",
    missing: "当前轮缺失",
  };
  const sourceText = sourceMap[String(data.strategy_signal_source || "missing")] || "当前轮缺失";
  setText("signals-time", data.run_id ? `运行 ${data.run_id}` : "等待策略信号...");

  if (!strategies.length && !mlCard) {
    setHtml("signals-content", '<div class="empty">当前轮没有策略信号。</div>');
    return;
  }

  const summary = `<div class="grid2" style="margin-bottom:12px">
    <div class="info"><div class="label">入池</div><div class="value">${counts.selected || 0}</div><div class="subtle">进入候选池</div></div>
    <div class="info"><div class="label">订单</div><div class="value">${(counts.orders_rebalance || 0) + (counts.orders_exit || 0)}</div><div class="subtle">本轮尝试</div></div>
    <div class="info"><div class="label">信号源</div><div class="value">${esc(sourceText)}</div><div class="subtle">${esc(data.strategy_run_id || data.run_id || "--")}</div></div>
  </div>`;
  const strategyCards = strategies.map((item) => {
    const preview = Array.isArray(item.signals) ? item.signals.slice(0, 3) : [];
    const previewHtml = preview.length
      ? `<div class="subtle" style="margin-top:10px">${preview.map((signal) => {
          const symbol = shortSymbol(signal.symbol || "--");
          const side = sideZh(signal.side || signal.direction || "--");
          const score = fmtNum(signal.score, 2);
          return `${esc(symbol)} ${esc(side)} ${esc(score)}`;
        }).join(" / ")}</div>`
      : '<div class="subtle" style="margin-top:10px">本轮无样例信号</div>';
    return `<div class="signal">
      <div class="label">${esc(item.strategy || "策略")}</div>
      <div class="value">${item.total_signals || 0} 个信号</div>
      <div class="subtle">买 ${item.buy_signals || 0} / 卖 ${item.sell_signals || 0} / 配置 ${(Number(item.allocation || 0) * 100).toFixed(0)}%</div>
      ${previewHtml}
    </div>`;
  });
  const cards = [mlCard, ...strategyCards].filter(Boolean).join("");

  setHtml("signals-content", `${summary}<div class="signal-grid">${cards}</div>`);
}

function renderDecision(data) {
  decisionCache = data;
  const counts = data.counts || {};
  const selected = Array.isArray(data.selected_orders) ? data.selected_orders : [];
  const blocked = Array.isArray(data.blocked_routes) ? data.blocked_routes : [];
  const runOrders = Array.isArray(data.run_orders) ? data.run_orders : [];
  const exec = data.execution_summary || {};
  const latestFill = data.recent_fill_summary?.latest_fill || null;

  const topSelected = selected.slice(0, 4).map((item) => `${item.symbol} ${sideZh(item.side || "")}`).join(" / ") || "无";
  const topBlocked = blocked.slice(0, 4).map((item) => `${item.symbol}:${messageZh(item.reason)}`).join(" / ") || "无";
  const fillText = latestFill ? `${latestFill.inst_id || "--"} ${sideZh(latestFill.side || "")} ${fmtUsd(latestFill.notional_usdt || 0)}` : "无";
  const rows = runOrders.slice(0, isMobileViewport() ? 4 : 6).map((item) => `<tr>
    <td>${esc(item.inst_id || "--")}</td>
    <td>${esc(sideZh(item.side || "--"))}</td>
    <td>${esc(orderStateZh(item.state || "--"))}</td>
    <td class="text-right mono">${fmtUsd(item.notional_usdt || 0)}</td>
  </tr>`).join("");

  setText("decision-flow-time", data.run_id ? `运行 ${data.run_id}` : "等待执行路径...");
  setHtml("decision-flow-content", `<div class="decision-grid">
    <div class="decision"><div class="label">执行结果</div><div class="value">成交 ${exec.filled || 0} / 总计 ${exec.total || 0}</div><div class="subtle">拒绝 ${exec.rejected || 0} / 挂单 ${exec.open_or_partial || 0}</div></div>
    <div class="decision"><div class="label">已选订单</div><div class="value">${esc(topSelected)}</div></div>
    <div class="decision"><div class="label">被拦路径</div><div class="value">${esc(topBlocked)}</div></div>
    <div class="decision"><div class="label">最近成交</div><div class="value">${esc(fillText)}</div></div>
    <div class="decision">
      <div class="label">本轮订单</div>
      ${rows ? `<div class="table-shell" style="margin-top:12px">
        <table>
          <thead><tr><th>标的</th><th>方向</th><th>状态</th><th class="text-right">金额</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>` : '<div class="subtle" style="margin-top:10px">当前这轮没有订单尝试。</div>'}
    </div>
  </div>`);
}

function renderHealth(data) {
  healthCache = data;
  const checks = Array.isArray(data.checks) ? data.checks : [];
  const warningCount = Number(data.warning_count || 0);
  const criticalCount = Number(data.critical_count || 0);
  const summary = [
    `上次 ${data.last_update || data.timestamp || "--"}`,
    criticalCount > 0 ? `${criticalCount} 严重` : null,
    warningCount > 0 ? `${warningCount} 告警` : (criticalCount === 0 ? "全部正常" : null),
  ].filter(Boolean).join(" · ");

  setText("health-update-time", statusZh(data.status || "warning"));
  setText("health-summary", summary);

  if (!checks.length) {
    setHtml("health-content", '<div class="empty">当前没有健康检查结果。</div>');
    return;
  }

  setHtml("health-content", checks.map((check) => `<div class="health">
    <div class="row">
      <strong>${esc(check.name || "--")}</strong>
      <span class="tiny ${healthTone[check.status] || "tone-warn"}">${esc(statusZh(check.status || "--"))}</span>
    </div>
    <div class="subtle" style="margin-top:10px">${esc(messageZh(check.detail || "--"))}</div>
  </div>`).join(""));
}

async function loadDashboard(signal) {
  const data = await fetchJson("/api/dashboard", signal);
  if (data) renderDashboard(data);
}

async function loadRisk(signal) {
  const [risk, account] = await Promise.all([
    fetchJson("/api/auto_risk_guard", signal),
    fetchJson("/api/account", signal),
  ]);
  if (!risk) return;
  riskCache = { risk, account };
  renderRiskPanels(risk, account);
}

async function loadMarket(signal) {
  const data = await fetchJson("/api/market_state", signal);
  if (data) renderMarket(data);
}

async function loadDecision(signal) {
  const data = await fetchJson("/api/decision_audit", signal);
  if (data) {
    renderSignalsV2(data);
    renderDecision(data);
  }
}

async function loadShadowMl(signal) {
  const data = await fetchJson("/api/shadow_ml_overlay", signal);
  renderShadowMlPanel(data || { available: false, error: "旁路模型归因数据暂不可用" });
}

async function loadHealth(signal) {
  const data = await fetchJson("/api/health", signal);
  if (data) renderHealth(data);
}

function cancelSecondaryWork() {
  if (secondaryTimer) {
    clearTimeout(secondaryTimer);
    secondaryTimer = null;
  }
  if (secondaryIdleHandle && "cancelIdleCallback" in window) {
    cancelIdleCallback(secondaryIdleHandle);
    secondaryIdleHandle = null;
  }
}

function scheduleSecondaryLoad(signal) {
  cancelSecondaryWork();
  const run = () => {
    secondaryTimer = null;
    secondaryIdleHandle = null;
    Promise.allSettled([loadDecision(signal), loadShadowMl(signal), loadHealth(signal)]);
  };
  if ("requestIdleCallback" in window) {
    secondaryIdleHandle = requestIdleCallback(run, { timeout: 1200 });
  } else {
    secondaryTimer = window.setTimeout(run, 80);
  }
}

async function loadAll() {
  if (document.hidden) return;
  cancelSecondaryWork();
  if (activeController) activeController.abort();
  activeController = new AbortController();
  const signal = activeController.signal;
  setText("update-time", "刷新中...");
  await Promise.allSettled([loadDashboard(signal), loadRisk(signal), loadMarket(signal)]);
  if (signal.aborted) return;
  scheduleSecondaryLoad(signal);
}

function rerenderForViewport() {
  if (dashboardCache) renderDashboard(dashboardCache);
  if (riskCache.risk) renderRiskPanels(riskCache.risk, riskCache.account);
  if (marketCache) renderMarket(marketCache);
  if (decisionCache) {
    renderSignalsV2(decisionCache);
    renderDecision(decisionCache);
  }
  if (shadowMlCache) renderShadowMlPanel(shadowMlCache);
  if (healthCache) renderHealth(healthCache);
}

function debounce(fn, wait) {
  let timer = 0;
  return (...args) => {
    clearTimeout(timer);
    timer = window.setTimeout(() => fn(...args), wait);
  };
}

function normalizeHeroCopy() {
  const headline = document.querySelector(".hero-headline");
  if (headline) headline.textContent = "生产交易总览";
  const copy = document.querySelector(".hero-copy");
  if (copy) {
    copy.textContent = "集中查看市场状态、风控档位、候选信号、持仓与执行结果。页面只保留当前决策真正需要的信息。";
  }
}

document.addEventListener("visibilitychange", () => {
  if (!document.hidden) loadAll();
});

window.addEventListener("resize", debounce(() => {
  const mobile = isMobileViewport();
  if (mobile === lastMobileState) return;
  lastMobileState = mobile;
  rerenderForViewport();
}, 120), { passive: true });

loadAll();
normalizeHeroCopy();
refreshTimer = window.setInterval(() => {
  loadAll();
}, REFRESH_MS);

// legacy-test: 娴泩浜?/ 鏀剁泭鐜?
