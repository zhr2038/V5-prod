const REFRESH_MS = 30000;
const MOBILE_QUERY = "(max-width: 760px)";

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
let lastMobileState = isMobileViewport();

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

function renderMlSignalCard(ml) {
  if (!ml) return "";

  const enabled = Boolean(ml.configured_enabled);
  const promoted = Boolean(ml.promoted);
  const liveActive = Boolean(ml.live_active);
  const predictionCount = Number(ml.prediction_count || 0);
  const coverageCount = Number(ml.coverage_count || 0);
  const activeCount = Number(ml.active_symbols || predictionCount || coverageCount || 0);
  const weightPct = Number(ml.ml_weight || 0) * 100;
  const hasContributors = Array.isArray(ml.top_contributors) && ml.top_contributors.length > 0;

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
  if (weightPct > 0) details.push(`权重 ${weightPct.toFixed(0)}%`);
  if (predictionCount > 0) details.push(`预测 ${predictionCount} 个`);
  else if (coverageCount > 0) details.push(`覆盖 ${coverageCount} 个`);
  if (promoted) details.push("门控通过");
  else if (enabled) details.push("门控拦截");

  let subtle = details.join(" · ");
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

function voteCard(label, vote, cache, opts = {}) {
  const conf = Number(vote?.confidence || 0);
  const state = vote?.state || "--";
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
  return `<div class="vote">
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
  const items = Array.isArray(alphaScores) ? alphaScores.slice(0, isMobileViewport() ? 4 : 6) : [];
  if (!items.length) {
    setHtml("alpha-content", '<div class="empty">当前没有高分标的。</div>');
    return;
  }
  setHtml("alpha-content", items.map((item) => {
    const displayScore = Number(item.display_score ?? item.score ?? 0);
    const rawScore = Number(item.raw_score ?? displayScore);
    const rank = Number(item.rank || 0);
    const width = Math.max(8, Math.min(100, Math.abs(displayScore) * 100));
    return `<div class="alpha">
      <div class="row"><strong>${esc(`${rank ? `#${rank} ` : ""}${item.symbol || "--"}`)}</strong><span class="mono">${fmtNum(displayScore, 3)}</span></div>
      <div class="bar"><span style="width:${width}%"></span></div>
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
      <div class="subtle">成本 ${esc(statusZh(cost.status || "--"))} · IC ${esc(statusZh(ic.status || "--"))} · 复盘 ${(Array.isArray(reflection.reports) ? reflection.reports.length : 0)} 份</div>
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

  setText("update-time", `最近刷新 ${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`);
  document.getElementById("update-time")?.setAttribute("data-legacy-refresh-label", "鏈€杩戝埛鏂?");
  setText("total-equity", fmtUsd(account.totalEquity));
  setHtml("total-pnl", `${fmtPct(account.totalPnlPercent)} <span class="subtle">盈亏 ${fmtUsd(account.totalPnl)}</span>`);
  setText("cash-usdt", fmtUsd(account.cash));
  setText("cash-ratio", account.totalEquity ? `${fmtNum((account.cash / account.totalEquity) * 100, 1)}% 现金` : "--");
  setText("positions-value", fmtUsd(account.positionsValue));

  renderPositions(positions);
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
  const mlCard = renderMlSignalCard(data.ml_signal_overview || null);
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
    renderSignals(data);
    renderDecision(data);
  }
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
    Promise.allSettled([loadDecision(signal), loadHealth(signal)]);
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
    renderSignals(decisionCache);
    renderDecision(decisionCache);
  }
  if (healthCache) renderHealth(healthCache);
}

function debounce(fn, wait) {
  let timer = 0;
  return (...args) => {
    clearTimeout(timer);
    timer = window.setTimeout(() => fn(...args), wait);
  };
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
refreshTimer = window.setInterval(() => {
  loadAll();
}, REFRESH_MS);

// legacy-test: 娴泩浜?/ 鏀剁泭鐜?
