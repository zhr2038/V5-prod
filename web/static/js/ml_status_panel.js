const ML_STATUS_REFRESH_MS = 30000;

function ensureMlStageCard() {
  const row = document.querySelector(".metric-row");
  if (!row) return null;

  let card = document.getElementById("ml-stage-card");
  if (card) return card;

  card = document.createElement("div");
  card.className = "metric";
  card.id = "ml-stage-card";
  card.innerHTML = [
    '<div class="metric-kicker">ML 链路</div>',
    '<div class="metric-value" id="ml-stage-status">--</div>',
    '<div class="muted" id="ml-stage-detail">等待 ML 链路状态...</div>',
  ].join("");
  row.appendChild(card);
  return card;
}

function mapMlPhaseLabel(phase) {
  switch (phase) {
    case "live_active":
      return "实盘已使用";
    case "promoted":
      return "门控已通过";
    case "trained":
      return "模型已训练";
    case "collecting":
      return "采样中";
    case "no_data":
      return "无样本";
    default:
      return "--";
  }
}

function buildMlStageDetail(data) {
  const parts = [];

  if (data.display_status) parts.push(data.display_status);
  if (data.last_training_ts) parts.push(`最近训练 ${data.last_training_ts}`);
  if (data.last_promotion_ts) parts.push(`最近门控 ${data.last_promotion_ts}`);
  if (data.last_runtime_ts) parts.push(`最近实盘使用 ${data.last_runtime_ts}`);
  if (data.runtime_reason && data.runtime_reason !== "ok") parts.push(`运行原因 ${data.runtime_reason}`);
  if (Array.isArray(data.promotion_fail_reasons) && data.promotion_fail_reasons.length) {
    parts.push(`门控失败 ${data.promotion_fail_reasons.join(", ")}`);
  }

  return parts.join(" / ") || "等待 ML 链路状态...";
}

async function fetchMlStageStatus() {
  const resp = await fetch(`/api/ml_training?_=${Date.now()}`);
  if (!resp.ok) {
    throw new Error(`HTTP ${resp.status}`);
  }
  return resp.json();
}

async function loadMlStagePanel() {
  ensureMlStageCard();

  const statusEl = document.getElementById("ml-stage-status");
  const detailEl = document.getElementById("ml-stage-detail");
  if (!statusEl || !detailEl) return;

  try {
    const data = await fetchMlStageStatus();
    statusEl.textContent = mapMlPhaseLabel(data.phase || data.status);
    detailEl.textContent = buildMlStageDetail(data);
  } catch (err) {
    console.error("failed to load ml stage status", err);
    statusEl.textContent = "--";
    detailEl.textContent = "ML 链路状态获取失败";
  }
}

loadMlStagePanel();
window.setInterval(loadMlStagePanel, ML_STATUS_REFRESH_MS);
