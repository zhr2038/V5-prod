const ML_STATUS_REFRESH_MS = 30000;

const ML_PHASE_LABELS = {
  live_active: "实盘已使用",
  promoted: "门控已通过",
  trained: "模型已训练",
  collecting: "采样中",
  no_data: "无数据",
};

const ML_STAGE_CONFIG = [
  { key: "sampling", title: "采样", summary: (data) => `${Number(data.labeled_samples || 0)} / ${Number(data.samples_needed || 0) || "--"}` },
  { key: "trained", title: "训练", summary: (data) => data.last_training_ts || "未训练" },
  { key: "promoted", title: "门控", summary: (data) => (Array.isArray(data.promotion_fail_reasons) && data.promotion_fail_reasons.length ? "未通过" : "待评估") },
  { key: "liveActive", title: "实盘", summary: (data) => data.last_runtime_ts || "未启用" },
];

function mlText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function mlHtml(id, html) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = html;
}

function mapMlPhaseLabel(phase) {
  return ML_PHASE_LABELS[String(phase || "")] || "--";
}

function buildMlDetail(data) {
  const parts = [];
  if (data.total_samples != null && data.labeled_samples != null) {
    parts.push(`样本 ${Number(data.labeled_samples || 0)} / ${Number(data.total_samples || 0)}`);
  }
  if (data.last_ic != null) {
    parts.push(`信息系数 ${Number(data.last_ic).toFixed(3)}`);
  }
  if (data.runtime_prediction_count != null && Number(data.runtime_prediction_count) > 0) {
    parts.push(`推理 ${Number(data.runtime_prediction_count)} 次`);
  }
  if (data.runtime_reason && data.runtime_reason !== "ok") {
    parts.push(`原因 ${data.runtime_reason}`);
  }
  if (Array.isArray(data.promotion_fail_reasons) && data.promotion_fail_reasons.length) {
    parts.push(`门控 ${data.promotion_fail_reasons.slice(0, 2).join(" / ")}`);
  }
  return parts.join(" · ") || "等待机器学习链路状态...";
}

function buildMlMeta(data) {
  const stages = data.stages || {};
  return [
    `采样 ${stages.sampling ? "是" : "否"}`,
    `训练 ${stages.trained ? "是" : "否"}`,
    `门控 ${stages.promoted ? "是" : "否"}`,
    `实盘 ${stages.liveActive ? "是" : "否"}`,
  ];
}

function buildStageCards(data) {
  const stages = data.stages || {};
  return ML_STAGE_CONFIG.map((stage) => {
    const active = Boolean(stages[stage.key]);
    let note = stage.summary(data);
    if (stage.key === "promoted" && active && data.last_promotion_ts) {
      note = data.last_promotion_ts;
    }
    if (stage.key === "liveActive" && active && data.runtime_reason && data.runtime_reason !== "ok") {
      note = data.runtime_reason;
    }
    return `<div class="ml-stage ${active ? "is-on" : "is-off"}">
      <div class="ml-stage-top">
        <div class="label">${stage.title}</div>
        <span class="ml-stage-state"></span>
      </div>
      <div class="ml-stage-value">${active ? "已到位" : "未完成"}</div>
      <div class="ml-stage-note">${note || "--"}</div>
    </div>`;
  }).join("");
}

async function fetchMlStageStatus() {
  const resp = await fetch(`/api/ml_training?_=${Date.now()}`, {
    cache: "no-store",
    headers: { Accept: "application/json" },
  });
  if (typeof resp.ok !== "undefined" && !resp.ok) {
    throw new Error(`HTTP ${resp.status}`);
  }
  return resp.json();
}

async function loadMlStagePanel() {
  const band = document.getElementById("ml-stage-band");
  if (!band) return;

  try {
    const data = await fetchMlStageStatus();
    const phaseLabel = mapMlPhaseLabel(data.phase || data.status);
    const progress = Math.max(0, Math.min(100, Number(data.progress_percent || 0)));

    mlText("ml-stage-status", phaseLabel);
    mlText("ml-stage-detail", buildMlDetail(data));
    mlHtml("ml-stage-meta", buildMlMeta(data).map((item) => `<span class="tiny">${item}</span>`).join(""));
    mlHtml("ml-stage-grid", buildStageCards(data));

    const progressEl = document.getElementById("ml-stage-progress");
    if (progressEl) progressEl.style.width = `${progress}%`;
  } catch (err) {
    console.error("failed to load ml stage status", err);
    mlText("ml-stage-status", "--");
    mlText("ml-stage-detail", "机器学习链路状态获取失败");
    mlHtml("ml-stage-meta", '<span class="tiny tone-negative">状态异常</span>');
    mlHtml("ml-stage-grid", '<div class="error">机器学习链路状态获取失败</div>');
    const progressEl = document.getElementById("ml-stage-progress");
    if (progressEl) progressEl.style.width = "0%";
  }
}

loadMlStagePanel();
window.setInterval(loadMlStagePanel, ML_STATUS_REFRESH_MS);
