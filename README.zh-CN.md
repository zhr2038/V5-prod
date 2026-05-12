# V5 生产交易程序

V5 是 OKX spot 真实交易执行程序。真实下单、撤单、成交同步、对账、账本、kill-switch 和 live preflight 都仍由 V5 本地负责。

## quant-lab 只读接入

V5 现在可以作为 quant-lab 的只读消费者运行：

- 启动和 live preflight 阶段读取 `GET /v1/risk/live-permission`。
- 下单前读取 `GET /v1/costs/estimate`。
- 按 `ALLOW` / `SELL_ONLY` / `ABORT` 过滤订单。
- 写入 `decision_audit.json`、`summary.json`、`reports/quant_lab_usage.jsonl`、`reports/quant_lab_requests.jsonl`。
- 导出脱敏 bundle 给 quant-lab 从 `qyun.hrhome.top` 拉取。

quant-lab 不交易、不写账户、不替代 V5 执行。V5 不写 quant-lab lake，也不向 quant-lab 推送数据。

当前 quant-lab bootstrap gate 是 `QUARANTINE`，risk permission 是 `SELL_ONLY`，因此 V5 当前不应新增 buy/open/rebalance 风险，只允许 sell/close/reduce-only。

上线模式通过 `quant_lab.mode` 和 `state/quant_lab_mode.json` 切换：

- `local_only`：完全跳过 quant-lab。
- `shadow`：调用 quant-lab 但只记录，不影响交易。
- `cost_only`：只启用成本过滤。
- `permission_only`：只启用权限过滤。
- `enforce`：成本和权限都生效。

## 常用命令

```bash
python scripts/quant_lab_selfcheck.py --config configs/config.yaml
python scripts/quant_lab_mode.py show --config configs/config.yaml
python scripts/export_v5_bundle.py --reports-dir reports --out-dir /var/lib/v5/exports/bundles
```

更多说明见：

- `docs/QUANT_LAB_INTEGRATION.md`
- `docs/V5_TELEMETRY_BUNDLE.md`

## Quant-lab 中台检查

每日 follow-up bundle 会输出 `summaries/quant_lab_config_audit.json`、`summaries/window_summary.json`、`summaries/quant_lab_compliance.csv`、`summaries/quant_lab_cost_usage.csv` 和 `summaries/quant_lab_fallbacks.csv`。

- `quant_lab_mode` / `quant_lab_mode_source`：确认模式来自配置还是 `state/quant_lab_mode.json`。
- `permission_gate_enforced` / `cost_gate_enforced`：确认中台门禁是否真正生效。
- `quant_lab_actual_filter_count` / `quant_lab_hypothetical_filter_count`：区分真实拦单和 shadow 观察。
- `quant_lab_request_success_count` / `quant_lab_request_error_count`：确认 API 请求是否成功。
- `quant_lab_fallback_count`：只统计真实 fallback。
- `allow_insecure_http_with_token` / `api_token_env` / `api_env_path_present`：确认公网 token 配置是否显式开启。
