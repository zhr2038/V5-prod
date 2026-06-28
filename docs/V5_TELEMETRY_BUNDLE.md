# V5 Telemetry Bundle

The V5 telemetry bundle is a sanitized local archive for quant-lab audit import, Web observation, expert analysis packs, and V5 behavior analysis. It is generated on the V5 host and pulled by quant-lab from `qyun.hrhome.top`.

## Command

```bash
python scripts/export_v5_bundle.py \
  --reports-dir reports \
  --out-dir /var/lib/v5/exports/bundles \
  --window-hours 72 \
  --keep-count 1000 \
  --max-age-days 7
```

Output files:

- `/var/lib/v5/exports/bundles/v5_live_followup_bundle_<YYYYMMDDTHHMMSSZ>.tar.gz`
- `/var/lib/v5/exports/bundles/v5_live_followup_bundle_<YYYYMMDDTHHMMSSZ>.tar.gz.sha256`

The exporter writes a `.tmp` archive first, renames it atomically, and writes the SHA256 sidecar after the archive is complete.

Retention is count- and age-bounded. The default keeps the latest 1000 complete bundle archives and removes archives older than 7 days, together with orphaned SHA256 sidecars. This prevents repeated manual exports from accumulating thousands of bundles while preserving the freshest research evidence.

## Structure

```text
raw/
  quant_lab/
    quant_lab_usage.jsonl
    quant_lab_requests.jsonl
  config/
    config.yaml
    live_prod.yaml
  logs/
    v5_runtime.log
summaries/
  quant_lab_compliance.csv
  quant_lab_permission_audit.csv
  quant_lab_mode_audit.csv
  quant_lab_cost_usage.csv
  quant_lab_fallbacks.csv
  enforce_readiness_snapshot.json
  window_summary.json
  issues_to_fix.json
manifest.json
```

`quant_lab_usage.jsonl` records permission, cost, fallback, mode, and order filter events. `quant_lab_cost_usage.csv` normalizes cost estimates and includes whether the cost gate was enforced or hypothetical. `quant_lab_compliance.csv` checks `ABORT`/`SELL_ONLY` compliance with `mode`, `hypothetical_violation`, and `actual_violation` so `shadow` observations do not count as live violations. `quant_lab_fallbacks.csv` lists fallback policy, mode, scope, and action taken.

`window_summary.json` includes Quant Lab cost-contract counters: `cost_degraded_count`, `global_default_cost_count`, `symbol_cost_hit_count`, and `cost_contract_version`. A `global_default` source, `GLOBAL_DEFAULT` fallback level, or `global_default_v0` model version is counted as a degraded cost model even when the HTTP request itself succeeded. The summary also separates old and current cost rows with `legacy_global_default_cost_count`, `current_contract_global_default_cost_count`, `latest_24h_global_default_cost_count`, `post_deployment_global_default_cost_count`, `cost_usage_legacy_rows`, `cost_usage_current_contract_rows`, and `cost_usage_latest_24h_rows` so readiness is not blocked by pre-current-contract history.

`quant_lab_mode_audit.csv` and `enforce_readiness_snapshot.json` record the requested mode, effective mode, readiness status, enforce blocked reasons, and contract/schema version checks. If `enforce` is requested while readiness is not `READY`, V5 reports `quant_lab_requested_mode=enforce`, `quant_lab_effective_mode=shadow`, and the blocked reason list.

## Redaction

The bundle must not include `.env` files or unredacted secrets. Redacted markers include API keys, API secrets, passphrases, private keys, OKX auth headers, bearer tokens, passwords, and `QUANT_LAB_API_TOKEN` values. Field names can remain, but values are replaced with `<REDACTED>`.

`manifest.json` contains:

```json
{
  "schema_version": "1.0.0",
  "contract_version": "v5.quant_lab.telemetry.v2",
  "config_hash": "<sha256>",
  "strategy_version": "5.0.0",
  "sanity_checks": {
    "no_env_files": true,
    "no_unredacted_secret_assignments": true,
    "redaction_applied": true,
    "secret_scan_findings_count": 0
  }
}
```

## systemd

Install the bundle service from `deploy/systemd/` on the V5 production host.
The live follow-up bundle is background telemetry for quant-lab ingest. Keep the
system timer enabled so qyun2 has fresh read-only V5 evidence before expert-pack
exports. The Web dashboard must not present these files as user-facing manual
packages; operator-facing downloads belong to the quant-lab expert-pack export
page.

```bash
sudo cp deploy/systemd/v5-export-bundle.* /etc/systemd/system/
sudo cp deploy/systemd/v5-live-followup-bundle-export.* /etc/systemd/system/
sudo cp deploy/systemd/v5-quant-lab-selfcheck.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now v5-export-bundle.timer
sudo systemctl enable --now v5-live-followup-bundle-export.timer
sudo systemctl enable --now v5-quant-lab-selfcheck.timer
```

The scheduled units are `v5-live-followup-bundle-export.service` and
`v5-live-followup-bundle-export.timer`. The live follow-up timer runs every
10 minutes so qyun2's 10-minute telemetry sync normally has a fresh completed
bundle to ingest instead of waiting for the next hourly boundary.

The remote bundle directory is `/var/lib/v5/exports/bundles`. quant-lab should pull only completed `.tar.gz` files with matching `.sha256` sidecars. If a production service invokes `scripts/generate_v5_bundle_remote.sh` directly, run `python scripts/prune_v5_bundles.py /var/lib/v5/exports/bundles --keep-count 1000 --max-age-days 7` after installing the latest archive.
