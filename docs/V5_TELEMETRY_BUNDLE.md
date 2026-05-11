# V5 Telemetry Bundle

The V5 telemetry bundle is a sanitized local archive for quant-lab audit import, Web observation, expert analysis packs, and V5 behavior analysis. It is generated on the V5 host and pulled by quant-lab from `qyun.hrhome.top`.

## Command

```bash
python scripts/export_v5_bundle.py \
  --reports-dir reports \
  --out-dir /var/lib/v5/exports/bundles \
  --window-hours 72
```

Output files:

- `/var/lib/v5/exports/bundles/v5_live_followup_bundle_<YYYYMMDDTHHMMSSZ>.tar.gz`
- `/var/lib/v5/exports/bundles/v5_live_followup_bundle_<YYYYMMDDTHHMMSSZ>.tar.gz.sha256`

The exporter writes a `.tmp` archive first, renames it atomically, and writes the SHA256 sidecar after the archive is complete.

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
  quant_lab_cost_usage.csv
  quant_lab_fallbacks.csv
  window_summary.json
  issues_to_fix.json
manifest.json
```

`quant_lab_usage.jsonl` records permission, cost, fallback, mode, and order filter events. `quant_lab_cost_usage.csv` normalizes cost estimates and includes whether the cost gate was enforced or hypothetical. `quant_lab_compliance.csv` checks `ABORT`/`SELL_ONLY` compliance with `mode`, `hypothetical_violation`, and `actual_violation` so `shadow` observations do not count as live violations. `quant_lab_fallbacks.csv` lists fallback policy, mode, scope, and action taken.

## Redaction

The bundle must not include `.env` files or unredacted secrets. Redacted markers include API keys, API secrets, passphrases, private keys, OKX auth headers, bearer tokens, passwords, and `QUANT_LAB_API_TOKEN` values. Field names can remain, but values are replaced with `<REDACTED>`.

`manifest.json` contains:

```json
{
  "sanity_checks": {
    "no_env_files": true,
    "no_unredacted_secret_assignments": true,
    "redaction_applied": true,
    "secret_scan_findings_count": 0
  }
}
```

## systemd

Install the timer files from `deploy/systemd/` on the V5 production host:

```bash
sudo cp deploy/systemd/v5-export-bundle.* /etc/systemd/system/
sudo cp deploy/systemd/v5-quant-lab-selfcheck.* /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now v5-export-bundle.timer
sudo systemctl enable --now v5-quant-lab-selfcheck.timer
```

The remote bundle directory is `/var/lib/v5/exports/bundles`. quant-lab should pull only completed `.tar.gz` files with matching `.sha256` sidecars.
