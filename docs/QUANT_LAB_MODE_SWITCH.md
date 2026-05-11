# quant-lab Mode Switch

Current recommended rollout mode is `shadow`: V5 calls quant-lab and records telemetry, but quant-lab does not affect orders.

To completely skip quant-lab:

```bash
python scripts/quant_lab_mode.py set --mode local_only --reason "temporary disable quant-lab"
```

To restore observation:

```bash
python scripts/quant_lab_mode.py set --mode shadow --reason "restore quant-lab observation"
```

To enable only cost filtering:

```bash
python scripts/quant_lab_mode.py set --mode cost_only --reason "enable cost-only"
```

To enable only permission filtering:

```bash
python scripts/quant_lab_mode.py set --mode permission_only --reason "enable permission gate"
```

To enable full enforcement:

```bash
python scripts/quant_lab_mode.py set --mode enforce --reason "enable full quant-lab gate"
```

Show the effective mode:

```bash
python scripts/quant_lab_mode.py show --config configs/config.yaml
```

## Mode Effects

- `local_only`: does not call quant-lab API and does not affect trading.
- `shadow`: calls quant-lab API, records permission/cost telemetry, and only records hypothetical filters.
- `cost_only`: calls quant-lab cost API and may filter edge-insufficient orders.
- `permission_only`: applies quant-lab `ALLOW` / `SELL_ONLY` / `ABORT`, but does not apply cost gate.
- `enforce`: applies both quant-lab permission and cost gate.

Runtime override is stored in `state/quant_lab_mode.json`. The script only writes that file; it does not access OKX, does not call quant-lab, and does not place orders.
