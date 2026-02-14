# v5-trading-bot

V5 cross-sectional trend rotation system (OKX spot), **dry-run first**.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# dry-run (default uses MockProvider)
python main.py

# run tests
pytest -q
```

### Use OKX public market data (optional)

```bash
export V5_DATA_PROVIDER=okx
python main.py
```

## Artifacts

After `python main.py`:
- `reports/alpha_snapshot.json`
- `reports/regime.json`
- `reports/portfolio.json`
- `reports/execution_report.json`
- `reports/slippage.sqlite` (dry-run placeholder records)

## Notes

- No shorting in v5 phase-1.
- No leverage.
- Execution engine is dry-run scaffold; live execution intentionally not implemented yet.
