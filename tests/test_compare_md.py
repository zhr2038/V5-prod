import json
from pathlib import Path

from scripts.compare_runs import compare


def test_compare_md_contains_keys(tmp_path):
    v4 = {"run_id": "v4", "num_trades": 1}
    v5 = {"run_id": "v5", "num_trades": 2}
    md = compare(v4, v5, window="[a,b]")
    assert "num_trades" in md
    assert "window" in md
    assert "v4" in md and "v5" in md
