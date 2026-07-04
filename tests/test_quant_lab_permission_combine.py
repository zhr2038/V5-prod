from __future__ import annotations

import itertools

from src.quant_lab_client.permissions import combine_permissions, normalize_permission


def test_combine_permissions_3x3() -> None:
    expected = {
        ("ALLOW", "ALLOW"): "ALLOW",
        ("ALLOW", "SELL_ONLY"): "SELL_ONLY",
        ("ALLOW", "ABORT"): "ABORT",
        ("SELL_ONLY", "ALLOW"): "SELL_ONLY",
        ("SELL_ONLY", "SELL_ONLY"): "SELL_ONLY",
        ("SELL_ONLY", "ABORT"): "ABORT",
        ("ABORT", "ALLOW"): "ABORT",
        ("ABORT", "SELL_ONLY"): "ABORT",
        ("ABORT", "ABORT"): "ABORT",
    }
    for local, quant in itertools.product(["ALLOW", "SELL_ONLY", "ABORT"], repeat=2):
        assert combine_permissions(local, quant) == expected[(local, quant)]


def test_unavailable_remote_permission_is_conservative_abort() -> None:
    assert normalize_permission("UNAVAILABLE") == "ABORT"
    assert normalize_permission("NO_FRESH_PERMISSION") == "ABORT"
    assert combine_permissions("ALLOW", "UNAVAILABLE") == "ABORT"
