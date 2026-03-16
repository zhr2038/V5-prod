from __future__ import annotations

# Smoke-level: ensure module imports.

def test_bootstrap_script_import() -> None:
    import scripts.bootstrap_from_okx_balance  # noqa: F401
