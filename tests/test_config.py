from configs.loader import load_config


def test_config_loads():
    cfg = load_config("configs/config.yaml", env_path=None)
    assert cfg.symbols
    assert cfg.timeframe_main == "1h"


def test_live_prod_exclude_symbols_loads():
    cfg = load_config("configs/live_prod.yaml", env_path=None)
    assert "PAXG/USDT" in cfg.universe.exclude_symbols
    assert "XAUT/USDT" in cfg.universe.exclude_symbols
