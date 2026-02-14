from configs.loader import load_config


def test_config_loads():
    cfg = load_config("configs/config.yaml", env_path=None)
    assert cfg.symbols
    assert cfg.timeframe_main == "1h"
