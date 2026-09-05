import json
import logging

import pytest

from configs.schema import ParticipationRuntimeConfig, RiskConfig
from src.reporting.participation_store import ParticipationStore


def test_legacy_ignored_risk_config_is_explicit(caplog):
    with caplog.at_level(logging.WARNING):
        cfg = RiskConfig(auto_risk_guard={"enabled": True})
    assert "has never controlled execution" in caplog.text
    assert cfg.max_positions_override is None


def test_configuration_cannot_promote_without_runtime_evidence():
    with pytest.raises(ValueError, match="forward_paper"):
        ParticipationRuntimeConfig(enabled=True, mode="live")


def test_portfolio_and_event_commit_together(tmp_path):
    store = ParticipationStore(tmp_path / "forward.sqlite")
    with store.transaction() as connection:
        assert store.load(connection, "locked-policy") is None
        store.save(connection, identity="locked-policy", state={"cash": 100},
                   event={"observed_ts": 3601, "bar_ts": 3600})
    with pytest.raises(RuntimeError):
        with store.transaction() as connection:
            store.save(connection, identity="locked-policy", state={"cash": 0},
                       event={"observed_ts": 7201, "bar_ts": 7200})
            raise RuntimeError("simulated interrupted cycle")
    with store.transaction() as connection:
        assert store.load(connection, "locked-policy") == {"cash": 100}
        assert connection.execute("SELECT count(*) FROM decisions").fetchone()[0] == 1
        with pytest.raises(ValueError, match="new explicit cohort"):
            store.load(connection, "different-policy")
        assert json.loads(connection.execute("SELECT event FROM decisions").fetchone()[0])["bar_ts"] == 3600
