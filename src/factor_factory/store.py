from __future__ import annotations

from src.factor_factory.models import FactorSnapshot, FactorSpec


def latest_key(symbol: str, timeframe: str, factor_id: str) -> str:
    return f"factor:latest:{symbol}:{timeframe}:{factor_id}"


def history_key(symbol: str, timeframe: str, factor_id: str, event_ts: int) -> str:
    return f"factor:history:{symbol}:{timeframe}:{factor_id}:{int(event_ts)}"


def registry_key(factor_id: str, version: str) -> str:
    return f"factor:registry:{factor_id}:{version}"


def snapshot_payload(snapshot: FactorSnapshot) -> str:
    return snapshot.model_dump_json()


def spec_payload(spec: FactorSpec) -> str:
    return spec.model_dump_json()
