"""Public OKX quotes for the isolated forward-paper execution loop.

No authenticated client, exchange order API, live ledger or live trigger is used.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import time
from pathlib import Path

import aiohttp
import yaml

from configs.schema import ParticipationRuntimeConfig
from src.reporting.participation_runtime import (
    PROJECT_ROOT, _save_report, isolated_path, process_quote_observation,
    publish_latest, runtime_identity,
)
from src.reporting.participation_store import ParticipationStore
from src.strategy import participation_policy as policy

LOG = logging.getLogger(__name__)
PUBLIC_WS = "wss://ws.okx.com:8443/ws/v5/public"
PUBLIC_TICKER = "https://www.okx.com/api/v5/market/ticker"
SYMBOLS = {"BTC-USDT": "BTC/USDT", "ETH-USDT": "ETH/USDT", "SOL-USDT": "SOL/USDT", "BNB-USDT": "BNB/USDT"}


class PublicQuoteFeed:
    def __init__(self):
        self.quotes = {}
        self.connected = False
        self.last_error = None
        self.received_count = 0
        self.reconnect_count = 0

    def accept(self, row, *, received_at, source):
        if not isinstance(row, dict):
            return False
        symbol = SYMBOLS.get(row.get("instId"))
        if symbol is None:
            return False
        try:
            quote = {"bid": float(row["bidPx"]), "ask": float(row["askPx"]), "ts": float(row["ts"]) / 1000,
                     "received_ts": received_at, "source": source}
        except (TypeError, ValueError, KeyError):
            return False
        if policy.quote_status({"quote": quote}, received_at, {"maximum_quote_age_seconds": 30}):
            return False
        if quote["ts"] <= self.quotes.get(symbol, {}).get("ts", 0):
            return False
        self.quotes[symbol] = quote
        self.received_count += 1
        return True

    async def stream(self, session):
        backoff = 1
        while True:
            try:
                async with session.ws_connect(PUBLIC_WS, heartbeat=20, max_msg_size=65536,
                                              timeout=aiohttp.ClientWSTimeout(ws_receive=25, ws_close=5)) as ws:
                    await ws.send_json({"op": "subscribe", "args": [{"channel": "tickers", "instId": s} for s in SYMBOLS]})
                    self.connected = True
                    backoff = 1
                    async for message in ws:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            if message.data == "pong":
                                continue
                            payload = json.loads(message.data)
                            if not isinstance(payload, dict):
                                raise ValueError("invalid public websocket payload")
                            if payload.get("event") == "error":
                                raise ValueError("OKX public subscription error: " + str(payload.get("code")))
                            for row in payload.get("data", []):
                                if self.accept(row, received_at=time.time(), source="okx_public_ws"):
                                    self.last_error = None
                        elif message.type == aiohttp.WSMsgType.ERROR:
                            raise ConnectionError("public websocket disconnected")
            except (aiohttp.ClientError, OSError, TimeoutError, ValueError) as exc:
                self.last_error = type(exc).__name__ + ": " + str(exc)[:200]
                LOG.warning("Public quote stream reconnect: %s", self.last_error)
            finally:
                self.connected = False
            self.reconnect_count += 1
            await asyncio.sleep(backoff)
            backoff = min(15, backoff * 2)

    async def refresh_stale(self, session, *, now):
        async def fetch(inst):
            try:
                async with session.get(PUBLIC_TICKER, params={"instId": inst},
                                       timeout=aiohttp.ClientTimeout(total=4), allow_redirects=False) as response:
                    response.raise_for_status()
                    payload = await response.json()
                    if payload.get("code") != "0":
                        raise ValueError("public ticker error: " + str(payload.get("code")))
                    for row in payload.get("data", []):
                        self.accept(row, received_at=time.time(), source="okx_public_rest_fallback")
            except (aiohttp.ClientError, OSError, TimeoutError, ValueError) as exc:
                self.last_error = "REST fallback: " + type(exc).__name__ + ": " + str(exc)[:160]
        stale = [inst for inst, symbol in SYMBOLS.items() if now - self.quotes.get(symbol, {}).get("ts", 0) > 10]
        if stale:
            await asyncio.gather(*(fetch(inst) for inst in stale))

    async def fallback_loop(self, session):
        while True:
            await self.refresh_stale(session, now=time.time())
            await asyncio.sleep(2)


def load_settings(config_path):
    cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    settings = ParticipationRuntimeConfig.model_validate(cfg.get("participation", {}))
    if not settings.enabled or not settings.quote_execution_enabled:
        raise ValueError("participation and quote execution must be explicitly enabled")
    path = Path(settings.policy_path)
    config = json.loads((path if path.is_absolute() else PROJECT_ROOT / path).read_text(encoding="utf-8"))
    policy.validate_policy(config)
    if set(config["symbols"]) != set(SYMBOLS.values()):
        raise ValueError("quote worker requires the explicit BTC/ETH/SOL/BNB spot universe")
    return settings, config


async def run_worker(config_path, *, stop=None):
    settings, config = load_settings(config_path)
    reports = PROJECT_ROOT / "reports"
    state_path = isolated_path(settings.state_path, reports, ".sqlite")
    latest_path = isolated_path(settings.latest_path, reports, ".json")
    status_path = state_path.with_suffix(".worker.json")
    identity, hashes = runtime_identity(config, settings)
    store = ParticipationStore(state_path)
    feed = PublicQuoteFeed()
    interval = settings.quote_execution_interval_seconds
    last_report = 0
    last_event = None
    stop = stop or asyncio.Event()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8), trust_env=False) as session:
        receiver = asyncio.create_task(feed.stream(session))
        fallback = asyncio.create_task(feed.fallback_loop(session))
        try:
            while not stop.is_set():
                for task in (receiver, fallback):
                    if task.done():
                        raise RuntimeError("public quote task stopped unexpectedly") from task.exception()
                started = time.monotonic()
                # Cache fallback refreshes run alongside quote delivery. They cannot
                # hold up processing a fresh WebSocket quote for a pending symbol.
                fresh = copy.deepcopy(feed.quotes)
                result = await asyncio.to_thread(process_quote_observation, quotes=fresh, config=config,
                                                  store=store, identity=identity, reports_dir=reports)
                if result["status"] == "observed":
                    await asyncio.to_thread(publish_latest, store=store, identity=identity,
                                            path=latest_path, source_hashes=hashes)
                    last_event = {key: result.get(key) for key in ("observed_ts", "decision", "execution", "source_run_id")}
                    if (result.get("execution") or {}).get("action") in ("fill", "cancel") or result["decision"]["action"] == "exit_intent":
                        LOG.info("PAPER_QUOTE_EVENT %s", json.dumps(last_event, ensure_ascii=True))
                now = time.time()
                if now - last_report >= 10:
                    current_settings, current_config = load_settings(config_path)
                    if (current_settings != settings or runtime_identity(current_config, current_settings)[0] != identity):
                        raise ValueError("participation worker code/settings changed; restart on the new explicit cohort")
                    healthy = all(not policy.quote_status({"quote": fresh.get(symbol)}, now, config) for symbol in config["symbols"])
                    _save_report(status_path, {"schema_version": "v5.participation_quote_worker.v1", "identity": identity,
                                              "observed_ts": now, "status": "observed" if healthy else "quotes_unavailable",
                                              "mode": "forward_paper", "live_order_effect": "none", "interval_seconds": interval,
                                              "websocket_connected": feed.connected, "last_error": feed.last_error,
                                              "received_count": feed.received_count, "reconnect_count": feed.reconnect_count,
                                              "quotes": fresh, "last_check_status": result["status"], "last_event": last_event})
                    last_report = now
                try:
                    await asyncio.wait_for(stop.wait(), timeout=max(.05, interval - (time.monotonic() - started)))
                except TimeoutError:
                    pass
        except Exception as exc:
            _save_report(status_path, {"schema_version": "v5.participation_quote_worker.v1", "identity": identity,
                                      "observed_ts": time.time(), "status": "error", "mode": "forward_paper",
                                      "live_order_effect": "none", "last_error": type(exc).__name__ + ": " + str(exc)[:300]})
            raise
        finally:
            receiver.cancel()
            fallback.cancel()
            await asyncio.gather(receiver, fallback, return_exceptions=True)
