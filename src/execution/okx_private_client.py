from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List
from urllib.parse import urlencode

import httpx

from configs.schema import ExchangeConfig
from src.utils.retry import RetryConfig, retry


log = logging.getLogger(__name__)


class OKXPrivateClientError(Exception):
    pass


class OKXRateLimitError(OKXPrivateClientError):
    """Triggered when OKX returns 50011 rate limit."""


@dataclass
class OKXResponse:
    data: Dict[str, Any]
    http_status: int
    okx_code: Optional[str] = None
    okx_msg: Optional[str] = None


def _utc_iso_ms() -> str:
    # OKX expects UTC ISO8601 timestamp; include milliseconds
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _epoch_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _json_dumps_compact(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def sign_okx(
    *,
    api_secret: str,
    timestamp: str,
    method: str,
    request_path: str,
    body: str,
) -> str:
    """Return Base64(HMAC_SHA256(prehash)).

    prehash = timestamp + method + request_path + body

    Note: request_path includes query string (e.g. /api/v5/account/balance?ccy=USDT)
    """
    m = str(method).upper()
    prehash = f"{timestamp}{m}{request_path}{body}".encode("utf-8")
    mac = hmac.new(api_secret.encode("utf-8"), prehash, digestmod=hashlib.sha256).digest()
    return base64.b64encode(mac).decode("utf-8")


class OKXPrivateClient:
    """Minimal OKX v5 private REST client.

    Focuses on correctness + observability:
    - request signing
    - consistent request_path w/ query
    - rate-limit aware retry for code=50011
    """

    def __init__(
        self,
        exchange: ExchangeConfig,
        *,
        base_url: str = "https://www.okx.com",
        timeout_sec: float = 10.0,
        retry_cfg: Optional[RetryConfig] = None,
        req_exptime_ms: Optional[int] = None,
    ):
        self.exchange = exchange
        self.base_url = str(base_url).rstrip("/")
        self.timeout_sec = float(timeout_sec)
        self.retry_cfg = retry_cfg or RetryConfig(max_attempts=5, base_delay_sec=0.25, max_delay_sec=5.0, jitter_frac=0.25)
        self.req_exptime_ms = req_exptime_ms

        if not exchange.api_key or not exchange.api_secret or not exchange.passphrase:
            raise OKXPrivateClientError("Missing exchange api_key/api_secret/passphrase")

        self._client = httpx.Client(base_url=self.base_url, timeout=self.timeout_sec)

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口，确保关闭连接"""
        self.close()
        return False

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    def _headers(self, *, timestamp: str, method: str, request_path: str, body: str) -> Dict[str, str]:
        sig = sign_okx(
            api_secret=str(self.exchange.api_secret),
            timestamp=timestamp,
            method=method,
            request_path=request_path,
            body=body,
        )
        h = {
            "OK-ACCESS-KEY": str(self.exchange.api_key),
            "OK-ACCESS-SIGN": sig,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": str(self.exchange.passphrase),
            "Content-Type": "application/json",
        }
        if self.req_exptime_ms is not None:
            # OKX trading endpoints support expTime header (milliseconds).
            # It is an epoch-millisecond timestamp. For convenience, if user passes a small number
            # (e.g. 1500), treat it as a delta ms from now.
            x = int(self.req_exptime_ms)
            exp = x if x > 1_000_000_000_000 else (_epoch_ms() + x)
            h["expTime"] = str(int(exp))
        return h

    def _build_request_path(self, path: str, params: Optional[Dict[str, Any]]) -> str:
        p = "/" + str(path).lstrip("/")
        if params:
            # OKX signature uses requestPath with query string.
            qs = urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
            if qs:
                return f"{p}?{qs}"
        return p

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        exp_time_ms: Optional[int] = None,
        retry_on_transport_errors: bool = True,
    ) -> OKXResponse:
        method_u = str(method).upper()
        request_path = self._build_request_path(path, params)
        body_str = "" if json_body is None else _json_dumps_compact(json_body)

        def _do() -> OKXResponse:
            ts = _utc_iso_ms()
            headers = self._headers(timestamp=ts, method=method_u, request_path=request_path, body=body_str)
            if exp_time_ms is not None:
                x = int(exp_time_ms)
                exp = x if x > 1_000_000_000_000 else (_epoch_ms() + x)
                headers["expTime"] = str(int(exp))
            try:
                resp = self._client.request(method_u, request_path, content=body_str if body_str else None, headers=headers)
            except httpx.TimeoutException as e:
                raise OKXPrivateClientError(f"timeout: {e}") from e
            except httpx.HTTPError as e:
                raise OKXPrivateClientError(f"http error: {e}") from e

            http_status = int(resp.status_code)
            try:
                payload = resp.json()
            except Exception:
                payload = {"raw": resp.text}

            code = None
            msg = None
            if isinstance(payload, dict):
                code = str(payload.get("code")) if payload.get("code") is not None else None
                msg = str(payload.get("msg")) if payload.get("msg") is not None else None

            # OKX rate limit code
            if code == "50011":
                raise OKXRateLimitError(f"rate limit (50011): {msg}")

            return OKXResponse(data=payload if isinstance(payload, dict) else {"data": payload}, http_status=http_status, okx_code=code, okx_msg=msg)

        def _should_retry(e: BaseException) -> bool:
            if isinstance(e, OKXRateLimitError):
                return True
            if isinstance(e, OKXPrivateClientError):
                # For non-idempotent requests like order placement, never replay POST on
                # transport ambiguity. The caller should query by clOrdId instead.
                return bool(retry_on_transport_errors)
            return False

        return retry(_do, should_retry=_should_retry, cfg=self.retry_cfg)

    # --- Convenience wrappers (G0.2) ---
    def place_order(self, payload: Dict[str, Any], *, exp_time_ms: Optional[int] = None) -> OKXResponse:
        return self.request(
            "POST",
            "/api/v5/trade/order",
            json_body=payload,
            exp_time_ms=exp_time_ms,
            retry_on_transport_errors=False,
        )

    def get_order(
        self,
        *,
        inst_id: str,
        ord_id: Optional[str] = None,
        cl_ord_id: Optional[str] = None,
    ) -> OKXResponse:
        if not inst_id:
            raise OKXPrivateClientError("inst_id is required")
        if not ord_id and not cl_ord_id:
            raise OKXPrivateClientError("ord_id or cl_ord_id is required")
        params = {"instId": inst_id, "ordId": ord_id, "clOrdId": cl_ord_id}
        return self.request("GET", "/api/v5/trade/order", params=params)

    def cancel_order(
        self,
        *,
        inst_id: str,
        ord_id: Optional[str] = None,
        cl_ord_id: Optional[str] = None,
    ) -> OKXResponse:
        if not inst_id:
            raise OKXPrivateClientError("inst_id is required")
        if not ord_id and not cl_ord_id:
            raise OKXPrivateClientError("ord_id or cl_ord_id is required")
        payload = {"instId": inst_id, "ordId": ord_id, "clOrdId": cl_ord_id}
        return self.request("POST", "/api/v5/trade/cancel-order", json_body=payload)

    def get_fills(
        self,
        *,
        inst_type: str = "SPOT",
        inst_id: Optional[str] = None,
        ord_id: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        begin: Optional[int] = None,
        end: Optional[int] = None,
        limit: int = 100,
    ) -> OKXResponse:
        params: Dict[str, Any] = {
            "instType": inst_type,
            "instId": inst_id,
            "ordId": ord_id,
            "after": after,
            "before": before,
            "begin": begin,
            "end": end,
            "limit": int(limit),
        }
        return self.request("GET", "/api/v5/trade/fills", params=params)

    def get_bills(
        self,
        *,
        ccy: Optional[str] = None,
        inst_type: Optional[str] = None,
        mgn_mode: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        begin: Optional[int] = None,
        end: Optional[int] = None,
        limit: int = 100,
    ) -> OKXResponse:
        params: Dict[str, Any] = {
            "ccy": ccy,
            "instType": inst_type,
            "mgnMode": mgn_mode,
            "after": after,
            "before": before,
            "begin": begin,
            "end": end,
            "limit": int(limit),
        }
        return self.request("GET", "/api/v5/account/bills", params=params)

    def get_bills_archive(
        self,
        *,
        ccy: Optional[str] = None,
        inst_type: Optional[str] = None,
        mgn_mode: Optional[str] = None,
        after: Optional[str] = None,
        before: Optional[str] = None,
        begin: Optional[int] = None,
        end: Optional[int] = None,
        limit: int = 100,
    ) -> OKXResponse:
        params: Dict[str, Any] = {
            "ccy": ccy,
            "instType": inst_type,
            "mgnMode": mgn_mode,
            "after": after,
            "before": before,
            "begin": begin,
            "end": end,
            "limit": int(limit),
        }
        return self.request("GET", "/api/v5/account/bills-archive", params=params)

    # Minimal self-check helper
    def get_balance(self, ccy: Optional[str] = None) -> OKXResponse:
        params = {"ccy": ccy} if ccy else None
        return self.request("GET", "/api/v5/account/balance", params=params)

    def get_account_config(self) -> OKXResponse:
        """Get account configuration (acctLv/posMode/autoLoan/enableSpotBorrow...)."""
        return self.request("GET", "/api/v5/account/config")

    def set_auto_repay(self, auto_repay: bool) -> OKXResponse:
        """Set spot auto-repay flag (only applicable when spot borrowing is enabled)."""
        return self.request("POST", "/api/v5/account/set-auto-repay", json_body={"autoRepay": bool(auto_repay)})

    def set_auto_loan(self, auto_loan: bool) -> OKXResponse:
        """Set auto-loan flag (only applicable to multi-currency/portfolio margin)."""
        return self.request("POST", "/api/v5/account/set-auto-loan", json_body={"autoLoan": bool(auto_loan)})
