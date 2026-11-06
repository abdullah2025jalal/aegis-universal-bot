"""
Aegis-Universal :: Direct Bybit REST v5 Execution Client
=========================================================
Contract Section 6 implementation:

  * HMAC-SHA256 signed REST v5 over httpx (no third-party SDK).
  * Token-bucket throttling (burst-safe) on every endpoint.
  * Bybit retCode != 0 -> BybitAPIError, routed to the omnipresent alerter.
  * Atomic Limit-IOC bracket entry with embedded exchange-side SL/TP.
  * Post-execution audit -> guaranteed ZERO NAKED EXPOSURE (6.2):
      filled-but-unhedged positions get an SL attach within 3s, else an
      emergency reduceOnly market close.

Absolute Prohibitions honored: entries are Limit-IOC only (#1); market
orders are used exclusively to CLOSE positions in failsafe/TTL paths.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import math
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx

from services.alerter import Alerter

TAKER_FEE_RATE = 0.00055


class BybitAPIError(Exception):
    def __init__(self, ret_code: int, ret_msg: str, endpoint: str = ""):
        super().__init__(f"Bybit retCode={ret_code} retMsg='{ret_msg}' ({endpoint})")
        self.ret_code = ret_code
        self.ret_msg = ret_msg
        self.endpoint = endpoint


class BybitHTTPError(Exception):
    pass


class TokenBucket:
    """Async token bucket: capacity 10, refill 4 tokens/second."""

    def __init__(self, capacity: int = 10, refill_per_sec: float = 4.0):
        self.capacity = capacity
        self.refill_per_sec = refill_per_sec
        self._tokens = float(capacity)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.refill_per_sec)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                await asyncio.sleep((1.0 - self._tokens) / self.refill_per_sec)


def _step_decimals(step: float) -> int:
    """Count decimals implied by an instrument step like 0.1 / 0.001 (integer-safe)."""
    if step <= 0:
        return 3
    decimals = 0
    x = float(step)
    while decimals < 12 and abs(x - round(x)) > 1e-12:
        x *= 10.0
        decimals += 1
    return decimals


def fmt_price(price: float, tick: float) -> str:
    """Round a price onto the instrument tick grid as an exchange-ready string."""
    decimals = _step_decimals(tick)
    steps = round(price / tick)
    return f"{steps * tick:.{decimals}f}"


def fmt_qty(qty: float, step: float) -> str:
    return f"{qty:.{_step_decimals(step)}f}"


@dataclass
class InstrumentSpec:
    symbol: str
    tick_size: float
    qty_step: float
    min_order_qty: float
    max_order_qty: float
    max_leverage: float


@dataclass
class PositionRecord:
    side: str              # "Buy" | "Sell" (one-way mode)
    size: float
    avg_price: float
    stop_loss: float
    take_profit: float
    leverage: str
    updated_time_ms: int
    raw: Dict[str, Any]


class BybitClient:
    def __init__(self, settings, alerter: Alerter):
        self.s = settings
        self.al = alerter
        self._http = httpx.AsyncClient(
            base_url=settings.bybit_base_url, timeout=httpx.Timeout(15.0))
        self._bucket = TokenBucket()
        self._time_offset_ms = 0.0          # bybit_time - local_time
        self._last_sync = 0.0
        self.spec: Optional[InstrumentSpec] = None
        self._signed_fail_count = 0

    # ------------------------------------------------------------------
    # Internal request plumbing
    # ------------------------------------------------------------------

    def now_ms(self) -> int:
        return int(time.time() * 1000 + self._time_offset_ms)

    def clock_drift_ms(self) -> int:
        return abs(int(self._time_offset_ms))

    async def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None,
                       body: Optional[Dict[str, Any]] = None, signed: bool = True) -> Dict[str, Any]:
        await self._bucket.acquire()
        if method.upper() == "GET":
            qs = urllib.parse.urlencode(sorted((str(k), str(v)) for k, v in (params or {}).items()))
            url = f"{path}?{qs}" if qs else path
            payload = qs
            raw_body: Optional[str] = None
        else:
            url = path
            raw_body = json.dumps(body, separators=(",", ":")) if body else None
            payload = raw_body or ""
            if not signed and not payload:
                payload = ""

        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if signed:
            if self._time_offset_ms == 0.0:
                try:
                    await self.sync_clock()
                except Exception:
                    pass
            ts = str(self.now_ms())
            signature = hmac.new(
                self.s.bybit_api_secret.encode("utf-8"),
                f"{ts}{self.s.bybit_api_key}{self.s.bybit_recv_window_ms}{payload}".encode("utf-8"),
                hashlib.sha256).hexdigest()
            headers.update({
                "X-BAPI-API-KEY": self.s.bybit_api_key,
                "X-BAPI-TIMESTAMP": ts,
                "X-BAPI-RECV-WINDOW": str(self.s.bybit_recv_window_ms),
                "X-BAPI-SIGN": signature,
                "X-BAPI-SIGN-TYPE": "2",
            })

        data: Dict[str, Any]
        try:
            if method.upper() == "GET":
                resp = await self._http.get(url, headers=headers)
            else:
                resp = await self._http.post(url, headers=headers, content=(raw_body or "") .encode("utf-8"))
            if resp.status_code >= 500:
                raise BybitHTTPError(f"HTTP {resp.status_code} on {path}: {resp.text[:200]}")
            if resp.status_code >= 400:
                # Auth/permission/format errors carry a plain-text body -> surface it raw.
                self._notify(BybitHTTPError(f"HTTP {resp.status_code} on {path}: "
                                            f"{resp.text[:200]}"), path)
                raise BybitHTTPError(f"HTTP {resp.status_code} on {path}: {resp.text[:200]}")
            data = resp.json()
        except (httpx.HTTPError, json.JSONDecodeError) as exc:
            notify = BybitHTTPError(f"{path}: {type(exc).__name__}: {exc}")
            self._notify(notify, path)
            raise notify

        if int(data.get("retCode", -1)) != 0:
            err = BybitAPIError(int(data.get("retCode")), str(data.get("retMsg", "?")), path)
            self._notify(err, path)
            raise err
        return data.get("result") or {}

    def _notify(self, exc: Exception, path: str) -> None:
        try:
            self.al.report("bybit.client", exc, {"endpoint": path})
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Clock service (Gate G9)
    # ------------------------------------------------------------------

    async def sync_clock(self) -> int:
        local = time.time() * 1000
        result = await self._request("GET", "/v5/market/time", signed=False, params={})
        remote = float(result.get("timeNano", "0")) / 1_000_000.0
        self._time_offset_ms = remote - local
        self._last_sync = time.time()
        return self.clock_drift_ms()

    async def assert_clock(self) -> int:
        """Gate G9: verify |local - server| < 1000ms, resyncing drift dynamically."""
        if time.time() - self._last_sync > self.s.clock_resync_seconds:
            await self.sync_clock()
        drift = self.clock_drift_ms()
        if drift > self.s.max_clock_drift_ms:
            await self.sync_clock()
            drift = self.clock_drift_ms()
        if drift > self.s.max_clock_drift_ms:
            raise BybitAPIError(10010, f"clock drift {drift}ms exceeds "
                                       f"{self.s.max_clock_drift_ms}ms", "G9.clock")
        return drift

    # ------------------------------------------------------------------
    # Market data / account
    # ------------------------------------------------------------------

    async def instrument(self) -> InstrumentSpec:
        if self.spec is None:
            result = await self._request(
                "GET", "/v5/market/instruments-info",
                params={"category": self.s.bybit_category, "symbol": self.s.bybit_symbol},
                signed=False)
            rows = result.get("list") or []
            if not rows:
                raise BybitAPIError(10019, "instrument not found", "instruments-info")
            row = rows[0]
            self.spec = InstrumentSpec(
                symbol=row["symbol"],
                tick_size=float(row["priceFilter"]["tickSize"]),
                qty_step=float(row["lotSizeFilter"]["qtyStep"]),
                min_order_qty=float(row["lotSizeFilter"]["minOrderQty"]),
                max_order_qty=float(row["lotSizeFilter"]["maxOrderQty"]),
                max_leverage=float(row["leverageFilter"]["maxLeverage"]))
        return self.spec

    async def wallet_balance(self) -> Tuple[float, float]:
        """Returns (total_equity_usdt, available_balance_usdt) for the Unified account."""
        result = await self._request(
            "GET", "/v5/account/wallet-balance", params={"accountType": "UNIFIED"})
        lst = result.get("list") or []
        if not lst:
            return 0.0, 0.0
        acc = lst[0]
        equity = float(acc.get("totalEquity") or 0.0)
        available = float(acc.get("totalAvailableBalance") or acc.get("totalWalletBalance") or 0.0)
        return equity, available

    async def orderbook_top(self) -> Tuple[float, float]:
        """Returns (best_bid, best_ask) with limit=1."""
        result = await self._request(
            "GET", "/v5/market/orderbook",
            params={"category": self.s.bybit_category, "symbol": self.s.bybit_symbol, "limit": 1},
            signed=False)
        bids = result.get("b") or result.get("bids") or []
        asks = result.get("a") or result.get("asks") or []
        if not bids or not asks:
            raise BybitHTTPError("empty orderbook")
        return float(bids[0][0]), float(asks[0][0])

    async def tickers(self) -> Dict[str, Any]:
        result = await self._request(
            "GET", "/v5/market/tickers",
            params={"category": self.s.bybit_category, "symbol": self.s.bybit_symbol},
            signed=False)
        rows = result.get("list") or []
        if not rows:
            raise BybitHTTPError("empty tickers")
        row = rows[0]
        return {
            "mark_price": float(row.get("markPrice") or 0),
            "last_price": float(row.get("lastPrice") or 0),
            "funding_rate": float(row.get("fundingRate") or 0),
            "next_funding_ms": int(row.get("nextFundingTime") or 0),
        }

    async def klines_15m(self, limit: int = 20) -> List[List[str]]:
        result = await self._request(
            "GET", "/v5/market/kline",
            params={"category": self.s.bybit_category, "symbol": self.s.bybit_symbol,
                    "interval": "15", "limit": str(limit), "invert": "false"},
            signed=False)
        return result.get("list") or []

    async def klines(self, interval: str, limit: int = 20) -> List[List[str]]:
        """Generic public kline fetch (raw newest-first rows; GemAI feed sorts)."""
        result = await self._request(
            "GET", "/v5/market/kline",
            params={"category": self.s.bybit_category, "symbol": self.s.bybit_symbol,
                    "interval": interval, "limit": str(limit), "invert": "false"},
            signed=False)
        return result.get("list") or []

    async def positions(self) -> List[PositionRecord]:
        result = await self._request(
            "GET", "/v5/position/list",
            params={"category": self.s.bybit_category, "symbol": self.s.bybit_symbol})
        out: List[PositionRecord] = []
        for row in result.get("list") or []:
            size = float(row.get("size") or 0)
            if size <= 0:
                continue
            out.append(PositionRecord(
                side=row.get("side", ""),
                size=size,
                avg_price=float(row.get("avgPrice") or 0),
                stop_loss=float(row.get("stopLoss") or 0),
                take_profit=float(row.get("takeProfit") or 0),
                leverage=str(row.get("leverage") or "?"),
                updated_time_ms=int(row.get("updatedTime") or 0),
                raw=row))
        return out

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------

    async def place_limit_ioc_bracket(self, side: str, qty: float, price: float,
                                      stop_loss: float, take_profit: float,
                                      order_link_id: str | None = None) -> Dict[str, Any]:
        """Contract 6.1 — Limit IOC entry with embedded exchange-side SL/TP (tpslMode Full)."""
        spec = await self.instrument()
        oid = order_link_id or f"aegis-uni-{int(time.time() * 1000)}"
        body = {
            "category": self.s.bybit_category,
            "symbol": self.s.bybit_symbol,
            "side": side,
            "orderType": "Limit",
            "qty": fmt_qty(qty, spec.qty_step),
            "price": fmt_price(price, spec.tick_size),
            "timeInForce": "IOC",
            "positionIdx": 0,
            "orderLinkId": oid,
            "takeProfit": fmt_price(take_profit, spec.tick_size),
            "stopLoss": fmt_price(stop_loss, spec.tick_size),
            "tpTriggerBy": "LastPrice",
            "slTriggerBy": "MarkPrice",
            "tpslMode": "Full",
        }
        result = await self._request("POST", "/v5/order/create", body=body)
        result["orderLinkId"] = oid
        return result

    async def attach_trading_stop(self, side: Optional[str], stop_loss: Optional[float] = None,
                                  take_profit: Optional[float] = None) -> Dict[str, Any]:
        spec = await self.instrument()
        body: Dict[str, Any] = {
            "category": self.s.bybit_category,
            "symbol": self.s.bybit_symbol,
            "positionIdx": 0,
            "tpslMode": "Full",
            "slTriggerBy": "MarkPrice",
            "tpTriggerBy": "LastPrice",
        }
        if side:
            body["side"] = side
        if stop_loss is not None:
            body["stopLoss"] = fmt_price(stop_loss, spec.tick_size)
        if take_profit is not None:
            body["takeProfit"] = fmt_price(take_profit, spec.tick_size)
        return await self._request("POST", "/v5/position/trading-stop", body=body)

    async def market_close(self, side: str, qty: float) -> Dict[str, Any]:
        """Failsafe/TTL ONLY: reduceOnly market exit (Never List #1 applies to entries)."""
        spec = await self.instrument()
        close_side = "Sell" if side.upper() == "BUY" else "Buy"
        body = {
            "category": self.s.bybit_category,
            "symbol": self.s.bybit_symbol,
            "side": close_side,
            "orderType": "Market",
            "qty": fmt_qty(qty, spec.qty_step),
            "reduceOnly": True,
            "positionIdx": 0,
            "orderLinkId": f"aegis-close-{int(time.time() * 1000)}",
        }
        return await self._request("POST", "/v5/order/create", body=body)

    async def cancel_all(self) -> Dict[str, Any]:
        return await self._request(
            "POST", "/v5/order/cancel-all",
            body={"category": self.s.bybit_category, "symbol": self.s.bybit_symbol})

    # ------------------------------------------------------------------
    # Leverage / margin-mode configuration (Never List #2, #4)
    # ------------------------------------------------------------------

    @staticmethod
    def _tolerable(msg: str) -> bool:
        lowered = (msg or "").lower()
        return any(k in lowered for k in (
            "not modified", "not changed", "same", "no change",
            "110044", "110026", "110009"))

    async def configure_leverage(self) -> str:
        """Set ISOLATED margin + MAX_LEVERAGE; validates; returns verified mode string."""
        lev = min(self.s.max_leverage, (await self.instrument()).max_leverage)
        try:
            await self._request("POST", "/v5/position/switch-isolated", body={
                "category": self.s.bybit_category,
                "symbol": self.s.bybit_symbol,
                "isIsolated": True,
                "buyLeverage": f"{lev:g}",
                "sellLeverage": f"{lev:g}",
            })
        except BybitAPIError as err:
            if not self._tolerable(err.ret_msg):
                raise
        try:
            await self._request("POST", "/v5/position/set-leverage", body={
                "category": self.s.bybit_category,
                "symbol": self.s.bybit_symbol,
                "buyLeverage": f"{lev:g}",
                "sellLeverage": f"{lev:g}",
            })
        except BybitAPIError as err:
            if not self._tolerable(err.ret_msg):
                raise
        positions = await self.positions()
        lev_str = f"{self.s.max_leverage:g}"
        if positions:
            verified = positions[0].leverage
            if abs(float(verified or 0) - self.s.max_leverage) > 1e-9:
                raise BybitAPIError(10017,
                                    f"leverage verify mismatch: {verified} != {self.s.max_leverage}",
                                    "position/verify")
            lev_str = str(verified)
        return f"{lev_str}x"

    # ------------------------------------------------------------------
    # Closed PnL (journaling)
    # ------------------------------------------------------------------

    async def closed_pnl(self, since_ms: int, until_ms: Optional[int] = None,
                         limit: int = 200) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "category": self.s.bybit_category, "symbol": self.s.bybit_symbol,
            "startTime": int(since_ms), "limit": str(limit)}
        if until_ms is not None:
            params["endTime"] = int(until_ms)
        result = await self._request("GET", "/v5/position/closed-pnl", params=params)
        return result.get("list") or []

    # ------------------------------------------------------------------
    # Post-execution audit (Contract 6.2 — ZERO NAKED EXPOSURE)
    # ------------------------------------------------------------------

    async def audit_execution(self, side: str, intended_qty: float, entry: float,
                              stop_loss: float, take_profit: float) -> Dict[str, Any]:
        """
        Sleep 500ms for matching-engine propagation, verify the position exists
        WITH an active exchange-side SL. Failsafe ladder:
          missing SL -> attach trading-stop -> if unconfirmed in 3s ->
          emergency reduceOnly market close. Never leaves unhedged exposure.
        """
        await asyncio.sleep(0.50)
        positions = await self.positions()
        position = positions[0] if positions else None
        if position is None:
            return {"filled": False, "position": None, "protected": False, "emergency": False}

        if position.stop_loss > 0:
            return {"filled": True, "position": position, "protected": True,
                    "emergency": False, "adopted_side": position.side}

        # Position filled but naked — attach SL immediately.
        try:
            await self.attach_trading_stop(position.side, stop_loss=stop_loss)
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                await asyncio.sleep(0.5)
                positions = await self.positions()
                if positions and positions[0].stop_loss > 0:
                    return {"filled": True, "position": positions[0], "protected": True,
                            "emergency": False, "adopted_side": positions[0].side}
            raise BybitAPIError(19001, "SL attach unconfirmed within 3s", "audit.failsafe")
        except Exception:
            # Circuit breaker: flatten — under no circumstances leave unhedged exposure.
            self.al.report("execution.audit", BybitAPIError(
                19003, "position filled WITHOUT exchange-side stop-loss and trading-stop "
                       "attach failed — executing EMERGENCY MARKET CLOSE (reduceOnly)",
                "audit.failsafe"),
                {"side": position.side, "size": position.size,
                 "entry": position.avg_price, "mark_price": entry})
            try:
                await self.market_close(position.side, position.size)
            except Exception:
                self.al.report("execution.audit", BybitAPIError(
                    19002, f"UNPROTECTED POSITION REMAINS after failed failsafe close "
                           f"(side={position.side} size={position.size}) — MANUAL INTERVENTION REQUIRED",
                    "audit.critical"), {"mark_price": entry})
            raise

    async def close_http(self) -> None:
        try:
            await self._http.aclose()
        except Exception:
            pass
