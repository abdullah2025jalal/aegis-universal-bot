"""
Aegis-Universal :: Market Scanner Daemon
=========================================
Contract Section 4 — asynchronous background loop aligned to the 1-minute
candle clock. Extracts multi-timeframe consensus from tradingview-ta
(15m signal + 1h trend filter + RSI ceiling + EMA alignment), synthesizes
the preliminary bracket (ATR clamped SL distance / 1.5R TP), and hands the
setup to the orchestrator callback (risk gates -> execution).

Every tick and every failure is wrapped: data-feed timeouts never kill the
daemon and are routed to the omnipresent error alerter with deduping.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, List, Optional

from tradingview_ta import TA_Handler, Analysis, Interval

logger = logging.getLogger("aegis.scanner")

_TV_VARIANTS: Dict[str, List[str]] = {"linear_like": ["BTCUSDT", "BTCUSDT.P"]}

_INTERVAL_MAP = {"15m": Interval.INTERVAL_15_MINUTES, "1h": Interval.INTERVAL_1_HOUR}


@dataclass
class MarketSetup:
    direction: str                     # "Buy" | "Sell"
    entry: float                       # Ask for longs, Bid for shorts
    trigger_close: float               # last traded price at scan time (G5 chase guard)
    atr_15m: float                     # ATR(14) in USD
    rsi_15m: float
    ema50_15m: float
    close_15m: float
    summary_15m: str
    summary_1h: str
    sl_distance: float = 0.0           # filled by the risk pipeline (authoritative)
    tp_distance: float = 0.0

    def to_ticket(self) -> Dict[str, Any]:
        return {
            "Direction": self.direction,
            "Entry Ref": f"${self.entry:,.1f}",
            "Mark/Last": f"${self.trigger_close:,.1f}",
            "ATR(15m)": f"${self.atr_15m:,.1f}",
            "RSI(15m)": f"{self.rsi_15m:.1f}",
            "15m Consensus": self.summary_15m,
            "1h Trend Filter": self.summary_1h,
        }


def compute_atr_14(klines_15m: List[List[str]]) -> float:
    """Wilder ATR(14) fallback computed from Bybit 15m kline rows."""
    trs: List[float] = []
    prev_close: Optional[float] = None
    for row in klines_15m:
        try:
            high, low, close = float(row[2]), float(row[3]), float(row[4])
        except (IndexError, TypeError, ValueError):
            continue
        if prev_close is not None:
            trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        prev_close = close
    if not trs:
        return 0.0
    recent = trs[-14:] if len(trs) >= 14 else trs
    return sum(recent) / len(recent)


def _get_analysis(symbol: str, interval_key: str, exchange_variants: List[str]) -> Analysis:
    last_err: Optional[Exception] = None
    for symbol_variant in exchange_variants:
        for exchange in ["BYBIT"]:
            try:
                handler = TA_Handler(symbol=symbol_variant, exchange=exchange,
                                     screener="crypto", interval=_INTERVAL_MAP[interval_key])
                return handler.get_analysis()
            except Exception as exc:  # noqa: BLE001 — variant probing
                last_err = exc
    raise last_err if last_err else RuntimeError("tradingview-ta produced no analysis")


class Scanner:
    def __init__(self, client, alerter, settings, on_setup: Callable[[MarketSetup], Coroutine]):
        self.client = client
        self.al = alerter
        self.s = settings
        self.on_setup = on_setup
        self._symbol_variants = [
            self.s.bybit_symbol, f"{self.s.bybit_symbol}.P"]
        self._task: Optional[asyncio.Task] = None
        self.shared: Dict[str, Any] = {"last_scan_ts": 0.0, "consensus": "", "error": ""}

    # ------------------------------------------------------------------
    # On-demand probe (boot drill)
    # ------------------------------------------------------------------

    async def probe(self) -> str:
        a15, a1h = await asyncio.to_thread(self._fetch_pair)
        s15 = str(a15.summary.get("RECOMMENDATION", "n/a"))
        s1h = str(a1h.summary.get("RECOMMENDATION", "n/a"))
        return f"15m={s15} | 1h={s1h} | RSI15={float(a15.indicators.get('RSI') or -1):.1f}"

    def _fetch_pair(self) -> tuple:
        a15 = _get_analysis(self.s.bybit_symbol, "15m", self._symbol_variants)
        a1h = _get_analysis(self.s.bybit_symbol, "1h", self._symbol_variants)
        return a15, a1h

    # ------------------------------------------------------------------
    # Daemon loop
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self.loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def loop(self) -> None:
        while True:
            try:
                await self._sleep_to_minute_boundary()
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — daemon never dies (Contract 9.2)
                self.shared["error"] = f"{type(exc).__name__}: {exc}"
                self.al.report("scanner.loop", exc, {"phase": "tick"})
                await asyncio.sleep(15)

    @staticmethod
    async def _sleep_to_minute_boundary() -> None:
        now = time.time()
        delay = 60.0 - (now % 60.0) + 0.25
        await asyncio.sleep(min(max(delay, 0.5), 60.5))

    async def tick(self) -> None:
        try:
            a15, a1h = await asyncio.to_thread(self._fetch_pair)
        except Exception as exc:  # noqa: BLE001 — 4.4 data feed failure -> alert + continue
            self.shared["error"] = f"tradingview: {type(exc).__name__}: {exc}"
            self.al.report("tradingview.datafeed", exc,
                           {"feed": "15m+1h", "symbol": self.s.bybit_symbol})
            raise

        self.shared["consensus"] = (f"15m={a15.summary.get('RECOMMENDATION')} "
                                    f"1h={a1h.summary.get('RECOMMENDATION')}")
        self.shared["last_scan_ts"] = time.time()
        self.shared["error"] = ""

        setup = await self._build_setup(a15, a1h)
        if setup is not None and self.on_setup is not None:
            try:
                await self.on_setup(setup)
            except Exception as exc:  # noqa: BLE001 — pipeline errors alert, never kill loop
                self.al.report("scanner.pipeline", exc, {"direction": setup.direction})

    # ------------------------------------------------------------------
    # 4.2 Directional momentum setup synthesis (+ 4.3 bracket)
    # ------------------------------------------------------------------

    async def _build_setup(self, a15: Analysis, a1h: Analysis) -> Optional[MarketSetup]:
        ind15 = a15.indicators or {}
        summary15 = (a15.summary or {}).get("RECOMMENDATION") or ""
        summary1h = (a1h.summary or {}).get("RECOMMENDATION") or ""
        rsi = ind15.get("RSI")
        ema50 = ind15.get("EMA50")
        close = ind15.get("close")
        if rsi is None or ema50 is None or close is None:
            self.al.report("scanner.indicators", RuntimeError(
                "tradingview-ta payload missing RSI/EMA50/close"), {"summary_15m": summary15})
            return None

        try:
            bid, ask = await self.client.orderbook_top()
            ticker = await self.client.tickers()
        except Exception as exc:  # noqa: BLE001
            self.al.report("scanner.marketdata", exc, {"phase": "orderbook"})
            return None

        atr = ind15.get("ATR")
        if not atr or float(atr) <= 0:
            try:
                klines = await self.client.klines_15m(limit=20)
                atr = compute_atr_14(klines)
            except Exception as exc:  # noqa: BLE001
                self.al.report("scanner.volatility", exc, {"phase": "atr_fallback"})
                return None
        atr = float(atr)

        is_long = summary15 in ("BUY", "STRONG_BUY")
        is_short = summary15 in ("SELL", "STRONG_SELL")

        if is_long and (summary1h not in ("SELL", "STRONG_SELL")) and float(rsi) < 72.0 \
                and float(close) > float(ema50):
            direction = "Buy"
        elif is_short and (summary1h not in ("BUY", "STRONG_BUY")) and float(rsi) > 28.0 \
                and float(close) < float(ema50):
            direction = "Sell"
        else:
            return None

        entry = ask if direction == "Buy" else bid
        sl_distance = self._clamp_sl(entry, atr)
        return MarketSetup(
            direction=direction,
            entry=entry,
            trigger_close=float(ticker.get("last_price") or entry),
            atr_15m=atr,
            rsi_15m=float(rsi),
            ema50_15m=float(ema50),
            close_15m=float(close),
            summary_15m=summary15,
            summary_1h=summary1h or "n/a",
            sl_distance=sl_distance,
            tp_distance=self.s.target_gross_rr * sl_distance,
        )

    def _clamp_sl(self, entry: float, atr: float) -> float:
        raw = 1.5 * atr
        floor = entry * self.s.min_sl_distance_pct
        ceiling = entry * self.s.max_sl_distance_pct
        return min(max(raw, floor), ceiling)
