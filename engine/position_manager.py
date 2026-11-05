"""
Aegis-Universal :: Position Manager Daemon
===========================================
Contract Section 7 — every 15 seconds while a position is tracked:

  * +1.0R  -> Breakeven+ fee lock: SL moved to Entry ± 0.12% (risk-free).
  * +1.2R  -> Volatility trail: SL = mark ∓ 1.0*ATR, strictly monotonic
              (the stop may NEVER loosen; loosening is banned).
  * 24h TTL -> reduceOnly market close.
  * Close detection -> closed-pnl fetched from Bybit, journal + daily
    aggregates + loss-streak counters updated, cooldown armed, ticket sent.

All exceptions are trapped onto the omnipresent alerter; the daemon never dies.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Dict, Optional

logger = logging.getLogger("aegis.position_manager")


@dataclass
class TrackedPosition:
    side: str                  # "Buy" | "Sell"
    qty: float
    entry: float
    R: float                   # initial SL distance in USD (the unit of risk)
    atr: float
    opened_ts: float
    journal_id: Optional[int] = None
    be_locked: bool = False
    trailing: bool = False


class PositionManager:
    def __init__(self, client, state, alerter, settings, shared):
        self.client = client
        self.state = state
        self.al = alerter
        self.s = settings
        self.shared = shared
        self.tracked: Optional[TrackedPosition] = None
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Adoption (boot reconciler + trade pipeline)
    # ------------------------------------------------------------------

    def adopt(self, pos, opened_ts: Optional[float] = None, source: str = "pipeline",
              atr: Optional[float] = None) -> TrackedPosition:
        entry = pos.avg_price
        sl = pos.stop_loss
        R = abs(entry - sl) if sl > 0 else max(entry * self.s.emergency_sl_pct, 1e-6)
        if atr:
            R = max(R, 0.0)
        tracked = TrackedPosition(
            side=pos.side, qty=pos.size, entry=entry, R=R, atr=atr or 0.0,
            opened_ts=opened_ts or time.time())
        self.tracked = tracked
        self.shared["position"] = tracked
        logger.info("position adopted (%s): %s %.3f @ %.1f R=%.1f", source,
                    tracked.side, tracked.qty, tracked.entry, tracked.R)
        return tracked

    def adopt_pipeline(self, side: str, qty: float, entry: float, sl_price: float,
                       atr: float, opened_ts: float, journal_id: Optional[int]
                       ) -> TrackedPosition:
        tracked = TrackedPosition(
            side=side, qty=qty, entry=entry,
            R=abs(entry - sl_price) if sl_price > 0 else max(entry * self.s.emergency_sl_pct, 1e-6),
            atr=atr, opened_ts=opened_ts, journal_id=journal_id)
        self.tracked = tracked
        self.shared["position"] = tracked
        return tracked

    # ------------------------------------------------------------------
    # Daemon
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
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — Contract 9.2: trap, alert, continue
                self.al.report("position_manager.loop", exc, {"tracked": str(self.tracked)[:160]})
            await asyncio.sleep(self.s.position_poll_seconds)

    async def _tick(self) -> None:
        positions = await self.client.positions()
        live = positions[0] if positions else None

        if self.tracked is None:
            if live is not None:
                self.adopt(live, opened_ts=(live.updated_time_ms / 1000) or time.time(),
                           source="poll-recovery")
        else:
            if live is None:
                await self._handle_close(self.tracked)
                self.tracked = None
                self.shared["position"] = None
                return
            self.tracked.qty = live.size
            await self._supervise(self.tracked, live)

        # keep shared snapshot fresh for the health endpoint
        try:
            equity, available = await self.client.wallet_balance()
            self.shared["equity"] = equity
            self.shared["available"] = available
        except Exception as exc:  # noqa: BLE001
            self.al.report("position_manager.equity", exc, {"phase": "wallet_refresh"})

    # ------------------------------------------------------------------
    # Supervision ladder
    # ------------------------------------------------------------------

    async def _supervise(self, t: TrackedPosition, live) -> None:
        ticker = await self.client.tickers()
        mark = ticker["mark_price"] or t.entry
        r_mult = (mark - t.entry) / t.R if t.side == "Buy" else (t.entry - mark) / t.R

        now_iso = time.strftime("%H:%M:%S", time.gmtime())
        spec = await self.client.instrument()
        tick = spec.tick_size

        # TTL decay exit (24h cap)
        if (time.time() - t.opened_ts) >= self.s.max_holding_hours * 3600:
            self.al.enqueue(self.al.send_trade_event, "ttl", {
                "Side": t.side, "Qty": t.qty, "Entry": t.entry,
                "Mark": mark, "Age (h)": round((time.time() - t.opened_ts) / 3600, 2),
            })
            await self.client.market_close(t.side, t.qty)
            return

        # Breakeven+ fee lock at +1.0R (SL -> Entry ± 0.12%)
        be_target = t.entry * (1 + self.s.breakeven_offset_pct) if t.side == "Buy" \
            else t.entry * (1 - self.s.breakeven_offset_pct)
        if r_mult >= self.s.breakeven_trigger_r and not t.be_locked:
            improves = (t.side == "Buy" and live.stop_loss < be_target - tick / 2) or \
                       (t.side == "Sell" and (live.stop_loss <= 0 or live.stop_loss > be_target + tick / 2))
            if improves:
                await self.client.attach_trading_stop(t.side, stop_loss=be_target)
            t.be_locked = True
            self.al.enqueue(self.al.send_trade_event, "breakeven", {
                "Side": t.side, "Qty": t.qty, "Entry": f"${t.entry:,.1f}",
                "New SL": f"${be_target:,.1f}", "Mark": f"${mark:,.1f}",
                "Unrealized R": f"{r_mult:.2f}R", "UTC": now_iso,
            })

        # Volatility trailing stop at +1.2R (1.0 * ATR behind mark, monotonic)
        if r_mult >= self.s.trail_trigger_r and t.atr > 0:
            trail_target = mark - self.s.trail_atr_multiplier * t.atr if t.side == "Buy" \
                else mark + self.s.trail_atr_multiplier * t.atr
            floor_or_cap = max(be_target, t.entry) if t.side == "Buy" else min(be_target, t.entry)
            improves = (t.side == "Buy" and trail_target > live.stop_loss + tick
                        and trail_target >= floor_or_cap) or \
                       (t.side == "Sell" and (live.stop_loss <= 0 or trail_target < live.stop_loss - tick)
                        and trail_target <= floor_or_cap)
            if improves:
                await self.client.attach_trading_stop(t.side, stop_loss=trail_target)
                if not t.trailing:
                    t.trailing = True
                    self.al.enqueue(self.al.send_trade_event, "trailing", {
                        "Side": t.side, "Qty": t.qty,
                        "Trail SL": f"${trail_target:,.1f}", "Mark": f"${mark:,.1f}",
                        "Unrealized R": f"{r_mult:.2f}R",
                    })

    # ------------------------------------------------------------------
    # Close handling (ledger sync)
    # ------------------------------------------------------------------

    async def _handle_close(self, t: TrackedPosition) -> None:
        now = time.time()
        realized = 0.0
        reason = "closed"
        try:
            rows = await self.client.closed_pnl(int(t.opened_ts * 1000) - 60_000)
            for row in rows:
                if row.get("symbol") == self.s.bybit_symbol:
                    realized += float(row.get("closedPnl") or 0)
            if t.journal_id:
                total_qty = sum(float(r.get("closedSize") or 0) for r in rows)
                reason = "SL/TP" if abs(total_qty - t.qty) < 1e-9 else "closed"
        except Exception as exc:  # noqa: BLE001
            self.al.report("position_manager.close", exc, {"phase": "closed_pnl_fetch"})

        self.state.set_last_trade_closed_ts(now)
        self.state.add_daily_realized_pnl(realized)
        if realized >= 0:
            self.state.set_consecutive_losses(0)
            self.state.count_daily_result(True)
        else:
            self.state.set_consecutive_losses(self.state.consecutive_losses() + 1)
            self.state.count_daily_result(False)
        if t.journal_id:
            self.state.journal_close(t.journal_id, int(now), realized, reason)

        self.al.enqueue(self.al.send_trade_event, "close", {
            "Side": t.side, "Qty": t.qty, "Entry": f"${t.entry:,.1f}",
            "Realized PnL": f"${realized:.4f}", "Duration": f"{(now - t.opened_ts) / 3600:.2f}h",
            "Consecutive Losses": self.state.consecutive_losses(),
            "Daily PnL": f"${self.state.daily_realized_pnl():.4f}",
            "Next Entry Cooldown": f"{self.s.cooldown_minutes}m",
        })
