"""
Aegis-Universal :: On-Boot Exchange Reconciliation
====================================================
Contract Section 8.1: the exchange is the master ledger. On every boot:

  1. Clock sync + wallet equity verification.
  2. Adopt any open BTCUSDT position immediately into memory.
  3. Verify attached stop-loss; if missing apply a conservative 1.0% SL.
     (Audit ladder in execution.py escalates to emergency close if unconfirmed.)
  4. Validate One-Way mode / ISOLATED margin / MAX_LEVERAGE (fail-closed).
  5. Drill the AI decision feed (one real, counted Gemini call).
  6. Initialize baseline equity for the preservation floor.

Every anomaly is appended to the boot report which drives the Section 9.1
"ONLINE & HEALTHY" email (only when zero errors) or the error email.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional


class Reconciler:
    def __init__(self, client, state, alerter, settings, position_manager, scanner):
        self.client = client
        self.state = state
        self.al = alerter
        self.s = settings
        self.pm = position_manager
        self.scanner = scanner

    async def boot(self) -> Dict[str, Any]:
        errors: List[str] = []
        summary: Dict[str, Any] = {"errors": errors}

        # 1) clock + auth plumbing
        try:
            drift = await self.client.sync_clock()
            summary["clock_drift_ms"] = drift
        except Exception as exc:
            self.al.report("reconciler.boot", exc, {"step": "clock_sync"})
            errors.append(f"clock sync failed: {type(exc).__name__}: {exc}")

        # 2) wallet equity
        equity: Optional[float] = None
        available: Optional[float] = None
        try:
            equity, available = await self.client.wallet_balance()
            summary["equity"] = equity
            summary["available"] = available
            if equity <= 0:
                raise ValueError(f"suspicious totalEquity={equity}")
        except Exception as exc:
            self.al.report("reconciler.boot", exc, {"step": "wallet_balance"})
            errors.append(f"wallet balance verification failed: {type(exc).__name__}: {exc}")

        # 3) instrument specs
        try:
            spec = await self.client.instrument()
            summary["tick_size"] = spec.tick_size
            summary["qty_step"] = spec.qty_step
            summary["min_order_qty"] = spec.min_order_qty
        except Exception as exc:
            self.al.report("reconciler.boot", exc, {"step": "instruments"})
            errors.append(f"instrument lookup failed: {type(exc).__name__}: {exc}")

        # 4) adopt open position (the exchange is the master ledger)
        try:
            positions = await self.client.positions()
            if len(positions) > 1 or (
                len(positions) == 1 and len({p.side for p in positions}) > 2
            ):
                raise AssertionError(
                    "hedge-mode (multiple concurrent positions) detected — "
                    "Aegis trades strictly one-way positionIdx=0")
            if positions:
                pos = positions[0]
                opened_ts = (pos.updated_time_ms / 1000.0) or time.time()
                self.pm.adopt(pos, opened_ts=opened_ts, source="boot-reconciler")
                if pos.stop_loss <= 0:
                    # Missing SL: apply conservative 1.0% stop immediately.
                    fallback_sl = (
                        pos.avg_price * (1 - self.s.emergency_sl_pct)
                        if pos.side == "Buy" else pos.avg_price * (1 + self.s.emergency_sl_pct))
                    await self.client.attach_trading_stop(pos.side, stop_loss=fallback_sl)
                    fresh = await self.client.positions()
                    if fresh and fresh[0].stop_loss > 0:
                        self.pm.adopt(fresh[0], opened_ts=opened_ts, source="boot-fsl")
                        summary["adopted_position"] = f"{pos.side} {pos.size} SL->{fallback_sl}"
                        summary["position_recovered"] = True
                    else:
                        raise AssertionError(
                            "naked position retry failed — execution audit escalates")
                else:
                    summary["adopted_position"] = f"{pos.side} {pos.size} @ {pos.avg_price} SL={pos.stop_loss}"
            else:
                summary["adopted_position"] = None
        except Exception as exc:
            self.al.report("reconciler.boot", exc, {"step": "position_reconciliation"})
            errors.append(f"position reconciliation failed: {type(exc).__name__}: {exc}")

        # 5) leverage / margin-mode validation (fail-closed on Active risk mode mismatch)
        try:
            lev = await self.client.configure_leverage()
            summary["leverage"] = lev
        except Exception as exc:
            self.al.report("reconciler.boot", exc, {"step": "leverage_config"})
            errors.append(f"leverage/margin validation failed: {type(exc).__name__}: {exc}")

        # 6) baseline equity for preservation floor (first boot only)
        if equity:
            if self.state.baseline_equity() is None:
                self.state.set_baseline_equity(equity)
                summary["baseline_initialized"] = True
            summary["baseline"] = self.state.baseline_equity()

        # 7) AI decision feed drill (Bybit klines + gemini-3.8-flash)
        try:
            consensus = await self.scanner.probe()
            summary["tv_consensus"] = consensus
        except Exception as exc:
            self.al.report("reconciler.boot", exc, {"step": "ai_feed_probe"})
            errors.append(f"AI decision feed test query failed: {type(exc).__name__}: {exc}")

        summary["ok"] = not errors
        return summary
