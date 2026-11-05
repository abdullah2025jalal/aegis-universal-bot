"""
Aegis-Universal :: Trade Pipeline (shared orchestrator)
========================================================
setup -> gates G0..G9 -> atomic Limit-IOC bracket -> post-execution audit
-> position adoption -> journal -> operator ticket. Used by both the
long-lived FastAPI hosting mode (main.py) and the GitHub Actions
one-shot hosting mode (tools/ci_cycle.py).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, Coroutine, Dict, Optional

logger = logging.getLogger("aegis.pipeline")


def make_pipeline(client, state, pm, alerter, shared, gates
                  ) -> Callable[[Any], Coroutine[Any, Any, None]]:
    async def run(setup) -> None:  # setup: engine.scanner.MarketSetup
        try:
            result = await gates.evaluate(setup)
            if not result.passed:
                logger.info("setup rejected at %s: %s", result.gate, result.reason)
                return
            plan = result.plan or {}
            order = await client.place_limit_ioc_bracket(
                side=setup.direction,
                qty=float(plan["qty"]),
                price=setup.entry,
                stop_loss=float(plan["sl_price"]),
                take_profit=float(plan["tp_price"]),
                order_link_id=plan["orderLinkId"])
            outcome = await client.audit_execution(
                side=setup.direction,
                intended_qty=float(plan["qty"]),
                entry=setup.entry,
                stop_loss=float(plan["sl_price"]),
                take_profit=float(plan["tp_price"]))
            if not outcome.get("filled"):
                logger.info("IOC entry did not fill (orderId=%s) — no exposure taken",
                            order.get("orderId"))
                return
            pos = outcome["position"]
            journal_id = state.journal_open(
                ts_open=int(time.time()), side=pos.side, qty=pos.size,
                entry=pos.avg_price, sl=float(plan["sl_price"]),
                tp=float(plan["tp_price"]), risk_usd=float(plan["risk_usd"]),
                net_rr=float(plan["net_rr"]), order_link_id=str(plan["orderLinkId"]))
            pm.adopt_pipeline(side=pos.side, qty=pos.size, entry=pos.avg_price,
                              atr=setup.atr_15m, opened_ts=time.time(),
                              journal_id=journal_id)
            ticket = {
                "Side": plan["side"], "Qty": plan["qty"],
                "Entry": f"${setup.entry:,.1f}", "SL": f"${plan['sl_price']:,.1f}",
                "TP": f"${plan['tp_price']:,.1f}",
                "Risk $": f"${plan['risk_usd']:.2f}", "Net R:R": f"{plan['net_rr']:.2f}",
                "Mode": "Limit IOC on exchange (tpsl Full)",
                "Direction": setup.direction,
                "Mark/Last": f"${setup.trigger_close:,.1f}",
                "ATR(15m)": f"${setup.atr_15m:,.1f}",
                "RSI(15m)": f"{setup.rsi_15m:.1f}",
                "15m Consensus": setup.summary_15m,
                "1h Trend Filter": setup.summary_1h,
            }
            if alerter:
                alerter.enqueue(alerter.send_trade_event, "open", ticket)
        except Exception as exc:  # noqa: BLE001 — every failure is emailed (Contract 9.2)
            if alerter:
                alerter.report("trade.pipeline", exc, {
                    "direction": getattr(setup, "direction", "?"),
                    "mark_price": f"${getattr(setup, 'trigger_close', 0):,.1f}",
                    "position_state": str(shared.get("position"))[:120],
                    "equity": f"${shared.get('equity', 0):.2f}",
                })
            raise
    return run
