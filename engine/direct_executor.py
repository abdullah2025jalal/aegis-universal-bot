"""
Aegis-Universal :: Direct AI Executor (operator-directed mode, 2026-11-06)
==========================================================================
The operator instructed: remove every blocking condition / control /
warning between the AI verdict and the exchange. The AI decision head
(day-trader-skill playbook is what the AI itself reasons with) now flows
straight to execution. Exactly ONE operator condition remains, enforced
here AND by the exchange itself:

    ** Never execute at a price more than $1 away from the AI decision
       price. ** Enforced by capping the Limit-IOC order:
         BUY  -> limit price = decision-ask + $1  (fills instantly at ask
                 if ask <= cap; otherwise IOC expires — zero exposure)
         SELL -> limit price = decision-bid - $1  (fills at bid if
                 bid >= cap; otherwise IOC expires)

    A favorable move past the cap fills at BETTER prices than the cap
    (limit semantics), so execution is always honest to the decision.

Everything else that remains is pure mechanical plumbing required to
reach and hold the market safely, not a condition on the AI:
  * instrument min-lot clamp (Bybit rejects smaller sizes),
  * the day-trader-skill exit discipline: ATR-clamped stop + 1.5R target
    attached exchange-side, atomic with the entry (tpslMode Full),
  * the zero-naked-exposure audit (SL confirmed or emergency flat),
  * the ledger (journal + position adoption) and email alerts.
"""

from __future__ import annotations

import logging
import math
import time
from typing import Any, Dict

logger = logging.getLogger("aegis.direct")


def plan_bracket(entry: float, atr: float, equity: float, spec, s,
                 side: str) -> Dict[str, Any]:
    """Exit-planner + budget formulary: qty from the max-loss budget,
    SL/TP prices from the ATR clamp. Pure arithmetic (also used by
    tools/smoke_test.py); the only feasibility clamp is the instrument's
    own min lot (Bybit rejects anything smaller) — no pass/block verdicts."""
    spec = spec or type("S", (), {"qty_step": 0.001, "min_order_qty": 0.001,
                                  "max_order_qty": 100.0, "tick_size": 0.1})()
    sl_distance = min(max(1.5 * atr, entry * s.min_sl_distance_pct),
                      entry * s.max_sl_distance_pct)
    tp_distance = s.target_gross_rr * sl_distance
    risk_usd = equity * s.risk_per_trade_pct
    fee_per_unit = entry * s.round_trip_fee_rate
    denom = sl_distance + fee_per_unit
    if denom <= 0:
        denom = max(denom, 1e-9)
    step = max(spec.qty_step, 1e-12)
    raw_qty = risk_usd / denom
    qty = math.floor(raw_qty / step) * step
    if qty < spec.min_order_qty:
        qty = spec.min_order_qty          # min-lot accommodation (no cap by directive)
    if spec.max_order_qty and qty > spec.max_order_qty:
        qty = math.floor(spec.max_order_qty / step) * step
    if side == "Buy":
        sl_price, tp_price = entry - sl_distance, entry + tp_distance
    else:
        sl_price, tp_price = entry + sl_distance, entry - tp_distance
    gross = s.target_gross_rr * sl_distance
    net_rr = (gross - fee_per_unit) / (sl_distance + fee_per_unit)
    return {
        "qty": qty, "sl_price": sl_price, "tp_price": tp_price,
        "sl_distance": sl_distance, "tp_distance": tp_distance,
        "risk_usd": risk_usd, "net_rr": net_rr,
        "orderLinkId": f"aegis-uni-{int(time.time() * 1000)}",
    }


class DirectExecutor:
    """AI verdict -> $1-capped Limit-IOC bracket -> exchange audit -> alerts."""

    def __init__(self, client, state, pm, alerter, settings, shared):
        self.client = client
        self.state = state
        self.pm = pm
        self.al = alerter
        self.s = settings
        self.shared = shared

    async def execute(self, setup, verdict: Dict[str, Any]) -> str:
        side = setup.direction
        cap = self.s.ai_max_price_deviation_usd
        ref = float(setup.entry)
        try:
            if self.shared.get("position") is not None:
                return ("SKIP: a position is already open — the day-trader playbook "
                        "manages one trade at a time (its SL/TP/trail owns the exit)")
            # Fresh top-of-book at the execution instant
            bid, ask = await self.client.orderbook_top()
            if side == "Buy":
                cap_price = ref + cap
                if ask > cap_price:
                    return (f"SKIP (operator $1 rule): AI decided ${ref:,.1f}; "
                            f"ask ${ask:,.1f} is ${ask - ref:,.2f} higher — more than "
                            f"the ${cap:g} cap (${cap_price:,.1f}). No purchase.")
                limit_price = cap_price                    # IOC fills at ask (better) when possible
            else:
                cap_price = ref - cap
                if bid < cap_price:
                    return (f"SKIP (operator $1 rule): AI decided ${ref:,.1f}; "
                            f"bid ${bid:,.1f} is ${ref - bid:,.2f} lower — more than "
                            f"the ${cap:g} cap (${cap_price:,.1f}). No sale.")
                limit_price = cap_price                    # IOC fills at bid (better) when possible

            spec = await self.client.instrument()
            equity, _available = await self.client.wallet_balance()
            plan = plan_bracket(ref, float(setup.atr_15m), float(equity or 0.0),
                                spec, self.s, side)

            order = await self.client.place_limit_ioc_bracket(
                side=side, qty=float(plan["qty"]), price=limit_price,
                stop_loss=float(plan["sl_price"]), take_profit=float(plan["tp_price"]),
                order_link_id=plan["orderLinkId"])
            outcome = await self.client.audit_execution(
                side=side, intended_qty=float(plan["qty"]), entry=ref,
                stop_loss=float(plan["sl_price"]), take_profit=float(plan["tp_price"]))
            if not outcome.get("filled"):
                return (f"IOC unfilled at the ${cap:g} cap — price left the cap before the "
                        f"match; no exposure taken (orderId {order.get('orderId', '?')})")

            pos = outcome["position"]
            journal_id = self.state.journal_open(
                ts_open=int(time.time()), side=pos.side, qty=pos.size,
                entry=pos.avg_price, sl=float(plan["sl_price"]),
                tp=float(plan["tp_price"]), risk_usd=float(plan["risk_usd"]),
                net_rr=float(plan["net_rr"]), order_link_id=str(plan["orderLinkId"]))
            self.pm.adopt_pipeline(side=pos.side, qty=pos.size, entry=pos.avg_price,
                                   sl_price=float(plan["sl_price"]),
                                   atr=setup.atr_15m, opened_ts=time.time(),
                                   journal_id=journal_id)
            ticket = {
                "Symbol": self.s.bybit_symbol,
                "Side": pos.side,
                "Qty": f"{pos.size:g}",
                "Entry (filled)": f"${pos.avg_price:,.1f}",
                "AI Ref Price": f"${ref:,.1f}",
                "SL": f"${plan['sl_price']:,.1f}",
                "TP": f"${plan['tp_price']:,.1f}",
                "Risk $": f"${plan['risk_usd']:.2f}",
                "Net R:R": f"{plan['net_rr']:.2f}",
                "Mode": "Direct AI verdict — Limit-IOC $1 cap (tpsl Full)",
                "AI Setup": str(verdict.get("setup", "n/a")),
                "AI Confidence": str(verdict.get("confidence", "")),
                "AI Reason": str(verdict.get("reason", ""))[:140],
                "Mark/Last": f"${setup.trigger_close:,.1f}",
                "ATR(15m)": f"${setup.atr_15m:,.1f}",
            }
            if self.al:
                self.al.enqueue(self.al.send_trade_event, "open", ticket)
            return (f"EXECUTED {pos.side} {pos.size:g} @ ${pos.avg_price:,.1f} "
                    f"(cap ${cap_price:,.1f}) SL ${plan['sl_price']:,.1f} "
                    f"TP ${plan['tp_price']:,.1f}")
        except Exception as exc:  # noqa: BLE001 — surfaces inside the decision email
            if self.al:
                self.al.report("direct.executor", exc, {
                    "side": side, "ai_price": f"${ref:,.1f}"})
            return f"EXECUTION ERROR: {type(exc).__name__}: {str(exc)[:160]}"
