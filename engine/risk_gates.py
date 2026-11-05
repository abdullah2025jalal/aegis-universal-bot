"""
Aegis-Universal :: The 10 Pragmatic Risk Gates (G0..G9)
========================================================
Contract Section 5 — sequential, fail-fast, deterministic. A setup must
pass ALL gates in order before any order reaches Bybit. No gate may be
skipped, re-ordered, or short-circuited (Never List #6).

Also implements the Contract Section 3 dynamic sizing formulary (G3) and
the margin liquidity safeguard (3.1).
"""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass
class GateOutcome:
    gate: str
    passed: bool
    reason: str = ""
    detail: str = ""


@dataclass
class GateResult:
    passed: bool
    gate: str
    reason: str = ""
    outcomes: List[GateOutcome] = field(default_factory=list)
    plan: Optional[Dict[str, Any]] = None


@dataclass
class SizingPlan:
    entry: float
    side: str
    qty: float
    sl_price: float
    tp_price: float
    sl_distance: float
    tp_distance: float
    risk_usd: float
    required_margin: float
    available: float
    equity: float
    net_rr: float
    atr: float
    orderLinkId: str

    def to_dict(self) -> Dict[str, Any]:
        """Complete machine-readable plan consumed by the execution pipeline."""
        return {
            "side": self.side, "qty": self.qty, "entry": self.entry,
            "sl_price": self.sl_price, "tp_price": self.tp_price,
            "sl_distance": self.sl_distance, "tp_distance": self.tp_distance,
            "risk_usd": self.risk_usd, "required_margin": self.required_margin,
            "available": self.available, "equity": self.equity,
            "net_rr": self.net_rr, "atr": self.atr,
            "orderLinkId": self.orderLinkId,
        }


def clamp_sl_distance(entry: float, atr: float, s) -> float:
    """SL_Distance_USD = clamp(1.5 * ATR_15m, entry*0.5%, entry*2.0%)."""
    return min(max(1.5 * atr, entry * s.min_sl_distance_pct), entry * s.max_sl_distance_pct)


def compute_sizing(self_equity: float, entry: float, atr: float, s, specs: Any) -> Tuple[
        Optional[SizingPlan], str]:
    """
    Contract Section 3 + 3.1 sizing formulary. Returns (plan|None, reject_reason).
    """
    sl_distance = clamp_sl_distance(entry, atr, s)
    tp_distance = s.target_gross_rr * sl_distance

    risk_usd = self_equity * s.risk_per_trade_pct
    fee_per_unit = entry * s.round_trip_fee_rate
    denom = sl_distance + fee_per_unit
    if denom <= 0:
        return None, "degenerate SL/fee denominator"

    raw_qty = risk_usd / denom
    step = max(specs.qty_step, 1e-12)
    sized_qty = math.floor(raw_qty / step) * step

    if sized_qty < specs.min_order_qty:
        micro_cost = specs.min_order_qty * (sl_distance + fee_per_unit)
        if micro_cost <= self_equity * s.micro_account_max_risk_pct:
            sized_qty = specs.min_order_qty          # micro-account accommodation (~$50 floor)
        else:
            return None, ("account balance insufficient for minimum lot size under safe risk "
                          f"(min-lot exposure ${micro_cost:.2f} > 2.5% of equity "
                          f"${self_equity * 0.025:.2f})")

    if sized_qty > specs.max_order_qty:
        sized_qty = math.floor(specs.max_order_qty / step) * step

    required_margin = (sized_qty * entry) / s.max_leverage
    if required_margin > self_equity * s.max_margin_alloc_pct:
        return None, (f"required margin ${required_margin:.2f} exceeds "
                      f"{s.max_margin_alloc_pct * 100:.0f}% margin allocation ceiling")

    gross = s.target_gross_rr * sl_distance
    net_profit_on_tp = gross - fee_per_unit
    net_loss_on_sl = sl_distance + fee_per_unit
    net_rr = (net_profit_on_tp / net_loss_on_sl) if net_loss_on_sl > 0 else 0.0

    return SizingPlan(
        entry=entry, side="", qty=sized_qty, sl_price=entry, tp_price=entry,
        sl_distance=sl_distance, tp_distance=tp_distance, risk_usd=risk_usd,
        required_margin=required_margin, available=0.0, equity=self_equity,
        net_rr=net_rr, atr=atr,
        orderLinkId=f"aegis-uni-{int(time.time() * 1000)}"), ""


class RiskGates:
    def __init__(self, client, state, alerter, settings, shared):
        self.client = client
        self.state = state
        self.al = alerter
        self.s = settings
        self.shared = shared
        self.history: deque = deque(maxlen=40)
        self._ctx: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    async def evaluate(self, setup) -> GateResult:
        outcomes: List[GateOutcome] = []
        plan: Optional[SizingPlan] = None

        checks = [
            ("G0", "Circuit Breaker & Preservation", self._g0),
            ("G1", "Post-Trade Cooldown", self._g1),
            ("G2", "Concurrency (size == 0)", self._g2),
            ("G3", "Sizing & Headroom", self._g3),
            ("G4", "Orderbook Spread", self._g4),
            ("G5", "Chase Guard", self._g5),
            ("G6", "Volatility Extreme", self._g6),
            ("G7", "Net Expectancy", self._g7),
            ("G8", "Carrying Cost / Funding", self._g8),
            ("G9", "Clock Synchronization", self._g9),
        ]

        for gate_id, gate_name, fn in checks:
            try:
                outcome = await fn(setup)
            except Exception as exc:  # noqa: BLE001 — a gate error is a failure, never silent
                self.al.report(f"risk.{gate_id}", exc, {"gate": gate_name})
                outcome = GateOutcome(gate_id, False, f"gate exception: {type(exc).__name__}: {exc!r}")
            outcomes.append(outcome)
            if not outcome.passed:
                self._record(setup, gate_id, outcome.reason, plan)
                return GateResult(False, gate_id, outcome.reason, outcomes, None)
            if gate_id == "G3" and plan is None:
                # following gates may use the plan
                plan = self._ctx.get("plan")

        self._record(setup, "PASS", "all 10 gates passed", plan)
        plan.side = setup.direction
        # bracket synthesis (4.3) — authoritative, tick-rounded in execution
        if setup.direction == "Buy":
            plan.sl_price = setup.entry - plan.sl_distance
            plan.tp_price = setup.entry + plan.tp_distance
        else:
            plan.sl_price = setup.entry + plan.sl_distance
            plan.tp_price = setup.entry - plan.tp_distance
        return GateResult(True, "G9", "", outcomes, plan.to_dict())

    def _record(self, setup, gate: str, reason: str, plan: Optional[SizingPlan]) -> None:
        entry = {
            "ts": time.time(),
            "direction": setup.direction,
            "gate": gate,
            "reason": reason,
            "mark": f"${setup.trigger_close:,.1f}",
            "consensus": f"{setup.summary_15m}/{setup.summary_1h}",
        }
        self.history.appendleft(entry)

    # ------------------------------------------------------------------
    # G0 — Circuit Breaker & Preservation Gate
    # ------------------------------------------------------------------

    async def _g0(self, setup) -> GateOutcome:
        s = self.s
        self.state.ensure_utc_day()
        now = time.time()

        if s.kill_switch_active or self.state.kill_switch():
            reason = self.state.get("kill_switch_reason", "") or "kill switch active"
            return GateOutcome("G0", False, f"circuit breaker: {reason}")

        equity, _available = await self.client.wallet_balance()
        self.shared["equity"] = equity
        if equity <= 0:
            return GateOutcome("G0", False, f"equity feed unavailable ({equity})")

        baseline = self.state.baseline_equity()
        if baseline is None:
            self.state.set_baseline_equity(equity)
            baseline = equity

        if equity < baseline * s.equity_preservation_floor_pct:
            self.state.set_kill_switch(True, "equity preservation floor breached")
            self.al.enqueue(self.al.send_trade_event, "halt", {
                "Floor": f"{s.equity_preservation_floor_pct * 100:.0f}% of baseline",
                "Baseline": f"${baseline:.2f}",
                "Equity": f"${equity:.2f}",
                "Remedy": "Entries disabled. Clear via dashboard /dashboard/resume "
                          "after manual review.",
            })
            return GateOutcome("G0", False, "HARD HALT: equity preservation floor breached "
                                            f"(${equity:.2f} < ${baseline * s.equity_preservation_floor_pct:.2f})")

        daily_pnl = self.state.daily_realized_pnl()
        if daily_pnl <= 0 and abs(daily_pnl) >= equity * s.daily_loss_limit_pct:
            pause_until = now + 24 * 3600
            self.state.set_pause_until(pause_until, "daily loss limit")
            self.al.enqueue(self.al.send_trade_event, "paused", {
                "Trigger": f"Daily loss ≥ {s.daily_loss_limit_pct * 100:.0f}% of equity",
                "Daily PnL": f"${daily_pnl:.4f}",
                "Paused Until (UTC)": time.strftime("%Y-%m-%d %H:%M", time.gmtime(pause_until)),
            })
            return GateOutcome("G0", False, f"daily loss limit engaged (${daily_pnl:.4f})")

        if daily_pnl >= equity * s.daily_profit_lock_pct and self.state.profit_lock_day() != self.state.utc_day():
            self.state.set_profit_lock_day(self.state.utc_day())
            self.al.enqueue(self.al.send_trade_event, "paused", {
                "Trigger": f"Daily profit ≥ {s.daily_profit_lock_pct * 100:.0f}% of equity",
                "Daily PnL": f"${daily_pnl:.4f}",
                "Lock": "Entries locked until UTC 00:00",
            })
            return GateOutcome("G0", False, "profit lock engaged for the day")

        if self.state.pause_until() > now:
            remaining = self.state.pause_until() - now
            return GateOutcome("G0", False, f"entries paused for "
                                            f"{remaining / 60:.0f} min ({self.state.get('pause_reason', '')})")

        if self.state.consecutive_losses() >= 4 and self.state.pause_until() <= now:
            self.state.set_pause_until(now + 4 * 3600, "4 consecutive losses")
            self.al.enqueue(self.al.send_trade_event, "paused", {
                "Trigger": f"{self.state.consecutive_losses()} consecutive losses",
                "Paused Until (UTC)": time.strftime("%Y-%m-%d %H:%M", time.gmtime(now + 4 * 3600)),
            })
            return GateOutcome("G0", False, "loss-streak pause engaged (4h)")

        return GateOutcome("G0", True, f"equity ${equity:.2f} / baseline ${baseline:.2f}")

    # ------------------------------------------------------------------
    # G1 — Post-Trade Cooldown
    # ------------------------------------------------------------------

    async def _g1(self, setup) -> GateOutcome:
        last_closed = self.state.last_trade_closed_ts()
        if last_closed <= 0:
            return GateOutcome("G1", True)
        elapsed = time.time() - last_closed
        needed = self.s.cooldown_minutes * 60
        if elapsed >= needed:
            return GateOutcome("G1", True, f"cooldown satisfied ({elapsed / 60:.1f}m)")
        return GateOutcome("G1", False, f"cooldown active: {(needed - elapsed) / 60:.1f}m remaining")

    # ------------------------------------------------------------------
    # G2 — Concurrency
    # ------------------------------------------------------------------

    async def _g2(self, setup) -> GateOutcome:
        positions = await self.client.positions()
        if positions:
            return GateOutcome("G2", False,
                               f"max 1 concurrent position; currently open: {positions[0].side} "
                               f"{positions[0].size}")
        return GateOutcome("G2", True, "flat")

    # ------------------------------------------------------------------
    # G3 — Sizing & Headroom (Section 3 + 3.1)
    # ------------------------------------------------------------------

    async def _g3(self, setup) -> GateOutcome:
        specs = await self.client.instrument()
        equity, available = await self.client.wallet_balance()
        plan, reject = compute_sizing(equity, setup.entry, setup.atr_15m, self.s, specs)
        if reject:
            return GateOutcome("G3", False, reject)
        assert plan is not None
        self._ctx["plan"] = plan
        if available < plan.required_margin * 1.20:
            return GateOutcome("G3", False,
                               f"liquid headroom short: available ${available:.2f} < "
                               f"required ${plan.required_margin * 1.2:.2f} (1.2x cushion)")
        return GateOutcome("G3", True,
                           f"qty {plan.qty} risk ${plan.risk_usd:.2f} margin ${plan.required_margin:.2f}")

    # ------------------------------------------------------------------
    # G4 — Orderbook Spread
    # ------------------------------------------------------------------

    async def _g4(self, setup) -> GateOutcome:
        bid, ask = await self.client.orderbook_top()
        ticker = await self.client.tickers()
        mark = ticker["mark_price"] or (bid + ask) / 2
        spread_pct = (ask - bid) / mark if mark > 0 else 1.0
        if spread_pct > self.s.max_spread_pct:
            return GateOutcome("G4", False,
                               f"spread {spread_pct * 100:.3f}% > {self.s.max_spread_pct * 100:.2f}% "
                               "(flash illiquidity)")
        return GateOutcome("G4", True, f"spread {spread_pct * 100:.4f}%")

    # ------------------------------------------------------------------
    # G5 — Chase Guard
    # ------------------------------------------------------------------

    async def _g5(self, setup) -> GateOutcome:
        ticker = await self.client.tickers()
        mark = ticker["mark_price"] or setup.trigger_close
        run_pct = abs(mark - setup.trigger_close) / setup.trigger_close
        if run_pct > self.s.max_chase_pct:
            return GateOutcome("G5", False,
                               f"price ran {run_pct * 100:.3f}% since scan (>{self.s.max_chase_pct * 100:.2f}%)")
        return GateOutcome("G5", True, f"price drift {run_pct * 100:.4f}%")

    # ------------------------------------------------------------------
    # G6 — Volatility Extreme
    # ------------------------------------------------------------------

    async def _g6(self, setup) -> GateOutcome:
        threshold = setup.entry * self.s.max_atr_vs_entry_pct
        if setup.atr_15m > threshold:
            return GateOutcome("G6", False,
                               f"15m ATR ${setup.atr_15m:.1f} > 2.5% of entry (${threshold:.1f}) "
                               "— black swan anomaly")
        return GateOutcome("G6", True, f"ATR ${setup.atr_15m:.1f}")

    # ------------------------------------------------------------------
    # G7 — Net Expectancy (after round-trip taker fees)
    # ------------------------------------------------------------------

    async def _g7(self, setup) -> GateOutcome:
        plan = self._ctx.get("plan")
        if plan is None:
            return GateOutcome("G7", False, "internal: sizing plan missing")
        if plan.net_rr < self.s.min_net_rr:
            return GateOutcome("G7", False,
                               f"net expectancy {plan.net_rr:.2f} < {self.s.min_net_rr:.2f} "
                               "after 0.11% round-trip fees")
        return GateOutcome("G7", True, f"net R:R {plan.net_rr:.2f}")

    # ------------------------------------------------------------------
    # G8 — Carrying Cost / Funding
    # ------------------------------------------------------------------

    async def _g8(self, setup) -> GateOutcome:
        ticker = await self.client.tickers()
        funding = ticker["funding_rate"]
        next_ms = ticker["next_funding_ms"]
        now_ms = self.client.now_ms()
        countdown_min = (next_ms - now_ms) / 60000.0 if next_ms else 9999.0
        adverse = (funding > self.s.adverse_funding_threshold and setup.direction == "Buy") or \
                  (funding < -self.s.adverse_funding_threshold and setup.direction == "Sell")
        if adverse and countdown_min < self.s.funding_lookahead_minutes:
            return GateOutcome("G8", False,
                               f"adverse funding {funding * 100:.4f}% within "
                               f"{countdown_min:.0f} min of event")
        return GateOutcome("G8", True, f"funding {funding * 100:.4f}%, window {countdown_min:.0f}m")

    # ------------------------------------------------------------------
    # G9 — Clock Synchronization
    # ------------------------------------------------------------------

    async def _g9(self, setup) -> GateOutcome:
        drift = await self.client.assert_clock()
        if drift >= self.s.max_clock_drift_ms:
            return GateOutcome("G9", False, f"clock drift {drift}ms >= {self.s.max_clock_drift_ms}ms")
        return GateOutcome("G9", True, f"drift {drift}ms")
