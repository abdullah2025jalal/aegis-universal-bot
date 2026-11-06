"""
Aegis-Universal :: Market Setup & Venue Indicators (Bybit-only)
================================================================
Shared structures for the AI decision feed. Bybit venue data only —
the market universe is exactly one instrument (BTCUSDT linear perp)
and no external data providers are used anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class MarketSetup:
    direction: str                     # "Buy" | "Sell"
    entry: float                       # Ask for longs, Bid for shorts (at AI decision time)
    trigger_close: float               # last traded price at AI decision time
    atr_15m: float                     # ATR(14) in USD
    rsi_15m: float
    ema50_15m: float
    close_15m: float
    summary_15m: str
    summary_1h: str
    sl_distance: float = 0.0           # filled by the execution planner (exit planner anchor)
    tp_distance: float = 0.0

    def to_ticket(self) -> Dict[str, Any]:
        return {
            "Direction": self.direction,
            "Entry Ref": f"${self.entry:,.1f}",
            "Mark/Last": f"${self.trigger_close:,.1f}",
            "ATR(15m)": f"${self.atr_15m:,.1f}",
            "RSI(15m)": f"{self.rsi_15m:.1f}",
            "15m Context": self.summary_15m,
            "1h Context": self.summary_1h,
        }


def compute_atr_14(klines_15m: List[List[str]]) -> float:
    """Simple averaged true-range (14) computed from Bybit 15m kline rows."""
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
