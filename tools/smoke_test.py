"""Offline smoke test: imports, execution-planner math, alerter dedupe — NO network, NO orders."""
import asyncio
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def approx(a, b, tol=1e-9):
    return abs(a - b) <= tol * max(1.0, abs(a))


def main():
    import config
    from engine.execution import InstrumentSpec, fmt_price, fmt_qty
    from engine.direct_executor import plan_bracket
    from engine.marketsetup import MarketSetup, compute_atr_14
    from services.alerter import install_process_hooks

    s = config.Settings.from_env()
    assert s.risk_per_trade_pct == 0.015 and s.max_leverage == 5.0 and s.target_gross_rr == 1.50

    # ATR computation
    klines = [[str(i), "0", "10", "9", "9.5", "1", "1"] for i in range(15)]
    atr = compute_atr_14(klines)
    assert approx(atr, 1.0), f"atr={atr}"

    # ---- FIXED ±$1 stakes (operator directive 2026-11-06) --------------
    # plan_bracket now: qty=exchange minimum; SL nets ≈ -$1, TP nets ≈ +$1
    # (fees included); the only widening force is the 1.5*ATR noise floor.
    specs = InstrumentSpec("BTCUSDT", tick_size=0.1, qty_step=0.001,
                           min_order_qty=0.001, max_order_qty=1000.0, max_leverage=100.0)

    entry = 110_000.0                     # BTC example price
    fee_u = entry * s.round_trip_fee_rate   # round-trip taker fee per BTC
    plan = plan_bracket(entry, 300.0, specs, s, "Buy")
    assert approx(plan["qty"], specs.min_order_qty), plan
    assert approx(plan["sl_distance"], 1.0 / 0.001 - fee_u), plan["sl_distance"]
    assert approx(plan["tp_distance"], 1.0 / 0.001 + fee_u), plan["tp_distance"]
    assert approx(plan["sl_price"], entry - plan["sl_distance"])
    assert approx(plan["tp_price"], entry + plan["tp_distance"])
    # net P/L at the levels: SL = -$1 and TP = +$1, fees included
    assert approx((plan["sl_distance"] + fee_u) * 0.001, 1.0)
    assert approx((plan["tp_distance"] - fee_u) * 0.001, 1.0)
    print(f"  fixed stake (Buy): SL=${plan['sl_price']:,.1f} TP=${plan['tp_price']:,.1f} "
          f"(distances ${plan['sl_distance']:,.1f} / ${plan['tp_distance']:,.1f})")

    plan_sell = plan_bracket(entry, 300.0, specs, s, "Sell")
    assert approx(plan_sell["sl_price"], entry + plan_sell["sl_distance"])
    assert approx(plan_sell["tp_price"], entry - plan_sell["tp_distance"])

    # noise floor: unusually high ATR widens the stop past the $1 target
    # (honest edge case — realized loss may exceed $1 modestly)
    plan_big = plan_bracket(entry, 800.0, specs, s, "Buy")
    assert approx(plan_big["sl_distance"], 1200.0)
    assert approx(plan_big["tp_distance"], 1.0 / 0.001 + fee_u)  # TP still nets +$1

    # $1 execution cap constants present (operator rules)
    assert s.ai_max_price_deviation_usd >= 0.0
    assert approx(s.fixed_risk_per_trade_usd, 1.0)

    # formatting helpers
    assert fmt_price(72140.51, 0.1) == "72140.5"
    assert fmt_qty(0.001234, 0.001) == "0.001"

    # alerter fingerprint dedupe (network disabled)
    from services.alerter import Alerter
    al = Alerter("", "", "", dedupe_window_seconds=900)  # disabled mode
    install_process_hooks(al)
    try:
        1 / 0
    except ZeroDivisionError as err:
        al.report("selftest", err, {"phase": "dedupe"})
    assert len(al.recent_errors) == 1 and al.recent_errors[0].sent is False

    # state store round trip
    with tempfile.TemporaryDirectory() as tmp:
        from engine.state import StateStore
        st = StateStore(os.path.join(tmp, "t.sqlite3"))
        st.set_baseline_equity(123.45)
        assert st.baseline_equity() == 123.45
        jid = st.journal_open(1, "Buy", 0.001, 65000.0, 64550.0, 65675.0, 750.0, 1.3, "link")
        st.journal_close(jid, 2, 12.5, "SL/TP")
        stats = st.journal_day_stats(0)
        assert stats["closed"] == 1 and stats["pnl"] == 12.5 and stats["win_rate"] == 100.0
        st.set_last_trade_closed_ts(time.time())
        assert st.last_trade_closed_ts() > 0
        st.close()

    print("SMOKE_OK")


if __name__ == "__main__":
    main()
