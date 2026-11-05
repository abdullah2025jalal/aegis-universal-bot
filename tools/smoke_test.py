"""Offline smoke test: imports, gate math, G7 expectancy, alerter dedupe — NO network, NO orders."""
import asyncio
import math
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
    from engine.risk_gates import clamp_sl_distance, compute_sizing
    from engine.scanner import MarketSetup, compute_atr_14
    from services.alerter import install_process_hooks

    s = config.Settings.from_env()
    assert s.risk_per_trade_pct == 0.015 and s.max_leverage == 5.0 and s.target_gross_rr == 1.50

    # ATR computation
    klines = [[str(i), "0", "10", "9", "9.5", "1", "1"] for i in range(15)]
    atr = compute_atr_14(klines)
    assert approx(atr, 1.0), f"atr={atr}"

    # SL clamp: ATR small -> floor 0.5%
    assert approx(clamp_sl_distance(100.0, 0.001, s), 0.5)
    assert approx(clamp_sl_distance(100.0, 1.0, s), 1.5)   # 1.50*ATR inside band
    assert approx(clamp_sl_distance(100.0, 1.5, s), 2.0)   # 1.5*ATR=2.25 -> ceiling 2%
    assert approx(clamp_sl_distance(100.0, 5.0, s), 2.0)   # 1.5*5=7.5 -> ceiling 2%

    specs = InstrumentSpec("BTCUSDT", tick_size=0.1, qty_step=0.001,
                           min_order_qty=0.001, max_order_qty=1000.0, max_leverage=100.0)

    # $50,000 equity @ 65,000 entry, ATR=800 -> SL=1200 (1.85%, in band); risk 750 USD
    equity, entry, atr_ok, atr_tight = 50_000.0, 65_000.0, 800.0, 300.0
    plan, reject = compute_sizing(equity, entry, atr_ok, s, specs)
    assert reject == "" and plan is not None, reject
    sl_dist = 1.5 * atr_ok  # 1200 (1.85%) inside clamp
    risk_usd = equity * 0.015  # 750
    fee = entry * 0.0011  # 71.5
    raw = risk_usd / (sl_dist + fee)
    expect_qty = math.floor(raw / 0.001) * 0.001
    assert approx(plan.qty, expect_qty), (plan.qty, expect_qty)
    assert approx(plan.net_rr, (s.target_gross_rr * sl_dist - fee) / (sl_dist + fee))
    assert plan.net_rr >= s.min_net_rr, f"netRR {plan.net_rr} must pass G7"
    print(f"  sizing: qty={plan.qty} slack SL=${sl_dist} netRR={plan.net_rr:.3f} "
          f"margin=${plan.required_margin:.2f} (<= ${equity*0.35})")

    # micro accounts: $20 equity -> raw qty floors to 0, min-lot cost $0.52 > 2.5%*20=$0.50 -> reject
    plan2, reject2 = compute_sizing(20.0, entry, atr_tight, s, specs)
    assert plan2 is None and "insufficient for minimum lot" in reject2, reject2
    # borderline full accommodation: 0.001*(450+71.5)=0.5215 <= 2.5% of 25 = 0.625
    # (isolate the min-lot branch: artifically raise leverage so the 35% margin
    #  ceiling does not mask the formulary path under test)
    s_test = config.Settings.from_env()
    s_test.max_leverage = 25.0
    plan3, reject3 = compute_sizing(25.0, entry, atr_tight, s_test, specs)
    assert reject3 == "" and approx(plan3.qty, 0.001), (reject3, plan3)

    # formatting helpers
    assert fmt_price(72140.51, 0.1) == "72140.5"
    assert fmt_qty(0.001234, 0.001) == "0.001"

    # alerter fingerprint dedupe megerges (network disabled)
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
