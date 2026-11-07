"""One-shot: upgrade the existing open position's SL/TP to the FIXED +-2 USD
net bracket (operator directive 2026-11-07). Uses the engine's own math
(plan_bracket) and the exchange-side attach endpoint, exactly like new trades.
"""
import asyncio
import sys

sys.path.insert(0, ".")
from config import SETTINGS                      # noqa: E402
from services.alerter import Alerter              # noqa: E402
from engine.execution import BybitClient         # noqa: E402
from engine.direct_executor import plan_bracket  # noqa: E402


async def main() -> None:
    cli = BybitClient(SETTINGS, Alerter(SETTINGS.resend_api_key,
                                        SETTINGS.receiver_email,
                                        SETTINGS.alert_from_email))
    spec = await cli.instrument()
    open_rows = [p for p in (await cli.positions())
                 if float(p.raw.get("size") or 0) > 0]
    if not open_rows:
        print("NO_POSITION")
        return
    p = open_rows[0]
    side = str(p.raw.get("side"))
    entry = float(p.raw.get("avgPrice") or 0)

    kl = await cli.klines_15m(limit=15)
    hs = [float(r[2]) for r in kl]
    ls = [float(r[3]) for r in kl]
    cs = [float(r[4]) for r in kl]
    trs = [max(hs[i] - ls[i], abs(hs[i] - cs[i - 1]), abs(ls[i] - cs[i - 1]))
           for i in range(1, len(kl))]
    atr = sum(trs) / len(trs) if trs else 0.0

    plan = plan_bracket(entry, atr, spec, SETTINGS, side)
    fee_u = entry * SETTINGS.round_trip_fee_rate
    print("PLAN side=%s entry=%.1f qty=%s ATR=%.1f" % (side, entry, plan["qty"], atr))
    print("PLAN SL=%.4f (dist %.1f, stop-outs net %.4f USD)" %
          (plan["sl_price"], plan["sl_distance"],
           (plan["sl_distance"] + fee_u) * plan["qty"]))
    print("PLAN TP=%.4f (dist %.1f, target-outs net %.4f USD)" %
          (plan["tp_price"], plan["tp_distance"],
           (plan["tp_distance"] - fee_u) * plan["qty"]))

    res = await cli.attach_trading_stop(
        side, stop_loss=float(plan["sl_price"]),
        take_profit=float(plan["tp_price"]))
    print("AMEND retCode=%s retMsg=%s" %
          (res.get("retCode"), res.get("retMsg")))

    for q in (await cli.positions()):
        if float(q.raw.get("size") or 0) > 0:
            print("AFTER side=%s size=%s entry=%s SL=%s TP=%s" %
                  (q.raw.get("side"), q.raw.get("size"),
                   q.raw.get("avgPrice"), q.raw.get("stopLoss"),
                   q.raw.get("takeProfit")))


asyncio.run(main())
