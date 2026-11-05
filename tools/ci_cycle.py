#!/usr/bin/env python3
"""
Aegis-Universal :: GitHub-Actions Hosting Mode — One-Shot Engine Cycle
=======================================================================
Runs on a 5-minute GitHub Actions schedule (the tightest interval GitHub
allows) and executes exactly ONE full engine cycle per run:

  1. Boot reconciliation (exchange is master ledger) + preflight.
  2. One immediate market-scan cycle (tradingview-ta + Bybit market data).
  3. Risk gates G0..G9 -> atomic Limit-IOC bracket -> zero-naked-exposure audit.
  4. Position supervision window (~4 min at 15s cadence when a position is
     open: Breakeven+ / ATR trail / TTL) then exit; the next scheduled run
     resumes seamlessly via exchange reconciliation.
  5. Daily-loss / streak / cooldown / kill-switch state is exchange-sourced;
     operator-facing windows persist via aegis_state.json committed by the
     workflow (repo file survives restarts).
  6. UTC 00:00 daily digest email + all Section-9 error telemetry.

Safety invariant: SL/TP live on the exchange inside the order itself, so a
missed/delayed cycle never leaves an unhedged or unmanaged position.
Entries always open via Limit IOC with embedded SL/TP (Never List #1).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logging.Formatter.converter = time.gmtime
logger = logging.getLogger("aegis.ci")


def _utc_midnight(days_back: int = 0) -> int:
    now = datetime.now(timezone.utc)
    return int((datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
                - timedelta(days=days_back)).timestamp())


async def main() -> None:
    from config import SETTINGS
    from engine.execution import BybitClient
    from engine.pipeline import make_pipeline
    from engine.position_manager import PositionManager
    from engine.reconciler import Reconciler
    from engine.risk_gates import RiskGates
    from engine.scanner import Scanner
    from engine.state import JsonStateStore
    from services.alerter import (Alerter, install_loop_hook, install_process_hooks)

    settings = SETTINGS
    shared: Dict[str, Any] = {"equity": 0.0, "available": 0.0, "position": None,
                              "gate_history": None}
    alerter = Alerter(settings.resend_api_key, settings.receiver_email,
                      settings.alert_from_email, settings.error_dedupe_window_seconds)
    install_process_hooks(alerter)
    await alerter.start()

    state = JsonStateStore(os.path.join(ROOT, "aegis_state.json"))
    client = BybitClient(settings, alerter)
    scanner = Scanner(client, alerter, settings, on_setup=None)
    pm = PositionManager(client, state, alerter, settings, shared)
    gates = RiskGates(client, state, alerter, settings, shared)
    reconciler = Reconciler(client, state, alerter, settings, pm, scanner)

    loop = asyncio.get_running_loop()
    install_loop_hook(alerter, loop)

    first_run = state.get("ci_boot_ok") is not True
    logger.info("=== Aegis-Universal CI cycle start (first_run=%s) ===", first_run)

    boot: Dict[str, Any] = {}
    try:
        boot = await reconciler.boot()
    except Exception as exc:  # noqa: BLE001
        alerter.report("ci.reconciler", exc, {"phase": "boot"})
        boot = {"ok": False, "errors": [f"reconciler.boot crashed: {type(exc).__name__}: {exc}"]}
    errors = list(boot.get("errors", []))
    shared["equity"] = boot.get("equity") or shared["equity"]

    # Section 9.1 handshake — once, on the first zero-error boot
    if first_run and not errors:
        try:
            await alerter.send_boot_success({
                "equity": boot.get("equity", 0.0),
                "leverage": str(boot.get("leverage", settings.max_leverage)),
                "risk_pct": settings.risk_per_trade_pct,
                "margin_alloc_pct": settings.max_margin_alloc_pct,
                "symbol": settings.bybit_symbol,
                "dashboard_url": "GitHub Actions: continuous 5-minute engine cycles",
                "health_url": "aegis_state.json in-repo telemetry + workflow runs",
            })
            state.set("ci_boot_ok", True)
            logger.info("boot-success email dispatched (first zero-error boot)")
        except Exception as exc:  # noqa: BLE001
            alerter.report("ci.boot_email", exc)
    elif first_run and errors:
        try:
            await alerter.send_boot_failure(errors, {"equity": boot.get("equity", 0.0)})
        except Exception as exc:  # noqa: BLE001
            alerter.report("ci.boot_email", exc)

    # ---- one scan cycle (immediate; no minute-boundary wait in CI mode) ----
    scanner.on_setup = make_pipeline(client, state, pm, alerter, shared, gates)
    try:
        await scanner.tick()
    except Exception as exc:  # noqa: BLE001 — already alerted by scanner internals
        logger.error("scan cycle failed: %s", exc)

    # ---- supervised maintenance window (Contract Section 7 cadence) ----
    if pm.tracked is not None:
        budget = float(os.getenv("CI_SUPERVISE_SECONDS", "240"))
        logger.info("position open — supervising for %.0fs", budget)
        deadline = time.time() + budget
        while time.time() < deadline and pm.tracked is not None:
            await asyncio.sleep(settings.position_poll_seconds)
            try:
                await pm.tick_once()
            except Exception as exc:  # noqa: BLE001
                alerter.report("position_manager.tick", exc, {"phase": "ci_window"})

    # ---- UTC-day rollover -> daily digest from exchange closed-pnl ----
    try:
        if state.get("day") and state.get("day") != state.utc_day() \
                and state.get("digest_sent_day") != state.get("day"):
            prev_day = state.get("day")
            start = _utc_midnight(1)
            end = _utc_midnight(0)
            rows = await client.closed_pnl(start * 1000, end * 1000)
            realized = sum(float(r.get("closedPnl") or 0) for r in rows)
            wins = sum(1 for r in rows if float(r.get("closedPnl") or 0) >= 0)
            closed = len(rows)
            equity = shared.get("equity", 0.0)
            baseline = state.baseline_equity() or 0.0
            if alerter.enabled and closed:
                await alerter.send_digest({
                    "day": prev_day,
                    "pnl": realized,
                    "closed": closed,
                    "win_rate": (wins / closed * 100.0) if closed else 0.0,
                    "equity": equity,
                    "baseline": baseline,
                    "vs_baseline": ((equity - baseline) / baseline * 100.0) if baseline else 0.0,
                    "halted": bool(state.kill_switch() or state.pause_until() > time.time()),
                    "position": "n/a (CI cycle)",
                })
            state.set_digest_sent(prev_day)
    except Exception as exc:  # noqa: BLE001
        alerter.report("ci.digest", exc)

    state.set("last_cycle_ts", time.time())
    state.set("last_cycle_utc", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
    state.set("last_equity", shared.get("equity", 0.0))
    state.set("last_consensus", scanner.shared.get("consensus", ""))
    if boot.get("baseline") and state.baseline_equity() is not None:
        state.set("floor_level", round(boot["baseline"] *
                                       settings.equity_preservation_floor_pct, 2))
    state.save()

    await alerter.close()
    await client.close_http()
    logger.info("=== CI cycle complete (ok=%s) ===", not errors)
    # exit 0: transient API/data-feed failures must not produce red runs every
    # 5m; every failure was already emailed with a full stack trace.


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except SystemExit:
        raise
    except Exception:  # noqa: BLE001 — final safety net: email + clean exit for probe
        traceback.print_exc()
