"""
Aegis-Universal :: FastAPI Web Layer & Lifespan Manager
========================================================
Contract Sections 2.1, 6, 8, 9:

  * GET /healthz/{HEALTH_UUID}  — keep-alive ping + real-time equity,
    position, daily PnL, uptime telemetry (UptimeRobot target).
  * GET /dashboard              — protected operator status monitor (Basic Auth).
  * GET /                       — root liveness for platform checks.
  * Lifespan: reconcile-on-boot -> pre-flight -> "ONLINE & HEALTHY" email
    handshake -> scanner + position-manager daemons -> digest watchdog.
  * Global exception handlers: any uncaught error is emailed INSTANTLY
    (Section 9.2) and returns a 500 JSON envelope.
"""

from __future__ import annotations

import base64
import asyncio
import html
import logging
import os
import secrets as pysecrets
import sys
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from config import SETTINGS, Settings
from engine.execution import BybitClient
from engine.position_manager import PositionManager
from engine.risk_gates import RiskGates
from engine.pipeline import make_pipeline
from engine.reconciler import Reconciler
from engine.scanner import MarketSetup, Scanner
from engine.state import StateStore
from services.alerter import Alerter, install_loop_hook, install_process_hooks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
logging.Formatter.converter = time.gmtime
logger = logging.getLogger("aegis.main")


# ============================================================================
# Global runtime context (bound during lifespan startup)
# ============================================================================
SHARED: Dict[str, Any] = {
    "equity": 0.0, "available": 0.0, "position": None,
    "boot": None, "booted": False, "gate_history": None,
}
ALERTER: Optional[Alerter] = None
CLIENT: Optional[BybitClient] = None
STATE: Optional[StateStore] = None
GATES: Optional[RiskGates] = None
SCANNER: Optional[Scanner] = None
PMGR: Optional[PositionManager] = None
BACKGROUND_TASKS: list = []


# ============================================================================
# Trade pipeline: shared implementation (engine/pipeline.py), wired in lifespan
# ============================================================================

TRADE_PIPELINE = None  # bound during lifespan startup


# ============================================================================
# Daily digest watchdog (Contract 9.4)
# ============================================================================

async def digest_watchdog() -> None:
    assert STATE is not None
    while True:
        try:
            now = time.gmtime()
            day = time.strftime("%Y-%m-%d", now)
            current_day = STATE.get("day")
            if current_day and current_day != day and STATE.get("digest_sent_day") != current_day:
                day_start = _utc_day_start_seconds(
                    now.tm_year, now.tm_mon, now.tm_mday, offset_days=1)
                stats = STATE.journal_day_stats(day_start)
                stats["day"] = current_day
                stats["equity"] = SHARED.get("equity", 0.0)
                baseline = STATE.baseline_equity() or 0.0
                stats["baseline"] = baseline
                stats["vs_baseline"] = ((stats["equity"] - baseline) / baseline * 100.0
                                        if baseline else 0.0)
                stats["halted"] = bool(STATE.kill_switch() or STATE.pause_until() > time.time())
                pos = SHARED.get("position")
                stats["position"] = (f"{pos.side} {pos.qty} @ ${pos.entry:,.1f}"
                                     if pos else "none")
                if ALERTER and ALERTER.enabled:
                    ALERTER.enqueue(ALERTER.send_digest, dict(stats))
                STATE.set_digest_sent(current_day)
                STATE.ensure_utc_day()
        except Exception as exc:  # noqa: BLE001
            if ALERTER:
                ALERTER.report("digest.watchdog", exc)
        await asyncio.sleep(60)


def _utc_day_start_seconds(y: int, m: int, d: int, offset_days: int = 0) -> int:
    """UTC timestamp (seconds) for 00:00 N days before the given calendar date."""
    from datetime import datetime, timedelta, timezone
    dt = datetime(y, m, d, tzinfo=timezone.utc) - timedelta(days=offset_days)
    return int(dt.timestamp())


# ============================================================================
# Lifespan
# ============================================================================

@asynccontextmanager
async def lifespan(_app: FastAPI):
    global ALERTER, CLIENT, STATE, GATES, SCANNER, PMGR

    s = SETTINGS
    ALERTER = Alerter(s.resend_api_key, s.receiver_email, s.alert_from_email,
                      s.error_dedupe_window_seconds)
    STATE = StateStore(s.state_db_path)
    CLIENT = BybitClient(s, ALERTER)

    SCANNER = Scanner(CLIENT, ALERTER, s, on_setup=make_pipeline(
        CLIENT, STATE, PMGR, ALERTER, SHARED, GATES))
    PMGR = PositionManager(CLIENT, STATE, ALERTER, s, SHARED)
    GATES = RiskGates(CLIENT, STATE, ALERTER, s, SHARED)
    reconciler = Reconciler(CLIENT, STATE, ALERTER, s, PMGR, SCANNER)

    install_process_hooks(ALERTER)
    loop = asyncio.get_running_loop()
    install_loop_hook(ALERTER, loop)

    await ALERTER.start()
    logger.info("=== Aegis-Universal booting (health uuid=%s...) ===", s.health_uuid[:8])

    boot_report: Dict[str, Any] = {}
    try:
        boot_report = await reconciler.boot()
    except Exception as exc:  # noqa: BLE001 — catastrophic boot failure still emails
        ALERTER.report("reconciler.boot", exc, {"phase": "lifespan"})
        boot_report = {"ok": False,
                       "errors": [f"reconciler.boot crashed: {type(exc).__name__}: {exc}"]}
    SHARED["boot"] = boot_report
    SHARED["booted"] = True
    if boot_report.get("equity"):
        SHARED["equity"] = boot_report["equity"]

    SCANNER.start()
    PMGR.start()
    BACKGROUND_TASKS.append(asyncio.create_task(digest_watchdog()))

    if boot_report.get("ok"):
        external = os.getenv("RENDER_EXTERNAL_URL") or ""
        health_url = f"{external}/healthz/{s.health_uuid}" if external else "/healthz/<uuid>"
        dash_url = f"{external}/dashboard" if external else "/dashboard"
        try:
            await ALERTER.send_boot_success({
                "equity": boot_report.get("equity", 0.0) or SHARED.get("equity", 0.0),
                "leverage": str(boot_report.get("leverage", s.max_leverage)),
                "risk_pct": s.risk_per_trade_pct,
                "margin_alloc_pct": s.max_margin_alloc_pct,
                "symbol": s.bybit_symbol,
                "dashboard_url": dash_url,
                "health_url": health_url,
            })
            logger.info("boot-success email dispatched")
        except Exception as exc:  # noqa: BLE001
            ALERTER.report("alerter.boot_email", exc)
    else:
        try:
            await ALERTER.send_boot_failure(list(boot_report.get("errors", [])),
                                            {"equity": boot_report.get("equity", 0.0)})
        except Exception as exc:  # noqa: BLE001
            ALERTER.report("alerter.boot_email", exc)

    logger.info("=== Aegis-Universal online ===")
    try:
        yield
    finally:
        logger.info("Aegis-Universal shutting down...")
        for task in BACKGROUND_TASKS:
            task.cancel()
        await SCANNER.stop()
        await PMGR.stop()
        await ALERTER.close()
        await CLIENT.close_http()
        STATE.close()


app = FastAPI(title="Aegis-Universal", version="1.0.0", lifespan=lifespan)


# ============================================================================
# Global exception trapping (Contract 9.2)
# ============================================================================

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    if ALERTER:
        ALERTER.report("fastapi.web", exc, {
            "path": request.url.path, "method": request.method})
    return JSONResponse(status_code=500, content={"detail": "internal error (operator alerted)"})


# ============================================================================
# Routes
# ============================================================================

@app.get("/")
async def root() -> JSONResponse:
    return JSONResponse({"app": "Aegis-Universal", "status": "running",
                         "health": f"/healthz/{SETTINGS.health_uuid[:8]}..."})


@app.get("/healthz/{uuid}")
async def healthz(uuid: str) -> JSONResponse:
    if not pysecrets.compare_digest(str(uuid), SETTINGS.health_uuid):
        raise HTTPException(status_code=404, detail="unknown health endpoint")
    pos = SHARED.get("position")
    state_day = STATE.ensure_utc_day() if STATE else ("", False)
    daily_pnl = STATE.daily_realized_pnl() if STATE else 0.0
    return JSONResponse({
        "status": "ok" if not (SETTINGS.kill_switch_active or (STATE and STATE.kill_switch())) else "halted",
        "app": "Aegis-Universal",
        "uptime_seconds": int(uptime()),
        "total_equity_usdt": round(SHARED.get("equity", 0.0), 4),
        "available_balance_usdt": round(SHARED.get("available", 0.0), 4),
        "active_position": ({"side": pos.side, "qty": pos.qty,
                             "entry": pos.entry, "entry_ts": pos.opened_ts}
                            if pos else None),
        "daily_pnl_usdt": round(daily_pnl, 4),
        "utc_day": state_day[0],
        "last_scan": (SCANNER.shared.get("last_scan_ts") if SCANNER else None),
        "last_consensus": (SCANNER.shared.get("consensus") if SCANNER else ""),
        "ts": int(time.time()),
    })


def uptime() -> float:
    t0 = SHARED.get("_t0")
    if t0 is None:
        SHARED["_t0"] = time.time()
        return 0.0
    return time.time() - t0


# ---------------------------------------------------------------------------
# Dashboard (HTTP Basic Auth)
# ---------------------------------------------------------------------------

def _basic_auth(request: Request) -> bool:
    header = request.headers.get("authorization", "")
    if not header.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(header[6:]).decode("utf-8")
        user, _, password = decoded.partition(":")
    except Exception:  # noqa: BLE001
        return False
    return pysecrets.compare_digest(user, SETTINGS.dashboard_username) and \
        pysecrets.compare_digest(password, SETTINGS.dashboard_password)


_STATUS_STYLE = """<!DOCTYPE html><html><head><title>Aegis-Universal</title></head>
<body style="font-family:'Segoe UI',sans-serif;background:#0f172a;color:#e5e7eb;margin:0;padding:30px;">
<h1 style="color:#10b981;">Aegis-Universal — Operator Monitor</h1>
<table style="border-collapse:collapse;font-size:14px;">%s</table>
<h2 style="color:#60a5fa;">Last Gate Decisions (newest first)</h2>
<table style="border-collapse:collapse;font-size:13px;">%s</table>
<h2 style="color:#f87171;">Recent Intercepted Errors</h2>
<table style="border-collapse:collapse;font-size:13px;">%s</table>
</body></html>"""

_ROW_D = ("<tr><td style='padding:4px 12px;color:#9ca3af;'>%s</td>"
          "<td style='padding:4px 12px;'><b>%s</b></td></tr>")


@app.get("/dashboard")
async def dashboard(request: Request):
    if not _basic_auth(request):
        return HTMLResponse(
            "<html><body style='background:#0f172a;color:#f87171;margin:40px;'>"
            "<h2>401 Unauthorized</h2></body></html>", status_code=401)
    if not SHARED.get("booted"):
        return HTMLResponse("<h3>booting…</h3>", status_code=200)

    boot = SHARED.get("boot") or {}
    pos = SHARED.get("position")
    rows = ""
    for k, v in [
        ("System Status", "OPERATIONAL (24/7 scanning)" if not (STATE and STATE.kill_switch())
         else "⛔ KILL SWITCH (entries halted)"),
        ("Uptime", f"{uptime() / 3600:.2f} h"),
        ("Total Equity", f"${SHARED.get('equity', 0):.2f} USDT"),
        ("Available", f"${SHARED.get('available', 0):.2f} USDT"),
        ("Baseline Equity", f"${STATE.baseline_equity() or 0:.2f} USDT"),
        ("Risk / Trade", f"{SETTINGS.risk_per_trade_pct * 100:.2f}%  @ {SETTINGS.max_leverage:g}x ISOLATED"),
        ("Daily Realized PnL", f"${STATE.daily_realized_pnl():.4f} USDT"),
        ("Consecutive Losses", STATE.consecutive_losses()),
        ("Cooldown Remaining",
         f"{max(0.0, SETTINGS.cooldown_minutes * 60 - (time.time() - STATE.last_trade_closed_ts())) / 60:.1f} min"
         if STATE.last_trade_closed_ts() else "n/a"),
        ("Position", (f"{pos.side} {pos.qty:.3f} @ ${pos.entry:,.1f} | opened "
                      f"{(time.time() - pos.opened_ts) / 3600:.2f}h ago") if pos else "flat"),
        ("Last Consensus", SCANNER.shared.get("consensus", "") if SCANNER else ""),
        ("Boot Pre-flight", "PASSED (zero errors)" if boot.get("ok") else f"ISSUES: {boot.get('errors', [])}"),
    ]:
        rows += _ROW_D % (k, v)

    gate_rows = ""
    if GATES:
        for item in list(GATES.history)[:15]:
            gate_rows += ("<tr><td style='padding:4px 12px;color:#9ca3af;'>%s</td>"
                          "<td style='padding:4px 12px;'>%s</td>"
                          "<td style='padding:4px 12px;'>%s</td>"
                          "<td style='padding:4px 12px;'>%s</td></tr>") % (
                time.strftime("%H:%M:%S", time.gmtime(item.get("ts", 0))),
                html.escape(item.get("gate", "")),
                html.escape(item.get("direction", "")),
                html.escape(item.get("reason", ""))[:120])

    err_rows = ""
    if ALERTER:
        for rec in list(ALERTER.recent_errors)[-12:][::-1]:
            err_rows += ("<tr><td style='padding:4px 12px;color:#9ca3af;'>%s</td>"
                         "<td style='padding:4px 12px;'>%s</td>"
                         "<td style='padding:4px 12px;'>%s</td>"
                         "<td style='padding:4px 12px;'>%s</td></tr>") % (
                time.strftime("%H:%M:%S", time.gmtime(rec.ts)),
                html.escape(rec.module), html.escape(rec.error_type),
                html.escape(rec.message)[:120])

    return HTMLResponse(_STATUS_STYLE % (rows, gate_rows or "<tr><td>none yet</td></tr>",
                                         err_rows or "<tr><td>none</td></tr>"))


@app.post("/dashboard/kill")
async def kill_switch(request: Request) -> JSONResponse:
    if not _basic_auth(request):
        raise HTTPException(status_code=401, detail="unauthorized")
    STATE.set_kill_switch(True, "manual operator kill switch")
    if ALERTER:
        ALERTER.enqueue(ALERTER.send_trade_event, "halt", {
            "Trigger": "Manual operator kill switch (dashboard)", })
    return JSONResponse({"status": "kill switch ENGAGED (entries halted)"})


@app.post("/dashboard/resume")
async def resume(request: Request) -> JSONResponse:
    if not _basic_auth(request):
        raise HTTPException(status_code=401, detail="unauthorized")
    STATE.set_kill_switch(False)
    STATE.set_pause_until(0.0)
    return JSONResponse({"status": "circuit breakers cleared (entries re-armed, gates remain active)"})


def main() -> None:
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
