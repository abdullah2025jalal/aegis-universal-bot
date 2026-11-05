"""
Aegis-Universal :: PythonAnywhere WSGI Adapter
==============================================
PythonAnywhere web apps run WSGI only (no ASGI), so the FastAPI daemon in
main.py cannot serve there directly. This adapter preserves the ORIGINAL
engine semantics unchanged:

  * Loads secrets.env (KEY=VALUE lines) into os.environ BEFORE importing any
    engine module (config.py reads env at import time). Never logs secrets.
  * Imports main.py UNCHANGED and enters its FastAPI Lifespan inside one
    background asyncio thread, so the reconciler + scanner + position
    manager + digest watchdog grow exactly as in the ASGI daemon.
  * Exposes the EXACT same routes as main.py over a stdlib WSGI callable
    (zero new dependencies; Flask is not used):
        GET  /
        GET  /healthz/{HEALTH_UUID}
        GET  /healthz/{HEALTH_UUID}/pingext    (diagnostic reachability probe)
        GET  /dashboard                        (HTTP Basic Auth)
        POST /dashboard/kill                   (HTTP Basic Auth)
        POST /dashboard/resume                 (HTTP Basic Auth)
  * WSGI handlers touch engine state ONLY on the engine thread via
    asyncio.run_coroutine_threadsafe(...).result(timeout=10).
  * Small PA-specific additions (documented deviations, fail-safe only):
        - Single-owner engine boot guarded by an exclusive file lock, so a
          second web worker can never double-run the trading engine.
        - aegis_shared.json heartbeat flush (every ~10 s) + aegis_commands.j
          (kill/resume) so cross-worker display keeps working even if PA
          ever splits requests across web workers.
        - /healthz/{uuid}/pingext: one-shot read-only probe of
          api.bybit.com/v5/market/time and api.resend.com (no trade calls).

Hard rules respected: no secrets in source, linear BTCUSDT only, all
engine logic and gates imported untouched.
"""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import html
import json
import logging
import os
import secrets as pysecrets
import sys
import threading
import time
import traceback
from urllib.parse import parse_qs

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# (a) secrets.env -> environment, BEFORE importing engine modules
# ---------------------------------------------------------------------------

def _load_dotenv_file(path: str) -> None:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())
    except OSError:
        pass


_load_dotenv_file(os.path.join(BASE_DIR, "secrets.env"))

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# Import the ASGI daemon module UNCHANGED (config SETTINGS materializes here).
try:
    import main as aegis
    AEGIS_IMPORT_ERROR = None
except Exception:  # noqa: BLE001 — surface via HTTP instead of a dead app
    aegis = None
    AEGIS_IMPORT_ERROR = "main import failed: " + traceback.format_exc()[-1200:]

_HEALTH_UUID = os.environ.get("HEALTH_UUID", "")
_DASH_USER = os.environ.get("DASHBOARD_USERNAME", "operator")
_DASH_PASS = os.environ.get("DASHBOARD_PASSWORD", "")

_SHARED_JSON_PATH = os.path.join(BASE_DIR, "aegis_shared.json")
_COMMANDS_JSON_PATH = os.path.join(BASE_DIR, "aegis_commands.json")
_LOCK_PATH = os.path.join(BASE_DIR, "aegis_engine.lock")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
logging.Formatter.converter = time.gmtime
logger = logging.getLogger("aegis.wsgi")

# ---------------------------------------------------------------------------
# (b/c) Engine thread: ONE loop that enters main.lifespan and never exits
# ---------------------------------------------------------------------------

_LOOP: asyncio.AbstractEventLoop | None = None
_ENGINE_OWNER = False
_ENGINE_STARTED = False
_ENGINE_READY_EVT = threading.Event()
_ENGINE_FATAL: list = []
_ENG_GUARD = threading.Lock()


def _engine_thread() -> None:
    """Own the single engine loop forever (same wiring as main.lifespan)."""
    global _LOOP
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        _LOOP = loop
        ctx = aegis.lifespan(None)  # reconciler -> preflight -> loops -> watchdog
        loop.run_until_complete(ctx.__aenter__())
        loop.create_task(_flush_shared_forever())
        _ENGINE_READY_EVT.set()
        logger.info("Aegis engine thread online (lifespan entered)")
        loop.run_forever()
    except Exception as exc:  # noqa: BLE001
        _ENGINE_FATAL.append(f"{type(exc).__name__}: {exc}")
        _ENGINE_READY_EVT.set()
        logger.error("Aegis engine thread FATAL: %s\n%s", exc, traceback.format_exc())


def ensure_engine() -> str:
    """Start the engine thread exactly once per process, guarded by a lock file."""
    global _ENGINE_STARTED, _ENGINE_OWNER
    if aegis is None:
        return "import-failed"
    if _ENGINE_STARTED:
        return "running" if _ENGINE_OWNER else "worker-standby"
    with _ENG_GUARD:
        if _ENGINE_STARTED:
            return "running" if _ENGINE_OWNER else "worker-standby"
        try:
            fh = open(_LOCK_PATH, "a")
            import fcntl
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            logger.warning("engine lock already held by another process; standby")
            _ENGINE_STARTED = True
            return "worker-standby"
        _ENGINE_OWNER = True
        threading.Thread(target=_engine_thread, name="aegis-engine", daemon=False).start()
        _ENGINE_STARTED = True
        return "starting"


def _engine_ready() -> bool:
    return (_ENGINE_OWNER and _ENGINE_READY_EVT.is_set()
            and _LOOP is not None and not _ENGINE_FATAL)


def _call_engine(coro_factory, timeout: float = 10.0):
    """Bridge a coroutine onto the engine loop (worker must own the engine)."""
    if _LOOP is None:
        raise RuntimeError("engine loop unavailable")
    fut = asyncio.run_coroutine_threadsafe(coro_factory(), _LOOP)
    return fut.result(timeout)


# ---------------------------------------------------------------------------
# Snapshot: single place that reads engine state ON the engine thread
# ---------------------------------------------------------------------------

_DASH_STYLE = """<!DOCTYPE html><html><head><title>Aegis-Universal</title></head>
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


async def _snapshot() -> dict:
    """Run on the engine loop. Mirrors main.py healthz + dashboard payloads."""
    sh = aegis.SHARED
    state = aegis.STATE
    scanner = aegis.SCANNER
    settings = aegis.SETTINGS
    gates = aegis.GATES
    al = aegis.ALERTER

    if not sh.get("_t0"):
        sh["_t0"] = time.time()
    uptime = time.time() - sh["_t0"]

    pos = sh.get("position")
    state_day = state.ensure_utc_day() if state else ("", False)
    daily_pnl = state.daily_realized_pnl() if state else 0.0
    halted = bool(getattr(settings, "kill_switch_active", False) or
                  (state and state.kill_switch()))

    payload = {
        "status": "halted" if halted else "ok",
        "app": "Aegis-Universal",
        "uptime_seconds": int(uptime),
        "total_equity_usdt": round(sh.get("equity", 0.0), 4),
        "available_balance_usdt": round(sh.get("available", 0.0), 4),
        "active_position": ({"side": pos.side, "qty": pos.qty,
                             "entry": pos.entry, "entry_ts": pos.opened_ts}
                            if pos else None),
        "daily_pnl_usdt": round(daily_pnl, 4),
        "utc_day": state_day[0],
        "last_scan": (scanner.shared.get("last_scan_ts") if scanner else None),
        "last_consensus": (scanner.shared.get("consensus") if scanner else ""),
        "ts": int(time.time()),
    }

    boot = sh.get("boot") or {}
    cooldown = "n/a"
    if state and state.last_trade_closed_ts():
        rem = max(0.0, settings.cooldown_minutes * 60 -
                  (time.time() - state.last_trade_closed_ts())) / 60
        cooldown = f"{rem:.1f} min"

    rows = [
        ("System Status", "OPERATIONAL (24/7 scanning)" if not (state and state.kill_switch())
         else "⛔ KILL SWITCH (entries halted)"),
        ("Uptime", f"{uptime / 3600:.2f} h"),
        ("Total Equity", f"${sh.get('equity', 0):.2f} USDT"),
        ("Available", f"${sh.get('available', 0):.2f} USDT"),
        ("Baseline Equity", f"${(state.baseline_equity() or 0) if state else 0:.2f} USDT"),
        ("Risk / Trade", f"{settings.risk_per_trade_pct * 100:.2f}%  @ {settings.max_leverage:g}x ISOLATED"),
        ("Daily Realized PnL", f"${daily_pnl:.4f} USDT"),
        ("Consecutive Losses", state.consecutive_losses() if state else 0),
        ("Cooldown Remaining", cooldown),
        ("Position", (f"{pos.side} {pos.qty:.3f} @ ${pos.entry:,.1f} | opened "
                      f"{(time.time() - pos.opened_ts) / 3600:.2f}h ago") if pos else "flat"),
        ("Last Consensus", (scanner.shared.get("consensus", "") if scanner else "")),
        ("Boot Pre-flight", "PASSED (zero errors)" if boot.get("ok") else f"ISSUES: {boot.get('errors', [])}"),
    ]

    gate_rows = []
    if gates:
        for item in list(gates.history)[:15]:
            gate_rows.append((
                time.strftime("%H:%M:%S", time.gmtime(item.get("ts", 0))),
                html.escape(str(item.get("gate", ""))),
                html.escape(str(item.get("direction", ""))),
                html.escape(str(item.get("reason", "")))[:120],
            ))

    err_rows = []
    if al:
        for rec in list(al.recent_errors)[-12:][::-1]:
            err_rows.append((
                time.strftime("%H:%M:%S", time.gmtime(rec.ts)),
                html.escape(str(rec.module)),
                html.escape(str(rec.error_type)),
                html.escape(str(rec.message))[:120],
            ))

    return {"payload": payload, "booted": bool(sh.get("booted")), "rows": rows,
            "gate_rows": gate_rows, "err_rows": err_rows}


async def _flush_shared_forever() -> None:
    """Persist a compact snapshot for cross-worker display (PA failsafe)."""
    while True:
        try:
            if _engine_ready():
                snap = await _snapshot()
                with open(_SHARED_JSON_PATH, "w", encoding="utf-8") as fh:
                    json.dump(snap, fh)
                await _drain_commands()
        except Exception:  # noqa: BLE001 — never kill the flusher
            pass
        await asyncio.sleep(10)


async def _drain_commands() -> None:
    """Execute operator kill/resume requested by a standby worker (bridge)."""
    try:
        with open(_COMMANDS_JSON_PATH, "r", encoding="utf-8") as fh:
            cmds = json.load(fh)
    except (OSError, ValueError):
        return
    changed = False
    now = time.time()
    pending = cmds if isinstance(cmds, list) else []
    for c in pending:
        if not isinstance(c, dict) or c.get("processed") or now - float(c.get("ts", 0)) > 300:
            continue
        try:
            if c.get("cmd") == "kill" and aegis.STATE:
                aegis.STATE.set_kill_switch(True, "manual operator kill switch")
            elif c.get("cmd") == "resume" and aegis.STATE:
                aegis.STATE.set_kill_switch(False)
                aegis.STATE.set_pause_until(0.0)
            c["processed"] = True
            changed = True
        except Exception:  # noqa: BLE001
            pass
    if changed and isinstance(cmds, list):
        with open(_COMMANDS_JSON_PATH, "w", encoding="utf-8") as fh:
            json.dump(cmds, fh)


async def _do_kill() -> dict:
    state = aegis.STATE
    if state:
        state.set_kill_switch(True, "manual operator kill switch")
    if aegis.ALERTER:
        aegis.ALERTER.enqueue(aegis.ALERTER.send_trade_event, "halt",
                              {"Trigger": "Manual operator kill switch (dashboard)"})
    return {"status": "kill switch ENGAGED (entries halted)"}


async def _do_resume() -> dict:
    if aegis.STATE:
        aegis.STATE.set_kill_switch(False)
        aegis.STATE.set_pause_until(0.0)
    return {"status": "circuit breakers cleared (entries re-armed, gates remain active)"}


# ---------------------------------------------------------------------------
# Reachability diagnostic (read-only; contract Phase E step 3)
# ---------------------------------------------------------------------------

def _pingext() -> dict:
    import httpx
    out = {}
    for name, url in [("bybit", "https://api.bybit.com/v5/market/time"),
                      ("resend", "https://api.resend.com")]:
        rec: dict = {}
        try:
            with httpx.Client(timeout=5.0) as client:
                r = client.get(url)
                rec["status"] = r.status_code
                try:
                    rec["retCode"] = r.json().get("retCode")
                except Exception:
                    rec["retCode"] = None
        except Exception as exc:  # noqa: BLE001
            rec["status"] = None
            rec["error"] = f"{type(exc).__name__}: {exc}"
        out[name] = rec
    out["pid"] = os.getpid()
    return {"app": "Aegis-Universal pingext", "bybit": out["bybit"],
            "resend": out["resend"], "pid": out["pid"],
            "engine_owner_process": bool(_ENGINE_OWNER)}


# ---------------------------------------------------------------------------
# WSGI layer (stdlib only)
# ---------------------------------------------------------------------------

def _json_bytes(obj, status: int = 200) -> tuple:
    body = json.dumps(obj).encode("utf-8")
    return (status, [("Content-Type", "application/json; charset=utf-8"),
                     ("Content-Length", str(len(body)))], body)


def _html_bytes(body, status: int = 200) -> tuple:
    data = body.encode("utf-8")
    return (status, [("Content-Type", "text/html; charset=utf-8"),
                     ("Content-Length", str(len(data)))], data)


def _basic_auth(environ) -> bool:
    header = environ.get("HTTP_AUTHORIZATION", "")
    if not header.startswith("Basic "):
        return False
    try:
        decoded = base64.b64decode(header[6:]).decode("utf-8")
        user, _, password = decoded.partition(":")
    except Exception:  # noqa: BLE001
        return False
    return pysecrets.compare_digest(user, _DASH_USER) and \
        pysecrets.compare_digest(password, _DASH_PASS)


def _uuid_ok(environ, segment: str) -> bool:
    if _HEALTH_UUID and pysecrets.compare_digest(str(segment), _HEALTH_UUID):
        return True
    qs = parse_qs(environ.get("QUERY_STRING", ""))
    keys = qs.get("key", [""])[0]
    return bool(_HEALTH_UUID and keys and pysecrets.compare_digest(keys, _HEALTH_UUID))


def _fallback_health(segment_ok: bool) -> tuple:
    info = {"status": "ok", "app": "Aegis-Universal", "booted": False,
            "engine": "wsgi adapter FAILSAFE (engine import error)",
            "ts": int(time.time())}
    if AEGIS_IMPORT_ERROR and segment_ok:
        info["detail"] = AEGIS_IMPORT_ERROR[:900]
    return _json_bytes(info)


def _dispatch(environ, start_response):
    global _ENGINE_STARTED
    path = environ.get("PATH_INFO", "")
    method = environ.get("REQUEST_METHOD", "GET").upper()

    if path == "/":
        ensure_engine()
        health_hint = f"/healthz/{(_HEALTH_UUID or '0' * 32)[:8]}..."
        status, headers, body = _json_bytes(
            {"app": "Aegis-Universal", "status": "running",
             "health": health_hint, "booted": _engine_ready()})
    elif path.startswith("/healthz/"):
        segment = path[len("/healthz/"):].strip("/")
        if segment == "pingext":
            ensure_engine()
            if _basic_auth(environ) or _uuid_ok(environ, segment):
                if not _uuid_ok(environ, segment) and not _basic_auth(environ):
                    status, headers, body = _json_bytes({"detail": 404}, 404)
                else:
                    status, headers, body = _json_bytes(_pingext())
            else:
                status, headers, body = _json_bytes({"detail": 404}, 404)
        elif _uuid_ok(environ, segment):
            ensure_engine()
            if _ENGINE_FATAL:
                snap = {"status": "error", "app": "Aegis-Universal",
                        "engine_fatal": _ENGINE_FATAL[-1], "ts": int(time.time()),
                        "booted": False}
                status, headers, body = _json_bytes(snap)
            elif _engine_ready():
                try:
                    snap = _call_engine(_snapshot, timeout=10.0)
                    status, headers, body = _json_bytes(snap["payload"])
                except Exception as exc:  # noqa: BLE001
                    status, headers, body = _json_bytes(
                        {"status": "error", "engine": f"snapshot: {type(exc).__name__}: {exc}",
                         "ts": int(time.time())}, 500)
            else:
                # Engine boots in another worker or still starting: file bridge.
                status, headers, body = _engine_bridge_health()
    elif path == "/dashboard" and method == "GET":
        ensure_engine()
        if not _basic_auth(environ):
            status, headers, body = _html_bytes(
                "<html><body style='background:#0f172a;color:#f87171;margin:40px;'>"
                "<h2>401 Unauthorized</h2></body></html>", 401)
        elif not _engine_ready():
            if AEGIS_IMPORT_ERROR:
                status, headers, body = _html_bytes(f"<h3>engine import error</h3>", 200)
            else:
                status, headers, body = _html_bytes("<h3>booting…</h3>", 200)
        else:
            try:
                snap = _call_engine(_snapshot, timeout=10.0)
                rows = "".join(_ROW_D % (k, v) for k, v in snap["rows"])
                gate_rows = "".join(
                    _ROW_D % (
                        "%s | %s | %s" % (item[0], item[1], item[2]), item[3])
                    for item in snap["gate_rows"])
                err_rows = "".join(
                    _ROW_D % (
                        "%s | %s | %s" % (item[0], item[1], item[2]), item[3])
                    for item in snap["err_rows"])
                status, headers, body = _html_bytes(_DASH_STYLE % (
                    rows, (gate_rows or "<tr><td>none yet</td></tr>"),
                    (err_rows or "<tr><td>none</td></tr>")))
            except Exception as exc:  # noqa: BLE001
                status, headers, body = _html_bytes(
                    f"<h3>dashboard snapshot unavailable: "
                    f"{html.escape(str(exc)[:200])}</h3>", 200)
    elif path == "/dashboard/kill" and method == "POST":
        ensure_engine()
        if not _basic_auth(environ):
            status, headers, body = _json_bytes({"detail": "unauthorized"}, 401)
        elif _engine_ready():
            try:
                res = _call_engine(_do_kill, timeout=10.0)
                status, headers, body = _json_bytes(res)
            except Exception as exc:  # noqa: BLE001
                status, headers, body = _json_bytes(
                    {"detail": f"kill dispatch failed: {type(exc).__name__}: {exc}"}, 500)
        else:
            status, headers, body = _json_bytes(
                _send_command_file("kill"))
    elif path == "/dashboard/resume" and method == "POST":
        ensure_engine()
        if not _basic_auth(environ):
            status, headers, body = _json_bytes({"detail": "unauthorized"}, 401)
        elif _engine_ready():
            try:
                res = _call_engine(_do_resume, timeout=10.0)
                status, headers, body = _json_bytes(res)
            except Exception as exc:  # noqa: BLE001
                status, headers, body = _json_bytes(
                    {"detail": f"resume dispatch failed: {type(exc).__name__}: {exc}"}, 500)
        else:
            status, headers, body = _json_bytes(
                _send_command_file("resume"))
    else:
        status, headers, body = _json_bytes({"detail": "Not Found"}, 404)
    start_response(f"{status} {'OK' if status == 200 else 'ERR'}",
                   headers)
    return [body]


def _engine_bridge_health() -> tuple:
    """Health from the heartbeat file when the engine lives in another worker."""
    import io
    try:
        with open(_SHARED_JSON_PATH, "r", encoding="utf-8") as fh:
            snap = json.load(fh)
        snap["payload"]["engine"] = "standby-worker bridge"
        return _json_bytes(snap["payload"])
    except (OSError, ValueError, KeyError):
        return _json_bytes({"status": "ok", "app": "Aegis-Universal",
                            "booted": False, "engine": "booting", "ts": int(time.time())})


def _send_command_file(cmd: str) -> dict:
    """Queue kill/resume for the owner process when this worker is standby."""
    entry = {"cmd": cmd, "ts": time.time(), "processed": False}
    try:
        with open(_COMMANDS_JSON_PATH, "r", encoding="utf-8") as fh:
            cmds = json.load(fh)
        if not isinstance(cmds, list):
            cmds = []
    except (OSError, ValueError):
        cmds = []
    cmds.append(entry)
    with open(_COMMANDS_JSON_PATH, "w", encoding="utf-8") as fh:
        json.dump(cmds, fh)
    return {"status": (f"{cmd} queued (applies within ~10s via heartbeat bridge)")}


def application(environ, start_response):
    try:
        if AEGIS_IMPORT_ERROR:
            # Keep the app minimally serving so we can diagnose over HTTP.
            path = environ.get("PATH_INFO", "/")
            status, headers, body = _fallback_health(True)
            if path != "/":
                status, headers, body = _json_bytes(
                    {"detail": "engine import failed (see /healthz/{uuid})"}, 503)
            start_response(f"{status} {'OK' if status == 200 else 'ERR'}", headers)
            return [body]
        return _dispatch(environ, start_response)
    except Exception:  # noqa: BLE001 — Contract 9.2: no silent failure path
        try:
            if aegis is not None and aegis.ALERTER:
                exc = sys.exc_info()[1]
                aegis.ALERTER.report("aegis.wsgi", exc, {
                    "path": environ.get("PATH_INFO", "?"),
                    "method": environ.get("REQUEST_METHOD", "?")})
        except Exception:  # noqa: BLE001
            logging.getLogger(__name__).error(traceback.format_exc())
        status, headers, body = _json_bytes(
            {"detail": "internal error (operator alerted)"}, 500)
        start_response(f"{status} {'OK' if status == 200 else 'ERR'}", headers)
        return [body]


if __name__ == "__main__":
    print("wsgi_entry self-check: import OK; AEGIS_IMPORT_ERROR =", AEGIS_IMPORT_ERROR)
    if AEGIS_IMPORT_ERROR:
        print(AEGIS_IMPORT_ERROR)
    print("engine address:", id(_engine_thread), "lock:", _LOCK_PATH)
    print("WSGI_ADAPTER_OK")
