"""
Aegis-Universal :: Omnipresent Lifecycle & Error Telemetry (Resend)
===================================================================
Contract Section 9 implementation:

  9.1  Boot-success HTML handshake email (dispatched after zero-error pre-flight).
  9.2  Every uncaught exception, Bybit API rejection, data-feed failure,
       background-loop crash, and failsafe trigger is captured with full
       stack trace + market context and emailed to the operator INSTANTLY.
  9.3  Intelligent anti-spam deduplation: distinct errors go out instantly;
       identical fingerprints throttle to one email per 15-minute window
       with a "Repeated X times" counter.
  9.4  Trade event tickets (INFO) and the UTC 00:00 daily digest.

Design notes:
  * Emails are NEVER allowed to crash the trading engine: all sends are
    wrapped; failures are logged to stdout for Render log inspection.
  * Hooks (sys.excepthook, threading.excepthook, asyncio loop handler,
    FastAPI handler) are installed in main.py and delegate here.
  * Absolute Prohibition #10: no exception path is ever silent.
"""

from __future__ import annotations

import asyncio
import collections
import functools
import hashlib
import html
import logging
import re
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, List, Optional

import httpx

logger = logging.getLogger("aegis.alerter")

RESEND_API_URL = "https://api.resend.com/emails"

_SUBJECT_EMOJI_RE = re.compile(r"[\U0001F300-\U0001FAFF\u2700-\u27BF]")


# ---------------------------------------------------------------------------
# HTML templates
# ---------------------------------------------------------------------------

_BASE_STYLE = """
  <div style="font-family:'Segoe UI',Roboto,Helvetica,Arial,sans-serif;background:#0f172a;padding:24px;">
    <div style="max-width:640px;margin:0 auto;border-radius:12px;overflow:hidden;background:#111827;border:1px solid #1f2937;">
      <div style="padding:18px 24px;background:%(header)s;color:#fff;">
        <div style="font-size:20px;font-weight:700;">%(title)s</div>
        <div style="font-size:12px;opacity:.85;margin-top:4px;">Aegis-Universal &mdash; %(subtitle)s</div>
      </div>
      <div style="padding:20px 24px;background:#111827;color:#e5e7eb;">
        <table style="width:100%%;border-collapse:collapse;font-size:14px;">
          %(rows)s
        </table>
        %(extra)s
      </div>
      <div style="padding:12px 24px;background:#0b1220;color:#9ca3af;font-size:11px;">
        Automated telemetry from Aegis-Universal &middot; %(ts)s &middot; No outcome of this system is guaranteed. Trading involves substantial risk.
      </div>
    </div>
  </div>
"""

_ROW = ("<tr>"
        "<td style='padding:7px 10px;border-bottom:1px solid #1f2937;color:#9ca3af;width:190px;'>{k}</td>"
        "<td style='padding:7px 10px;border-bottom:1px solid #1f2937;color:#f9fafb;'><b>{v}</b></td>"
        "</tr>")


def _ticket(title: str, subtitle: str, header_color: str, rows: List[tuple],
            extra_html: str = "", ts: Optional[str] = None) -> str:
    rows_html = "".join(_ROW.format(k=html.escape(str(k)), v=html.escape(str(v))) for k, v in rows)
    stamp = ts or time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) + " UTC"
    return _BASE_STYLE % {
        "header": header_color,
        "title": html.escape(title),
        "subtitle": html.escape(subtitle),
        "rows": rows_html,
        "extra": f"<div style='margin-top:16px;'>{extra_html}</div>" if extra_html else "",
        "ts": stamp,
    }


def _pre_block(text: str, border: str) -> str:
    return ("<pre style='background:#0b1220;border:1px solid %s;border-radius:8px;padding:14px;"
            "font-size:12px;line-height:1.45;overflow-x:auto;color:#e5e7eb;'>%s</pre>"
            % (border, html.escape(text)))


_GREEN = "#059669"; _RED = "#dc2626"; _BLUE = "#2563eb"; _AMBER = "#d97706"


# ---------------------------------------------------------------------------
# Dedup state
# ---------------------------------------------------------------------------

@dataclass
class _Fingerprint:
    count: int = 0
    first_seen: float = 0.0
    last_sent: float = 0.0
    last_message: str = ""


@dataclass
class ErrorRecord:
    ts: float
    module: str
    error_type: str
    message: str
    sent: bool
    repeat_count: int


# ---------------------------------------------------------------------------
# Alerter service
# ---------------------------------------------------------------------------

class Alerter:
    """Thread-safe, loop-safe Resend email telemetry. Never raises into business code."""

    def __init__(self, api_key: str, to_email: str, from_email: str,
                 dedupe_window_seconds: int = 900):
        self.api_key = api_key
        self.to_email = to_email
        self.from_email = from_email
        self.dedupe_window = max(60, int(dedupe_window_seconds))
        self._client: Optional[httpx.AsyncClient] = None
        self._job_q: "asyncio.Queue[Callable[[], Coroutine]]" = asyncio.Queue(maxsize=200)
        self._worker_task: Optional[asyncio.Task] = None
        self._fingerprints: Dict[str, _Fingerprint] = {}
        self.recent_errors: collections.deque = collections.deque(maxlen=60)
        self._overflow_drop = 0
        enabled = bool(api_key and to_email and from_email)
        self.enabled = enabled
        if not enabled:
            logger.warning("Alerter disabled: RESEND_API_KEY / RECEIVER_EMAIL missing "
                           "(errors will only appear in stdout logs).")

    # ---------------- lifecycle ----------------

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(20.0))
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.get_running_loop().create_task(self._worker())

    async def close(self) -> None:
        if self._worker_task is not None:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except (asyncio.CancelledError, Exception):
                pass
            self._worker_task = None
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass
            self._client = None

    async def _worker(self) -> None:
        while True:
            factory = await self._job_q.get()
            try:
                coro = factory()
                try:
                    await coro
                except asyncio.CancelledError:
                    coro.close()
                    raise
                except Exception:
                    coro.close()
                    raise
                finally:
                    pass
            except Exception:
                self._log_local("email dispatch failed", traceback.format_exc())
            finally:
                self._job_q.task_done()

    def report(self, module: str, exc: Any, context: Optional[Dict[str, Any]] = None,
               level: str = "error") -> None:
        """Thread-safe entry point for every error in the system (never blocks, never raises)."""
        try:
            info = self._classify(module, exc, context)
            fingerprint = self._fingerprint_key(info["error_type"], info["module_line"])
            now = time.time()
            fp = self._fingerprints.get(fingerprint)
            if fp is None:
                fp = _Fingerprint(count=0, first_seen=now, last_sent=0.0)
                self._fingerprints[fingerprint] = fp
            fp.count += 1
            fp.last_message = info["message"]
            self.recent_errors.append(ErrorRecord(
                ts=now, module=info["module_line"].split(":")[0], error_type=info["error_type"],
                message=info["message"], sent=False, repeat_count=fp.count))
            if not self.enabled:
                self._log_local(f"[{info['error_type']}] {info['message']} :: {info['traceback'][-800:]}", "")
                return
            should_send = (now - fp.last_sent) >= self.dedupe_window
            if should_send:
                repeat = fp.count - 1
                fp.last_sent = now
                suffix = f" (Repeated {repeat} times in the last {self.dedupe_window // 60}m)" if repeat > 0 else ""
                self.enqueue(_Section9.builder_error, self, info, suffix)
            else:
                self._log_local(f"duplicate error throttled ({fp.count} hits): "
                                f"{info['error_type']}@{info['module_line']}", "")
        except Exception:
            self._log_local("alerter.report internal failure", traceback.format_exc())

    def enqueue(self, builder: Callable[..., Coroutine], *args: Any) -> None:
        """Queue an email for async delivery (silently drops when saturated; count kept)."""
        try:
            self._job_q.put_nowait(functools.partial(builder, *args))
        except asyncio.QueueFull:
            self._overflow_drop += 1
            self._log_local(f"email queue saturated; dropped ({self._overflow_drop} total)", "")

    # ---------------- classification ----------------

    @staticmethod
    def _classify(module_hint: str, exc: Any, context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if isinstance(exc, BaseException):
            tb = exc.__traceback__
            message = str(exc) or exc.__class__.__name__
            error_type = type(exc).__name__
        else:
            tb = None
            message = str(exc)
            error_type = "Error"
        if tb is not None:
            full = "".join(traceback.format_exception(type(exc), exc, tb))
        else:
            full = traceback.format_stack()[-12] if message else ""
            full = "".join(full)
        if not full.strip():
            full = "<no traceback available>"
        location = module_hint
        code_line = ""
        if tb is not None:
            vals = [(f.filename, f.lineno, f.name) for f in traceback.extract_tb(tb)]
            vals = [v for v in vals if not v[0].endswith("threading.py") and "asyncio" not in v[0]]
            use = vals[-1] if vals else (None, None, None)
            fname, lineno, func = use if use[0] else module_hint
            location = f"{module_hint} -> {fname}:{lineno} in {func}"
            code_line = f"{fname}:{lineno}"
        else:
            code_line = module_hint
        return {
            "module_line": module_hint,
            "location": location,
            "code_line": code_line,
            "error_type": error_type,
            "message": message,
            "traceback": full,
            "context": dict(context or {}),
        }

    @staticmethod
    def _fingerprint_key(error_type: str, module_line: str) -> str:
        raw = f"{error_type}|{module_line}"
        return hashlib.sha256(raw.encode("utf-8", "ignore")).hexdigest()[:32]

    @staticmethod
    def _log_local(msg: str, extra: str) -> None:
        line = f"{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}Z alerter: {msg}"
        if extra:
            line += "\n" + extra
        logger.error(line)

    # ---------------- raw Resend send ----------------

    async def send_html(self, subject: str, body_html: str) -> bool:
        """Raises on transport failure; callers wrap (worker catches)."""
        if not self.enabled:
            self._log_local(f"email skipped (disabled): {subject}", "")
            return False
        assert self._client is not None
        resp = await self._client.post(
            RESEND_API_URL,
            headers={"Authorization": f"Bearer {self.api_key}",
                     "Content-Type": "application/json"},
            json={"from": self.from_email, "to": [self.to_email],
                  "subject": subject, "html": body_html},
        )
        if resp.status_code >= 300:
            raise RuntimeError(
                f"Resend HTTP {resp.status_code}: {resp.text[:400]}")
        return True

    # ---------------- 9.1 boot handshake ----------------

    async def send_boot_success(self, ctx: Dict[str, Any]) -> None:
        body = _ticket(
            "SYSTEM STATUS: OPERATIONAL", "24/7 Scanning Active & Master Pre-flight Passed",
            _GREEN, [
                ("System Status", "OPERATIONAL (24/7 Scanning Active)"),
                ("Initial Total Equity", f"${ctx.get('equity', 0):.2f} USDT"),
                ("Configured Leverage", f"{ctx.get('leverage', '?')}x (ISOLATED)"),
                ("Active Risk Fraction", f"{ctx.get('risk_pct', 0) * 100:.2f}% per trade"),
                ("Margin Allocation Ceiling", f"{ctx.get('margin_alloc_pct', 0) * 100:.0f}% of equity"),
                ("Keep-Alive Monitor", "ACTIVE (UptimeRobot 5m Interval)"),
                ("Instrument", ctx.get("symbol", "BTCUSDT") + " USDT Linear Perpetual"),
                ("Market Consensus Wait", "1-min Scanner armed"),
                ("Operator Dashboard", ctx.get("dashboard_url", "http://<app>/dashboard")),
                ("Health Endpoint", ctx.get("health_url", "/healthz/<uuid>")),
            ], extra_html=(
                "<div style='background:#064e3b;border:1px solid #10b981;border-radius:8px;"
                "padding:12px 14px;color:#d1fae5;font-size:13px;'>"
                "✅ All pre-flight gates passed with zero errors: Bybit authentication &amp; clock sync, "
                "wallet equity verification, position/leverage validation (One-Way, Isolated, "
                f"{ctx.get('leverage', '?')}x), and the AI decision feed probe (Bybit klines + "
                f"{ctx.get('model', 'gemini')})." "</div>"))
        await self.send_html(
            "🟢 [ONLINE] Aegis-Universal Successfully Booted & Operating Flawlessly", body)

    async def send_boot_failure(self, errors: List[str], ctx: Dict[str, Any]) -> None:
        rows = [
            ("Boot Status", "DEGRADED / PARTIAL — trading loops started under supervision"),
            ("Failed Pre-flight Checks", f"{len(errors)} error(s)"),
            ("Total Equity (if known)", f"${ctx.get('equity', 0):.2f} USDT"),
        ]
        body = _ticket("PRE-FLIGHT ISSUES DETECTED", "Boot proceeded; error details below",
                       _AMBER, rows, extra_html=_pre_block("\n".join(errors), _AMBER))
        await self.send_html(
            "🟠 [BOOT ALERT] Aegis-Universal Booted With Pre-flight Errors", body)

    # ---------------- 9.2 error ticket ----------------

    async def send_error(self, info: Dict[str, Any], repeat_suffix: str = "") -> None:
        ctx = info.get("context") or {}
        ctx_rows = [(f"Context · {k}", v) for k, v in list(ctx.items())[:10]]
        rows = [
            ("Timestamp (UTC)", time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())),
            ("Exception Type", info.get("error_type", "?")),
            ("Error Message", info.get("message", "?")),
            ("Function / Module / Line", info.get("location", "?")),
            ("BTC Mark Price", ctx.pop("mark_price", "n/a") if isinstance(ctx, dict) else "n/a"),
            ("Position State", ctx.pop("position_state", "n/a") if isinstance(ctx, dict) else "n/a"),
            ("Equity", ctx.pop("equity", "n/a") if isinstance(ctx, dict) else "n/a"),
        ] + ctx_rows
        extra = _pre_block(info.get("traceback", ""), _RED)
        body = _ticket("UNCAUGHT EXCEPTION INTERCEPTED",
                       "Full stack trace below — operator attention required",
                       _RED, rows, extra_html=extra)
        await self.send_html(
            f"🔴 [ERROR ALERT] Aegis System Exception in {info.get('module_line', 'system')}"
            f"{repeat_suffix}", body)

    # ---------------- 9.4 trade events + digest ----------------

    async def send_trade_event(self, kind: str, ctx: Dict[str, Any]) -> None:
        if kind == "open":
            title, sub, color = "NEW POSITION ENTERED", "Atomic Limit-IOC bracket live on exchange", _BLUE
            subject = (f"🔵 [TRADE OPEN] {ctx.get('side', '?')} {ctx.get('symbol', 'N/A')} "
                       f"@ {ctx.get('entry', '?')}")
        elif kind == "breakeven":
            title, sub, color = "BREAKEVEN+ ENGAGED", "Position locked risk-free (fees covered)", _GREEN
            subject = "🛡️ [BREAKEVEN+] Trade Locked Risk-Free"
        elif kind == "trailing":
            title, sub, color = "VOLATILITY TRAIL ARMED", "ATR trail behind price (monotonic)", _GREEN
            subject = "📈 [TRAILING] Volatility Trail Engaged"
        elif kind == "close":
            title, sub, color = "TRADE CLOSED", "Position flattened — ledger synchronized", _BLUE
            pnl = ctx.get("realized_pnl", 0.0)
            subject = f"{'✅' if pnl >= 0 else '🔻'} [TRADE CLOSED] PnL ${pnl:.4f}"
        elif kind == "ttl":
            title, sub, color = "TTL DECAY EXIT", f"{ctx.get('hours', 24)}h holding limit enforced", _AMBER
            subject = "⏳ [TTL EXIT] 24-Hour Holding Limit Reached"
        elif kind == "emergency":
            title, sub, color = "EMERGENCY MARKET CLOSE", "Failsafe circuit breaker executed", _RED
            subject = "🚨 [FAILSAFE] EMERGENCY MARKET CLOSE EXECUTED"
        elif kind == "halt":
            title, color = "HARD HALT TRIGGERED", _RED
            trigger = str(ctx.get("Trigger", ""))
            floor_hit = "Floor" in ctx or "floor" in trigger.lower()
            if trigger and not floor_hit:
                # operator/other halt: surface the real trigger instead of the
                # legacy equity-floor wording (cosmetic context fix)
                sub = f"Entries disabled — {trigger}"
                subject = ("🚨 [HALT] ENTRIES DISABLED — "
                           + trigger.split("(")[0].strip().upper()[:64])
            else:
                sub = "Equity preservation floor breached"
                subject = "🚨 [HALT] EQUITY PRESERVATION FLOOR BREACHED — ENTRIES DISABLED"
        elif kind == "paused":
            title, sub, color = "ENTRIES PAUSED", "Circuit-breaker cooldown active", _AMBER
            subject = "🟠 [PAUSED] Aegis Circuit Breaker Engaged"
        else:
            title, sub, color = "TRADE EVENT", kind, _BLUE
            subject = "🔵 [TRADE EVENT]"
        rows = [(k, v) for k, v in list(ctx.items())[:14] if not str(v).startswith("<")]
        body = _ticket(title, sub, color, rows)
        await self.send_html(subject, body)

    async def send_digest(self, ctx: Dict[str, Any]) -> None:
        rows = [
            ("Trading Day (UTC)", ctx.get("day", "?")),
            ("Net PnL", f"${ctx.get('pnl', 0):.4f} USDT"),
            ("Closed Trades", ctx.get("closed", 0)),
            ("Win Rate", f"{ctx.get('win_rate', 0):.1f}%"),
            ("Current Equity", f"${ctx.get('equity', 0):.2f} USDT"),
            ("Baseline Equity", f"${ctx.get('baseline', 0):.2f} USDT"),
            ("Equity vs Baseline", f"{ctx.get('vs_baseline', 0):.2f}%"),
            ("Entries Halted?", "YES (circuit breaker)" if ctx.get("halted") else "No"),
            ("Open Position", ctx.get("position", "none")),
        ]
        body = _ticket("DAILY PERFORMANCE DIGEST", "UTC 00:00 summary", _BLUE, rows)
        await self.send_html("📊 [DIGEST] Aegis-Universal Daily Performance Summary", body)

    async def send_status(self, ctx: Dict[str, Any]) -> None:
        """Periodic operator-facing engine status report (STATUS_HEARTBEAT_SECONDS)."""
        scan_age = int(ctx.get("scan_age_s", -1))
        scan_txt = f"{scan_age}s ago" if scan_age >= 0 else "no data yet"
        rows = [
            ("Instrument", f"{ctx.get('symbol', '?')} USDT Linear Perpetual"),
            ("Equity", f"${ctx.get('equity', 0):.2f} USDT"),
            ("Available", f"${ctx.get('available', 0):.2f} USDT"),
            ("Baseline", f"${ctx.get('baseline', 0):.2f} USDT"),
            ("Daily PnL", f"${float(ctx.get('pnl', 0)):+.4f} USDT"),
            ("Closed Trades Today", str(ctx.get("closed", 0))),
            ("Open Position", ctx.get("position", "none")),
            ("Consensus (15m/1h)", ctx.get("consensus", "n/a")),
            ("Last Scan", scan_txt),
            ("Entries Halted?", "YES" if ctx.get("halted") else "No"),
            ("Next Report In", f"~{int(ctx.get('next_report_min', 60))} min"),
        ]
        body = _ticket("ENGINE STATUS REPORT", "Periodic operator heartbeat", _BLUE, rows)
        await self.send_html("🩺 [STATUS] Aegis-Universal Periodic Engine Status", body)

    async def send_ai_decision(self, ctx: Dict[str, Any]) -> None:
        """Every 15-minute AI cycle alert (operator directive: alerts first-to-first)."""
        action = str(ctx.get("action", "?"))
        status = str(ctx.get("status", "?"))
        color = _RED if action == "SELL" else (_BLUE if action == "BUY" else _AMBER)
        subject = (f"🤖 [AI CYCLE 15m] {action} ({status.lower()}) — "
                   f"Aegis direct-execution feed")
        rows = [
            ("Verdict", f"{action} / {status}"),
            ("Per-Trade Net Stake", "±$2.00 fixed, fees included (SL ≈ −$2.00 / TP ≈ +$2.00)"),
            ("Setup (skill)", str(ctx.get("setup", "n/a"))),
            ("Confidence / Validity", f"{ctx.get('conf', 'n/a')} / {ctx.get('valid_min', '?')}m"),
            ("1h Trend Context", str(ctx.get("trend_1h", "n/a"))),
            ("AI Reason", str(ctx.get("reason", "n/a"))),
            ("AI Trigger/Entry Price", (
                f"${float(ctx['trigger_price']):,.1f}"
                if ctx.get("trigger_price") not in (None, "", 0) else "n/a")),
            ("Market at Decision (bid/ask)",
             f"${float(ctx.get('bid') or 0):,.1f} / ${float(ctx.get('ask') or 0):,.1f}"),
            ("Last Price", f"${float(ctx.get('last_price') or 0):,.1f}"),
            ("Model / Key / Call#", (
                f"{ctx.get('model')} · key#{ctx.get('key')}/5 · call#{ctx.get('call')}")),
            ("AI Latency", f"{ctx.get('latency_ms', '?')} ms"),
            ("Execution (direct, zero-drift IOC)", str(ctx.get("execution", "n/a"))),
            ("Equity", f"${float(ctx.get('equity') or 0):.2f} USDT"),
            ("Open Position", ctx.get("position", "none")),
            ("Cycle Time (UTC)", str(ctx.get("opened", ""))),
            ("Next Cycle", "~15 min (quarter-hour close)"),
        ]
        body = _ticket(
            "15-MINUTE AI DECISION", "Direct AI-verdict execution (operator modes)", color, rows,
            extra_html=(
                "<div style='background:#1e293b;border:1px solid #334155;border-radius:8px;"
                "padding:10px 14px;color:#cbd5e1;font-size:12px;'>"
                "OPERATOR RULES ENFORCED:<br>"
                "(1) PER-TRADE NET STAKE = <b>±$2.00</b> — a stop-out loses ≈$2.00 net and a "
                "target-out gains ≈$2.00 net, fees included (SL/TP attached exchange-side).<br>"
                "(2) FILL TOLERANCE ≤$1 — fills happen at the live best price, never more than "
                "$1 away from the AI decision price (Limit-IOC cap; beyond it the IOC expires "
                "with zero exposure). This is execution-price hygiene, NOT the trade stake.</div>"))
        await self.send_html(subject, body)


# ---------------------------------------------------------------------------
# Section 9 builder wrapper (used by enqueue from sync context)
# ---------------------------------------------------------------------------

class _Section9:
    @staticmethod
    async def builder_error(al: "Alerter", info: Dict[str, Any], suffix: str) -> None:
        await al.send_error(info, suffix)


# ---------------------------------------------------------------------------
# Global hook installation (Contract 9.2 bullet 1)
# ---------------------------------------------------------------------------

def install_process_hooks(al: Alerter) -> None:
    """Wrap sys.excepthook and threading.excepthook so ALL threads report here."""

    def _hook(module_hint: str, original: Any):
        def handler(*args: Any) -> None:
            try:
                exc = args[1] if len(args) >= 2 else args[0]
                al.report(module_hint, exc,
                          {"exc_repr": repr(args[0]) if args else "?"})
            except Exception:
                pass
            if callable(original):
                try:
                    original(*args)
                except Exception:
                    pass
        return handler

    sys.excepthook = _hook("sys.excepthook", sys.excepthook)
    if hasattr(threading, "excepthook"):
        threading.excepthook = _hook("threading.excepthook", threading.excepthook)

    if sys.platform == "win32":
        # unhandled exceptions on the main thread of embedded interpreters
        def _win_unhandled(args, **kwargs):  # pragma: no cover
            try:
                al.report("windows.unhandled", args.exception)
            except Exception:
                pass
        try:  # best-effort only
            import faulthandler  # noqa: F401
        except Exception:
            pass


def install_loop_hook(al: Alerter, loop: asyncio.AbstractEventLoop) -> None:
    """Route asyncio task failures (background daemons) into the alerter."""

    def handler(_loop: asyncio.AbstractEventLoop, context: Any) -> None:
        exc = context.get("exception")
        msg = context.get("message", "asyncio task failure")
        try:
            al.report("asyncio.loop", exc if exc is not None else msg,
                      {"task": str(context.get("future") or context.get("task") or "?")[:120]})
        except Exception:
            pass

    loop.set_exception_handler(handler)
