"""
Aegis-Universal :: Gemini Decision Feed (tradingview-ta replacement)
====================================================================
User-directed data-source change (documented deviation):
the decision cycle is a 15-minute AI cadence powered by gemini-3.8-flash
(Google AI Studio / Generative Language API) applying the day-trader-skill
playbook (github.com/Jakeschincariol/day-trader-skill — methodology port)
on venue data only (Bybit public endpoints; NO external market-data
providers are contacted from anywhere in this codebase).

Playbook mapping for a 24/7 crypto session (BTC/USDT linear perp only):
  scanner  -> UTC-day opening range (first 3 x 15m bars) + flag /
              20-bar close breakout / MA20 reclaim / oversold bounce /
              overbought fade (short mirror).
  entry    -> only CLOSED 15m bars may trigger (a wick never triggers);
              waiting / triggered / expired; one trigger per setup per
              UTC day; late bars downgrade to expired (HOLD).
  exit     -> FIXED ±$2 NET stake (operator directive 2026-11-06 v2):
              exchange-minimum quantity, stop nets ≈ -$2 after fees,
              target nets ≈ +$2 after fees, attached exchange-side.
  risk     -> DIRECT mode (operator directive 2026-11-06): every gate /
              limit / warning between the verdict and the exchange was
              removed. The AI verdict alone drives execution; the only
              operator conditions kept are the $1 execution-price cap
              (AI_MAX_PRICE_DEVIATION_USD) enforced by the DirectExecutor
              through Limit-IOC cap prices and the fixed ±$2 stake.

Model discipline (operator directive 2026-11-06 v2):
  gemini-3.8-flash ONLY (fallback chain emptied per operator emphasis).
  Two request shapes per call: (1) thinkingLevel=high + google_search
  grounding (verdict parsed from free text), (2) thinkingLevel=high +
  strict JSON. Each shape retries once after ~6s on transient errors
  (HTTP 429/5xx, transport timeouts); _ask_gemini allows one extra whole
  -chain round after a 12s pause when the first failed transiently.

Key & model rotation (AI Studio free tier survival matrix, user-specified):
  5 keys x AI_CALLS_PER_KEY calls (8) -> key#1(8) #2(8) #3(8) #4(8) #5(8)
  -> loop to #1 forever; persisted in AI_STATE_PATH JSON (atomic rename):
  counter_total / key_index / calls_current / decision-history tail.
  Model chain: the primary model (gemini-3.8-flash) is attempted first
  every cycle; on failure the chain descends GEMINI_FALLBACK_MODELS
  (e.g. 3.7-flash -> 3.5-flash) inside the same cycle — exactly one
  counted attempt per candidate, and ANY attempt (success or failure)
  consumes one rotation slot (no amplification). The first success
  wins, and the model that produced the verdict is recorded by name
  and surfaced in the consensus/status reports. The reconciler boot
  drill also counts as one real call.

Latency: end-to-end decision latency (data -> verdict, ms) is measured,
persisted and surfaced on the status reports; the model additionally
returns valid_minutes — the trigger validity window ("a late entry is a
different trade").

Every tick every failure is wrapped: feed failures never kill the daemon
and are routed to the error alerter with deduping.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

from engine.marketsetup import MarketSetup, compute_atr_14

logger = logging.getLogger("aegis.gemini")

_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models"


def _transient_tag(tag: str) -> bool:
    """Is this recorded failure tag transient (worth a same-shape retry)?"""
    return any(m in tag for m in ("Timeout", "Error", "HTTP429", "HTTP500",
                                  "HTTP502", "HTTP503", "HTTP504"))


def _is_transient(exc: Exception) -> bool:
    """Classify a raised exception as transient (503/429/timeouts) vs permanent.

    Used by _ask_gemini's bounded second round: retry only demand-503-style
    failures; never re-fire on permanent errors (bad key, bad request).
    """
    try:
        import httpx as _h
        if isinstance(exc, _h.TimeoutException):
            return True
        if isinstance(exc, _h.TransportError):
            return True
    except Exception:      # pragma: no cover — httpx always present here
        pass
    m = re.search(r"HTTP (\d{3})", str(exc))
    if m and m.group(1) in ("429", "500", "502", "503", "504"):
        return True
    return any(k in str(exc) for k in ("ReadTimeout", "Timeout", "connection"))

_KL_MS = {"5": 300_000, "15": 900_000, "60": 3_600_000}

_SYSTEM_PROMPT = """You are the decision head of an automated BTCUSDT USDT-linear perpetual day-trade engine on Bybit demo. You are bound EXACTLY AND IN DETAIL to the day-trader-skill playbook (github.com/Jakeschincariol/day-trader-skill). Its rules are quoted verbatim below. You do NOT improvise, soften, reinterpret or upgrade anything: where ADAPTATIONS are listed, apply them mechanically and identically, nothing more.

==================== VERBATIM DOCTRINE (day-trader-skill) ====================

--- trader-scanner (the Chart Scanner) ---
"A scan is a list of charts that match a rule, not a list of stocks to buy, and a setup on a chart is not a prediction."
Setups (quote): "breakout_20d: the last close is above every close of the 20 sessions before it" (here: the last closed bar closed beyond the highest high / lowest low of the previous 20 closed bars). "bull_flag: up more than 10% over the 10 sessions before the last 5, then the last 5 closes inside a 5% range" (adapted A3 to intraday magnitude). "reclaim_ma20: the close before was under its 20-day average, the last close is above it". "oversold: RSI 14 (Wilder) under 30". "oversold_bounce: RSI 14 was under 30 yesterday and today closed up".
Opening-range breakout (quote): "The opening range is the first 15 minutes of the session ... A hit is the first completed bar to CLOSE above the range high, at that bar's close time."
Scanner rules (quote): "Say 'matched a rule', never 'a buy', 'a winner' or 'a good trade'." / "A hit is stale once the price has moved on. Re-run before acting on it." / "Never round a partial scan up to 'the whole market'."

--- trader-entry (the Entry Timer) ---
"The Entry Timer turns a setup into one moment: the bar that confirms it."
"A price that pokes above the high in the middle of a bar is not an entry. A bar that CLOSES above it is."
"The trigger is the first completed bar after the range that CLOSES above the level. A wick above is not a trigger. A close exactly at the level is not above it."
"Only closed bars count."
"What it says. Waiting (and how far the last close is from the level), triggered, expired, or no data."
"One trigger per setup per day: the first close above. Later closes do not reset it."
"Never call an entry early because a bar looks like it will close above."
"If the trigger is more than a bar old, say that too: a late entry is a different trade."

--- trader-exit (the Exit Planner) ---
"The Exit Planner decides how a trade ends before it starts. The stop is where the setup has failed. The target is where you take what you came for. Both are set BEFORE the entry, written down and handed on, so nobody has to decide anything in the middle of a fast move."
Stop rules (quote): "or-mid: the opening-range midpoint (the default for an opening-range breakout)" / "or-low: the opening-range low; wider, so fewer shares for the same max loss" / "pct:X: X percent under the entry" / "atr:K: K times the 14-bar ATR under the entry".
"Refused, with the reason. A stop at or above the entry. A target at or below it. A risk under 0.05% of the price: too tight to survive the spread, normal wiggle would stop you out."
"Never move a stop further from the entry once the trade is on." / "Out by the flat-by time, win or lose: no overnight hold."

--- trader-budget (the Budget Manager) ---
"The stop decides the size: the further away the stop, the fewer shares." / "Sizing to a max loss keeps each loss small; it does not make a trade win." / "A stop too far away for your max loss is a plain refusal, not a smaller order."

--- trader-news (the News Monitor) ---
"Headlines are leads, not facts. Before telling the user something happened, open the story or the filing and read it. Quote the source. Never summarise a story you could not read: say so."
"Filings beat headlines. If a filing and a headline disagree, the filing is what the company told the regulator."
"No predictions. Report what was published and how far the price is from yesterday's close. Never say what the stock will do next."
"A chart can look perfect and still be about to gap on an earnings date, a halt or a lawsuit." / "Never act on a headline alone."

==================== OPERATOR STANDING (binding, 2026-11-06 v2) ====================
1. STAKE: every order stakes a FIXED $2.00 NET. The engine takes the exchange-minimum quantity (0.001 BTC), places the stop so a stop-out loses ~$2 net after fees and the target so a target-out gains ~$2 net after fees. Sizing, stops, targets, fees, funding and flat-by are ENGINE-OWNED: never compute them, never mention them, never weigh them.
2. GATES: the operator has CANCELLED all trading conditions and vetoes (no ATR/extreme-volatility veto, no funding veto, no chop veto, no 1h hard-contradiction, no daily-loss or trade-count limit, no session-window veto; the old G0-G9 are deleted). The ONLY validity condition is the closed-bar trigger doctrine above. Never reinstate a gate in your own voice: with-trend preference is a mild bias, never a HOLD source.
3. UNIVERSE: one instrument, BTCUSDT perpetual, trading 24/7. You run right after each :00/:15/:30/:45 UTC close; your bars are the last CLOSED 15m bars at utc_now.
4. EXECUTION IS PRE-AUTHORIZED (the operator's standing yes replaces the trader-broker human gate: 'nothing in this pack places an order except trader-broker, and only when trader-risk passed + the user typed an explicit yes + the user clicked Allow'). You output the verdict; the engine places it immediately and reports back.
5. HONEST REPORTING (scanner rules verbatim): never invent a setup; never sit out a setup that just confirmed because the market "feels risky"; say 'matched a rule' in fact language, never 'a winner' or 'a good trade'.

==================== ADAPTATIONS (mechanical only — apply identically) ====================
A1 SHORT MIRROR (trader-entry says "Long side only. v2 plans longs only. If the user wants a short, say that." — the operator wants BOTH directions): apply every long rule verbatim, mirrored on the short side. ORB: SELL = the first completed bar after the range that CLOSES strictly below the range low. break20: SELL = a closed bar below the lowest low of the previous 20 closed bars. flag: bear flag = a directional push (>=1.5% down over ~8 bars) then a consolidation (<0.8%) of the last ~4 bars inside that push; trigger = a closed bar beyond the consolidation edge. reclaim: SELL = the previous closed bar was above ema20_15m and the last closed bar closed below it. overbought_fade: rsi14_15m was above 70 within the last 3 closed bars and the last closed bar closed down. Everything else in the doctrine is unchanged.
A2 SESSION (NYSE 9:30-15:55 ET is replaced by the 24/7 crypto UTC anchor): the opening range is the first three 15m bars of the UTC day (00:00-00:45 closes). "Flat by 15:55 / no overnight hold" is replaced by the engine's exit manager (breakeven, ATR trail, 24h TTL) — exits are engine-owned, not yours.
A3 CHART SCALE (daily/5m stock bars become 15m BTC bars): the scanner's close-based setups run on 15m closes: break20 = last closed 15m bar beyond the high/low of the previous 20 closed 15m bars; bull/bear flag at intraday magnitude = a >=1.5% directional push over ~8 bars then a <0.8% consolidation of the last ~4 bars inside that push, confirmed by a closed bar beyond the consolidation edge. The word "session" everywhere means the UTC day.
A4 SIZE MATH (replaces the budget share formula): "A stop too far away for your max loss is a plain refusal" is inverted by the operator's fixed-stake directive: the distance is computed FROM the fixed $2 stake (engine-side). There is therefore never a size-based refusal; if the bars present a trigger, output it.
A5 NEWS (trader-news runs on your Google Search grounding tool when you choose to use it): headlines are leads, not facts; filings beat headlines; never act on a headline alone. News NEVER creates a verdict by itself — only the closed-bar trigger doctrine does. A fresh big move with no headline is still just data: report it in the reason.
A6 1h EMA50 (scanner context): context only, mild with-trend preference, never a veto.

==================== STATUS VOCABULARY (verbatim semantics) ====================
- "waiting": the level stands and no closed bar has gone beyond it yet. action=HOLD, valid_minutes=0; the reason states how far the last close is from the level.
- "triggered": the LAST closed bar CLOSED strictly beyond the level (a close exactly AT the level is not beyond it). action=BUY (or the A1 mirror SELL), confidence 0.5-1.0, valid_minutes = 10 (execution window until the next cycle), trigger_price = that bar's close, setup = the matched rule name.
- "expired": (i) the setup's one trigger per UTC day is already consumed (its first qualifying close happened earlier today), or (ii) the confirming bar is more than one bar old and the newest closed bar does not re-confirm it (a late entry is a different trade). action=HOLD, valid_minutes=0.

HOLD is correct ONLY under waiting/expired (or when literally no setup matches). Never sit out a fresh closed-bar trigger.

Return STRICT JSON ONLY (no markdown fences, no prose):
{"action":"BUY|SELL|HOLD","status":"waiting|triggered|expired","confidence":<0.0-1.0>,"valid_minutes":<int>,"setup":"orb|flag|break20|reclaim|oversold_bounce|overbought_fade|none","trigger_price":<number>,"reason":"<=200 chars, fact language","trend_1h":"up|down|range"}
"""


def _rsi_wilder(closes: List[float], period: int = 14) -> float:
    """Classic Wilder RSI on chronological closes."""
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(0.0, d) for d in deltas]
    losses = [max(0.0, -d) for d in deltas]
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l <= 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_g / avg_l)


def _ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


class _RotationState:
    """dat-file persistence for the 5-key sequential rotation engine."""

    def __init__(self, settings) -> None:
        self.path = settings.ai_state_path
        self.calls_per_key = max(1, settings.ai_calls_per_key)
        self.keys = list(settings.gemini_api_keys)
        parent = os.path.dirname(os.path.abspath(self.path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.d: Dict[str, Any] = {
            "counter_total": 0, "key_index": 0, "calls_current": 0,
            "model": settings.gemini_model, "updated_ts": 0.0, "history": [],
        }
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as fh:
                    stored = json.load(fh)
                self.d.update({k: v for k, v in stored.items() if k in self.d})
            except (OSError, ValueError, TypeError):
                logger.warning("ai_state.json unreadable — rotation restarts at key#1")
        self._save()

    def _save(self) -> None:
        self.d["updated_ts"] = round(time.time(), 1)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self.d, fh)
        os.replace(tmp, self.path)

    def begin_call(self) -> int:
        """Reserve the current key slot for this request attempt."""
        return self.d["key_index"] % max(1, len(self.keys))

    def advance_after_call(self) -> None:
        """Count this attempt (a failure still consumes its slot, no retries)."""
        self.d["calls_current"] += 1
        self.d["counter_total"] += 1
        if self.d["calls_current"] >= self.calls_per_key:
            self.d["key_index"] = (self.d["key_index"] + 1) % max(1, len(self.keys))
            self.d["calls_current"] = 0
        self._save()

    def record(self, **kv: Any) -> None:
        self.d["history"] = [{"ts": round(time.time(), 1), **kv}] + self.d["history"]
        self.d["history"] = self.d["history"][:20]
        self._save()


class GeminiScanner:
    """15-minute Gemini decision cycle feeding the standard trade pipeline."""

    def __init__(self, client, alerter, settings,
                 on_setup=None, shared: Optional[Dict[str, Any]] = None):
        self.client = client
        self.al = alerter
        self.s = settings
        self.on_setup = on_setup
        self.shared_account = shared  # operator-facing account snapshot (equity/position)
        if not settings.gemini_api_keys:
            raise RuntimeError("GEMINI_FEED enabled but GEMINI_API_KEYS is empty")
        self.rot = _RotationState(settings)
        self._task: Optional[asyncio.Task] = None
        self.shared: Dict[str, Any] = {
            "last_scan_ts": 0.0, "consensus": "", "error": "",
            "ai_latency_ms": 0.0, "last_action": "n/a", "last_model": "",
        }

    # ------------------------------------------------------------------
    # Boot drill (reconciler step 7) — one real counted call
    # ------------------------------------------------------------------

    async def probe(self) -> str:
        return await self.tick()

    # ------------------------------------------------------------------
    # Daemon loop (15-minute candle clock; first decision ~12s after boot —
    # the operator wants the first AI cycle to fire immediately)
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self.loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def loop(self) -> None:
        first = True
        while True:
            try:
                if first:
                    first = False
                    await asyncio.sleep(12.0)
                else:
                    await self._sleep_to_quarter_boundary()
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — daemon never dies (Contract 9.2)
                self.shared["error"] = f"{type(exc).__name__}: {exc}"
                self.al.report("gemini.loop", exc, {"phase": "tick"})
                await asyncio.sleep(30)

    @staticmethod
    async def _sleep_to_quarter_boundary() -> None:
        now = time.time()
        delay = 900.0 - (now % 900.0) + 0.25
        await asyncio.sleep(min(max(delay, 0.5), 905.0))

    # ------------------------------------------------------------------
    # One decision cycle: venue data -> Gemini -> pipeline hand-off
    # ------------------------------------------------------------------

    async def tick(self) -> str:
        ctx = await self._collect()
        t0 = time.time()
        try:
            verdict, model_used, key_idx = await self._ask_gemini(ctx["features"])
        except Exception as exc:  # noqa: BLE001 — whole chain down: the cycle
            # EMAILS ANYWAY (operator directive: alerts first-to-first every
            # 15 minutes). Honest synthetic status, no execution, no retry
            # amplification; the loop's own handler no longer needed for this.
            model_used, key_idx = "chain-down", -1
            verdict = {
                "action": "SCAN", "status": "ERROR",
                "reason": (f"no AI verdict this cycle — every model in the chain "
                           f"failed: {type(exc).__name__}: {str(exc)[:120]}"),
                "confidence": None, "setup": "chain-down",
                "valid_minutes": 0, "trend_1h": "n/a",
                "trigger_price": None, "avg_price": None,
            }
        ai_ms = (time.time() - t0) * 1000.0

        action = str(verdict.get("action", "HOLD")).upper()
        status = str(verdict.get("status", "expired")).upper()
        tradable = action in ("BUY", "SELL") and status == "TRIGGERED"

        self.shared["ai_latency_ms"] = round(ai_ms)
        self.shared["last_action"] = action
        self.shared["last_model"] = model_used
        self.shared["consensus"] = (
            f"AI={action} {status.lower()} conf={verdict.get('confidence')} "
            f"setup={verdict.get('setup')} valid={verdict.get('valid_minutes')}m "
            f"1h={verdict.get('trend_1h')} lat={round(ai_ms)}ms "
            f"model={model_used} key#{key_idx + 1}/5 call#{self.rot.d['counter_total']}")
        self.shared["last_scan_ts"] = time.time()
        self.shared["error"] = ""

        execution_result = "n/a (HOLD / not triggered)"
        if verdict.get("status") == "ERROR":
            execution_result = "n/a (no AI verdict made — model chain down; no order placed)"
        if tradable and self.on_setup is not None:
            setup = self._build_setup(verdict, ctx, action)
            if setup is not None:
                try:
                    verdict["model"] = model_used
                    verdict["key_idx"] = key_idx
                    execution_result = await self.on_setup(setup, verdict) or "no result"
                except Exception as exc:  # noqa: BLE001 — pipeline errors alert
                    self.al.report("gemini.pipeline", exc, {"direction": setup.direction})
                    execution_result = f"PIPELINE ERROR: {type(exc).__name__}: {str(exc)[:140]}"

        # Every cycle emails its own alert (operator wants first-to-first 15m)
        acct = dict(self.shared_account or {})
        pos = acct.get("position")
        self.al.enqueue(self.al.send_ai_decision, {
            "action": action, "status": status,
            "conf": verdict.get("confidence"), "setup": verdict.get("setup"),
            "valid_min": verdict.get("valid_minutes"), "trend_1h": verdict.get("trend_1h"),
            "reason": str(verdict.get("reason", ""))[:200],
            "trigger_price": verdict.get("trigger_price"),
            "avg_price": verdict.get("avg_price"),
            "model": model_used, "key": key_idx + 1,
            "slot": self.rot.d["calls_current"], "call": self.rot.d["counter_total"],
            "latency_ms": round(ai_ms),
            "last_price": ctx["last_price"], "bid": ctx["entry_bid"], "ask": ctx["entry_ask"],
            "execution": execution_result,
            "opened": ctx["features"].get("utc_now"),
            "symbol": self.s.bybit_symbol,
            "equity": acct.get("equity"), "position": (
                f"{pos.side} {pos.qty} @ ${pos.entry:,.1f}" if pos else "none"),
        })
        return self.shared["consensus"]

    # ------------------------------------------------------------------
    # Venue-only feature build (Bybit endpoints only)
    # ------------------------------------------------------------------

    async def _collect(self) -> Dict[str, Any]:
        k15 = await self.client.klines("15", 120)
        k1h = await self.client.klines("60", 48)
        if len(k15) < 30:
            raise RuntimeError(f"kline feed thin: 15m rows={len(k15)}")
        bid, ask = await self.client.orderbook_top()
        ticker = await self.client.tickers()

        now_ms = int(time.time() * 1000)

        def asc(rows: List[List[str]], step_ms: int) -> List[List[str]]:
            rows = sorted(rows, key=lambda r: int(r[0]))
            return [r for r in rows if int(r[0]) + step_ms <= now_ms]  # closed only

        a15 = asc(k15, _KL_MS["15"])
        a1h = asc(k1h, _KL_MS["60"])
        closes15 = [float(r[4]) for r in a15]
        highs15 = [float(r[2]) for r in a15]
        lows15 = [float(r[3]) for r in a15]
        last = closes15[-1]

        rsi15 = _rsi_wilder(closes15)
        ema20 = _ema(closes15, 20)[-1]
        ema50 = _ema(closes15, 50)[-1]
        atr15 = compute_atr_14(a15)
        if atr15 <= 0:
            raise RuntimeError("ATR unavailable from feed")
        closes1h = [float(r[4]) for r in a1h]
        rsi1h = _rsi_wilder(closes1h)
        ema50_h = _ema(closes1h, 50)[-1]

        # UTC-day opening range: first three 15m bars of the day
        day_start_s = int(time.time()) - (int(time.time()) % 86400)
        or_rows = [r for r in a15 if day_start_s <= int(r[0]) / 1000 < day_start_s + 2700]
        or_ready = len(or_rows) >= 3
        if or_ready:
            or_high = max(float(r[2]) for r in or_rows)
            or_low = min(float(r[3]) for r in or_rows)
        else:
            or_high = or_low = 0.0
        post_or = [float(r[4]) for r in a15 if int(r[0]) / 1000 >= day_start_s + 2700]
        above_today = or_ready and any(c > or_high for c in post_or)
        below_today = or_ready and any(c < or_low for c in post_or)

        sell_range_hi = max(highs15[-21:-1]) if len(highs15) >= 21 else max(highs15)
        buy_range_lo = min(lows15[-21:-1]) if len(lows15) >= 21 else min(lows15)

        features = {
            "utc_now": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
            "symbol": self.s.bybit_symbol,
            "closed_bar_age_s": round(time.time() - int(a15[-1][0]) / 1000 - 900, 1),
            "ask": round(ask, 1), "bid": round(bid, 1),
            "spread_pct": round((ask - bid) / ((ask + bid) / 2) * 100, 4),
            "or_utc": {"ready": or_ready, "bars": len(or_rows),
                       "high": round(or_high, 1), "low": round(or_low, 1),
                       "close_above_high": bool(above_today),
                       "close_below_low": bool(below_today)},
            "mf_15m": {"rsi14": round(rsi15, 1), "ema20": round(ema20, 1),
                       "ema50": round(ema50, 1), "atr14": round(atr15, 1),
                       "atr_pct": round(atr15 / last * 100, 3),
                       "prev20_high": round(sell_range_hi, 1),
                       "prev20_low": round(buy_range_lo, 1),
                       "closes_last_20": [round(c, 1) for c in closes15[-20:]]},
            "mf_1h": {"rsi14": round(rsi1h, 1), "ema50": round(ema50_h, 1),
                      "close_vs_ema50_pct": round((last / ema50_h - 1) * 100, 3)},
            "funding": {"rate": ticker.get("funding_rate"),
                        "next_in_min": (
                            round((ticker.get("next_funding_ms") - now_ms) / 60000)
                            if ticker.get("next_funding_ms") else None)},
        }
        return {
            "features": features,
            "entry_ask": ask, "entry_bid": bid,
            "last_price": float(ticker.get("last_price") or last),
            "atr15": atr15, "rsi15": rsi15, "ema50": ema50, "close15": last,
        }

    # ------------------------------------------------------------------
    # Gemini call (AI Studio API) — model fallback chain; every attempt
    # (success or failure) consumes one rotation slot, no amplification
    # ------------------------------------------------------------------

    async def _ask_gemini(self, features: Dict[str, Any]) -> Tuple[Dict[str, Any], str, int]:
        """Model fallback chain: primary first every cycle, then fallbacks.

        Each candidate gets exactly one counted attempt with the current
        rotation slot; the next candidate in the same cycle picks up the
        following slot. The first successful verdict wins; the model that
        produced it is returned for transparent telemetry. Bounded
        transient recovery (operator directive: FIX gemini-3.8-flash
        demand 503s / read-timeouts, do not abandon them): if the whole
        chain failed on transient errors only, ONE extra round runs after
        a 12s pause.
        """
        candidates = [self.s.gemini_model] + list(self.s.gemini_fallback_models)
        last_exc: Optional[Exception] = None
        for round_no in (1, 2):
            if round_no == 2:
                if last_exc is None or not _is_transient(last_exc):
                    break
                await asyncio.sleep(12)
            for model in candidates:
                key_idx = self.rot.begin_call()
                try:
                    verdict = await self._call_model(model, key_idx, features)
                except Exception as exc:  # noqa: BLE001 — descend / retry
                    last_exc = exc
                    if not _is_transient(exc):
                        break       # permanent: further attempts are pointless
                    continue
                return verdict, model, key_idx
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("no gemini candidates configured")

    async def _call_model(self, model: str, key_idx: int,
                          features: Dict[str, Any]) -> Dict[str, Any]:
        """One counted attempt (candidate model + current rotation slot).

        Request shapes, in operator priority order:
          1. shape 'gs'   — HIGH THINKING + GOOGLE SEARCH grounding: the
             operator explicitly requires "high thinking" and internet
             access. A grounded response cannot also use a strict JSON
             response-mime, so the verdict is parsed out of free text.
          2. shape 'json' — HIGH THINKING + strict JSON (no tools,
             responseMimeType application/json) as the shape-2 fallback.

        Each shape gets up to two attempts: on a transient failure
        (HTTP 429/5xx or a transport timeout) the SAME shape retries
        once after ~6s. A permanent failure (400/403, envelope, empty
        text) moves straight to the next shape. The rotation slot is
        consumed exactly once for the whole outcome; ai_state records
        it once with the shape detail path.
        """
        headers = {
            "x-goog-api-key": self.s.gemini_api_keys[key_idx],
            "Content-Type": "application/json",
        }
        gen: Dict[str, Any] = {
            "temperature": 0.2, "maxOutputTokens": 32768,
            "thinkingConfig": {"thinkingLevel": "high"},
        }
        shapes = (
            ("gs", {"tools": [{"google_search": {}}]}),
            ("json", {}),
        )
        url = f"{_GEMINI_URL}/{model}:generateContent"
        details: List[str] = []
        for shape, extra in shapes:
            gen_cfg = dict(gen)
            if shape == "json":
                gen_cfg["responseMimeType"] = "application/json"
            payload = {
                "contents": [{"role": "user",
                              "parts": [{"text": json.dumps(features, separators=(",", ":"))}]}],
                "systemInstruction": {"parts": [{"text": _SYSTEM_PROMPT}]},
                "generationConfig": gen_cfg,
                **extra,
            }
            for attempt in (1, 2):
                if attempt == 2:
                    await asyncio.sleep(6)
                try:
                    async with httpx.AsyncClient(
                            timeout=self.s.ai_call_timeout_seconds) as cli:
                        resp = await cli.post(url, json=payload, headers=headers)
                except Exception as exc:  # transport (timeout / network)
                    tag = f"{shape}.a{attempt}:{type(exc).__name__}"
                    details.append(tag)
                    if attempt < 2 and _transient_tag(tag):
                        continue        # retry the same shape after the 6s pause
                    break               # move to the next shape
                if resp.status_code in (429, 500, 502, 503, 504):
                    tag = f"{shape}.a{attempt}:HTTP{resp.status_code}"
                    details.append(tag)
                    if attempt < 2:
                        continue        # retry the same shape after the 6s pause
                    break
                if resp.status_code != 200:
                    details.append(f"{shape}.a{attempt}:HTTP{resp.status_code}")
                    break               # permanent — straight to the next shape
                try:
                    body = resp.json()
                    cands = body.get("candidates") or []
                    if not cands:
                        details.append(f"{shape}.a{attempt}:no-candidates")
                        break
                    cand0 = cands[0]
                    finish = str(cand0.get("finishReason", ""))
                    parts = (cand0.get("content") or {}).get("parts") or []
                    # grounded + thinking responses interleave thought-only
                    # parts before the answer; join answer parts only.
                    texts = [p.get("text", "") for p in parts
                             if isinstance(p, dict) and not p.get("thought")]
                    text = "\n".join(t for t in texts if t and t.strip()).strip()
                except (ValueError, AttributeError, KeyError, IndexError) as exc:
                    details.append(f"{shape}.a{attempt}:envelope-{type(exc).__name__}")
                    break
                if not text:
                    details.append(f"{shape}.a{attempt}:empty-text finish={finish}")
                    break
                verdict = self._parse_verdict(text)
                if verdict is None:
                    details.append(
                        f"{shape}.a{attempt}:unparseable finish={finish} {text[:80]!r}")
                    break
                self.rot.advance_after_call()
                trail = ",".join(details)
                self.rot.record(ok=True, model=model, key=key_idx + 1,
                                action=verdict.get("action"),
                                status=verdict.get("status"),
                                conf=verdict.get("confidence"),
                                setup=verdict.get("setup"),
                                error=f"shape={shape} tries={attempt}"
                                      + (f" after [{trail}]" if trail else ""))
                return verdict
        self.rot.advance_after_call()
        self.rot.record(ok=False, model=model, key=key_idx + 1,
                        error="3.8 all-shapes-failed: " + ";".join(details))
        raise RuntimeError(
            f"gemini all request shapes failed: {';'.join(details)}")

    @staticmethod
    def _parse_verdict(text: str) -> Optional[Dict[str, Any]]:
        raw = text.strip()
        raw = re.sub(r"^```(?:json)?|```$", "", raw.strip(), flags=re.M).strip()
        candidate: Optional[str] = None
        try:
            json.loads(raw)
            candidate = raw
        except ValueError:
            match = re.search(r"\{.*\}", raw, flags=re.S)
            if match:
                blob = match.group(0)
                try:
                    json.loads(blob)
                    candidate = blob
                except ValueError:
                    candidate = None
        if not candidate:
            return None
        v = json.loads(candidate)
        if str(v.get("action", "")).upper() not in ("BUY", "SELL", "HOLD"):
            return None
        v["action"] = str(v.get("action", "HOLD")).upper()
        v["status"] = str(v.get("status", "expired")).lower()
        try:
            v["confidence"] = max(0.0, min(1.0, float(v.get("confidence", 0.5))))
        except (TypeError, ValueError):
            v["confidence"] = 0.5
        try:
            v["valid_minutes"] = max(0, int(float(v.get("valid_minutes", 0))))
        except (TypeError, ValueError):
            v["valid_minutes"] = 0
        return v

    # ------------------------------------------------------------------
    # MarketSetup synthesis (marketdata + ATR clamp identical to scanner)
    # ------------------------------------------------------------------

    def _build_setup(self, verdict: Dict[str, Any], ctx: Dict[str, Any],
                     action: str) -> Optional[MarketSetup]:
        direction = "Buy" if action == "BUY" else "Sell"
        entry = ctx["entry_ask"] if direction == "Buy" else ctx["entry_bid"]
        atr = ctx["atr15"]
        sl_distance = self._clamp_sl(entry, atr)
        tag = str(verdict.get("setup") or "none")
        conf = verdict.get("confidence")
        return MarketSetup(
            direction=direction,
            entry=entry,
            trigger_close=float(ctx["last_price"] or entry),
            atr_15m=atr,
            rsi_15m=float(ctx["rsi15"]),
            ema50_15m=float(ctx["ema50"]),
            close_15m=float(ctx["close15"]),
            summary_15m=f"AI {action} ({tag}, {verdict.get('status')}, conf {conf})",
            summary_1h=f"AI 1h {verdict.get('trend_1h', 'n/a')} "
                       f"(valid {verdict.get('valid_minutes')}m)",
            sl_distance=sl_distance,
            tp_distance=self.s.target_gross_rr * sl_distance,
        )

    def _clamp_sl(self, entry: float, atr: float) -> float:
        raw = 1.5 * atr
        floor = entry * self.s.min_sl_distance_pct
        ceiling = entry * self.s.max_sl_distance_pct
        return min(max(raw, floor), ceiling)
