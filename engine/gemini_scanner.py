"""
Aegis-Universal :: Gemini Decision Feed (tradingview-ta replacement)
====================================================================
User-directed data-source change (documented deviation; G0-G9 untouched):
the tradingview-ta consensus is replaced by a 15-minute decision cycle
powered by gemini-3.8-flash (Google AI Studio / Generative Language API)
applying the day-trader-skill playbook (github.com/Jakeschincariol/
day-trader-skill — methodology port) on venue data only (Bybit public
endpoints; TradingView is no longer queried anywhere).

Playbook mapping for a 24/7 crypto session (BTC/USDT linear perp only):
  scanner  -> UTC-day opening range (first 3 x 15m bars) + flag /
              20-bar close breakout / MA20 reclaim / oversold bounce.
  entry    -> only CLOSED 15m bars may trigger (a wick never triggers);
              waiting / triggered / expired; one trigger per setup per
              UTC day; late bars downgrade to expired (HOLD).
  exit     -> engine-owned: ATR-clamped stop (1.5xATR seeded, 0.5%-2.0%
              band) + 1.5R gross target — unchanged gate domain.
  risk     -> G0-G9 remain the single authority. The AI verdict only
              proposes a direction; every gate may veto it.

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
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

import httpx

from engine.scanner import MarketSetup, compute_atr_14

logger = logging.getLogger("aegis.gemini")

_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models"

_KL_MS = {"5": 300_000, "15": 900_000, "60": 3_600_000}

_SYSTEM_PROMPT = """You are the decision head of an automated BTCUSDT USDT-linear perpetual day-trade engine on Bybit. You apply the day-trader-skill playbook (trader-scanner + trader-entry synthesis, ported to a 24/7 crypto session):

- CLOSED bars only: a bar triggers when it CLOSES beyond a level. A forming bar or a wick never triggers. Evaluate against the last CLOSED 15m bar as of utc_now; you run every 15 minutes right after each :00/:15/:30/:45 close.
- Opening-range breakout (orb): the range is the first three 15m bars of the UTC day (00:00-00:45 closes bar by bar). BUY trigger = a closed 15m bar strictly above or high after the range completed; SELL = a closed 15m bar strictly below or low. One trigger per setup per UTC day: if close_above_high or close_below_low is already true, the orb setup is expired for the rest of this UTC day.
- bull/bear flag: a >=1.5% directional push over ~8 bars followed by a <0.8% consolidation of the last ~4 bars inside that push; confirmation is a closed bar beyond the consolidation edge.
- break20: the last CLOSED bar closed above the highest of the previous 20 bars' highs (long), or below the lowest of their lows (short), with the level not already triggered today in that direction.
- reclaim: the previous closed bar was below ema20_15m and the last closed bar closed above it (long); mirrored logic applies for the short side.
- oversold_bounce / overbought_fade: rsi14_15m went below 30 (above 70) within the last 3 closed bars and the last closed bar turned up (down); the 1h EMA50 context must not hard-contradict the direction (BUY blocked only when 1h is decisively down; SELL blocked only when 1h is decisively up — you keep the final judgement).
- HONESTY: most of the time the honest answer is HOLD. HOLD whenever: the orb range is not complete, structure is mixed/choppy, the trigger already fired today, ATR% is extreme, funding is strongly adverse, or there is no clean closed-bar confirmation.
- LATE RULE: if the confirming bar closed more than 10 minutes before utc_now, status="expired" and action="HOLD", unless the newest closed bar itself re-confirms the same setup.
- Validity: valid_minutes = how many minutes from utc_now this verdict stays executable (0 for HOLD).
- Risk, sizing, stops, targets, fees belong to the engine's deterministic gates (G0-G9). Never compute them and never mention them.

Return STRICT JSON ONLY (no markdown fences, no prose):
{"action":"BUY|SELL|HOLD","status":"waiting|triggered|expired","confidence":<0.0-1.0>,"valid_minutes":<int>,"setup":"orb|flag|break20|reclaim|oversold_bounce|none","trigger_price":<number>,"reason":"<=200 chars","trend_1h":"up|down|range"}
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
                 on_setup: Optional[Callable[[MarketSetup], Coroutine[Any, Any, None]]]):
        self.client = client
        self.al = alerter
        self.s = settings
        self.on_setup = on_setup
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
    # Daemon loop (15-minute candle clock, first decision ~90s after boot)
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
                    await asyncio.sleep(90.0)
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
        verdict, model_used, key_idx = await self._ask_gemini(ctx["features"])
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

        if tradable and self.on_setup is not None:
            setup = self._build_setup(verdict, ctx, action)
            if setup is not None:
                try:
                    await self.on_setup(setup)
                except Exception as exc:  # noqa: BLE001 — pipeline errors alert
                    self.al.report("gemini.pipeline", exc, {"direction": setup.direction})
        return self.shared["consensus"]

    # ------------------------------------------------------------------
    # Venue-only feature build (no TradingView anywhere)
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
        produced it is returned for transparent telemetry.
        """
        candidates = [self.s.gemini_model] + list(self.s.gemini_fallback_models)
        last_exc: Optional[Exception] = None
        for model in candidates:
            key_idx = self.rot.begin_call()
            try:
                verdict = await self._call_model(model, key_idx, features)
            except Exception as exc:  # noqa: BLE001 — descend to next model
                last_exc = exc
                continue
            return verdict, model, key_idx
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("no gemini candidates configured")

    async def _call_model(self, model: str, key_idx: int,
                          features: Dict[str, Any]) -> Dict[str, Any]:
        """One counted attempt (candidate model + current rotation key).

        The rotation slot is consumed on every exit path (transport error,
        HTTP error, malformed envelope, unparseable verdict, success) —
        a failure never triggers a retry on the same slot (no request
        amplification); the slot the next attempt picks reflects exactly
        the sequential 5-key rotation matrix.
        """
        headers = {
            "x-goog-api-key": self.s.gemini_api_keys[key_idx],
            "Content-Type": "application/json",
        }
        payload = {
            "contents": [{"role": "user",
                          "parts": [{"text": json.dumps(features, separators=(",", ":"))}]}],
            "systemInstruction": {"parts": [{"text": _SYSTEM_PROMPT}]},
            "generationConfig": {
                "temperature": 0.2, "maxOutputTokens": 400,
                "responseMimeType": "application/json",
            },
        }
        url = f"{_GEMINI_URL}/{model}:generateContent"
        try:
            async with httpx.AsyncClient(timeout=self.s.ai_call_timeout_seconds) as cli:
                resp = await cli.post(url, json=payload, headers=headers)
        except Exception as exc:  # transport failure — slot consumed, no retry
            self.rot.advance_after_call()
            self.rot.record(ok=False, model=model, key=key_idx + 1,
                            error=f"transport {type(exc).__name__}")
            raise
        if resp.status_code != 200:
            self.rot.advance_after_call()
            self.rot.record(ok=False, model=model, key=key_idx + 1,
                            error=f"HTTP {resp.status_code}: {resp.text[:160]}")
            raise RuntimeError(
                f"Gemini HTTP {resp.status_code}: {resp.text[:200]}")
        try:
            text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError, ValueError) as exc:
            self.rot.advance_after_call()
            self.rot.record(ok=False, model=model, key=key_idx + 1,
                            error="malformed envelope")
            raise RuntimeError("gemini response missing candidate text") from exc
        verdict = self._parse_verdict(text)
        if verdict is None:
            self.rot.advance_after_call()
            self.rot.record(ok=False, model=model, key=key_idx + 1,
                            error="unparseable verdict")
            raise RuntimeError(f"gemini verdict not JSON: {text[:160]}")
        self.rot.advance_after_call()
        self.rot.record(ok=True, model=model, key=key_idx + 1,
                        action=verdict.get("action"), status=verdict.get("status"),
                        conf=verdict.get("confidence"), setup=verdict.get("setup"))
        return verdict

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
