"""Certify glm-5.3-flash (OpenRouter) against the DEPLOYED verbatim skill
prompt: mirrored bull/bear scenes through the EXACT _call_openrouter shape
(request #1: response_format json_object + reasoning effort high excluded).
Two requests total; keys come from env OUTR_PROBE_KEYS (A,B / B,A order)."""
import asyncio
import json
import os
import sys

sys.path.insert(0, ".")
import httpx  # noqa: E402
from engine.gemini_scanner import _SYSTEM_PROMPT  # noqa: E402

MODEL = "z-ai/glm-5.3-flash"

SCENE_BULL = {
    "utc_now": "2026-11-07 12:02:10", "symbol": "BTCUSDT", "closed_bar_age_s": 10.0,
    "ask": 76350.0, "bid": 76349.9, "spread_pct": 0.0001,
    "or_utc": {"ready": True, "bars": 3, "high": 75900.0, "low": 75750.0,
               "close_above_high": False, "close_below_low": False},
    "mf_15m": {"rsi14": 58.0, "ema20": 76150.0, "ema50": 76050.0,
               "atr14": 400.0, "atr_pct": 0.524,
               "prev20_high": 76280.0, "prev20_low": 75700.0,
               "closes_last_20": [75920.0, 75930.0, 75940.0, 75950.0, 75960.0,
                                  75970.0, 75980.0, 75990.0, 76000.0, 76010.0,
                                  76020.0, 76040.0, 76060.0, 76080.0, 76100.0,
                                  76120.0, 76140.0, 76170.0, 76200.0, 76360.0]},
    "mf_1h": {"rsi14": 55.0, "ema50": 75800.0, "close_vs_ema50_pct": 0.733},
    "session_marks": {"or_consumed": False},
}
SCENE_BEAR = {
    "utc_now": "2026-11-07 12:02:10", "symbol": "BTCUSDT", "closed_bar_age_s": 10.0,
    "ask": 76140.1, "bid": 76140.0, "spread_pct": 0.0001,
    "or_utc": {"ready": True, "bars": 3, "high": 76250.0, "low": 76000.0,
               "close_above_high": False, "close_below_low": False},
    "mf_15m": {"rsi14": 41.0, "ema20": 76300.0, "ema50": 76400.0,
               "atr14": 400.0, "atr_pct": 0.526,
               "prev20_high": 76700.0, "prev20_low": 76240.0,
               "closes_last_20": [76700.0, 76680.0, 76660.0, 76640.0, 76620.0,
                                  76600.0, 76580.0, 76560.0, 76540.0, 76520.0,
                                  76500.0, 76480.0, 76460.0, 76440.0, 76420.0,
                                  76400.0, 76380.0, 76360.0, 76340.0, 76140.0]},
    "mf_1h": {"rsi14": 45.0, "ema50": 76800.0, "close_vs_ema50_pct": -0.861},
    "funding": {"rate": 0.0001, "next_in_min": 45},
    "session_marks": {"or_consumed": False},
}
SCENE_BULL["funding"] = {"rate": 0.0001, "next_in_min": 45}


async def call(key: str, scene: dict, tag: str, shape: str = "json") -> None:
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://aegis-universal.local",
        "X-Title": "Aegis-Universal",
    }
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(scene, separators=(",", ":"))},
        ],
        "temperature": 0.2, "max_tokens": 8000,
    }
    if shape == "json":
        payload["response_format"] = {"type": "json_object"}
        payload["reasoning"] = {"effort": "high", "exclude": True}
    async with httpx.AsyncClient(timeout=75) as cli:
        t0 = asyncio.get_event_loop().time()
        r = await cli.post(url, json=payload, headers=headers)
        dt = asyncio.get_event_loop().time() - t0
    if r.status_code != 200:
        print(f"{tag}: HTTP {r.status_code} {r.text[:160]}")
        return
    body = r.json()
    msg = (body.get("choices") or [{}])[0].get("message") or {}
    text = str(msg.get("content") or "").strip()
    usage = body.get("usage") or {}
    print(f"{tag}: HTTP 200 in {dt * 1000:.0f} ms  usage in={usage.get('prompt_tokens')} "
          f"out={usage.get('completion_tokens')} cost={usage.get('cost')}")
    try:
        v = json.loads(text)
    except ValueError:
        print(f"{tag}: RAW {text[:220]!r}")
        return
    print(f"{tag}: action={v.get('action')} status={v.get('status')} "
          f"setup={v.get('setup')} conf={v.get('confidence')} "
          f"valid={v.get('valid_minutes')}m trend={v.get('trend_1h')}")
    print(f"{tag}: reason={str(v.get('reason'))[:150]!r}")


async def main() -> None:
    keys = [k.strip() for k in os.environ.get("OUTR_PROBE_KEYS", "").split(",") if k.strip()]
    if not keys:
        raise SystemExit("set OUTR_PROBE_KEYS env (keyA,keyB)")
    if os.environ.get("OUTR_BEAR_ONLY"):
        shape = os.environ.get("OUTR_SHAPE", "plain")
        for i in range(2):
            await call(keys[0], SCENE_BEAR, f"BEAR.{i + 1}", shape=shape)
            await asyncio.sleep(4)
        return
    await call(keys[1], SCENE_BULL, "BULL")   # key#2 first — key#1 is the engine's wave key
    await asyncio.sleep(5)
    for i in range(2):
        await call(keys[0], SCENE_BEAR, f"BEAR.{i + 1}",
                   shape=os.environ.get("OUTR_SHAPE", "json").strip().lower() or "plain")
        await asyncio.sleep(4)


asyncio.run(main())
