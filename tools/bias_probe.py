"""Bias probe FINAL: bull/bear mirrored scenes across ALL 5 API keys, shapes
gs->json with pauses (engine-faithful). Designed to run in a background loop;
appends one line per attempt-set to tools/bias_results.txt for later proof."""
import asyncio
import json
import re
import sys
import time

sys.path.insert(0, ".")
import httpx  # noqa: E402
from config import SETTINGS                 # noqa: E402
from engine.gemini_scanner import _SYSTEM_PROMPT, _GEMINI_URL  # noqa: E402

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
    "session_marks": {"or_consumed": False},
}


def extract_verdict(text: str) -> dict:
    t = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    try:
        return json.loads(t)
    except ValueError:
        m = re.search(r"\{.*\}", t, flags=re.S)
        if m:
            try:
                return json.loads(m.group(0))
            except ValueError:
                pass
    return {}


async def probe(scene: dict, tag: str) -> str:
    url = f"{_GEMINI_URL}/{SETTINGS.gemini_model}:generateContent"
    gen = {"temperature": 0.2, "maxOutputTokens": 32768,
           "thinkingConfig": {"thinkingLevel": "high"}}
    async with httpx.AsyncClient(timeout=130) as cli:
        for key_i, key in enumerate(SETTINGS.gemini_api_keys):
            headers = {"x-goog-api-key": key, "Content-Type": "application/json"}
            for shape in ("gs", "json"):
                cfg = dict(gen)
                extra: dict = {}
                if shape == "json":
                    cfg["responseMimeType"] = "application/json"
                else:
                    extra["tools"] = [{"google_search": {}}]
                payload = {
                    "contents": [{"role": "user",
                                  "parts": [{"text": json.dumps(scene, separators=(",", ":"))}]}],
                    "systemInstruction": {"parts": [{"text": _SYSTEM_PROMPT}]},
                    "generationConfig": cfg,
                    **extra,
                }
                try:
                    r = await cli.post(url, json=payload, headers=headers)
                except Exception as exc:  # noqa: BLE001
                    continue
                if r.status_code != 200:
                    continue
                text = "\n".join(p.get("text", "") for p in
                                 r.json()["candidates"][0]["content"]["parts"]
                                 if isinstance(p, dict) and not p.get("thought")).strip()
                v = extract_verdict(text)
                if v.get("action"):
                    return (f"{tag} SOLVED key#{key_i + 1}/5 shape={shape} -> "
                            f"action={v.get('action')} status={v.get('status')} "
                            f"setup={v.get('setup')} conf={v.get('confidence')} "
                            f"reason={str(v.get('reason'))[:100]!r}")
    return f"{tag} UNSOLVED (all keys/shapes busy)"


async def main() -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime())
    line_b = f"[{ts}] " + await probe(SCENE_BULL, "BULL")
    await asyncio.sleep(15)
    line_s = f"[{ts}] " + await probe(SCENE_BEAR, "BEAR")
    with open("tools/bias_results.txt", "a", encoding="utf-8") as fh:
        fh.write(line_b + "\n" + line_s + "\n")
    print(line_b)
    print(line_s)


asyncio.run(main())
