"""Local unit check for the OpenRouter 5-hour wave rotation state."""
import os
import sys
import tempfile
import time

sys.path.insert(0, ".")
from engine.gemini_scanner import _RotationState  # noqa: E402


class S:
    ai_state_path = os.path.join(tempfile.gettempdir(), "wave_test_ai_state.json")
    ai_platform = "openrouter"
    openrouter_api_keys = ["KEY_A", "KEY_B"]
    openrouter_model = "z-ai/glm-5.3-flash"
    gemini_api_keys = []
    gemini_model = "gemini-3.8-flash"
    ai_calls_per_key = 8
    ai_key_window_seconds = 5 * 3600


if os.path.exists(S.ai_state_path):
    os.remove(S.ai_state_path)

rs = _RotationState(S())
seq = [rs.begin_window_call() for _ in range(3)]
print("first-wave keys:", seq, "counter:", rs.d["counter_total"],
      "or_calls:", rs.d["or_calls"], "model:", rs.d["model"])
assert seq == [0, 0, 0], "key#1 must serve the whole first wave"
assert rs.d["model"] == "z-ai/glm-5.3-flash"

# simulate the wall clock crossing the 5-hour boundary
rs.d["or_window_started"] = time.time() - (5 * 3600 + 30)
nxt = rs.begin_window_call()
print("after 5h boundary:", nxt, "counter:", rs.d["counter_total"],
      "or_calls:", rs.d["or_calls"], "window:", round(rs.d["or_window_started"], 1))
assert nxt == 1, "after the boundary the SECOND key must own the feed"

# simulate another boundary -> back to key#1 forever
rs.d["or_window_started"] = time.time() - (5 * 3600 + 1)
nxt2 = rs.begin_window_call()
print("after another 5h:", nxt2)
assert nxt2 == 0, "rotation must return to key#1"

# persistence: reopen and continue the SAME wave
rs2 = _RotationState(S())
print("reload or_key_index:", rs2.d["or_key_index"],
      "window_started:", round(rs2.d["or_window_started"], 1),
      "counter:", rs2.d["counter_total"])
assert rs2.d["or_key_index"] == 0
assert abs(rs2.d["or_window_started"] - rs.d["or_window_started"]) < 2, \
    "wave schedule must persist across reboots"

# partial window must NOT flip (reboot continuity)
rs2.d["or_window_started"] = time.time() - (2 * 3600)
same = rs2.begin_window_call()
assert same == 0, "a partially elapsed window must not flip the key"

# legacy path still works (gemini platform state untouched in behavior)
class Sg(S):
    ai_platform = "gemini"
    gemini_api_keys = ["G1", "G2"]
    openrouter_api_keys = []

if os.path.exists(Sg.ai_state_path):
    os.remove(Sg.ai_state_path)
rsg = _RotationState(Sg())
idx = rsg.begin_call()
rsg.advance_after_call()
print("legacy gemini path ok:", idx, rsg.d["calls_current"], rsg.d["model"])
assert idx == 0 and rsg.d["calls_current"] == 1

print("WAVE_STATE_OK")
os.remove(S.ai_state_path)
