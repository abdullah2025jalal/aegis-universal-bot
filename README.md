# Aegis-Universal

Algorithmic trading system for Bybit USDT-linear perpetuals (BTCUSDT), built to the
Aegis-Universal Master Operating Contract.

```
main.py                     FastAPI web layer, lifespan, direct-execution wiring, exception trapping
config.py                   Typed environment configuration (no secrets in source)
engine/
  gemini_scanner.py         AI decision feed: day-trader-skill playbook via gemini (15-min cadence),
                            5-key rotation with ai_state.json persistence, model fallback chain
  marketsetup.py            MarketSetup + ATR(14) from Bybit venue data
  direct_executor.py        DIRECT AI execution: Limit-IOC capped at the operator $1 rule,
                            ATR/1.5R exit planner, zero-naked-exposure audit
  execution.py              Direct Bybit REST v5: HMAC signing, token bucket, Limit-IOC brackets,
                            post-execution audit (zero naked exposure), failsafe market close
  position_manager.py       15s monitor: Breakeven+ @+1.0R, ATR trail @+1.2R (monotonic), 24h TTL
  reconciler.py             On-boot state recovery from the exchange (master ledger)
  state.py                  Ephemeral SQLite cache (daily aggregates, journal)
services/
  alerter.py                Resend telemetry: boot ticket, error tickets with stack traces
                            (fingerprint dedupe), trade events, 15-minute AI-cycle alerts
tools/
  bootstrap_system.py       Zero-touch deployment: GitHub repo+push, Render service+env+deploy,
                            UptimeRobot keep-alive monitor
```

**Operator mode (2026-11-06):** the AI verdict executes directly — no risk gates,
no cooldowns, no loss limits. The single operator condition: execution never
happens at a price more than $1 away from the AI decision price (Limit-IOC caps;
a drift beyond the cap expires the order with zero exposure).

## Local run

```bash
python -m pip install -r requirements.txt
python main.py          # binds 0.0.0.0:$PORT or 8000
```

## Endpoints

- `GET /healthz/{HEALTH_UUID}` — keep-alive + equity/position/PnL telemetry (200 OK)
- `GET /dashboard` — operator monitor (HTTP Basic Auth; credentials injected at deploy)
- `POST /dashboard/kill` — manual hard halt (entries disabled, critical email)
- `POST /dashboard/resume` — clear circuit breakers after manual review

## Deployment (zero-touch)

Put your manifest in `secrets.env` (gitignored — never pushed):

```
GITHUB_TOKEN=...
RENDER_API_KEY=...
UPTIMEROBOT_API_KEY=...
BYBIT_API_KEY=...
BYBIT_API_SECRET=...
BYBIT_TESTNET=false
RESEND_API_KEY=...
RECEIVER_EMAIL=...
ALERT_FROM_EMAIL=onboarding@resend.dev
```

Then: `python tools/bootstrap_system.py`

The bootstrap resolves the GitHub user, creates the private repo `aegis-universal-bot`,
pushes this codebase, provisions the Render free-tier web service `aegis-universal-live`,
injects all runtime env vars (risk formulary per contract defaults), deploys, polls to
`live`, and registers the 5-minute UptimeRobot keep-alive monitor on
`https://.../healthz/{HEALTH_UUID}`.

## Safety architecture (Never List)

1. Entries are strictly **Limit IOC** with embedded exchange-side SL/TP (`tpslMode=Full`);
   market orders are used **only** to close (failsafe/TTL), never to open.
2. Margin mode verified **ISOLATED**; leverage capped at `MAX_LEVERAGE` (default 5x).
3. Symbol is locked to `BTCUSDT` linear perpetual.
4. 10 fail-fast gates before any order; none may be skipped or reordered.
5. Post-execution audit guarantees no naked position: missing SL -> attach -> unconfirmed
   in 3s -> emergency reduceOnly close + critical alert.
6. Post-trade cooldown, daily loss limit, loss-streak pause, profit lock, and a hard
   equity-preservation floor (kill switch, operator-caused reset only).
7. Every exception, API rejection, data-feed failure, and failsafe trigger is emailed to
   the operator with full stack trace (fingerprint-deduped to 1/15min for repeats).
8. No profit guarantees are ever claimed — this is a risk-managed system, not a
   promise of profit. Trading involves substantial risk.
