"""
Aegis-Universal :: Typed Environment Configuration
==================================================
All runtime configuration is sourced from environment variables
(injected by Render Cloud / the zero-touch bootstrapper). No secrets are
ever hardcoded in application source (Absolute Prohibition #8).

Every quantitative parameter defaults to the Master Operating Contract
values and may be overridden per-environment.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field


def _env_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env_str(name, str(default))
    return raw.lower() in {"1", "true", "yes", "on", "enabled"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env_str(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env_str(name, str(default)))
    except (TypeError, ValueError):
        return default


@dataclass
class Settings:
    # ------------------------------------------------------------------
    # Identity / Web layer
    # ------------------------------------------------------------------
    app_name: str = "Aegis-Universal"
    health_uuid: str = field(default_factory=lambda: uuid.uuid4().hex)
    dashboard_username: str = "operator"
    dashboard_password: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    # ------------------------------------------------------------------
    # Bybit Execution Venue (USDT Linear Perpetuals)
    # ------------------------------------------------------------------
    bybit_api_key: str = ""
    bybit_api_secret: str = ""
    bybit_testnet: bool = False
    bybit_demo: bool = False               # Demo Trade venue (api-demo.bybit.com) — documented deployer deviation
    bybit_symbol: str = "BTCUSDT"
    bybit_category: str = "linear"          # NEVER trade anything else (Never List #3)
    bybit_recv_window_ms: int = 10000

    # ------------------------------------------------------------------
    # Resend Telemetry / Lifecycle Alerting
    # ------------------------------------------------------------------
    resend_api_key: str = ""
    receiver_email: str = ""
    alert_from_email: str = "onboarding@resend.dev"

    # ------------------------------------------------------------------
    # Core Quantitative Risk Formulary (Contract Section 3 / 5 defaults)
    # ------------------------------------------------------------------
    risk_per_trade_pct: float = 0.015                     # 1.5% of equity
    max_leverage: float = 5.0                             # Never exceed (Never List #4)
    max_margin_alloc_pct: float = 0.35                    # 35% margin allocation ceiling
    target_gross_rr: float = 1.50                         # Gross 1.5R TP
    daily_loss_limit_pct: float = 0.05                    # 5% daily loss -> 24h halt
    daily_profit_lock_pct: float = 0.08                   # 8% daily profit -> lock until UTC 00:00
    equity_preservation_floor_pct: float = 0.80           # Hard preservation floor (80% of baseline)
    cooldown_minutes: int = 10                            # Post-trade cooldown
    max_holding_hours: float = 24.0                       # TTL decay exit

    # ------------------------------------------------------------------
    # Gate constants (deterministic, fail-fast risk pipeline)
    # ------------------------------------------------------------------
    taker_fee_rate: float = 0.00055                       # 0.055% per side (taker)
    round_trip_fee_rate: float = 0.0011                   # 0.055% * 2
    micro_account_max_risk_pct: float = 0.025             # Min-lot accommodation ceiling (2.5%)
    min_sl_distance_pct: float = 0.005                    # 0.50% noise floor
    max_sl_distance_pct: float = 0.020                    # 2.00% max adverse allowance
    max_spread_pct: float = 0.0008                        # G4: 0.08% orderbook spread
    max_chase_pct: float = 0.006                          # G5: 0.60% chase guard
    max_atr_vs_entry_pct: float = 0.025                   # G6: ATR <= 2.5% of entry
    min_net_rr: float = 1.30                              # G7: Net expectancy floor
    adverse_funding_threshold: float = 0.0003             # G8: 0.03% adverse funding
    funding_lookahead_minutes: float = 45.0               # G8: within 45m of funding event
    max_clock_drift_ms: int = 1000                        # G9: < 1000ms Bybit server sync

    # Position management rhythm (Contract Section 7)
    breakeven_trigger_r: float = 1.0                      # +1.0R -> Breakeven+ fee lock
    breakeven_offset_pct: float = 0.0012                  # +0.12% lock offset
    trail_trigger_r: float = 1.2                          # +1.2R -> ATR trailing stop
    trail_atr_multiplier: float = 1.0                     # Trail distance = 1.0 * ATR
    emergency_sl_pct: float = 0.01                        # Reconciler conservative fallback SL (1.0%)

    # ------------------------------------------------------------------
    # Daemon Topology
    # ------------------------------------------------------------------
    scan_interval_seconds: int = 60                       # 1-minute candle clock
    position_poll_seconds: int = 15                       # 15s monitor loop
    clock_resync_seconds: int = 900                       # Periodic server-time resync

    # ------------------------------------------------------------------
    # Error telemetry anti-spam
    # ------------------------------------------------------------------
    error_dedupe_window_seconds: int = 900                # 15 minutes
    state_db_path: str = "aegis_state.sqlite3"            # Ephemeral performance cache

    @property
    def bybit_base_url(self) -> str:
        if self.bybit_demo:
            return "https://api-demo.bybit.com"
        return "https://api-testnet.bybit.com" if self.bybit_testnet else "https://api.bybit.com"

    @classmethod
    def from_env(cls) -> "Settings":
        s = cls(
            health_uuid=uuid.uuid4().hex if not _env_str("HEALTH_UUID") else _env_str("HEALTH_UUID"),
            dashboard_username=_env_str("DASHBOARD_USERNAME", "operator") or "operator",
            dashboard_password=_env_str("DASHBOARD_PASSWORD") or uuid.uuid4().hex[:12],
            bybit_api_key=_env_str("BYBIT_API_KEY"),
            bybit_api_secret=_env_str("BYBIT_API_SECRET"),
            bybit_testnet=_env_bool("BYBIT_TESTNET", False),
            bybit_demo=_env_bool("BYBIT_DEMO", False),
            bybit_symbol=_env_str("BYBIT_SYMBOL", "BTCUSDT") or "BTCUSDT",
            bybit_recv_window_ms=_env_int("BYBIT_RECV_WINDOW_MS", 10000),
            resend_api_key=_env_str("RESEND_API_KEY"),
            receiver_email=_env_str("RECEIVER_EMAIL"),
            alert_from_email=_env_str("ALERT_FROM_EMAIL", "onboarding@resend.dev") or "onboarding@resend.dev",
            risk_per_trade_pct=_env_float("RISK_PER_TRADE_PCT", 0.015),
            max_leverage=_env_float("MAX_LEVERAGE", 5.0),
            max_margin_alloc_pct=_env_float("MAX_MARGIN_ALLOC_PCT", 0.35),
            target_gross_rr=_env_float("TARGET_GROSS_RR", 1.50),
            daily_loss_limit_pct=_env_float("DAILY_LOSS_LIMIT_PCT", 0.05),
            daily_profit_lock_pct=_env_float("DAILY_PROFIT_LOCK_PCT", 0.08),
            equity_preservation_floor_pct=_env_float("EQUITY_PRESERVATION_FLOOR_PCT", 0.80),
            cooldown_minutes=_env_int("COOLDOWN_MINUTES", 10),
            max_holding_hours=_env_float("MAX_HOLDING_HOURS", 24.0),
            kill_switch_active=_env_bool("KILL_SWITCH_ACTIVE", False),
            scan_interval_seconds=_env_int("SCAN_INTERVAL_SECONDS", 60),
            position_poll_seconds=_env_int("POSITION_POLL_SECONDS", 15),
            state_db_path=_env_str("STATE_DB_PATH", "aegis_state.sqlite3") or "aegis_state.sqlite3",
            error_dedupe_window_seconds=_env_int("ERROR_DEDUPE_WINDOW_SECONDS", 900),
        )
        return s

    kill_switch_active: bool = False


SETTINGS = Settings.from_env()
