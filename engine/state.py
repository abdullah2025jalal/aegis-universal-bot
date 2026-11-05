"""
Aegis-Universal :: Ephemeral SQLite Performance Cache
=====================================================
Contract Section 8: "The exchange is the master ledger; local SQLite is an
ephemeral performance cache." Nothing here is authoritative for custody —
positions and equity always re-verified against Bybit REST.

Keys persisted: baseline_equity, kill_switch, daily aggregates, cooldown
timestamps, loss streaks, pause windows, trade journal.
Disk is ephemeral on Render free tier by design (resync happens on boot).
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Dict, Optional, Tuple

_SCHEMA = """
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_open INTEGER NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    entry REAL NOT NULL,
    sl REAL NOT NULL,
    tp REAL NOT NULL,
    risk_usd REAL NOT NULL,
    net_rr REAL,
    order_link_id TEXT,
    ts_close INTEGER,
    realized_pnl REAL,
    exit_reason TEXT
);
"""


class StateStore:
    def __init__(self, path: str):
        self.path = path
        self._ensure_dir()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.executescript(_SCHEMA)
        self._db.commit()

    def _ensure_dir(self) -> None:
        folder = os.path.dirname(os.path.abspath(self.path))
        if folder:
            os.makedirs(folder, exist_ok=True)

    # ---------------- kv store ----------------

    def get(self, key: str, default: Any = None) -> Any:
        cur = self._db.execute("SELECT v FROM kv WHERE k = ?", (key,))
        row = cur.fetchone()
        if row is None:
            return default
        try:
            return json.loads(row[0])
        except (json.JSONDecodeError, ValueError):
            return default

    def set(self, key: str, value: Any) -> None:
        self._db.execute(
            "INSERT INTO kv (k, v) VALUES (?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            (key, json.dumps(value)))
        self._db.commit()

    # ---------------- lifecycle semantics ----------------

    def utc_day(self) -> str:
        return time.strftime("%Y-%m-%d", time.gmtime())

    def ensure_utc_day(self) -> Tuple[str, bool]:
        """Rolls daily aggregates at UTC 00:00. Returns (day, rolled)."""
        today = self.utc_day()
        if self.get("day") != today:
            self.set("day", today)
            self.set("daily_realized_pnl", 0.0)
            self.set("daily_wins", 0)
            self.set("daily_losses", 0)
            self.set("cyborg_digest_sent", False)
            return today, True
        return today, False

    # baseline equity (preservation floor denominator)
    def baseline_equity(self) -> Optional[float]:
        v = self.get("baseline_equity")
        return float(v) if v else None

    def set_baseline_equity(self, equity: float) -> None:
        self.set("baseline_equity", float(equity))

    # kill switch
    def kill_switch(self) -> bool:
        return bool(self.get("kill_switch", False))

    def set_kill_switch(self, value: bool, reason: str = "") -> None:
        self.set("kill_switch", bool(value))
        if value and reason:
            self.set("kill_switch_reason", reason)
        if not value:
            self.set("kill_switch_reason", "")

    # cooldown / pause windows
    def last_trade_closed_ts(self) -> float:
        return float(self.get("last_trade_closed_ts", 0.0) or 0.0)

    def set_last_trade_closed_ts(self, ts: float) -> None:
        self.set("last_trade_closed_ts", float(ts))

    def pause_until(self) -> float:
        return float(self.get("pause_until_ts", 0.0) or 0.0)

    def set_pause_until(self, ts: float, reason: str = "") -> None:
        self.set("pause_until_ts", float(ts))
        self.set("pause_reason", reason)

    def consecutive_losses(self) -> int:
        return int(self.get("consecutive_losses", 0) or 0)

    def set_consecutive_losses(self, n: int) -> None:
        self.set("consecutive_losses", int(n))

    def profit_lock_day(self) -> str:
        return str(self.get("profit_lock_day", "") or "")

    def set_profit_lock_day(self, day: str) -> None:
        self.set("profit_lock_day", day)

    def daily_realized_pnl(self) -> float:
        return float(self.get("daily_realized_pnl", 0.0) or 0.0)

    def add_daily_realized_pnl(self, pnl: float) -> None:
        self.set("daily_realized_pnl", self.daily_realized_pnl() + float(pnl))

    def count_daily_result(self, is_win: bool) -> None:
        key = "daily_wins" if is_win else "daily_losses"
        self.set(key, int(self.get(key, 0) or 0) + 1)

    def set_digest_sent(self, day: str) -> None:
        self.set("cyborg_digest_sent", True)
        self.set("digest_sent_day", day)

    # ---------------- trade journal ----------------

    def journal_open(self, ts_open: int, side: str, qty: float, entry: float,
                     sl: float, tp: float, risk_usd: float, net_rr: float,
                     order_link_id: str) -> int:
        cur = self._db.execute(
            "INSERT INTO trades (ts_open, side, qty, entry, sl, tp, risk_usd, net_rr, order_link_id) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (int(ts_open), side, float(qty), float(entry), float(sl), float(tp),
             float(risk_usd), float(net_rr), order_link_id))
        self._db.commit()
        return int(cur.lastrowid)

    def journal_close(self, journal_id: int, ts_close: int, realized_pnl: float,
                      exit_reason: str) -> None:
        self._db.execute(
            "UPDATE trades SET ts_close = ?, realized_pnl = ?, exit_reason = ? WHERE id = ?",
            (int(ts_close), float(realized_pnl), exit_reason, journal_id))
        self._db.commit()

    def journal_day_stats(self, day_start_ts: int) -> Dict[str, Any]:
        cur = self._db.execute(
            "SELECT COUNT(*), "
            "SUM(CASE WHEN realized_pnl >= 0 THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN realized_pnl <  0 THEN 1 ELSE 0 END), "
            "COALESCE(SUM(realized_pnl), 0) "
            "FROM trades WHERE ts_close IS NOT NULL AND ts_close >= ?",
            (day_start_ts,))
        row = cur.fetchone() or (0, 0, 0, 0.0)
        closed, wins, losses, pnl = int(row[0] or 0), int(row[1] or 0), int(row[2] or 0), float(row[3] or 0)
        win_rate = (wins / closed * 100.0) if closed else 0.0
        return {"closed": closed, "wins": wins, "losses": losses,
                "pnl": pnl, "win_rate": win_rate}

    def close(self) -> None:
        try:
            self._db.close()
        except Exception:
            pass


class JsonStateStore:
    """
    File-backed state for ephemeral CI containers (GitHub Actions hosting mode).
    Same public surface as StateStore; the trade journal is exchange-sourced
    (no local ledger) and daily aggregates come straight from Bybit REST in the
    risk gates. The file is committed back to the repository by the workflow
    after each cycle, so kill-switch / pause / baseline windows survive restarts.
    """

    def __init__(self, path: str):
        self.path = path
        self._data: Dict[str, Any] = {}
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    self._data = json.load(fh)
            except (json.JSONDecodeError, OSError):
                self._data = {}

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value

    def save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh, indent=2, sort_keys=True)
            fh.write("\n")

    # ---------------- lifecycle semantics (mirror StateStore) ----------------

    def utc_day(self) -> str:
        return time.strftime("%Y-%m-%d", time.gmtime())

    def ensure_utc_day(self) -> Tuple[str, bool]:
        today = self.utc_day()
        if self.get("day") != today:
            self.set("day", today)
            return today, True
        return today, False

    def baseline_equity(self) -> Optional[float]:
        v = self.get("baseline_equity")
        return float(v) if v else None

    def set_baseline_equity(self, equity: float) -> None:
        self.set("baseline_equity", float(equity))

    def kill_switch(self) -> bool:
        return bool(self.get("kill_switch", False))

    def set_kill_switch(self, value: bool, reason: str = "") -> None:
        self.set("kill_switch", bool(value))
        self.set("kill_switch_reason", reason if value and reason else "")

    def last_trade_closed_ts(self) -> float:
        return float(self.get("last_trade_closed_ts", 0.0) or 0.0)

    def set_last_trade_closed_ts(self, ts: float) -> None:
        self.set("last_trade_closed_ts", float(ts))

    def pause_until(self) -> float:
        return float(self.get("pause_until_ts", 0.0) or 0.0)

    def set_pause_until(self, ts: float, reason: str = "") -> None:
        self.set("pause_until_ts", float(ts))
        self.set("pause_reason", reason)

    def consecutive_losses(self) -> int:
        return int(self.get("consecutive_losses", 0) or 0)

    def set_consecutive_losses(self, n: int) -> None:
        self.set("consecutive_losses", int(n))

    def profit_lock_day(self) -> str:
        return str(self.get("profit_lock_day", "") or "")

    def set_profit_lock_day(self, day: str) -> None:
        self.set("profit_lock_day", day)

    def daily_realized_pnl(self) -> float:
        return float(self.get("daily_realized_pnl", 0.0) or 0.0)

    def add_daily_realized_pnl(self, pnl: float) -> None:
        self.set("daily_realized_pnl", self.daily_realized_pnl() + float(pnl))

    def count_daily_result(self, is_win: bool) -> None:
        k = "daily_wins" if is_win else "daily_losses"
        self.set(k, int(self.get(k, 0) or 0) + 1)

    def set_digest_sent(self, day: str) -> None:
        self.set("digest_sent", True)
        self.set("digest_sent_day", day)

    # ---------------- journal is exchange-sourced in CI mode ----------------

    def journal_open(self, *args: Any, **kwargs: Any) -> Optional[int]:
        return None

    def journal_close(self, *args: Any, **kwargs: Any) -> None:
        return None

    def journal_day_stats(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return {"closed": 0, "wins": 0, "losses": 0, "pnl": 0.0, "win_rate": 0.0}

    def close(self) -> None:
        return None
