"""SQLite database for storing all backtest and scan results.

Tables:
  - scan_results:     Quick WR scans across instruments
  - backtest_results: Full strategy matrix backtests (Rise/Fall, Digits, etc.)
  - digit_analysis:   Digit distribution stats per instrument
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).parent / "logs" / "results.db"


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db():
    conn = _connect()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    with get_db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS scan_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            instrument TEXT NOT NULL,
            contract_type TEXT NOT NULL,      -- 'rise_fall', 'digiteven', 'digitover_4', etc.
            strategy_name TEXT NOT NULL,       -- 'jump_momentum', 'markov1', 'freq_rebal', etc.
            total_ticks INTEGER,
            total_signals INTEGER,
            wins INTEGER,
            losses INTEGER,
            raw_wr REAL,
            filtered_wr REAL,
            breakeven_wr REAL,
            edge_pct REAL,                    -- filtered_wr - breakeven_wr
            payout_rate REAL,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS backtest_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            instrument TEXT NOT NULL,
            contract_type TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            money_mgmt TEXT NOT NULL,         -- 'flat', 'fib2g', 'soros1', etc.
            filter_desc TEXT,                 -- 'nofilter', 'imb>=6', etc.
            cooldown INTEGER,
            starting_balance REAL,
            final_balance REAL,
            max_balance REAL,
            min_balance REAL,
            max_drawdown_pct REAL,
            total_trades INTEGER,
            wins INTEGER,
            losses INTEGER,
            win_rate REAL,
            profit_factor REAL,
            bankrupt INTEGER,                 -- 1 = went to 0
            payout_rate REAL,
            base_stake REAL,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS digit_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            instrument TEXT NOT NULL,
            total_ticks INTEGER,
            pip_decimals INTEGER,
            digit_0_pct REAL, digit_1_pct REAL, digit_2_pct REAL,
            digit_3_pct REAL, digit_4_pct REAL, digit_5_pct REAL,
            digit_6_pct REAL, digit_7_pct REAL, digit_8_pct REAL,
            digit_9_pct REAL,
            chi_squared REAL,
            chi_p_value REAL,
            even_pct REAL,
            max_digit_bias REAL,              -- max deviation from 10%
            max_biased_digit INTEGER,
            autocorr_lag1 REAL,
            notes TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_scan_instr ON scan_results(instrument, contract_type);
        CREATE INDEX IF NOT EXISTS idx_bt_instr ON backtest_results(instrument, contract_type);
        CREATE INDEX IF NOT EXISTS idx_da_instr ON digit_analysis(instrument);
        """)


def save_scan(instrument: str, contract_type: str, strategy_name: str,
              total_ticks: int, total_signals: int, wins: int, losses: int,
              raw_wr: float, filtered_wr: float, breakeven_wr: float,
              payout_rate: float, notes: str = "") -> int:
    with get_db() as conn:
        cur = conn.execute(
            """INSERT INTO scan_results
               (timestamp, instrument, contract_type, strategy_name,
                total_ticks, total_signals, wins, losses,
                raw_wr, filtered_wr, breakeven_wr, edge_pct, payout_rate, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now(UTC).isoformat(), instrument, contract_type, strategy_name,
             total_ticks, total_signals, wins, losses,
             raw_wr, filtered_wr, breakeven_wr,
             filtered_wr - breakeven_wr if filtered_wr and breakeven_wr else None,
             payout_rate, notes),
        )
        return cur.lastrowid


def save_backtest(instrument: str, contract_type: str, strategy_name: str,
                  money_mgmt: str, filter_desc: str, cooldown: int,
                  starting_balance: float, final_balance: float,
                  max_balance: float, min_balance: float,
                  max_drawdown_pct: float, total_trades: int,
                  wins: int, losses: int, win_rate: float,
                  profit_factor: float, bankrupt: bool,
                  payout_rate: float, base_stake: float,
                  notes: str = "") -> int:
    with get_db() as conn:
        cur = conn.execute(
            """INSERT INTO backtest_results
               (timestamp, instrument, contract_type, strategy_name,
                money_mgmt, filter_desc, cooldown,
                starting_balance, final_balance, max_balance, min_balance,
                max_drawdown_pct, total_trades, wins, losses, win_rate,
                profit_factor, bankrupt, payout_rate, base_stake, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now(UTC).isoformat(), instrument, contract_type, strategy_name,
             money_mgmt, filter_desc, cooldown,
             starting_balance, final_balance, max_balance, min_balance,
             max_drawdown_pct, total_trades, wins, losses, win_rate,
             profit_factor, int(bankrupt), payout_rate, base_stake, notes),
        )
        return cur.lastrowid


def save_digit_analysis(instrument: str, total_ticks: int, pip_decimals: int,
                        digit_pcts: list[float], chi_sq: float, chi_p: float,
                        even_pct: float, max_bias: float, max_biased_digit: int,
                        autocorr_lag1: float, notes: str = "") -> int:
    with get_db() as conn:
        cur = conn.execute(
            """INSERT INTO digit_analysis
               (timestamp, instrument, total_ticks, pip_decimals,
                digit_0_pct, digit_1_pct, digit_2_pct, digit_3_pct, digit_4_pct,
                digit_5_pct, digit_6_pct, digit_7_pct, digit_8_pct, digit_9_pct,
                chi_squared, chi_p_value, even_pct, max_digit_bias,
                max_biased_digit, autocorr_lag1, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now(UTC).isoformat(), instrument, total_ticks, pip_decimals,
             *digit_pcts, chi_sq, chi_p, even_pct, max_bias, max_biased_digit,
             autocorr_lag1, notes),
        )
        return cur.lastrowid


def query(sql: str, params: tuple = ()) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
