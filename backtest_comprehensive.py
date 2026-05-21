"""Comprehensive JD50 Rise/Fall Strategy Matrix Backtest.

Tests ALL strategy combos (flat, gale, soros, and combinations) across
multiple filter levels and cooldowns, with time-of-day analysis and
time-to-double calculations.

Usage:
    python backtest_comprehensive.py [--ticks data/ticks_JD50_max.csv]
"""
from __future__ import annotations

import argparse
import csv
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from strategy import (
    JumpMomentumConfig,
    calculate_tick_indicators,
    generate_jump_momentum_signal,
)


# ── Constants ──────────────────────────────────────────────────────────
FIB_SEQUENCE = [1, 1, 2, 3, 5, 8, 13, 21]
BASE_STAKE = 0.35
PAYOUT_RATE = 0.953
STARTING_BALANCE = 50.0
BALANCE_FLOOR = 0.35
INDICATOR_WINDOW = 300
SIGNAL_MIN_VOTES = 7
SIGNAL_MIN_CONFIDENCE = 0.60
LOOKAHEAD = 5  # ticks to determine rise/fall outcome


# ── Strategy Definitions ──────────────────────────────────────────────

@dataclass
class StrategyConfig:
    """Defines a specific strategy combination."""
    name: str
    use_gale: bool = False
    gale_mode: str = "fibonacci"  # "fibonacci" or "classic"
    max_gales: int = 2
    use_soros: bool = False
    soros_max_steps: int = 1
    soros_profit_factor: float = 1.0
    soros_decay: bool = True  # decay reinvestment per step
    cooldown: int = 3
    quality_gate: bool = False
    qg_imbalance: float = 6.0

    @property
    def label(self) -> str:
        parts = []
        if self.use_soros:
            parts.append(f"soros{self.soros_max_steps}")
        if self.use_gale:
            if self.gale_mode == "fibonacci":
                parts.append(f"fib{self.max_gales}g")
            else:
                parts.append(f"classic{self.max_gales}g")
        if not parts:
            parts.append("flat")
        mode = "+".join(parts)
        gate = f"imb>={int(self.qg_imbalance)}" if self.quality_gate else "nofilter"
        return f"{mode} | {gate} | cd={self.cooldown}"


@dataclass
class SimState:
    """Mutable simulation state."""
    balance: float = STARTING_BALANCE
    gale_step: int = 0
    gale_locked_dir: str | None = None
    soros_step: int = 0
    soros_profit: float = 0.0
    gale_accum_loss: float = 0.0
    gale_base_stake: float = 0.0
    last_trade_idx: int = -100
    cooldown_remaining: int = 0
    peak_balance: float = STARTING_BALANCE
    max_drawdown: float = 0.0
    trades: list = field(default_factory=list)
    # Time-of-day tracking
    hourly_wins: dict = field(default_factory=lambda: {h: 0 for h in range(24)})
    hourly_losses: dict = field(default_factory=lambda: {h: 0 for h in range(24)})
    hourly_pnl: dict = field(default_factory=lambda: {h: 0.0 for h in range(24)})
    # Ruin tracking
    ruined: bool = False
    ruin_trade_num: int = 0


def get_stake(cfg: StrategyConfig, state: SimState) -> float:
    """Calculate stake for current state, matching risk_manager.py logic."""
    raw_stake = BASE_STAKE
    in_gale = cfg.use_gale and state.gale_step > 0

    # Soros: reinvest profit from clean (G0) wins
    if cfg.use_soros and not in_gale and 0 < state.soros_step <= cfg.soros_max_steps and state.soros_profit > 0:
        raw_stake = raw_stake + state.soros_profit

    # Gale recovery stake
    if cfg.use_gale and state.gale_step > 0:
        if cfg.gale_mode == "fibonacci":
            fib_idx = min(state.gale_step, len(FIB_SEQUENCE) - 1)
            raw_stake = BASE_STAKE * FIB_SEQUENCE[fib_idx]
        else:
            # Classic: accumulated_loss / payout + base
            raw_stake = state.gale_accum_loss / PAYOUT_RATE + state.gale_base_stake

    # Balance cap
    stake = min(raw_stake, state.balance)
    if stake < BASE_STAKE * 0.5:  # min viable stake
        return 0.0
    return round(stake, 2)


def update_state_after_trade(cfg: StrategyConfig, state: SimState, won: bool, stake: float, profit: float, hour: int):
    """Update simulation state after a trade, matching risk_manager.py logic."""
    state.balance += profit

    # Track drawdown
    state.peak_balance = max(state.peak_balance, state.balance)
    dd = state.peak_balance - state.balance
    state.max_drawdown = max(state.max_drawdown, dd)

    # Track time-of-day
    if won:
        state.hourly_wins[hour] += 1
    else:
        state.hourly_losses[hour] += 1
    state.hourly_pnl[hour] += profit

    was_gale_win = cfg.use_gale and state.gale_step > 0 and won

    if won:
        if cfg.use_gale and state.gale_step > 0:
            # Gale win: subtract profit from accumulated loss
            state.gale_accum_loss -= profit
            if state.gale_accum_loss <= 0:
                state.gale_step = 0
                state.gale_accum_loss = 0.0
                state.gale_base_stake = 0.0
            elif state.gale_step >= cfg.max_gales:
                # Max gales reached with residual
                state.gale_step = 0
                state.gale_accum_loss = 0.0
                state.gale_base_stake = 0.0
            # else: partial recovery, keep going
        else:
            state.gale_step = 0
            state.gale_accum_loss = 0.0
            state.gale_base_stake = 0.0

        # Soros: only on clean (G0) wins, not gale recoveries
        if cfg.use_soros and cfg.soros_max_steps > 0 and not was_gale_win:
            if state.soros_step < cfg.soros_max_steps:
                state.soros_step += 1
                if cfg.soros_decay:
                    decay = max(0.5, 1.0 - (state.soros_step - 1) * 0.25)
                else:
                    decay = 1.0
                reinvest = round(profit * cfg.soros_profit_factor * decay, 2)
                state.soros_profit = round(state.soros_profit + reinvest, 2)
            else:
                state.soros_step = 0
                state.soros_profit = 0.0

        state.gale_locked_dir = None
    else:
        # LOSS
        state.soros_step = 0
        state.soros_profit = 0.0

        if cfg.use_gale:
            if state.gale_step == 0:
                state.gale_base_stake = BASE_STAKE
                state.gale_accum_loss = BASE_STAKE
            else:
                state.gale_accum_loss += stake

            effective_max = cfg.max_gales
            if cfg.gale_mode == "fibonacci":
                effective_max = min(effective_max, len(FIB_SEQUENCE) - 1)

            if state.gale_step >= effective_max:
                # Exhausted
                state.gale_step = 0
                state.gale_accum_loss = 0.0
                state.gale_base_stake = 0.0
                state.cooldown_remaining = 30  # cooldown after exhaustion
            else:
                state.gale_step += 1

    # Balance floor check
    if state.balance < BALANCE_FLOOR:
        state.ruined = True
        state.ruin_trade_num = len(state.trades)


def simulate_strategy(signals: list[dict], cfg: StrategyConfig) -> dict:
    """Simulate a strategy on pre-generated signals.

    Returns summary dict with all stats.
    """
    state = SimState()

    # Filter signals by quality gate if enabled
    if cfg.quality_gate:
        eligible = [s for s in signals if abs(s["imbalance"]) >= cfg.qg_imbalance]
    else:
        eligible = signals

    for sig in eligible:
        # Cooldown check
        if state.cooldown_remaining > 0:
            state.cooldown_remaining -= 1
            continue

        # Normal cooldown between independent trades
        if state.gale_step == 0:
            ticks_since = sig["idx"] - state.last_trade_idx
            if ticks_since <= cfg.cooldown:
                continue

        # Gale direction lock
        if cfg.use_gale and state.gale_step > 0 and state.gale_locked_dir:
            if sig["direction"] != state.gale_locked_dir:
                continue

        # Balance floor
        if state.ruined:
            break

        # Calculate stake
        stake = get_stake(cfg, state)
        if stake <= 0 or stake > state.balance:
            if cfg.use_gale and state.gale_step > 0:
                state.gale_step = 0
                state.gale_accum_loss = 0.0
                state.gale_base_stake = 0.0
                state.gale_locked_dir = None
                state.cooldown_remaining = 30
            continue

        # Execute trade
        won = sig["won"]
        if won:
            profit = round(stake * PAYOUT_RATE, 4)
        else:
            profit = round(-stake, 4)

        state.trades.append({
            "epoch": sig["epoch"],
            "direction": sig["direction"],
            "gale_step": state.gale_step,
            "soros_step": state.soros_step,
            "stake": stake,
            "profit": profit,
            "won": won,
            "score": sig["score"],
            "imbalance": sig["imbalance"],
            "hour": sig["hour"],
            "balance": state.balance + profit,
        })

        # Set gale direction lock BEFORE updating state
        if not won and cfg.use_gale:
            state.gale_locked_dir = sig["direction"]

        state.last_trade_idx = sig["idx"]
        update_state_after_trade(cfg, state, won, stake, profit, sig["hour"])

    return build_summary(cfg, state)


def build_summary(cfg: StrategyConfig, state: SimState) -> dict:
    """Build summary statistics from simulation state."""
    trades = state.trades
    if not trades:
        return {
            "label": cfg.label,
            "total": 0, "wins": 0, "losses": 0, "wr": 0.0,
            "pnl": 0.0, "final": STARTING_BALANCE, "max_dd": 0.0,
            "ruined": False, "ruin_trade": 0,
            "g0": 0, "g1": 0, "g2": 0, "g3": 0,
            "g0_wr": 0.0, "cascades": 0,
            "hourly_wr": {}, "hourly_pnl": {},
            "time_to_double": None,
            "soros_trades": 0,
        }

    wins = sum(1 for t in trades if t["won"])
    losses = len(trades) - wins
    total = len(trades)
    wr = wins / total * 100 if total > 0 else 0.0
    pnl = sum(t["profit"] for t in trades)

    # Gale distribution
    g0 = [t for t in trades if t["gale_step"] == 0]
    g1 = [t for t in trades if t["gale_step"] == 1]
    g2 = [t for t in trades if t["gale_step"] == 2]
    g3 = [t for t in trades if t["gale_step"] >= 3]
    g0_wr = sum(1 for t in g0 if t["won"]) / len(g0) * 100 if g0 else 0.0

    # Cascades: max gale step losses
    max_g = cfg.max_gales if cfg.use_gale else 0
    if cfg.gale_mode == "fibonacci" and cfg.use_gale:
        max_g = min(max_g, len(FIB_SEQUENCE) - 1)
    cascades = sum(1 for t in trades if t["gale_step"] == max_g and not t["won"]) if cfg.use_gale else 0

    # Soros trades
    soros_trades = sum(1 for t in trades if t["soros_step"] > 0)

    # Time-to-double: find first trade where balance >= 2 * starting
    time_to_double = None
    for i, t in enumerate(trades):
        if t["balance"] >= STARTING_BALANCE * 2:
            time_to_double = i + 1
            break

    # Hourly win rates
    hourly_wr = {}
    hourly_pnl_summary = {}
    for h in range(24):
        hw = state.hourly_wins[h]
        hl = state.hourly_losses[h]
        ht = hw + hl
        hourly_wr[h] = hw / ht * 100 if ht > 0 else 0.0
        hourly_pnl_summary[h] = round(state.hourly_pnl[h], 2)

    return {
        "label": cfg.label,
        "total": total,
        "wins": wins,
        "losses": losses,
        "wr": wr,
        "pnl": round(pnl, 2),
        "final": round(state.balance, 2),
        "max_dd": round(state.max_drawdown, 2),
        "ruined": state.ruined,
        "ruin_trade": state.ruin_trade_num if state.ruined else 0,
        "g0": len(g0),
        "g1": len(g1),
        "g2": len(g2),
        "g3": len(g3),
        "g0_wr": round(g0_wr, 1),
        "cascades": cascades,
        "soros_trades": soros_trades,
        "hourly_wr": hourly_wr,
        "hourly_pnl": hourly_pnl_summary,
        "time_to_double": time_to_double,
    }


def define_strategies() -> list[StrategyConfig]:
    """Define ALL strategy combinations to test."""
    strategies = []

    for gate in [False, True]:
        for cd in [3, 10, 30]:
            # 1. Flat (no gale, no soros)
            strategies.append(StrategyConfig(
                name="flat", cooldown=cd, quality_gate=gate,
            ))

            # 2. Soros only (1 step)
            strategies.append(StrategyConfig(
                name="soros1", use_soros=True, soros_max_steps=1,
                cooldown=cd, quality_gate=gate,
            ))

            # 3. Soros only (2 steps)
            strategies.append(StrategyConfig(
                name="soros2", use_soros=True, soros_max_steps=2,
                cooldown=cd, quality_gate=gate,
            ))

            # 4. Fib 2g (martingale fibonacci, max 2 gales)
            strategies.append(StrategyConfig(
                name="fib2g", use_gale=True, gale_mode="fibonacci", max_gales=2,
                cooldown=cd, quality_gate=gate,
            ))

            # 5. Fib 3g
            strategies.append(StrategyConfig(
                name="fib3g", use_gale=True, gale_mode="fibonacci", max_gales=3,
                cooldown=cd, quality_gate=gate,
            ))

            # 6. Classic 2g (2x multiplier)
            strategies.append(StrategyConfig(
                name="classic2g", use_gale=True, gale_mode="classic", max_gales=2,
                cooldown=cd, quality_gate=gate,
            ))

            # 7. Soros + Fib 2g
            strategies.append(StrategyConfig(
                name="soros+fib2g", use_soros=True, soros_max_steps=1,
                use_gale=True, gale_mode="fibonacci", max_gales=2,
                cooldown=cd, quality_gate=gate,
            ))

            # 8. Soros + Fib 3g
            strategies.append(StrategyConfig(
                name="soros+fib3g", use_soros=True, soros_max_steps=1,
                use_gale=True, gale_mode="fibonacci", max_gales=3,
                cooldown=cd, quality_gate=gate,
            ))

            # 9. Soros + Classic 2g
            strategies.append(StrategyConfig(
                name="soros+classic2g", use_soros=True, soros_max_steps=1,
                use_gale=True, gale_mode="classic", max_gales=2,
                cooldown=cd, quality_gate=gate,
            ))

    return strategies


def print_results_table(results: list[dict]) -> None:
    """Print compact comparison table."""
    # Sort: profitable first (by PnL desc), then non-profitable
    results_sorted = sorted(results, key=lambda r: (-1 if r["pnl"] > 0 else 1, -r["pnl"]))

    print(f"\n{'='*130}")
    print(f"  STRATEGY MATRIX RESULTS")
    print(f"{'='*130}")

    header = f"{'Strategy':<35} {'Trades':>6} {'WR%':>6} {'PnL':>10} {'Final$':>8} {'MaxDD':>8} {'Ruin':>5} {'Casc':>5} {'G0%':>6} {'T2x':>5} {'Soros':>5}"
    print(header)
    print("-" * 130)

    for r in results_sorted:
        t2x = str(r["time_to_double"]) if r["time_to_double"] else "—"
        ruin = "YES" if r["ruined"] else "no"
        pnl_str = f"${r['pnl']:+.2f}"
        if r["pnl"] > 0:
            pnl_str = f"\033[32m{pnl_str}\033[0m"
        elif r["ruined"]:
            pnl_str = f"\033[31m{pnl_str}\033[0m"

        print(f"  {r['label']:<33} {r['total']:>6} {r['wr']:>5.1f}% {pnl_str:>20} ${r['final']:>7.2f} ${r['max_dd']:>7.2f} {ruin:>5} {r['cascades']:>5} {r['g0_wr']:>5.1f}% {t2x:>5} {r['soros_trades']:>5}")

    # Best config
    profitable = [r for r in results_sorted if r["pnl"] > 0 and not r["ruined"]]
    if profitable:
        print(f"\n{'='*130}")
        print(f"  TOP 5 PROFITABLE STRATEGIES")
        print(f"{'='*130}")
        for i, r in enumerate(profitable[:5], 1):
            t2x_info = f", doubles in {r['time_to_double']} trades" if r["time_to_double"] else ""
            print(f"  #{i}: {r['label']}")
            print(f"       PnL=${r['pnl']:+.2f} | WR={r['wr']:.1f}% | MaxDD=${r['max_dd']:.2f} | {r['total']} trades{t2x_info}")
    else:
        print(f"\n  ⚠ NO PROFITABLE STRATEGIES FOUND")


def print_hourly_analysis(results: list[dict]) -> None:
    """Print best/worst trading hours analysis from profitable strategies."""
    profitable = [r for r in results if r["pnl"] > 0 and not r["ruined"]]
    if not profitable:
        return

    # Aggregate hourly data across top profitable strategies
    agg_wr = {h: [] for h in range(24)}
    agg_pnl = {h: 0.0 for h in range(24)}
    agg_trades = {h: 0 for h in range(24)}

    for r in profitable[:5]:
        for h in range(24):
            wr_h = r["hourly_wr"].get(h, 0)
            pnl_h = r["hourly_pnl"].get(h, 0)
            if wr_h > 0 or pnl_h != 0:
                agg_wr[h].append(wr_h)
                agg_pnl[h] += pnl_h
                # Count trades from hourly data
                total_h = 0
                for key_h, val in r["hourly_wr"].items():
                    if key_h == h and val > 0:
                        total_h += 1
                agg_trades[h] += total_h

    print(f"\n{'='*80}")
    print(f"  HOURLY ANALYSIS (aggregated from top profitable strategies)")
    print(f"{'='*80}")
    print(f"  {'Hour(UTC)':>10} {'AvgWR%':>8} {'TotalPnL':>12}")
    print(f"  {'-'*35}")

    for h in range(24):
        avg_wr = np.mean(agg_wr[h]) if agg_wr[h] else 0
        total_pnl = agg_pnl[h]
        marker = ""
        if avg_wr >= 55:
            marker = " ✅ BEST"
        elif avg_wr <= 45 and avg_wr > 0:
            marker = " ❌ AVOID"
        print(f"  {h:>8}:00 {avg_wr:>7.1f}% ${total_pnl:>10.2f}{marker}")


def save_results_csv(results: list[dict], path: Path) -> None:
    """Save all results to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "label", "total", "wins", "losses", "wr", "pnl", "final", "max_dd",
        "ruined", "ruin_trade", "g0", "g1", "g2", "g3", "g0_wr", "cascades",
        "soros_trades", "time_to_double",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            row = {k: r.get(k, "") for k in fieldnames}
            writer.writerow(row)
    print(f"\nResults saved to {path}")


def split_by_day(tick_list: list[dict]) -> list[tuple[str, int, int]]:
    """Split tick indices by UTC day. Returns list of (date_str, start_idx, end_idx)."""
    days = []
    current_date = None
    day_start = 0

    for i, t in enumerate(tick_list):
        d = datetime.fromtimestamp(t["epoch"], tz=UTC).strftime("%Y-%m-%d")
        if d != current_date:
            if current_date is not None:
                days.append((current_date, day_start, i - 1))
            current_date = d
            day_start = i
    if current_date is not None:
        days.append((current_date, day_start, len(tick_list) - 1))

    return days


# Safety block thresholds (must match JumpMomentumConfig defaults)
SAFETY_LYAPUNOV_MAX = 2.0
SAFETY_RETURN_Z_MAX = 3.0
SAFETY_CUSUM_MAX = 8.0
SAFETY_JERK_Z_MAX = 3.0
SAFETY_TAIL_DEP_MAX = 0.6


def prefilter_eligible_positions(df: pd.DataFrame, start: int, end: int) -> np.ndarray:
    """Vectorized pre-filter: return array of positions that PASS all safety blocks.

    This eliminates 50-80% of positions without calling generate_jump_momentum_signal.
    """
    sub = df.iloc[start:end + 1]
    mask = pd.Series(True, index=sub.index)

    # Safety block checks (vectorized)
    lyap = pd.to_numeric(sub.get("lyapunov_exponent", pd.Series(0.0, index=sub.index)), errors="coerce").fillna(0.0)
    ret_z = pd.to_numeric(sub.get("return_zscore", pd.Series(0.0, index=sub.index)), errors="coerce").fillna(0.0)
    cusum = pd.to_numeric(sub.get("cusum_score", pd.Series(0.0, index=sub.index)), errors="coerce").fillna(0.0)
    jerk_z = pd.to_numeric(sub.get("jerk_zscore", pd.Series(0.0, index=sub.index)), errors="coerce").fillna(0.0)
    tail = pd.to_numeric(sub.get("tail_dependence", pd.Series(0.0, index=sub.index)), errors="coerce").fillna(0.0)

    mask &= lyap <= SAFETY_LYAPUNOV_MAX
    mask &= ret_z.abs() <= SAFETY_RETURN_Z_MAX
    mask &= cusum <= SAFETY_CUSUM_MAX
    mask &= jerk_z.abs() <= SAFETY_JERK_Z_MAX
    mask &= tail <= SAFETY_TAIL_DEP_MAX

    # Return absolute positions (indices into tick_list, not into sub)
    return sub.index[mask].values


def generate_signals_for_day(
    tick_list: list[dict],
    day_start: int,
    day_end: int,
    overlap: int = INDICATOR_WINDOW,
) -> list[dict]:
    """Generate signals for one day's worth of ticks.

    Computes indicators on day + overlap context, then generates signals
    only for positions in [day_start, day_end - LOOKAHEAD].
    Pre-filters positions using vectorized safety block checks.
    """
    n = len(tick_list)
    # Include overlap ticks before day_start for indicator context
    chunk_start = max(0, day_start - overlap)
    # Include LOOKAHEAD ticks after day_end for outcome determination
    chunk_end = min(n - 1, day_end + LOOKAHEAD)

    chunk_ticks = tick_list[chunk_start:chunk_end + 1]

    # Compute indicators on this chunk
    chunk_df = calculate_tick_indicators(chunk_ticks)

    # Signal generation range (absolute positions in tick_list)
    sig_start = max(day_start, chunk_start + overlap)
    sig_end = min(day_end, n - LOOKAHEAD - 1)

    if sig_start > sig_end:
        return []

    # Map absolute positions to chunk-relative positions
    # chunk_df index is 0-based, position i in tick_list → (i - chunk_start) in chunk_df
    offset = chunk_start

    # Pre-filter: find positions that pass safety blocks (vectorized)
    rel_start = sig_start - offset
    rel_end = sig_end - offset
    eligible_abs = prefilter_eligible_positions(chunk_df, rel_start, rel_end)
    # Convert back to absolute positions
    eligible_positions = eligible_abs + offset

    total = sig_end - sig_start + 1
    n_eligible = len(eligible_positions)

    jm_config = JumpMomentumConfig(
        mom_lookback=5,
        mom_horizon=5,
        ema_fast=5,
        ema_slow=20,
        rev_lookback=7,
        min_score=SIGNAL_MIN_VOTES,
        min_confidence=SIGNAL_MIN_CONFIDENCE,
        min_ticks=30,
        quality_gate_enabled=False,
    )

    signals = []
    t0 = time.time()

    for count, abs_i in enumerate(eligible_positions, 1):
        # Window for signal generation (INDICATOR_WINDOW ticks of context)
        win_start = max(0, abs_i - INDICATOR_WINDOW + 1)
        # Convert to chunk-relative
        rel_win_start = win_start - offset
        rel_i = abs_i - offset

        if rel_win_start < 0:
            rel_win_start = 0

        tick_window = chunk_ticks[rel_win_start:rel_i + 1]
        df_slice = chunk_df.iloc[rel_win_start:rel_i + 1]

        signal, score, confidence = generate_jump_momentum_signal(
            tick_window, config=jm_config, df=df_slice,
        )

        if signal in {"CALL", "PUT"}:
            last_row = df_slice.iloc[-1]
            imb = float(last_row.get("tick_imbalance", 0.0) or 0.0)
            bay = float(last_row.get("bayesian_prob_up", 0.5) or 0.5)
            hur = float(last_row.get("hurst_exponent", 0.5) or 0.5)

            # Outcome: use absolute positions
            entry_price = tick_list[abs_i]["quote"]
            exit_price = tick_list[abs_i + LOOKAHEAD]["quote"]
            if signal == "CALL":
                won = exit_price > entry_price
            else:
                won = exit_price < entry_price

            hour = datetime.fromtimestamp(tick_list[abs_i]["epoch"], tz=UTC).hour

            signals.append({
                "idx": abs_i,
                "epoch": tick_list[abs_i]["epoch"],
                "direction": signal,
                "score": score,
                "confidence": confidence,
                "imbalance": imb,
                "bayes": bay,
                "hurst": hur,
                "won": won,
                "hour": hour,
            })

        if count % 5000 == 0:
            elapsed = time.time() - t0
            pct = count / n_eligible * 100
            rate = count / elapsed if elapsed > 0 else 0
            eta = (n_eligible - count) / rate if rate > 0 else 0
            print(f"      {pct:.0f}% ({count:,}/{n_eligible:,} eligible of {total:,}) "
                  f"{len(signals):,} signals [{elapsed:.0f}s, ETA {eta:.0f}s]")

    return signals


def save_signals_csv(signals: list[dict], path: Path) -> None:
    """Save signals to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["idx", "epoch", "direction", "score", "confidence",
                  "imbalance", "bayes", "hurst", "won", "hour"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(signals)


def load_signals_csv(path: Path) -> list[dict]:
    """Load signals from CSV."""
    signals = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            signals.append({
                "idx": int(row["idx"]),
                "epoch": int(row["epoch"]),
                "direction": row["direction"],
                "score": int(row["score"]),
                "confidence": float(row["confidence"]),
                "imbalance": float(row["imbalance"]),
                "bayes": float(row["bayes"]),
                "hurst": float(row["hurst"]),
                "won": row["won"] == "True",
                "hour": int(row["hour"]),
            })
    return signals


def main():
    import logging
    import os
    # Suppress strategy logger spam during backtest
    # Logger is named via BOT_NAME env var (default "Pegasus")
    bot_name = os.getenv("BOT_NAME", "Pegasus")
    logging.getLogger(bot_name).setLevel(logging.CRITICAL)
    logging.getLogger().setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(description="Comprehensive JD50 Strategy Backtest")
    parser.add_argument("--ticks", type=Path, default=Path("data/ticks_JD50_max.csv"))
    parser.add_argument("--quick", action="store_true", help="Use only last 100K ticks for quick test")
    parser.add_argument("--signals-only", action="store_true", help="Only generate signals, don't simulate")
    parser.add_argument("--sim-only", action="store_true", help="Only simulate (signals must exist)")
    args = parser.parse_args()

    # Load ticks
    print(f"Loading ticks from {args.ticks}...")
    tick_list: list[dict] = []
    with args.ticks.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tick_list.append({"epoch": int(row["epoch"]), "quote": float(row["quote"])})

    if args.quick and len(tick_list) > 100000:
        tick_list = tick_list[-100000:]
        print(f"  Quick mode: using last 100K ticks")

    print(f"Loaded {len(tick_list):,} ticks")

    if len(tick_list) < INDICATOR_WINDOW + LOOKAHEAD + 10:
        print("Not enough ticks. Need at least 315.")
        return

    t_start = datetime.fromtimestamp(tick_list[0]["epoch"], tz=UTC)
    t_end = datetime.fromtimestamp(tick_list[-1]["epoch"], tz=UTC)
    days_total = (tick_list[-1]["epoch"] - tick_list[0]["epoch"]) / 86400
    print(f"Range: {t_start:%Y-%m-%d %H:%M} → {t_end:%Y-%m-%d %H:%M} UTC ({days_total:.1f} days)")

    # ── Split by day ────────────────────────────────────────────────────
    day_chunks = split_by_day(tick_list)
    print(f"Days: {len(day_chunks)} ({', '.join(d[0] for d in day_chunks)})")

    signals_dir = Path("data/signals")
    signals_dir.mkdir(parents=True, exist_ok=True)

    all_signals: list[dict] = []

    if not args.sim_only:
        # ── Step 1+2: Compute indicators + generate signals PER DAY ─────
        print(f"\n[1/2] Generating signals day-by-day ({len(day_chunks)} days)...")
        print(f"  Each day: compute indicators (~60K ticks) + generate signals")
        print(f"  Pre-filters positions with vectorized safety block checks")
        total_t0 = time.time()

        for day_num, (date_str, d_start, d_end) in enumerate(day_chunks, 1):
            sig_file = signals_dir / f"signals_{date_str}.csv"
            day_ticks = d_end - d_start + 1

            if sig_file.exists():
                day_signals = load_signals_csv(sig_file)
                print(f"  Day {day_num}/{len(day_chunks)} [{date_str}] — CACHED ({len(day_signals):,} signals)")
                all_signals.extend(day_signals)
                continue

            print(f"  Day {day_num}/{len(day_chunks)} [{date_str}] ({day_ticks:,} ticks, idx {d_start}-{d_end})...")
            dt0 = time.time()
            day_signals = generate_signals_for_day(tick_list, d_start, d_end)
            elapsed = time.time() - dt0
            print(f"    → {len(day_signals):,} signals in {elapsed:.0f}s")

            # Save to disk for resume
            save_signals_csv(day_signals, sig_file)
            all_signals.extend(day_signals)

        total_elapsed = time.time() - total_t0
        print(f"\n  All days done: {len(all_signals):,} total signals in {total_elapsed:.0f}s")

        if args.signals_only:
            print("Signals saved. Use --sim-only to run simulations.")
            return

    else:
        # Load pre-generated signals
        print(f"\n[1/2] Loading pre-generated signals...")
        for date_str, _, _ in day_chunks:
            sig_file = signals_dir / f"signals_{date_str}.csv"
            if sig_file.exists():
                day_signals = load_signals_csv(sig_file)
                all_signals.extend(day_signals)
                print(f"  {date_str}: {len(day_signals):,} signals")
            else:
                print(f"  {date_str}: MISSING — run without --sim-only first")

    if not all_signals:
        print("No signals available. Run signal generation first.")
        return

    # Sort signals by idx to maintain chronological order
    all_signals.sort(key=lambda s: s["idx"])

    # Basic signal stats
    total_won = sum(1 for s in all_signals if s["won"])
    raw_wr = total_won / len(all_signals) * 100
    filtered_signals = [s for s in all_signals if abs(s["imbalance"]) >= 6.0]
    filt_won = sum(1 for s in filtered_signals if s["won"])
    filt_wr = filt_won / len(filtered_signals) * 100 if filtered_signals else 0

    print(f"\n  Signal Stats:")
    print(f"    Total signals: {len(all_signals):,} (raw WR: {raw_wr:.1f}%)")
    print(f"    Filtered (imb>=6): {len(filtered_signals):,} (WR: {filt_wr:.1f}%)")

    # ── Step 2: Run ALL strategy combos ─────────────────────────────────
    strategies = define_strategies()
    print(f"\n[2/2] Simulating {len(strategies)} strategy combinations...")
    results = []

    t0 = time.time()
    for i, cfg in enumerate(strategies, 1):
        result = simulate_strategy(all_signals, cfg)
        results.append(result)
        if i % 10 == 0:
            print(f"  {i}/{len(strategies)} strategies simulated...")

    print(f"  All {len(strategies)} strategies simulated in {time.time() - t0:.1f}s")

    # Print results
    print_results_table(results)
    print_hourly_analysis(results)

    # Time-to-double summary
    doublers = [r for r in results if r["time_to_double"] and not r["ruined"]]
    if doublers:
        doublers.sort(key=lambda r: r["time_to_double"])
        print(f"\n{'='*80}")
        print(f"  FASTEST TO DOUBLE ${STARTING_BALANCE:.0f} → ${STARTING_BALANCE*2:.0f}")
        print(f"{'='*80}")
        for r in doublers[:10]:
            print(f"  {r['label']:<35} → {r['time_to_double']:>5} trades (PnL=${r['pnl']:+.2f}, MaxDD=${r['max_dd']:.2f})")

    # Save results
    save_results_csv(results, Path("logs/backtest_comprehensive.csv"))

    # VERDICTS
    print(f"\n{'#'*80}")
    print(f"  VERDICTS")
    print(f"{'#'*80}")

    profitable = [r for r in results if r["pnl"] > 0 and not r["ruined"]]
    dangerous = [r for r in results if r["ruined"]]

    if profitable:
        best = max(profitable, key=lambda r: r["pnl"])
        safest = min(profitable, key=lambda r: r["max_dd"])
        print(f"\n  ✅ BEST PnL:    {best['label']}")
        print(f"     PnL=${best['pnl']:+.2f} | WR={best['wr']:.1f}% | MaxDD=${best['max_dd']:.2f}")
        print(f"\n  🛡 SAFEST:      {safest['label']}")
        print(f"     PnL=${safest['pnl']:+.2f} | WR={safest['wr']:.1f}% | MaxDD=${safest['max_dd']:.2f}")
    else:
        print(f"\n  ⚠ NO PROFITABLE STRATEGIES — all configs lose money or bust!")

    if dangerous:
        print(f"\n  ❌ RUIN STRATEGIES ({len(dangerous)} configs bust the bank):")
        ruin_names = set()
        for r in dangerous:
            name = r["label"].split("|")[0].strip()
            ruin_names.add(name)
        for name in sorted(ruin_names):
            print(f"     - {name}")

    print(f"\n{'#'*80}")
    print(f"  DONE — {len(results)} strategies tested on {len(tick_list):,} ticks ({days_total:.1f} days)")
    print(f"{'#'*80}")


if __name__ == "__main__":
    main()
