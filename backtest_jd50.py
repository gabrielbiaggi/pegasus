"""Backtest JD50 Rise/Fall with quality gate filters on real tick data.

Simulates the full trading pipeline:
  1. Load tick data from CSV
  2. Compute indicators via calculate_tick_indicators()
  3. Generate signals via generate_jump_momentum_signal()
  4. Simulate Fib 2g martingale (G0=$0.35, G1=$0.35, G2=$0.70)
  5. Compare FILTERED vs UNFILTERED results

Usage:
    python backtest_jd50.py [--ticks data/ticks_JD50_1week.csv]
"""
from __future__ import annotations

import argparse
import csv
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from strategy import (
    AccumulatorStrategyConfig,
    JumpMomentumConfig,
    calculate_tick_indicators,
    generate_jump_momentum_signal,
)


# ── Fib 2g parameters ──────────────────────────────────────────────────
FIB_SEQUENCE = [1, 1, 2]  # Fib 2g: G0=1x, G1=1x, G2=2x
BASE_STAKE = 0.35
PAYOUT_RATE = 0.953
STARTING_BALANCE = 50.0
BALANCE_FLOOR = 0.35  # stop trading below this

# ── Signal params (must match bot.py) ──────────────────────────────────
SIGNAL_MIN_VOTES = 7  # RISE_FALL_MIN_VOTES=7 (was 5, try 7 for quality)
SIGNAL_MIN_CONFIDENCE = 0.60
COOLDOWN_TICKS = 3

# ── Tick window for indicator calculation ──────────────────────────────
INDICATOR_WINDOW = 300  # same as TICK_COUNT in bot config


@dataclass
class TradeResult:
    epoch: int
    direction: str
    gale_step: int
    stake: float
    profit: float
    result: str  # "WIN" / "LOSS"
    score: int
    imbalance: float
    bayes: float
    hurst: float
    balance_after: float


def simulate_trade_outcome(direction: str, tick_buffer: list[dict], duration: int = 5) -> str:
    """Simulate Rise/Fall outcome: check if price after `duration` ticks
    is higher (CALL) or lower (PUT) than entry price."""
    if len(tick_buffer) < duration + 1:
        return "LOSS"  # not enough data

    entry_price = tick_buffer[-1]["quote"]
    # We look ahead `duration` ticks from the current position
    # Since we're iterating through historical data, this will be handled
    # in the main loop by looking at future ticks
    return "PENDING"  # resolved in main loop


def run_backtest(
    ticks: list[dict],
    filtered: bool = True,
    min_votes: int = SIGNAL_MIN_VOTES,
    label: str = "",
    qg_imb: float = 4.0,
) -> tuple[list[TradeResult], dict]:
    """Run backtest on tick data.

    Args:
        ticks: List of {epoch, quote} dicts
        filtered: Whether to enable quality gate filters
        min_votes: Minimum votes threshold
        label: Label for this run
        qg_imb: Minimum absolute imbalance for quality gate

    Returns:
        (trades, summary_stats)
    """
    n = len(ticks)
    if n < INDICATOR_WINDOW + 10:
        print(f"Not enough ticks for backtest: {n}")
        return [], {}

    # Build JumpMomentumConfig
    jm_config = JumpMomentumConfig(
        mom_lookback=5,
        mom_horizon=5,
        ema_fast=5,
        ema_slow=20,
        rev_lookback=7,
        min_score=min_votes,
        min_confidence=SIGNAL_MIN_CONFIDENCE,
        min_ticks=30,
        quality_gate_enabled=filtered,
        qg_min_abs_imbalance=qg_imb,
        qg_bayes_strong=0.70,
        qg_hurst_max=0.50,
    )

    # Simulation state
    balance = STARTING_BALANCE
    trades: list[TradeResult] = []
    gale_step = 0
    gale_locked_direction: str | None = None
    last_signal_idx = -COOLDOWN_TICKS - 1
    cooldown_after_exhaust = 0

    # Pre-compute full indicator DataFrame
    print(f"  Computing indicators on {n} ticks...", flush=True)
    t0 = time.time()
    full_df = calculate_tick_indicators(ticks)
    print(f"  Indicators computed in {time.time() - t0:.1f}s")

    # Iterate through ticks, generating signals at each point
    print(f"  Simulating trades...", flush=True)
    signals_generated = 0
    signals_rejected_by_gate = 0

    for i in range(INDICATOR_WINDOW, n - 5):  # need 5 ticks lookahead for outcome
        # Cooldown check
        if i - last_signal_idx <= COOLDOWN_TICKS and gale_step == 0:
            continue

        # Cooldown after gale exhaustion
        if cooldown_after_exhaust > 0:
            cooldown_after_exhaust -= 1
            continue

        # Balance floor
        if balance < BALANCE_FLOOR:
            break

        # Build tick buffer window
        window_start = max(0, i - INDICATOR_WINDOW + 1)
        tick_window = ticks[window_start:i + 1]

        # Get the indicator slice
        df_slice = full_df.iloc[window_start:i + 1].copy()

        # Generate signal
        signal, score, confidence = generate_jump_momentum_signal(
            tick_window, config=jm_config, df=df_slice,
        )

        if signal not in {"CALL", "PUT"}:
            if gale_step > 0:
                # In gale sequence but no signal — skip this tick
                pass
            continue

        # Gale direction lock
        if gale_step > 0 and gale_locked_direction and signal != gale_locked_direction:
            continue  # wrong direction for gale

        signals_generated += 1

        # Determine stake
        fib_mult = FIB_SEQUENCE[min(gale_step, len(FIB_SEQUENCE) - 1)]
        stake = BASE_STAKE * fib_mult

        # Check if we can afford it
        if stake > balance:
            if gale_step > 0:
                # Can't afford gale — reset
                gale_step = 0
                gale_locked_direction = None
                cooldown_after_exhaust = 30
            continue

        # Determine outcome — look ahead 5 ticks
        entry_price = ticks[i]["quote"]
        exit_price = ticks[i + 5]["quote"]

        if signal == "CALL":
            won = exit_price > entry_price
        else:
            won = exit_price < entry_price

        if won:
            profit = stake * PAYOUT_RATE
            result = "WIN"
        else:
            profit = -stake
            result = "LOSS"

        balance += profit

        # Get indicator values for logging
        last_row = df_slice.iloc[-1]
        imb = float(last_row.get("tick_imbalance", 0.0) or 0.0)
        bay = float(last_row.get("bayesian_prob_up", 0.5) or 0.5)
        hur = float(last_row.get("hurst_exponent", 0.5) or 0.5)

        trades.append(TradeResult(
            epoch=ticks[i]["epoch"],
            direction=signal,
            gale_step=gale_step,
            stake=stake,
            profit=profit,
            result=result,
            score=score,
            imbalance=imb,
            bayes=bay,
            hurst=hur,
            balance_after=balance,
        ))

        last_signal_idx = i

        # Gale logic
        if won:
            gale_step = 0
            gale_locked_direction = None
        else:
            gale_step += 1
            gale_locked_direction = signal
            if gale_step > len(FIB_SEQUENCE) - 1:
                # Gale exhausted — reset with cooldown
                gale_step = 0
                gale_locked_direction = None
                cooldown_after_exhaust = 30

    # Summary
    if not trades:
        return trades, {"label": label, "total": 0}

    wins = sum(1 for t in trades if t.result == "WIN")
    losses = sum(1 for t in trades if t.result == "LOSS")
    total = len(trades)
    wr = wins / total * 100 if total > 0 else 0
    total_pnl = sum(t.profit for t in trades)
    max_dd = 0.0
    peak = STARTING_BALANCE
    for t in trades:
        peak = max(peak, t.balance_after)
        dd = peak - t.balance_after
        max_dd = max(max_dd, dd)

    # Count G0, G1, G2 trades
    g0_trades = [t for t in trades if t.gale_step == 0]
    g1_trades = [t for t in trades if t.gale_step == 1]
    g2_trades = [t for t in trades if t.gale_step == 2]

    # Count cascades (full gale sequences that failed)
    cascades = sum(1 for t in trades if t.gale_step == len(FIB_SEQUENCE) - 1 and t.result == "LOSS")

    summary = {
        "label": label,
        "total": total,
        "wins": wins,
        "losses": losses,
        "wr": wr,
        "pnl": total_pnl,
        "final_balance": balance,
        "max_drawdown": max_dd,
        "cascades": cascades,
        "g0": len(g0_trades),
        "g1": len(g1_trades),
        "g2": len(g2_trades),
        "g0_wr": sum(1 for t in g0_trades if t.result == "WIN") / len(g0_trades) * 100 if g0_trades else 0,
        "signals_generated": signals_generated,
    }

    return trades, summary


def print_summary(s: dict) -> None:
    """Print a formatted summary."""
    if s.get("total", 0) == 0:
        print(f"\n{'='*60}")
        print(f"  {s.get('label', '?')} — NO TRADES")
        print(f"{'='*60}")
        return

    print(f"\n{'='*60}")
    print(f"  {s['label']}")
    print(f"{'='*60}")
    print(f"  Trades:     {s['total']} (G0:{s['g0']} G1:{s['g1']} G2:{s['g2']})")
    print(f"  Wins:       {s['wins']}  |  Losses: {s['losses']}")
    print(f"  Win Rate:   {s['wr']:.1f}%  (G0: {s['g0_wr']:.1f}%)")
    print(f"  PnL:        ${s['pnl']:+.2f}")
    print(f"  Final $:    ${s['final_balance']:.2f}  (started: ${STARTING_BALANCE:.2f})")
    print(f"  Max DD:     ${s['max_drawdown']:.2f}")
    print(f"  Cascades:   {s['cascades']} (full gale failures)")
    print(f"  Signals:    {s['signals_generated']}")
    print(f"{'='*60}")


def save_trades_csv(trades: list[TradeResult], path: Path) -> None:
    """Save trade results to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "epoch", "direction", "gale_step", "stake", "profit",
            "result", "score", "imbalance", "bayes", "hurst", "balance_after",
        ])
        for t in trades:
            writer.writerow([
                t.epoch, t.direction, t.gale_step, f"{t.stake:.2f}",
                f"{t.profit:.4f}", t.result, t.score,
                f"{t.imbalance:.2f}", f"{t.bayes:.3f}", f"{t.hurst:.3f}",
                f"{t.balance_after:.2f}",
            ])


def main():
    parser = argparse.ArgumentParser(description="Backtest JD50 Rise/Fall with quality gate filters")
    parser.add_argument("--ticks", type=Path, default=Path("data/ticks_JD50_1week.csv"))
    parser.add_argument("--min-votes", type=int, default=SIGNAL_MIN_VOTES)
    args = parser.parse_args()

    # Load ticks
    print(f"Loading ticks from {args.ticks}...")
    tick_list: list[dict] = []
    with args.ticks.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tick_list.append({"epoch": int(row["epoch"]), "quote": float(row["quote"])})
    print(f"Loaded {len(tick_list)} ticks")

    if len(tick_list) < INDICATOR_WINDOW + 10:
        print("Not enough ticks for backtest. Need at least 310.")
        return

    from datetime import datetime, UTC
    t0 = datetime.fromtimestamp(tick_list[0]["epoch"], tz=UTC)
    t1 = datetime.fromtimestamp(tick_list[-1]["epoch"], tz=UTC)
    print(f"Range: {t0.strftime('%Y-%m-%d %H:%M')} → {t1.strftime('%Y-%m-%d %H:%M')} UTC")
    hours = (tick_list[-1]["epoch"] - tick_list[0]["epoch"]) / 3600
    print(f"Duration: {hours:.1f} hours")

    # ── Run 1: UNFILTERED (baseline) ──────────────────────────────────
    print(f"\n{'#'*60}")
    print(f"  RUN 1: UNFILTERED (quality_gate=OFF)")
    print(f"{'#'*60}")
    trades_nf, summary_nf = run_backtest(
        tick_list, filtered=False, min_votes=args.min_votes,
        label="UNFILTERED (no quality gate)",
    )
    print_summary(summary_nf)

    # ── Run 2: FILTERED imb>=4 ────────────────────────────────────────
    print(f"\n{'#'*60}")
    print(f"  RUN 2: FILTERED (imb>=4 required)")
    print(f"{'#'*60}")
    trades_f, summary_f = run_backtest(
        tick_list, filtered=True, min_votes=args.min_votes,
        label="FILTERED (imb>=4 required)",
    )
    print_summary(summary_f)

    # ── Run 3: FILTERED imb>=4 + min_votes=5 (looser votes, strict gate)
    print(f"\n{'#'*60}")
    print(f"  RUN 3: FILTERED (imb>=4 + min_votes=5)")
    print(f"{'#'*60}")
    trades_f5, summary_f5 = run_backtest(
        tick_list, filtered=True, min_votes=5,
        label="FILTERED (imb>=4 + min_votes=5)",
    )
    print_summary(summary_f5)

    # ── Run 4: FILTERED imb>=6 (very strict) ──────────────────────────
    print(f"\n{'#'*60}")
    print(f"  RUN 4: FILTERED (imb>=6, very strict)")
    print(f"{'#'*60}")
    trades_f6, summary_f6 = run_backtest(
        tick_list, filtered=True, min_votes=args.min_votes,
        label="FILTERED (imb>=6 very strict)",
        qg_imb=6.0,
    )
    print_summary(summary_f6)

    # ── Comparison ────────────────────────────────────────────────────
    print(f"\n{'#'*60}")
    print(f"  COMPARISON")
    print(f"{'#'*60}")
    all_summaries = [summary_nf, summary_f, summary_f5, summary_f6]
    headers = [s.get("label", "?")[:30] for s in all_summaries]
    print(f"  {'Metric':<15}", end="")
    for h in headers:
        print(f" {h:>15}", end="")
    print()
    print(f"  {'-'*75}")
    for key in ["total", "wr", "pnl", "final_balance", "max_drawdown", "cascades", "g0_wr"]:
        print(f"  {key:<15}", end="")
        for s in all_summaries:
            v = s.get(key, 0)
            if key in ("wr", "g0_wr"):
                print(f" {v:>14.1f}%", end="")
            elif key in ("pnl", "final_balance", "max_drawdown"):
                print(f" ${v:>13.2f}", end="")
            else:
                print(f" {v:>15}", end="")
        print()

    # Save trade logs
    if trades_nf:
        p = Path("logs/backtest_jd50_unfiltered.csv")
        save_trades_csv(trades_nf, p)
        print(f"\nUnfiltered trades saved to {p}")
    if trades_f:
        p = Path("logs/backtest_jd50_filtered.csv")
        save_trades_csv(trades_f, p)
        print(f"Filtered trades saved to {p}")


if __name__ == "__main__":
    main()
