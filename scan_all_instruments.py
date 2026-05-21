"""Quick Win Rate Scanner — tests signal edge on ALL instruments.

For each instrument's tick data:
1. Compute indicators on the full dataset
2. Generate signals (same JumpMomentum strategy)
3. Calculate raw WR and filtered WR
4. Report which instruments have WR > break-even (51.2% for 95.3% payout)

This is a FAST SCAN — no strategy simulation, just signal quality check.
Instruments with positive edge go to full 54-strategy backtest.

Usage:
    python scan_all_instruments.py [--payout 0.953] [--quick]
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
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
INDICATOR_WINDOW = 300
LOOKAHEAD = 5
SIGNAL_MIN_VOTES = 7
SIGNAL_MIN_CONFIDENCE = 0.60
CHUNK_SIZE = 60_000  # Process in day-sized chunks

# Safety block thresholds
SAFETY_LYAPUNOV_MAX = 2.0
SAFETY_RETURN_Z_MAX = 3.0
SAFETY_CUSUM_MAX = 8.0
SAFETY_JERK_Z_MAX = 3.0
SAFETY_TAIL_DEP_MAX = 0.6


def load_ticks(path: Path) -> list[dict]:
    """Load tick data from CSV."""
    ticks = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticks.append({"epoch": int(row["epoch"]), "quote": float(row["quote"])})
    return ticks


def prefilter_eligible(df: pd.DataFrame, start: int, end: int) -> np.ndarray:
    """Vectorized safety block pre-filter."""
    sub = df.iloc[start:end + 1]
    mask = pd.Series(True, index=sub.index)

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

    return sub.index[mask].values


def scan_instrument(tick_path: Path, sample_size: int = 0, verbose: bool = True) -> dict:
    """Scan a single instrument for signal edge.

    Returns dict with WR stats or None if insufficient data.
    """
    symbol = tick_path.stem.replace("ticks_", "").replace("_max", "")

    ticks = load_ticks(tick_path)
    n = len(ticks)

    if n < 5000:
        if verbose:
            print(f"  ⏭  {symbol:12s} — only {n:,} ticks, skipping (need ≥5000)")
        return {"symbol": symbol, "ticks": n, "status": "insufficient"}

    # Sample if requested (for quick scan)
    if sample_size > 0 and n > sample_size:
        # Take evenly spaced samples across the dataset
        # But we need contiguous chunks for indicators, so take last N ticks
        ticks = ticks[-sample_size:]
        n = len(ticks)

    days = (ticks[-1]["epoch"] - ticks[0]["epoch"]) / 86400

    if verbose:
        print(f"  🔍 {symbol:12s} — {n:,} ticks ({days:.1f} days)...", end="", flush=True)

    # Process in chunks (like backtest_comprehensive)
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

    all_signals = []
    t0 = time.time()

    # Process in CHUNK_SIZE chunks with overlap
    chunk_starts = list(range(0, n, CHUNK_SIZE))

    for ci, chunk_start in enumerate(chunk_starts):
        chunk_end = min(chunk_start + CHUNK_SIZE - 1, n - 1)

        # Add overlap for indicators
        actual_start = max(0, chunk_start - INDICATOR_WINDOW)
        actual_end = min(n - 1, chunk_end + LOOKAHEAD)

        chunk_ticks = ticks[actual_start:actual_end + 1]

        # Compute indicators
        try:
            chunk_df = calculate_tick_indicators(chunk_ticks)
        except Exception:
            continue

        offset = actual_start

        # Signal range
        sig_start = max(chunk_start, actual_start + INDICATOR_WINDOW) - offset
        sig_end = min(chunk_end, n - LOOKAHEAD - 1) - offset

        if sig_start > sig_end:
            continue

        # Pre-filter
        eligible = prefilter_eligible(chunk_df, sig_start, sig_end)
        eligible_abs = eligible + offset

        # Sample eligible positions for speed (max 2000 per chunk)
        if len(eligible_abs) > 2000:
            rng = np.random.RandomState(42 + ci)
            eligible_abs = rng.choice(eligible_abs, 2000, replace=False)
            eligible_abs.sort()

        for abs_i in eligible_abs:
            rel_i = abs_i - offset
            win_start = max(0, rel_i - INDICATOR_WINDOW + 1)

            tick_window = chunk_ticks[win_start:rel_i + 1]
            df_slice = chunk_df.iloc[win_start:rel_i + 1]

            signal, score, confidence = generate_jump_momentum_signal(
                tick_window, config=jm_config, df=df_slice,
            )

            if signal in {"CALL", "PUT"}:
                last_row = df_slice.iloc[-1]
                imb = float(last_row.get("tick_imbalance", 0.0) or 0.0)

                entry_price = ticks[abs_i]["quote"]
                exit_price = ticks[abs_i + LOOKAHEAD]["quote"]

                if signal == "CALL":
                    won = exit_price > entry_price
                else:
                    won = exit_price < entry_price

                hour = datetime.fromtimestamp(ticks[abs_i]["epoch"], tz=UTC).hour

                all_signals.append({
                    "won": won,
                    "imbalance": imb,
                    "score": score,
                    "hour": hour,
                })

    elapsed = time.time() - t0
    total = len(all_signals)

    if total < 100:
        if verbose:
            print(f" only {total} signals, insufficient")
        return {"symbol": symbol, "ticks": n, "signals": total, "status": "few_signals"}

    # Calculate WR
    wins_raw = sum(1 for s in all_signals if s["won"])
    wr_raw = wins_raw / total * 100

    # Filtered WR (imb >= 6)
    filtered = [s for s in all_signals if abs(s["imbalance"]) >= 6]
    wins_filt = sum(1 for s in filtered if s["won"])
    wr_filt = wins_filt / len(filtered) * 100 if filtered else 0.0

    # High-score WR (score >= 10)
    high_score = [s for s in all_signals if s["score"] >= 10]
    wins_hs = sum(1 for s in high_score if s["won"])
    wr_hs = wins_hs / len(high_score) * 100 if high_score else 0.0

    # Best hour
    hourly = {}
    for s in all_signals:
        h = s["hour"]
        if h not in hourly:
            hourly[h] = {"wins": 0, "total": 0}
        hourly[h]["total"] += 1
        if s["won"]:
            hourly[h]["wins"] += 1

    best_hour = max(hourly.items(), key=lambda x: x[1]["wins"] / x[1]["total"] if x[1]["total"] >= 20 else 0)
    best_hour_wr = best_hour[1]["wins"] / best_hour[1]["total"] * 100 if best_hour[1]["total"] >= 20 else 0

    result = {
        "symbol": symbol,
        "ticks": n,
        "days": round(days, 1),
        "signals": total,
        "signals_filt": len(filtered),
        "wr_raw": round(wr_raw, 2),
        "wr_filt": round(wr_filt, 2),
        "wr_high_score": round(wr_hs, 2),
        "best_hour": best_hour[0],
        "best_hour_wr": round(best_hour_wr, 1),
        "elapsed": round(elapsed, 1),
        "status": "ok",
    }

    if verbose:
        marker = ""
        if wr_filt >= 53:
            marker = " 🟢 EDGE!"
        elif wr_filt >= 51.2:
            marker = " 🟡 MARGINAL"
        elif wr_raw >= 51.2:
            marker = " 🟠 RAW ONLY"
        else:
            marker = " 🔴 NO EDGE"

        print(f" {total:,} sigs | Raw={wr_raw:.1f}% | Filt={wr_filt:.1f}% | HS={wr_hs:.1f}%{marker} [{elapsed:.0f}s]")

    return result


def main():
    import logging
    bot_name = os.getenv("BOT_NAME", "Pegasus")
    logging.getLogger(bot_name).setLevel(logging.CRITICAL)
    logging.getLogger().setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(description="Quick WR Scanner for All Instruments")
    parser.add_argument("--payout", type=float, default=0.953, help="Payout rate")
    parser.add_argument("--quick", action="store_true", help="Use only last 100K ticks per instrument")
    parser.add_argument("--symbols", nargs="*", help="Specific symbols to scan")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()

    break_even = 1 / (1 + args.payout) * 100
    sample = 100_000 if args.quick else 0

    print(f"{'='*90}")
    print(f"  INSTRUMENT EDGE SCANNER — Pegasus JumpMomentum Strategy")
    print(f"{'='*90}")
    print(f"  Payout: {args.payout*100:.1f}% | Break-even WR: {break_even:.2f}%")
    print(f"  Mode: {'Quick (100K ticks max)' if args.quick else 'Full (all available data)'}")
    print(f"  Signal: min_votes={SIGNAL_MIN_VOTES}, min_conf={SIGNAL_MIN_CONFIDENCE}")
    print(f"  Lookahead: {LOOKAHEAD} ticks")
    print(f"{'='*90}\n")

    # Find all tick files
    data_dir = args.data_dir
    tick_files = sorted(data_dir.glob("ticks_*_max.csv"))

    if args.symbols:
        tick_files = [f for f in tick_files if any(s in f.stem for s in args.symbols)]

    if not tick_files:
        print("  No tick data files found! Run download_all_instruments.py first.")
        sys.exit(1)

    print(f"  Found {len(tick_files)} instrument data files\n")

    results = []
    for tf in tick_files:
        result = scan_instrument(tf, sample_size=sample)
        results.append(result)

    # Summary table
    ok_results = [r for r in results if r["status"] == "ok"]
    ok_results.sort(key=lambda r: -r["wr_filt"])

    print(f"\n{'='*90}")
    print(f"  RESULTS RANKING (by filtered WR)")
    print(f"  Break-even: {break_even:.2f}% | 🟢 ≥53% = EDGE | 🟡 ≥{break_even:.1f}% = MARGINAL")
    print(f"{'='*90}")
    print(f"  {'Symbol':<12} {'Ticks':>10} {'Days':>5} {'Signals':>8} {'Filt':>6} {'RawWR':>7} {'FiltWR':>7} {'HSWR':>7} {'BestH':>6} {'BhWR':>6} {'Status'}")
    print(f"  {'-'*90}")

    edge_found = []
    marginal = []

    for r in ok_results:
        marker = "🔴"
        if r["wr_filt"] >= 53:
            marker = "🟢 EDGE!"
            edge_found.append(r)
        elif r["wr_filt"] >= break_even:
            marker = "🟡 MARGINAL"
            marginal.append(r)
        elif r["wr_raw"] >= break_even:
            marker = "🟠"

        print(f"  {r['symbol']:<12} {r['ticks']:>10,} {r['days']:>5} {r['signals']:>8,} {r['signals_filt']:>6,} "
              f"{r['wr_raw']:>6.1f}% {r['wr_filt']:>6.1f}% {r['wr_high_score']:>6.1f}% "
              f"{r['best_hour']:>4}h {r['best_hour_wr']:>5.1f}% {marker}")

    # Skipped
    skipped = [r for r in results if r["status"] != "ok"]
    if skipped:
        print(f"\n  Skipped: {', '.join(r['symbol'] for r in skipped)}")

    # Verdict
    print(f"\n{'='*90}")
    if edge_found:
        print(f"  🟢 INSTRUMENTS WITH EDGE (WR ≥ 53%):")
        for r in edge_found:
            print(f"     → {r['symbol']}: Filtered WR = {r['wr_filt']:.1f}% ({r['signals_filt']:,} signals)")
        print(f"\n  ➡ NEXT: Run full 54-strategy backtest on these!")
    elif marginal:
        print(f"  🟡 MARGINAL INSTRUMENTS (WR ≥ {break_even:.1f}%):")
        for r in marginal:
            print(f"     → {r['symbol']}: Filtered WR = {r['wr_filt']:.1f}%")
        print(f"\n  ⚠ These are borderline — full backtest may reveal if edge holds")
    else:
        print(f"  🔴 NO INSTRUMENTS WITH POSITIVE EDGE FOUND")
        print(f"  The JumpMomentum strategy doesn't have edge on any tested instrument")
        print(f"  Consider: different contract types, different signal system")
    print(f"{'='*90}")

    # Save results
    output_csv = Path("logs/instrument_scan.csv")
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["symbol", "ticks", "days", "signals", "signals_filt",
                  "wr_raw", "wr_filt", "wr_high_score", "best_hour", "best_hour_wr", "status"]
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in ok_results:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
    print(f"\n  Results saved to {output_csv}")


if __name__ == "__main__":
    main()
