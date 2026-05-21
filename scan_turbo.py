#!/usr/bin/env python3
"""TURBO Instrument Scanner — multiprocessing across all CPU cores.

Uses existing calculate_tick_indicators() for indicator accuracy
+ vectorized 21-vote system from scan_fast.py
+ multiprocessing.Pool to parallelize across instruments.

8 cores → 8 instruments simultaneously → ~70 min for 35 instruments.

Usage:
    python scan_turbo.py [--symbols SYM1 SYM2 ...] [--workers N]
"""
from __future__ import annotations

import csv
import os
import sys
import time
from datetime import UTC, datetime
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd

# ── Constants ──────────────────────────────────────────────────────────
INDICATOR_WINDOW = 300
LOOKAHEAD = 5
MIN_VOTES = 7
MIN_CONFIDENCE = 0.60
CHUNK_SIZE = 120_000

# Safety thresholds
LYAP_MAX = 2.0
RET_Z_MAX = 3.0
CUSUM_MAX = 8.0
JERK_Z_MAX = 3.0
TAIL_DEP_MAX = 0.6

# JumpMomentum defaults
MOM_LOOKBACK = 5
EMA_FAST = 5
EMA_SLOW = 20
REV_LOOKBACK = 7
SHORT_MOM = 3
CURVATURE_REV_Z = 1.5
BAYESIAN_STRONG = 0.65
HURST_TRENDING = 0.55
HURST_REVERTING = 0.40
RENYI_LOW = 0.3
MI_FLOW_MIN = 0.1
WAVELET_SNR_MIN = 0.6
FISHER_MIN = 0.5
ENERGY_CALM = 0.01
EXHAUSTION_EXTREME = 0.8


def _safe_col(df: pd.DataFrame, name: str, default: float = 0.0) -> np.ndarray:
    if name in df.columns:
        return pd.to_numeric(df[name], errors="coerce").fillna(default).values
    return np.full(len(df), default, dtype=np.float64)


def vectorized_votes(df: pd.DataFrame, quotes_arr: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    n = len(df)
    up = np.zeros(n, dtype=np.int8)
    dn = np.zeros(n, dtype=np.int8)

    vel = _safe_col(df, "price_velocity")
    accel = _safe_col(df, "price_acceleration")
    curv_z = _safe_col(df, "curvature_zscore")
    int_div = _safe_col(df, "integral_momentum_div")
    energy = _safe_col(df, "derivative_energy")
    bayes_up = _safe_col(df, "bayesian_prob_up", 0.5)
    kalman_z = _safe_col(df, "kalman_residual_zscore")
    hurst = _safe_col(df, "hurst_exponent", 0.5)
    markov_up = _safe_col(df, "markov_p_up_given_up", 0.5)
    markov_dn = _safe_col(df, "markov_p_down_given_down", 0.5)
    imbalance = _safe_col(df, "tick_imbalance")
    shannon = _safe_col(df, "shannon_entropy", 1.0)
    renyi = _safe_col(df, "renyi_entropy", 0.5)
    mi = _safe_col(df, "mi_flow")
    wavelet = _safe_col(df, "wavelet_energy_ratio", 0.5)
    fisher = _safe_col(df, "fisher_information")
    exhaust = _safe_col(df, "trend_exhaustion")
    vel_z = _safe_col(df, "velocity_zscore")
    accel_z = _safe_col(df, "acceleration_zscore")

    # V1: Momentum majority
    tick_up = np.zeros(n, dtype=np.float32)
    tick_up[1:] = (quotes_arr[1:] > quotes_arr[:-1]).astype(np.float32)
    cum = np.cumsum(tick_up)
    rolling_up = np.zeros(n)
    rolling_up[MOM_LOOKBACK:] = cum[MOM_LOOKBACK:] - cum[:-MOM_LOOKBACK]
    rolling_dn = MOM_LOOKBACK - rolling_up
    up += (rolling_up >= MOM_LOOKBACK * 0.6).astype(np.int8)
    dn += (rolling_dn >= MOM_LOOKBACK * 0.6).astype(np.int8)

    # V2: EMA crossover
    ema_f = pd.Series(quotes_arr).ewm(span=EMA_FAST, adjust=False).mean().values
    ema_s = pd.Series(quotes_arr).ewm(span=EMA_SLOW, adjust=False).mean().values
    fast_above = ema_f > ema_s
    fast_above_prev = np.roll(fast_above, 1); fast_above_prev[0] = fast_above[0]
    cross_up = fast_above & ~fast_above_prev
    cross_dn = ~fast_above & fast_above_prev
    gap_pct = np.abs(ema_f - ema_s) / np.maximum(ema_s, 1e-10) * 100
    gap_up = ~cross_up & ~cross_dn & (gap_pct > 0.005) & (ema_f > ema_s)
    gap_dn = ~cross_up & ~cross_dn & (gap_pct > 0.005) & (ema_f < ema_s)
    up += (cross_up | gap_up).astype(np.int8)
    dn += (cross_dn | gap_dn).astype(np.int8)

    # V3: EMA alignment
    up += (quotes_arr > ema_s).astype(np.int8)
    dn += (quotes_arr < ema_s).astype(np.int8)

    # V4: Short momentum
    rolling_up_short = np.zeros(n)
    rolling_up_short[SHORT_MOM:] = cum[SHORT_MOM:] - cum[:-SHORT_MOM]
    rolling_dn_short = SHORT_MOM - rolling_up_short
    up += (rolling_up_short >= 2).astype(np.int8)
    dn += (rolling_dn_short >= 2).astype(np.int8)

    # V5: Reversal after streak
    rolling_up_rev = np.zeros(n)
    rolling_up_rev[REV_LOOKBACK:] = cum[REV_LOOKBACK:] - cum[:-REV_LOOKBACK]
    rolling_dn_rev = REV_LOOKBACK - rolling_up_rev
    up += (rolling_dn_rev >= REV_LOOKBACK - 2).astype(np.int8)
    dn += (rolling_up_rev >= REV_LOOKBACK - 2).astype(np.int8)

    # V6: Velocity direction
    up += (vel > 0).astype(np.int8)
    dn += (vel < 0).astype(np.int8)

    # V7: Acceleration confirms velocity
    up += ((vel > 0) & (accel > 0)).astype(np.int8)
    dn += ((vel < 0) & (accel < 0)).astype(np.int8)

    # V8: Curvature inflection
    curv_high = curv_z > CURVATURE_REV_Z
    up += (curv_high & (vel < 0) & (accel > 0)).astype(np.int8)
    dn += (curv_high & (vel > 0) & (accel < 0)).astype(np.int8)

    # V9: Integral momentum divergence
    safe_energy = np.maximum(energy, 1e-10)
    norm_div = int_div / safe_energy
    energy_ok = energy > 1e-10
    up += (energy_ok & (norm_div > 1.0)).astype(np.int8)
    dn += (energy_ok & (norm_div < -1.0)).astype(np.int8)

    # V10: Bayesian posterior
    up += (bayes_up > BAYESIAN_STRONG).astype(np.int8)
    dn += (bayes_up < (1.0 - BAYESIAN_STRONG)).astype(np.int8)

    # V11: Kalman residual
    up += (kalman_z > 1.0).astype(np.int8)
    dn += (kalman_z < -1.0).astype(np.int8)

    # V12: Hurst regime
    trending = hurst > HURST_TRENDING
    reverting = hurst < HURST_REVERTING
    up += (trending & (vel > 0)).astype(np.int8)
    dn += (trending & (vel < 0)).astype(np.int8)
    up += (reverting & (vel < 0)).astype(np.int8)
    dn += (reverting & (vel > 0)).astype(np.int8)

    # V13: Markov transition
    up += ((markov_up > 0.55) & (markov_up > markov_dn)).astype(np.int8)
    dn += ((markov_dn > 0.55) & (markov_dn > markov_up)).astype(np.int8)

    # V14: Tick imbalance
    up += (imbalance > 0.1).astype(np.int8)
    dn += (imbalance < -0.1).astype(np.int8)

    # V15: Shannon entropy gate
    low_shannon = shannon < 0.7
    up += (low_shannon & (vel > 0)).astype(np.int8)
    dn += (low_shannon & (vel < 0)).astype(np.int8)

    # V16: Rényi concentration
    low_renyi = renyi < RENYI_LOW
    up += (low_renyi & (vel > 0)).astype(np.int8)
    dn += (low_renyi & (vel < 0)).astype(np.int8)

    # V17: MI flow
    high_mi = mi > MI_FLOW_MIN
    up += (high_mi & (vel_z > 0.5)).astype(np.int8)
    dn += (high_mi & (vel_z < -0.5)).astype(np.int8)

    # V18: Wavelet SNR
    high_wav = wavelet > WAVELET_SNR_MIN
    up += (high_wav & (vel > 0)).astype(np.int8)
    dn += (high_wav & (vel < 0)).astype(np.int8)

    # V19: Fisher information
    high_fish = fisher > FISHER_MIN
    up += (high_fish & (accel_z > 0.5)).astype(np.int8)
    dn += (high_fish & (accel_z < -0.5)).astype(np.int8)

    # V20: Calm market
    calm = (energy < ENERGY_CALM) & (hurst < 0.5)
    up += (calm & (quotes_arr > ema_s)).astype(np.int8)
    dn += (calm & (quotes_arr < ema_s)).astype(np.int8)

    # V21: Trend exhaustion
    up += (exhaust < -EXHAUSTION_EXTREME).astype(np.int8)
    dn += (exhaust > EXHAUSTION_EXTREME).astype(np.int8)

    return up, dn


def scan_one(tick_path_str: str) -> dict:
    """Scan a single instrument — designed to run in a worker process."""
    # Import inside worker to avoid pickling issues
    import logging
    logging.getLogger().setLevel(logging.CRITICAL)
    logging.getLogger("Pegasus").setLevel(logging.CRITICAL)

    from strategy import calculate_tick_indicators

    tick_path = Path(tick_path_str)
    symbol = tick_path.stem.replace("ticks_", "").replace("_max", "")

    # Load ticks
    ticks = []
    with tick_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticks.append({"epoch": int(row["epoch"]), "quote": float(row["quote"])})

    n = len(ticks)
    if n < 5000:
        return {"symbol": symbol, "ticks": n, "status": "insufficient"}

    days = (ticks[-1]["epoch"] - ticks[0]["epoch"]) / 86400
    t0 = time.time()

    all_won = []
    all_imb = []
    all_score = []
    all_hour = []

    chunk_starts = list(range(0, n, CHUNK_SIZE))

    for chunk_start in chunk_starts:
        chunk_end = min(chunk_start + CHUNK_SIZE - 1, n - 1)
        actual_start = max(0, chunk_start - INDICATOR_WINDOW)
        actual_end = min(n - 1, chunk_end + LOOKAHEAD)
        chunk_ticks = ticks[actual_start:actual_end + 1]

        try:
            chunk_df = calculate_tick_indicators(chunk_ticks)
        except Exception:
            continue

        offset = actual_start
        quotes_arr = np.array([t["quote"] for t in chunk_ticks], dtype=np.float64)

        sig_start = max(chunk_start, actual_start + INDICATOR_WINDOW) - offset
        sig_end = min(chunk_end, n - LOOKAHEAD - 1) - offset
        if sig_start > sig_end:
            continue

        # Safety block mask
        lyap = _safe_col(chunk_df, "lyapunov_exponent")
        ret_z = _safe_col(chunk_df, "return_zscore")
        cusum = _safe_col(chunk_df, "cusum_score")
        jerk_z = _safe_col(chunk_df, "jerk_zscore")
        tail_dep = _safe_col(chunk_df, "tail_dependence")

        safe = (
            (lyap <= LYAP_MAX) &
            (np.abs(ret_z) <= RET_Z_MAX) &
            (cusum <= CUSUM_MAX) &
            (np.abs(jerk_z) <= JERK_Z_MAX) &
            (tail_dep <= TAIL_DEP_MAX)
        )

        up_votes, dn_votes = vectorized_votes(chunk_df, quotes_arr)

        total_votes = up_votes + dn_votes
        winning_votes = np.maximum(up_votes, dn_votes)
        confidence = np.where(total_votes > 0, winning_votes / total_votes, 0.0)
        has_signal = (winning_votes >= MIN_VOTES) & (confidence >= MIN_CONFIDENCE) & (total_votes > 0)

        is_call = has_signal & (up_votes > dn_votes)

        valid_range = np.zeros(len(chunk_df), dtype=bool)
        valid_range[sig_start:sig_end + 1] = True
        eligible = safe & valid_range & has_signal

        imb = _safe_col(chunk_df, "tick_imbalance")

        for rel_i in np.where(eligible)[0]:
            abs_i = rel_i + offset
            entry_price = ticks[abs_i]["quote"]
            exit_price = ticks[abs_i + LOOKAHEAD]["quote"]

            if is_call[rel_i]:
                won = exit_price > entry_price
            else:
                won = exit_price < entry_price

            hour = datetime.fromtimestamp(ticks[abs_i]["epoch"], tz=UTC).hour
            all_won.append(won)
            all_imb.append(imb[rel_i])
            all_score.append(int(winning_votes[rel_i]))
            all_hour.append(hour)

    elapsed = time.time() - t0
    total = len(all_won)

    if total < 100:
        return {"symbol": symbol, "ticks": n, "days": round(days, 1),
                "signals": total, "status": "few_signals", "elapsed": round(elapsed, 1)}

    won_arr = np.array(all_won)
    imb_arr = np.array(all_imb)
    score_arr = np.array(all_score)
    hour_arr = np.array(all_hour)

    wr_raw = won_arr.sum() / total * 100
    filt_mask = np.abs(imb_arr) >= 6
    n_filt = filt_mask.sum()
    wr_filt = won_arr[filt_mask].sum() / n_filt * 100 if n_filt > 0 else 0.0
    hs_mask = score_arr >= 10
    n_hs = hs_mask.sum()
    wr_hs = won_arr[hs_mask].sum() / n_hs * 100 if n_hs > 0 else 0.0

    best_h, best_h_wr = 0, 0.0
    for h in range(24):
        h_mask = hour_arr == h
        if h_mask.sum() >= 20:
            h_wr = won_arr[h_mask].sum() / h_mask.sum() * 100
            if h_wr > best_h_wr:
                best_h_wr = h_wr
                best_h = h

    return {
        "symbol": symbol, "ticks": n, "days": round(days, 1),
        "signals": total, "signals_filt": int(n_filt),
        "wr_raw": round(wr_raw, 2), "wr_filt": round(wr_filt, 2),
        "wr_high_score": round(wr_hs, 2),
        "best_hour": best_h, "best_hour_wr": round(best_h_wr, 1),
        "elapsed": round(elapsed, 1), "status": "ok",
    }


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--workers", type=int, default=0,
                        help="Number of parallel workers (0=auto)")
    args = parser.parse_args()

    payout = 0.953
    break_even = 1 / (1 + payout) * 100
    workers = args.workers if args.workers > 0 else max(1, os.cpu_count() - 1)

    print(f"{'='*90}")
    print(f"  TURBO INSTRUMENT SCANNER — {workers} parallel workers")
    print(f"{'='*90}")
    print(f"  Payout: {payout*100:.1f}% | Break-even WR: {break_even:.2f}%")
    print(f"  Votes: min={MIN_VOTES}, conf={MIN_CONFIDENCE}")
    print(f"  Lookahead: {LOOKAHEAD} ticks")
    print(f"{'='*90}\n")

    data_dir = Path("data")
    tick_files = sorted(data_dir.glob("ticks_*_max.csv"))
    if args.symbols:
        tick_files = [f for f in tick_files if any(s in f.stem for s in args.symbols)]

    if not tick_files:
        print("  No tick data files found!")
        sys.exit(1)

    print(f"  Found {len(tick_files)} instruments\n")
    print(f"  Starting parallel scan...\n")

    t_start = time.time()

    # Pass string paths (Path objects can't be pickled reliably across platforms)
    file_strs = [str(f) for f in tick_files]

    with Pool(processes=workers) as pool:
        results = []
        for i, result in enumerate(pool.imap_unordered(scan_one, file_strs)):
            marker = ""
            if result["status"] == "ok":
                wr = result.get("wr_filt", 0)
                if wr >= 53:
                    marker = "🟢 EDGE!"
                elif wr >= break_even:
                    marker = "🟡 MARGINAL"
                elif result.get("wr_raw", 0) >= break_even:
                    marker = "🟠 RAW"
                else:
                    marker = "🔴"
                print(f"  [{i+1:2d}/{len(tick_files)}] {result['symbol']:12s} "
                      f"| {result['signals']:>8,} sigs "
                      f"| Raw={result['wr_raw']:.1f}% "
                      f"| Filt={result['wr_filt']:.1f}% "
                      f"| HS={result['wr_high_score']:.1f}% "
                      f"{marker} [{result['elapsed']:.0f}s]")
            else:
                print(f"  [{i+1:2d}/{len(tick_files)}] {result['symbol']:12s} "
                      f"| {result['status']} [{result.get('elapsed', 0):.0f}s]")
            results.append(result)

    total_elapsed = time.time() - t_start

    # Summary
    ok_results = [r for r in results if r["status"] == "ok"]
    ok_results.sort(key=lambda r: -r["wr_filt"])

    print(f"\n{'='*90}")
    print(f"  RESULTS RANKING (by filtered WR) — Total time: {total_elapsed/60:.1f} min")
    print(f"  Break-even: {break_even:.2f}% | 🟢 ≥53% = EDGE | 🟡 ≥{break_even:.1f}% = MARGINAL")
    print(f"{'='*90}")
    print(f"  {'Symbol':<12} {'Ticks':>10} {'Days':>5} {'Signals':>8} {'Filt':>6} "
          f"{'RawWR':>7} {'FiltWR':>7} {'HSWR':>7} {'BestH':>6} {'BhWR':>6} {'Status'}")
    print(f"  {'-'*88}")

    edge_found = []
    marginal = []

    for r in ok_results:
        mk = "🔴"
        if r["wr_filt"] >= 53:
            mk = "🟢 EDGE!"
            edge_found.append(r)
        elif r["wr_filt"] >= break_even:
            mk = "🟡 MARGINAL"
            marginal.append(r)
        elif r["wr_raw"] >= break_even:
            mk = "🟠"

        print(f"  {r['symbol']:<12} {r['ticks']:>10,} {r['days']:>5} {r['signals']:>8,} "
              f"{r.get('signals_filt', 0):>6,} "
              f"{r['wr_raw']:>6.1f}% {r['wr_filt']:>6.1f}% {r['wr_high_score']:>6.1f}% "
              f"{r['best_hour']:>4}h {r['best_hour_wr']:>5.1f}% {mk}")

    skipped = [r for r in results if r["status"] != "ok"]
    if skipped:
        print(f"\n  Skipped: {', '.join(r['symbol'] for r in skipped)}")

    print(f"\n{'='*90}")
    if edge_found:
        print(f"  🟢 INSTRUMENTS WITH EDGE (WR ≥ 53%):")
        for r in edge_found:
            print(f"     → {r['symbol']}: Filtered WR = {r['wr_filt']:.1f}% ({r.get('signals_filt', 0):,} signals)")
        print(f"\n  ➡ NEXT: Run full 54-strategy backtest on these!")
    elif marginal:
        print(f"  🟡 MARGINAL INSTRUMENTS (WR ≥ {break_even:.1f}%):")
        for r in marginal:
            print(f"     → {r['symbol']}: Filtered WR = {r['wr_filt']:.1f}%")
        print(f"\n  ⚠ Borderline — full backtest may reveal if edge holds")
    else:
        print(f"  🔴 NO INSTRUMENTS WITH POSITIVE EDGE FOUND")
        print(f"  JumpMomentum strategy has no edge on any Rise/Fall instrument")
        print(f"  ➡ NEXT STEP: Test different contract types (Digits, Over/Under, etc.)")
    print(f"{'='*90}")

    # Save CSV
    output_csv = Path("logs/instrument_scan.csv")
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["symbol", "ticks", "days", "signals", "signals_filt",
                  "wr_raw", "wr_filt", "wr_high_score", "best_hour", "best_hour_wr",
                  "elapsed", "status"]
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
    print(f"\n  Results saved to {output_csv}")
    print(f"  Total wall time: {total_elapsed/60:.1f} min")


if __name__ == "__main__":
    main()
