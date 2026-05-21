"""FAST Vectorized WR Scanner — tests signal edge on ALL instruments.

Instead of calling generate_jump_momentum_signal() per position (slow),
this vectorizes the 21-vote system across all positions at once using
the pre-computed indicator DataFrame.

Usage:
    python scan_fast.py [--symbols SYM1 SYM2 ...] [--payout 0.953]
"""
from __future__ import annotations

import csv
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from strategy import calculate_tick_indicators

# ── Constants ──────────────────────────────────────────────────────────
INDICATOR_WINDOW = 300
LOOKAHEAD = 5
MIN_VOTES = 7
MIN_CONFIDENCE = 0.60
CHUNK_SIZE = 120_000  # larger chunks for vectorized (no per-row call)

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


def load_ticks(path: Path) -> list[dict]:
    """Load tick data from CSV."""
    ticks = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticks.append({"epoch": int(row["epoch"]), "quote": float(row["quote"])})
    return ticks


def _safe_col(df: pd.DataFrame, name: str, default: float = 0.0) -> pd.Series:
    """Get column as float series with NaN→default."""
    if name in df.columns:
        return pd.to_numeric(df[name], errors="coerce").fillna(default)
    return pd.Series(default, index=df.index)


def vectorized_votes(df: pd.DataFrame, quotes_arr: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Compute up_votes and dn_votes for ALL rows at once. Returns (up, dn) arrays."""
    n = len(df)
    up = np.zeros(n, dtype=np.int8)
    dn = np.zeros(n, dtype=np.int8)

    # Pre-extract all indicator columns
    vel = _safe_col(df, "price_velocity").values
    accel = _safe_col(df, "price_acceleration").values
    curv_z = _safe_col(df, "curvature_zscore").values
    int_div = _safe_col(df, "integral_momentum_div").values
    energy = _safe_col(df, "derivative_energy").values
    bayes_up = _safe_col(df, "bayesian_prob_up", 0.5).values
    kalman_z = _safe_col(df, "kalman_residual_zscore").values
    hurst = _safe_col(df, "hurst_exponent", 0.5).values
    markov_up = _safe_col(df, "markov_p_up_given_up", 0.5).values
    markov_dn = _safe_col(df, "markov_p_down_given_down", 0.5).values
    imbalance = _safe_col(df, "tick_imbalance").values
    shannon = _safe_col(df, "shannon_entropy", 1.0).values
    renyi = _safe_col(df, "renyi_entropy", 0.5).values
    mi = _safe_col(df, "mi_flow").values
    wavelet = _safe_col(df, "wavelet_energy_ratio", 0.5).values
    fisher = _safe_col(df, "fisher_information").values
    exhaust = _safe_col(df, "trend_exhaustion").values
    vel_z = _safe_col(df, "velocity_zscore").values
    accel_z = _safe_col(df, "acceleration_zscore").values

    # V1: Momentum majority (last MOM_LOOKBACK ticks)
    # Count up-ticks in rolling window
    tick_up = (quotes_arr[1:] > quotes_arr[:-1]).astype(np.float32)
    tick_up = np.concatenate([[0.0], tick_up])
    # Rolling sum of last MOM_LOOKBACK
    cum = np.cumsum(tick_up)
    rolling_up = np.zeros(n)
    rolling_up[MOM_LOOKBACK:] = cum[MOM_LOOKBACK:] - cum[:-MOM_LOOKBACK]
    rolling_dn = MOM_LOOKBACK - rolling_up
    mask_v1_up = rolling_up >= MOM_LOOKBACK * 0.6
    mask_v1_dn = rolling_dn >= MOM_LOOKBACK * 0.6
    up += mask_v1_up.astype(np.int8)
    dn += mask_v1_dn.astype(np.int8)

    # V2: EMA crossover — approximate with exponential moving averages
    ema_f = pd.Series(quotes_arr).ewm(span=EMA_FAST, adjust=False).mean().values
    ema_s = pd.Series(quotes_arr).ewm(span=EMA_SLOW, adjust=False).mean().values
    fast_above = ema_f > ema_s
    fast_above_prev = np.roll(fast_above, 1)
    fast_above_prev[0] = fast_above[0]
    cross_up = fast_above & ~fast_above_prev
    cross_dn = ~fast_above & fast_above_prev
    # Gap votes when no cross
    gap_pct = np.abs(ema_f - ema_s) / np.maximum(ema_s, 1e-10) * 100
    gap_up = ~cross_up & ~cross_dn & (gap_pct > 0.005) & (ema_f > ema_s)
    gap_dn = ~cross_up & ~cross_dn & (gap_pct > 0.005) & (ema_f < ema_s)
    up += (cross_up | gap_up).astype(np.int8)
    dn += (cross_dn | gap_dn).astype(np.int8)

    # V3: EMA alignment
    up += (quotes_arr > ema_s).astype(np.int8)
    dn += (quotes_arr < ema_s).astype(np.int8)

    # V4: Short momentum (last 3 ticks)
    rolling_up_short = np.zeros(n)
    rolling_up_short[SHORT_MOM:] = cum[SHORT_MOM:] - cum[:-SHORT_MOM]
    rolling_dn_short = SHORT_MOM - rolling_up_short
    up += (rolling_up_short >= 2).astype(np.int8)
    dn += (rolling_dn_short >= 2).astype(np.int8)

    # V5: Reversal after streak (5+ of 7 same → expect reversal)
    rolling_up_rev = np.zeros(n)
    rolling_up_rev[REV_LOOKBACK:] = cum[REV_LOOKBACK:] - cum[:-REV_LOOKBACK]
    rolling_dn_rev = REV_LOOKBACK - rolling_up_rev
    up += (rolling_dn_rev >= REV_LOOKBACK - 2).astype(np.int8)  # oversold → bounce
    dn += (rolling_up_rev >= REV_LOOKBACK - 2).astype(np.int8)  # overbought → drop

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
    up += (reverting & (vel < 0)).astype(np.int8)  # mean-reverting: counter
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

    # V20: Calm market — simplified (skip EMA recalc, use V3 alignment)
    calm = (energy < ENERGY_CALM) & (hurst < 0.5)
    up += (calm & (quotes_arr > ema_s)).astype(np.int8)
    dn += (calm & (quotes_arr < ema_s)).astype(np.int8)

    # V21: Trend exhaustion
    up += (exhaust < -EXHAUSTION_EXTREME).astype(np.int8)  # oversold → rise
    dn += (exhaust > EXHAUSTION_EXTREME).astype(np.int8)   # overbought → fall

    return up, dn


def scan_instrument(tick_path: Path) -> dict:
    """Scan instrument for signal edge using vectorized votes."""
    symbol = tick_path.stem.replace("ticks_", "").replace("_max", "")
    ticks = load_ticks(tick_path)
    n = len(ticks)

    if n < 5000:
        print(f"  ⏭  {symbol:12s} — only {n:,} ticks, skipping")
        return {"symbol": symbol, "ticks": n, "status": "insufficient"}

    days = (ticks[-1]["epoch"] - ticks[0]["epoch"]) / 86400
    print(f"  🔍 {symbol:12s} — {n:,} ticks ({days:.1f} days)...", end="", flush=True)

    t0 = time.time()
    all_won = []
    all_imb = []
    all_score = []
    all_hour = []

    chunk_starts = list(range(0, n, CHUNK_SIZE))

    for ci, chunk_start in enumerate(chunk_starts):
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

        # Signal range within chunk
        sig_start = max(chunk_start, actual_start + INDICATOR_WINDOW) - offset
        sig_end = min(chunk_end, n - LOOKAHEAD - 1) - offset
        if sig_start > sig_end:
            continue

        # Safety block mask
        lyap = _safe_col(chunk_df, "lyapunov_exponent").values
        ret_z = _safe_col(chunk_df, "return_zscore").values
        cusum = _safe_col(chunk_df, "cusum_score").values
        jerk_z = _safe_col(chunk_df, "jerk_zscore").values
        tail_dep = _safe_col(chunk_df, "tail_dependence").values

        safe = (
            (lyap <= LYAP_MAX) &
            (np.abs(ret_z) <= RET_Z_MAX) &
            (cusum <= CUSUM_MAX) &
            (np.abs(jerk_z) <= JERK_Z_MAX) &
            (tail_dep <= TAIL_DEP_MAX)
        )

        # Vectorized votes
        up_votes, dn_votes = vectorized_votes(chunk_df, quotes_arr)

        # Signal conditions
        total_votes = up_votes + dn_votes
        winning_votes = np.maximum(up_votes, dn_votes)
        confidence = np.where(total_votes > 0, winning_votes / total_votes, 0.0)
        has_signal = (winning_votes >= MIN_VOTES) & (confidence >= MIN_CONFIDENCE) & (total_votes > 0)

        # Direction
        is_call = has_signal & (up_votes > dn_votes)
        is_put = has_signal & (dn_votes > up_votes)

        # Apply safety + range
        valid_range = np.zeros(len(chunk_df), dtype=bool)
        valid_range[sig_start:sig_end + 1] = True
        eligible = safe & valid_range & has_signal

        imb = _safe_col(chunk_df, "tick_imbalance").values

        # Check win/loss for eligible positions
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
        print(f" only {total} signals, insufficient [{elapsed:.0f}s]")
        return {"symbol": symbol, "ticks": n, "signals": total, "status": "few_signals"}

    won_arr = np.array(all_won)
    imb_arr = np.array(all_imb)
    score_arr = np.array(all_score)
    hour_arr = np.array(all_hour)

    # Raw WR
    wr_raw = won_arr.sum() / total * 100

    # Filtered WR (|imb| >= 6)
    filt_mask = np.abs(imb_arr) >= 6
    n_filt = filt_mask.sum()
    wr_filt = won_arr[filt_mask].sum() / n_filt * 100 if n_filt > 0 else 0.0

    # High-score WR (score >= 10)
    hs_mask = score_arr >= 10
    n_hs = hs_mask.sum()
    wr_hs = won_arr[hs_mask].sum() / n_hs * 100 if n_hs > 0 else 0.0

    # Best hour
    best_h, best_h_wr = 0, 0.0
    for h in range(24):
        h_mask = hour_arr == h
        if h_mask.sum() >= 20:
            h_wr = won_arr[h_mask].sum() / h_mask.sum() * 100
            if h_wr > best_h_wr:
                best_h_wr = h_wr
                best_h = h

    result = {
        "symbol": symbol, "ticks": n, "days": round(days, 1),
        "signals": total, "signals_filt": int(n_filt),
        "wr_raw": round(wr_raw, 2), "wr_filt": round(wr_filt, 2),
        "wr_high_score": round(wr_hs, 2),
        "best_hour": best_h, "best_hour_wr": round(best_h_wr, 1),
        "elapsed": round(elapsed, 1), "status": "ok",
    }

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
    logging.getLogger().setLevel(logging.WARNING)
    logging.getLogger("Pegasus").setLevel(logging.CRITICAL)

    payout = 0.953
    break_even = 1 / (1 + payout) * 100

    # Parse args
    symbols = None
    if len(sys.argv) > 1:
        if sys.argv[1] == "--symbols":
            symbols = sys.argv[2:]
        else:
            symbols = sys.argv[1:]

    print(f"{'='*90}")
    print(f"  FAST INSTRUMENT SCANNER — Pegasus JumpMomentum (Vectorized)")
    print(f"{'='*90}")
    print(f"  Payout: {payout*100:.1f}% | Break-even WR: {break_even:.2f}%")
    print(f"  Votes: min={MIN_VOTES}, conf={MIN_CONFIDENCE}")
    print(f"  Lookahead: {LOOKAHEAD} ticks")
    print(f"{'='*90}\n")

    data_dir = Path("data")
    tick_files = sorted(data_dir.glob("ticks_*_max.csv"))
    if symbols:
        tick_files = [f for f in tick_files if any(s in f.stem for s in symbols)]

    if not tick_files:
        print("  No tick data files found!")
        sys.exit(1)

    print(f"  Found {len(tick_files)} instrument data files\n")

    results = []
    for tf in tick_files:
        result = scan_instrument(tf)
        results.append(result)

    # Summary
    ok_results = [r for r in results if r["status"] == "ok"]
    ok_results.sort(key=lambda r: -r["wr_filt"])

    print(f"\n{'='*90}")
    print(f"  RESULTS RANKING (by filtered WR)")
    print(f"  Break-even: {break_even:.2f}% | 🟢 ≥53% = EDGE | 🟡 ≥{break_even:.1f}% = MARGINAL")
    print(f"{'='*90}")
    print(f"  {'Symbol':<12} {'Ticks':>10} {'Days':>5} {'Signals':>8} {'Filt':>6} {'RawWR':>7} {'FiltWR':>7} {'HSWR':>7} {'BestH':>6} {'BhWR':>6} {'Status'}")
    print(f"  {'-'*88}")

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

    skipped = [r for r in results if r["status"] != "ok"]
    if skipped:
        print(f"\n  Skipped: {', '.join(r['symbol'] for r in skipped)}")

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
        print(f"\n  ⚠ Borderline — full backtest may reveal if edge holds")
    else:
        print(f"  🔴 NO INSTRUMENTS WITH POSITIVE EDGE FOUND")
        print(f"  JumpMomentum strategy has no edge on any tested instrument")
        print(f"  Consider: different contract types, different signal system")
    print(f"{'='*90}")

    # Save CSV
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
