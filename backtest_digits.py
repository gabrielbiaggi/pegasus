"""Comprehensive Digits Contract Backtest — scans ALL instruments.

Tests contract types: DIGITEVEN, DIGITODD, DIGITOVER X, DIGITMATCH X
Tests strategies:
  - baseline (random/unconditional)
  - markov1/2/3 (predict from last N digits)
  - freq_rebal (bet underrepresented digit in window)
  - streak_reversal (bet against even/odd streaks)
  - autocorr (exploit digit autocorrelation)

Money management: flat, fib2g, fib3g, soros1
All results saved to SQLite (logs/results.db).

Usage:
    python backtest_digits.py                           # scan all instruments
    python backtest_digits.py --instrument 1HZ10V       # single instrument
    python backtest_digits.py --deep 1HZ10V             # full money mgmt matrix
"""
from __future__ import annotations

import argparse
import glob
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

import results_db

# ── Constants ──────────────────────────────────────────────────────────
STARTING_BALANCE = 50.0
BASE_STAKE = 0.35
FIB_SEQUENCE = [1, 1, 2, 3, 5, 8, 13, 21]

# Payout rates by contract type (estimated with ~2.5% house edge)
# Format: { contract_key: (base_probability, payout_rate) }
PAYOUTS = {
    "DIGITEVEN": (0.50, 0.953),      # P=50%, payout 95.3%
    "DIGITODD": (0.50, 0.953),
    "DIGITOVER_0": (0.90, 0.085),    # P=90%, low payout
    "DIGITOVER_1": (0.80, 0.219),
    "DIGITOVER_2": (0.70, 0.393),
    "DIGITOVER_3": (0.60, 0.627),
    "DIGITOVER_4": (0.50, 0.953),
    "DIGITOVER_5": (0.40, 1.438),
    "DIGITOVER_6": (0.30, 2.250),
    "DIGITOVER_7": (0.20, 3.875),
    "DIGITOVER_8": (0.10, 8.765),
    "DIGITMATCH": (0.10, 8.765),     # P=10%, ~876% payout
    "DIGITDIFF": (0.90, 0.085),      # P=90%, ~8.5% payout
}


# ── Decimal padding ───────────────────────────────────────────────────
def detect_pip_decimals(prices_str: pd.Series) -> int:
    """Detect the number of decimal places the instrument uses."""
    return prices_str.str.split(".").str[1].str.len().max()


def extract_last_digits(prices_str: pd.Series, pip_decimals: int) -> np.ndarray:
    """Extract last digit from string prices, properly handling trailing zeros."""
    def _last_digit(p: str) -> int:
        parts = p.split(".")
        if len(parts) == 1:
            return int(p[-1])
        decimals = parts[1]
        # Pad with zeros to correct length
        padded = decimals.ljust(pip_decimals, "0")
        return int(padded[-1])
    return prices_str.apply(_last_digit).values


# ── Digit Analysis ────────────────────────────────────────────────────
def analyze_digit_distribution(digits: np.ndarray, instrument: str,
                               pip_decimals: int) -> dict:
    """Full statistical analysis of digit distribution."""
    n = len(digits)
    counts = np.bincount(digits, minlength=10)
    pcts = counts / n * 100

    # Chi-squared test for uniformity
    expected = np.full(10, n / 10)
    chi2, p_value = stats.chisquare(counts, expected)

    # Even/odd
    even_pct = np.mean(digits % 2 == 0) * 100

    # Max bias
    deviations = np.abs(pcts - 10.0)
    max_bias = deviations.max()
    max_biased_digit = int(deviations.argmax())

    # Autocorrelation lag-1
    d_centered = digits - digits.mean()
    if d_centered.std() > 0:
        autocorr = np.corrcoef(d_centered[:-1], d_centered[1:])[0, 1]
    else:
        autocorr = 0.0

    result = {
        "total_ticks": n,
        "pip_decimals": pip_decimals,
        "digit_pcts": pcts.tolist(),
        "chi_squared": chi2,
        "chi_p_value": p_value,
        "even_pct": even_pct,
        "max_bias": max_bias,
        "max_biased_digit": max_biased_digit,
        "autocorr_lag1": autocorr,
    }

    # Save to DB
    results_db.save_digit_analysis(
        instrument=instrument,
        total_ticks=n,
        pip_decimals=pip_decimals,
        digit_pcts=pcts.tolist(),
        chi_sq=chi2,
        chi_p=p_value,
        even_pct=even_pct,
        max_bias=max_bias,
        max_biased_digit=max_biased_digit,
        autocorr_lag1=autocorr,
    )
    return result


# ── Prediction Strategies ─────────────────────────────────────────────

def strategy_baseline(digits: np.ndarray) -> np.ndarray:
    """No prediction — always returns True (unconditional bet every tick)."""
    return np.ones(len(digits), dtype=bool)


def strategy_markov1_even(digits: np.ndarray) -> np.ndarray:
    """Predict even if last digit was odd (mean reversion on even/odd)."""
    signals = np.zeros(len(digits), dtype=bool)
    for i in range(1, len(digits)):
        signals[i] = digits[i - 1] % 2 == 1  # last was odd → bet even
    return signals


def strategy_markov1_same(digits: np.ndarray) -> np.ndarray:
    """Predict same parity as last digit (momentum on even/odd)."""
    signals = np.zeros(len(digits), dtype=bool)
    for i in range(1, len(digits)):
        signals[i] = True  # always signal, but the "prediction" is same parity
    return signals


def strategy_streak_reversal(digits: np.ndarray, streak_len: int = 3) -> np.ndarray:
    """After N consecutive same-parity digits, bet the opposite parity."""
    signals = np.zeros(len(digits), dtype=bool)
    for i in range(streak_len, len(digits)):
        parities = digits[i - streak_len:i] % 2
        if np.all(parities == parities[0]):
            signals[i] = True
    return signals


def strategy_freq_rebal(digits: np.ndarray, window: int = 50) -> tuple[np.ndarray, np.ndarray]:
    """Bet on the most underrepresented digit in rolling window.
    Returns (signals, predicted_digit)."""
    n = len(digits)
    signals = np.zeros(n, dtype=bool)
    predicted = np.zeros(n, dtype=int)
    for i in range(window, n):
        w = digits[i - window:i]
        counts = np.bincount(w, minlength=10)
        # Most underrepresented digit
        least_common = int(counts.argmin())
        signals[i] = True
        predicted[i] = least_common
    return signals, predicted


def strategy_markov_transition(digits: np.ndarray, order: int = 1) -> tuple[np.ndarray, np.ndarray]:
    """Build Markov transition matrix of given order and predict most likely next digit.
    Returns (signals, predicted_digit)."""
    n = len(digits)
    signals = np.zeros(n, dtype=bool)
    predicted = np.zeros(n, dtype=int)

    # Build transition counts
    transitions = defaultdict(lambda: np.zeros(10, dtype=int))
    for i in range(order, n):
        key = tuple(digits[i - order:i])
        transitions[key][digits[i]] += 1

    # Now predict using accumulated knowledge up to each point
    # (Walk-forward: only use data before current point)
    live_trans = defaultdict(lambda: np.zeros(10, dtype=int))
    for i in range(order, n):
        key = tuple(digits[i - order:i])
        if live_trans[key].sum() >= order * 10:  # need enough data
            probs = live_trans[key] / live_trans[key].sum()
            best_digit = int(probs.argmax())
            best_prob = probs[best_digit]
            if best_prob > 0.12:  # > 12% threshold (above 10% uniform)
                signals[i] = True
                predicted[i] = best_digit
        # Update transition counts
        live_trans[key][digits[i]] += 1

    return signals, predicted


def strategy_autocorr_even(digits: np.ndarray, window: int = 100) -> np.ndarray:
    """Use rolling autocorrelation to decide even/odd bet direction."""
    n = len(digits)
    signals = np.zeros(n, dtype=bool)
    parities = (digits % 2).astype(float)
    for i in range(window, n):
        w = parities[i - window:i]
        centered = w - w.mean()
        if centered.std() > 0:
            ac = np.corrcoef(centered[:-1], centered[1:])[0, 1]
            # If positive autocorr → bet same as last; if negative → bet opposite
            signals[i] = abs(ac) > 0.05
    return signals


# ── Backtest Engine ───────────────────────────────────────────────────

@dataclass
class TradeResult:
    win: bool
    payout_rate: float
    stake: float
    profit: float


def check_win(digit: int, contract_type: str, target: int = 0) -> bool:
    """Check if a digit wins for a given contract type."""
    if contract_type == "DIGITEVEN":
        return digit % 2 == 0
    elif contract_type == "DIGITODD":
        return digit % 2 == 1
    elif contract_type.startswith("DIGITOVER_"):
        threshold = int(contract_type.split("_")[1])
        return digit > threshold
    elif contract_type == "DIGITMATCH":
        return digit == target
    elif contract_type == "DIGITDIFF":
        return digit != target
    return False


def run_money_mgmt(outcomes: list[bool], payout_rate: float,
                   mode: str, base_stake: float = BASE_STAKE,
                   starting_bal: float = STARTING_BALANCE) -> dict:
    """Run money management simulation on a sequence of win/loss outcomes."""
    balance = starting_bal
    max_bal = balance
    min_bal = balance
    wins = 0
    losses = 0
    total_won = 0.0
    total_lost = 0.0
    bankrupt = False

    # Money management state
    consec_losses = 0
    fib_idx = 0
    soros_step = 0

    for won in outcomes:
        # Calculate stake based on mode
        if mode == "flat":
            stake = base_stake
        elif mode == "fib2g":
            stake = base_stake * FIB_SEQUENCE[min(fib_idx, len(FIB_SEQUENCE) - 1)]
        elif mode == "fib3g":
            stake = base_stake * FIB_SEQUENCE[min(fib_idx, len(FIB_SEQUENCE) - 1)]
        elif mode == "soros1":
            if soros_step == 0:
                stake = base_stake
            else:
                stake = base_stake + (base_stake * payout_rate * soros_step)
        elif mode == "classic2g":
            stake = base_stake * (2 ** min(consec_losses, 7))
        else:
            stake = base_stake

        stake = min(stake, balance)
        if stake < 0.01 or balance < base_stake * 0.5:
            bankrupt = True
            break

        if won:
            profit = stake * payout_rate
            balance += profit
            total_won += profit
            wins += 1
            consec_losses = 0
            fib_idx = 0
            # Soros: advance step up to max
            if mode == "soros1" and soros_step < 3:
                soros_step += 1
            else:
                soros_step = 0
        else:
            balance -= stake
            total_lost += stake
            losses += 1
            consec_losses += 1
            soros_step = 0
            # Fib advance
            if mode == "fib2g" and consec_losses <= 2:
                fib_idx = min(fib_idx + 1, len(FIB_SEQUENCE) - 1)
            elif mode == "fib2g":
                fib_idx = 0
            elif mode == "fib3g" and consec_losses <= 3:
                fib_idx = min(fib_idx + 1, len(FIB_SEQUENCE) - 1)
            elif mode == "fib3g":
                fib_idx = 0

        max_bal = max(max_bal, balance)
        min_bal = min(min_bal, balance)

    wr = wins / (wins + losses) if (wins + losses) > 0 else 0
    dd = (max_bal - min_bal) / max_bal * 100 if max_bal > 0 else 0
    pf = total_won / total_lost if total_lost > 0 else float("inf")

    return {
        "final_balance": balance,
        "max_balance": max_bal,
        "min_balance": min_bal,
        "max_drawdown_pct": dd,
        "total_trades": wins + losses,
        "wins": wins,
        "losses": losses,
        "win_rate": wr,
        "profit_factor": pf,
        "bankrupt": bankrupt,
    }


# ── Main Scan Logic ──────────────────────────────────────────────────

def scan_instrument(filepath: str, instrument: str, deep: bool = False) -> dict:
    """Run complete Digits analysis on one instrument."""
    t0 = time.time()
    print(f"\n{'='*70}")
    print(f"  {instrument}")
    print(f"{'='*70}")

    # Load data
    df = pd.read_csv(filepath, dtype={"quote": str})
    prices_str = df["quote"]
    pip_dec = detect_pip_decimals(prices_str)
    digits = extract_last_digits(prices_str, pip_dec)
    n = len(digits)
    print(f"  Ticks: {n:,} | Pip decimals: {pip_dec}")

    # 1. Distribution analysis
    da = analyze_digit_distribution(digits, instrument, pip_dec)
    print(f"  Chi²: {da['chi_squared']:.2f} (p={da['chi_p_value']:.4f})")
    print(f"  Even: {da['even_pct']:.2f}% | Autocorr(1): {da['autocorr_lag1']:.4f}")
    print(f"  Max bias: digit {da['max_biased_digit']} at {10 + da['max_bias']:.2f}% (deviation {da['max_bias']:.2f}%)")
    print(f"  Digits: {' '.join(f'{d}:{p:.1f}%' for d, p in enumerate(da['digit_pcts']))}")

    # 2. Strategy scans
    results = {}
    strategies_to_test = [
        # (name, contract_type, description)
        ("baseline", "DIGITEVEN", "unconditional even bet"),
        ("markov1_reversal", "DIGITEVEN", "bet even after odd (mean reversion)"),
        ("streak3_reversal", "DIGITEVEN", "bet opposite after 3-streak"),
        ("streak5_reversal", "DIGITEVEN", "bet opposite after 5-streak"),
    ]

    print(f"\n  {'Strategy':<25} {'Contract':<15} {'Signals':>8} {'Wins':>8} {'WR%':>7} {'BE%':>7} {'Edge':>7}")
    print(f"  {'-'*25} {'-'*15} {'-'*8} {'-'*8} {'-'*7} {'-'*7} {'-'*7}")

    # --- DIGITEVEN strategies ---
    payout_even = PAYOUTS["DIGITEVEN"][1]
    be_even = 1 / (1 + payout_even) * 100  # break-even WR

    # Baseline DIGITEVEN
    even_wins = np.sum(digits[1:] % 2 == 0)  # predict next tick
    even_total = len(digits) - 1
    even_wr = even_wins / even_total * 100
    edge = even_wr - be_even
    print(f"  {'baseline':<25} {'DIGITEVEN':<15} {even_total:>8,} {even_wins:>8,} {even_wr:>7.2f} {be_even:>7.2f} {edge:>+7.2f}")
    results_db.save_scan(instrument, "DIGITEVEN", "baseline", n, even_total,
                         int(even_wins), even_total - int(even_wins),
                         even_wr, even_wr, be_even, payout_even)

    # Markov1 reversal: bet even when last was odd
    sig_m1 = strategy_markov1_even(digits)
    sig_positions = np.where(sig_m1[:-1])[0]  # signal at i → outcome at i (predict digit[i])
    if len(sig_positions) > 0:
        # Actually for digits, we signal at tick i and the CONTRACT resolves at tick i
        # The prediction is about digit[i], and we can only see digits up to i-1
        m1_outcomes = digits[sig_positions]
        m1_wins = np.sum(m1_outcomes % 2 == 0)  # bet even
        m1_wr = m1_wins / len(m1_outcomes) * 100
        edge = m1_wr - be_even
        print(f"  {'markov1_reversal':<25} {'DIGITEVEN':<15} {len(m1_outcomes):>8,} {m1_wins:>8,} {m1_wr:>7.2f} {be_even:>7.2f} {edge:>+7.2f}")
        results_db.save_scan(instrument, "DIGITEVEN", "markov1_reversal", n,
                             len(m1_outcomes), int(m1_wins),
                             len(m1_outcomes) - int(m1_wins),
                             m1_wr, m1_wr, be_even, payout_even)

    # Streak reversal (3)
    sig_s3 = strategy_streak_reversal(digits, 3)
    sig_positions = np.where(sig_s3)[0]
    if len(sig_positions) > 100:
        # After 3 even streak → bet odd, and vice versa
        last_parities = digits[sig_positions - 1] % 2
        # Predict opposite of the streak
        predicted_even = last_parities == 1  # streak was odd → bet even
        actual_even = digits[sig_positions] % 2 == 0
        s3_wins = np.sum(predicted_even == actual_even)
        s3_total = len(sig_positions)
        s3_wr = s3_wins / s3_total * 100
        edge = s3_wr - be_even
        print(f"  {'streak3_reversal':<25} {'DIGITEVEN':<15} {s3_total:>8,} {s3_wins:>8,} {s3_wr:>7.2f} {be_even:>7.2f} {edge:>+7.2f}")
        results_db.save_scan(instrument, "DIGITEVEN", "streak3_reversal", n,
                             s3_total, int(s3_wins), s3_total - int(s3_wins),
                             s3_wr, s3_wr, be_even, payout_even)

    # Streak reversal (5)
    sig_s5 = strategy_streak_reversal(digits, 5)
    sig_positions = np.where(sig_s5)[0]
    if len(sig_positions) > 50:
        last_parities = digits[sig_positions - 1] % 2
        predicted_even = last_parities == 1
        actual_even = digits[sig_positions] % 2 == 0
        s5_wins = np.sum(predicted_even == actual_even)
        s5_total = len(sig_positions)
        s5_wr = s5_wins / s5_total * 100
        edge = s5_wr - be_even
        print(f"  {'streak5_reversal':<25} {'DIGITEVEN':<15} {s5_total:>8,} {s5_wins:>8,} {s5_wr:>7.2f} {be_even:>7.2f} {edge:>+7.2f}")
        results_db.save_scan(instrument, "DIGITEVEN", "streak5_reversal", n,
                             s5_total, int(s5_wins), s5_total - int(s5_wins),
                             s5_wr, s5_wr, be_even, payout_even)

    # --- DIGITOVER strategies ---
    for threshold in [3, 4, 5]:
        ct = f"DIGITOVER_{threshold}"
        base_p, payout = PAYOUTS[ct]
        be = 1 / (1 + payout) * 100

        # Baseline
        over_wins = np.sum(digits[1:] > threshold)
        over_total = len(digits) - 1
        over_wr = over_wins / over_total * 100
        edge = over_wr - be
        print(f"  {'baseline':<25} {ct:<15} {over_total:>8,} {over_wins:>8,} {over_wr:>7.2f} {be:>7.2f} {edge:>+7.2f}")
        results_db.save_scan(instrument, ct, "baseline", n, over_total,
                             int(over_wins), over_total - int(over_wins),
                             over_wr, over_wr, be, payout)

    # --- DIGITMATCH strategies ---
    payout_match = PAYOUTS["DIGITMATCH"][1]
    be_match = 1 / (1 + payout_match) * 100

    # Baseline: for each digit, what's the hit rate?
    for target_d in range(10):
        match_wins = np.sum(digits[1:] == target_d)
        match_total = len(digits) - 1
        match_wr = match_wins / match_total * 100
        edge = match_wr - be_match
        if target_d in (0, 5, 9):  # print only a few
            print(f"  {'baseline':<25} {'MATCH_' + str(target_d):<15} {match_total:>8,} {match_wins:>8,} {match_wr:>7.2f} {be_match:>7.2f} {edge:>+7.2f}")
        results_db.save_scan(instrument, f"DIGITMATCH_{target_d}", "baseline", n,
                             match_total, int(match_wins),
                             match_total - int(match_wins),
                             match_wr, match_wr, be_match, payout_match)

    # --- Markov transition (order 1, 2) for DIGITMATCH ---
    for order in (1, 2):
        sig, pred = strategy_markov_transition(digits, order=order)
        sig_positions = np.where(sig[1:])[0] + 1  # shift for next-tick prediction
        if len(sig_positions) > 100:
            actual = digits[sig_positions]
            predicted_digits = pred[sig_positions]
            mk_wins = np.sum(actual == predicted_digits)
            mk_total = len(sig_positions)
            mk_wr = mk_wins / mk_total * 100
            edge = mk_wr - be_match
            name = f"markov{order}_match"
            print(f"  {name:<25} {'DIGITMATCH':<15} {mk_total:>8,} {mk_wins:>8,} {mk_wr:>7.2f} {be_match:>7.2f} {edge:>+7.2f}")
            results_db.save_scan(instrument, "DIGITMATCH", name, n,
                                 mk_total, int(mk_wins), mk_total - int(mk_wins),
                                 mk_wr, mk_wr, be_match, payout_match)

    # --- Frequency rebalancing for DIGITMATCH ---
    for window in (30, 50, 100):
        sig, pred = strategy_freq_rebal(digits, window=window)
        sig_positions = np.where(sig[1:])[0] + 1
        if len(sig_positions) > 100:
            actual = digits[sig_positions]
            predicted_digits = pred[sig_positions]
            fr_wins = np.sum(actual == predicted_digits)
            fr_total = len(sig_positions)
            fr_wr = fr_wins / fr_total * 100
            edge = fr_wr - be_match
            name = f"freq_rebal_w{window}"
            print(f"  {name:<25} {'DIGITMATCH':<15} {fr_total:>8,} {fr_wins:>8,} {fr_wr:>7.2f} {be_match:>7.2f} {edge:>+7.2f}")
            results_db.save_scan(instrument, "DIGITMATCH", name, n,
                                 fr_total, int(fr_wins), fr_total - int(fr_wins),
                                 fr_wr, fr_wr, be_match, payout_match)

    # 3. Deep backtest (money management) if requested
    if deep:
        print(f"\n  --- DEEP BACKTEST (money management) ---")
        _deep_backtest(digits, instrument, payout_even, be_even)

    elapsed = time.time() - t0
    print(f"\n  Done in {elapsed:.1f}s")
    return results


def _deep_backtest(digits: np.ndarray, instrument: str,
                   payout: float, be_wr: float) -> None:
    """Run full money management matrix on DIGITEVEN baseline."""
    # Generate outcomes: predict next digit is even
    outcomes = [(digits[i] % 2 == 0) for i in range(1, len(digits))]

    modes = ["flat", "fib2g", "fib3g", "soros1", "classic2g"]
    ct = "DIGITEVEN"

    print(f"  {'Mode':<15} {'Trades':>8} {'WR%':>7} {'Final$':>10} {'MaxDD%':>8} {'Bankrupt':>9}")
    print(f"  {'-'*15} {'-'*8} {'-'*7} {'-'*10} {'-'*8} {'-'*9}")

    for mode in modes:
        r = run_money_mgmt(outcomes, payout, mode)
        status = "💀 YES" if r["bankrupt"] else f"${r['final_balance']:.2f}"
        print(f"  {mode:<15} {r['total_trades']:>8,} {r['win_rate']*100:>7.2f} "
              f"{r['final_balance']:>10.2f} {r['max_drawdown_pct']:>8.1f} "
              f"{'💀' if r['bankrupt'] else '✅'}")

        results_db.save_backtest(
            instrument=instrument, contract_type=ct,
            strategy_name="baseline", money_mgmt=mode,
            filter_desc="nofilter", cooldown=0,
            starting_balance=STARTING_BALANCE,
            final_balance=r["final_balance"],
            max_balance=r["max_balance"], min_balance=r["min_balance"],
            max_drawdown_pct=r["max_drawdown_pct"],
            total_trades=r["total_trades"], wins=r["wins"],
            losses=r["losses"], win_rate=r["win_rate"],
            profit_factor=r["profit_factor"], bankrupt=r["bankrupt"],
            payout_rate=payout, base_stake=BASE_STAKE,
        )

    # Also test DIGITOVER_4 (same payout as even, but different strategy)
    outcomes_over4 = [(digits[i] > 4) for i in range(1, len(digits))]
    ct = "DIGITOVER_4"
    print(f"\n  --- DIGITOVER_4 ---")
    print(f"  {'Mode':<15} {'Trades':>8} {'WR%':>7} {'Final$':>10} {'MaxDD%':>8} {'Bankrupt':>9}")
    print(f"  {'-'*15} {'-'*8} {'-'*7} {'-'*10} {'-'*8} {'-'*9}")
    for mode in modes:
        r = run_money_mgmt(outcomes_over4, payout, mode)
        print(f"  {mode:<15} {r['total_trades']:>8,} {r['win_rate']*100:>7.2f} "
              f"{r['final_balance']:>10.2f} {r['max_drawdown_pct']:>8.1f} "
              f"{'💀' if r['bankrupt'] else '✅'}")
        results_db.save_backtest(
            instrument=instrument, contract_type=ct,
            strategy_name="baseline", money_mgmt=mode,
            filter_desc="nofilter", cooldown=0,
            starting_balance=STARTING_BALANCE,
            final_balance=r["final_balance"],
            max_balance=r["max_balance"], min_balance=r["min_balance"],
            max_drawdown_pct=r["max_drawdown_pct"],
            total_trades=r["total_trades"], wins=r["wins"],
            losses=r["losses"], win_rate=r["win_rate"],
            profit_factor=r["profit_factor"], bankrupt=r["bankrupt"],
            payout_rate=payout, base_stake=BASE_STAKE,
        )

    # Test DIGITMATCH with Markov2
    print(f"\n  --- DIGITMATCH (markov2 prediction) ---")
    payout_match = PAYOUTS["DIGITMATCH"][1]
    sig, pred = strategy_markov_transition(digits, order=2)
    sig_positions = np.where(sig[1:])[0] + 1
    if len(sig_positions) > 100:
        outcomes_match = [(digits[i] == pred[i]) for i in sig_positions]
        print(f"  {'Mode':<15} {'Trades':>8} {'WR%':>7} {'Final$':>10} {'MaxDD%':>8} {'Bankrupt':>9}")
        print(f"  {'-'*15} {'-'*8} {'-'*7} {'-'*10} {'-'*8} {'-'*9}")
        for mode in modes:
            r = run_money_mgmt(outcomes_match, payout_match, mode)
            print(f"  {mode:<15} {r['total_trades']:>8,} {r['win_rate']*100:>7.2f} "
                  f"{r['final_balance']:>10.2f} {r['max_drawdown_pct']:>8.1f} "
                  f"{'💀' if r['bankrupt'] else '✅'}")
            results_db.save_backtest(
                instrument=instrument, contract_type="DIGITMATCH",
                strategy_name="markov2_match", money_mgmt=mode,
                filter_desc="nofilter", cooldown=0,
                starting_balance=STARTING_BALANCE,
                final_balance=r["final_balance"],
                max_balance=r["max_balance"], min_balance=r["min_balance"],
                max_drawdown_pct=r["max_drawdown_pct"],
                total_trades=r["total_trades"], wins=r["wins"],
                losses=r["losses"], win_rate=r["win_rate"],
                profit_factor=r["profit_factor"], bankrupt=r["bankrupt"],
                payout_rate=payout_match, base_stake=BASE_STAKE,
            )


# ── CLI ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Digits Contract Backtest")
    parser.add_argument("--instrument", type=str, help="Single instrument to scan")
    parser.add_argument("--deep", type=str, help="Instrument for deep money mgmt backtest")
    args = parser.parse_args()

    results_db.init_db()

    if args.deep:
        filepath = f"data/ticks_{args.deep}_max.csv"
        if not Path(filepath).exists():
            print(f"ERROR: {filepath} not found")
            sys.exit(1)
        scan_instrument(filepath, args.deep, deep=True)
        return

    if args.instrument:
        filepath = f"data/ticks_{args.instrument}_max.csv"
        if not Path(filepath).exists():
            print(f"ERROR: {filepath} not found")
            sys.exit(1)
        scan_instrument(filepath, args.instrument)
        return

    # Scan ALL instruments
    files = sorted(glob.glob("data/ticks_*_max.csv"))
    if not files:
        print("ERROR: No tick data files found in data/")
        sys.exit(1)

    print(f"\n🎰 DIGITS CONTRACT SCANNER — {len(files)} instruments")
    print(f"   Starting balance: ${STARTING_BALANCE} | Base stake: ${BASE_STAKE}")
    print(f"   Database: {results_db.DB_PATH}")

    t_start = time.time()
    for f in files:
        sym = Path(f).stem.replace("ticks_", "").replace("_max", "")
        scan_instrument(f, sym, deep=True)

    elapsed = time.time() - t_start
    print(f"\n{'='*70}")
    print(f"  COMPLETE — {len(files)} instruments in {elapsed:.1f}s")
    print(f"  Results saved to {results_db.DB_PATH}")
    print(f"{'='*70}")

    # Summary
    _print_summary()


def _print_summary():
    """Print top findings from the database."""
    print(f"\n📊 TOP FINDINGS:")
    print(f"\n  Highest edge (DIGITEVEN):")
    rows = results_db.query(
        """SELECT instrument, strategy_name, filtered_wr, edge_pct
           FROM scan_results
           WHERE contract_type = 'DIGITEVEN'
           ORDER BY edge_pct DESC LIMIT 10"""
    )
    for r in rows:
        print(f"    {r['instrument']:12s} {r['strategy_name']:25s} WR={r['filtered_wr']:.2f}% edge={r['edge_pct']:+.2f}%")

    print(f"\n  Highest edge (DIGITMATCH):")
    rows = results_db.query(
        """SELECT instrument, strategy_name, filtered_wr, edge_pct
           FROM scan_results
           WHERE contract_type = 'DIGITMATCH'
           ORDER BY edge_pct DESC LIMIT 10"""
    )
    for r in rows:
        print(f"    {r['instrument']:12s} {r['strategy_name']:25s} WR={r['filtered_wr']:.2f}% edge={r['edge_pct']:+.2f}%")

    print(f"\n  Non-bankrupt backtests:")
    rows = results_db.query(
        """SELECT instrument, contract_type, strategy_name, money_mgmt,
                  win_rate, final_balance
           FROM backtest_results
           WHERE bankrupt = 0 AND final_balance > starting_balance
           ORDER BY final_balance DESC LIMIT 10"""
    )
    if rows:
        for r in rows:
            print(f"    {r['instrument']:12s} {r['contract_type']:15s} {r['strategy_name']:20s} "
                  f"{r['money_mgmt']:10s} WR={r['win_rate']*100:.1f}% final=${r['final_balance']:.2f}")
    else:
        print(f"    ⚠ NONE — all strategies bankrupt")

    print(f"\n  Strongest digit biases:")
    rows = results_db.query(
        """SELECT instrument, max_biased_digit, max_digit_bias, chi_p_value, even_pct, autocorr_lag1
           FROM digit_analysis
           ORDER BY max_digit_bias DESC LIMIT 10"""
    )
    for r in rows:
        sig = "⚡" if r["chi_p_value"] < 0.05 else "  "
        print(f"    {sig} {r['instrument']:12s} digit {r['max_biased_digit']} bias={r['max_digit_bias']:.2f}% "
              f"chi²_p={r['chi_p_value']:.4f} even={r['even_pct']:.2f}% autocorr={r['autocorr_lag1']:.4f}")


if __name__ == "__main__":
    main()
