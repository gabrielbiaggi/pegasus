#!/usr/bin/env python3
"""
BACKTEST REAL — código idêntico ao bot, amostragem a cada 30 ticks
Tempo estimado: ~2h para 15 dias
"""

import sys
import time
from collections import deque
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from strategy import (
    AccumulatorStrategyConfig,
    calculate_tick_indicators,
    generate_calm_accu_signal,
)

STAKE = 5.0
START_BAL = 50.0
TP_PCT = 0.30
MAX_HOLD = 80
GROWTH_RATE = 0.03
MAX_STAKE = 10.0
SOROS_STEPS = 3
SOROS_COOLDOWN = 2
STOP_GAIN = 1.00
TRAILING_S = 0.30
TRAILING_L = 0.05
BLOCKED_HOURS = set(range(0, 6))
TICK_COUNT = 100
CUSUM_MAX = 5.0
HURST_MIN = (
    0.0  # Sem filtro de Hurst minimo — dados historicos pre-22maio nao tinham este gate
)
CALM_THRESH = 1.5e-6
CALM_MIN_SCORE = 20
BOOM_THRESH = 0.000242
SAMPLE_EVERY = 30

accu_cfg = AccumulatorStrategyConfig(
    min_score=CALM_MIN_SCORE,
    bb_window=20,
    bb_std_dev=2.0,
    max_bb_width_percent=0.12,
    atr_window=20,
    max_tick_atr_percent=0.015,
    recent_window=5,
    max_recent_move_percent=0.05,
    hawkes_alpha=1.0,
    hawkes_beta=0.85,
    hawkes_jump_atr_multiplier=1.5,
    max_hawkes_intensity=1.0,
    imbalance_window=10,
    max_abs_tick_imbalance=4,
    hurst_window=30,
    max_hurst_exponent=0.60,
    derivative_window=20,
    max_velocity_zscore=2.0,
    max_acceleration_zscore=2.0,
    integral_window=20,
    max_pmi_distance_percent=0.02,
    markov_window=50,
    max_markov_continuation_prob=0.65,
    shannon_entropy_window=30,
    min_shannon_entropy=0.80,
    kalman_q=1e-5,
    kalman_r=1e-2,
    max_kalman_residual_zscore=2.0,
    use_ensemble=False,
    ensemble_min_prob=0.294,
    calm_min_score=CALM_MIN_SCORE,
)

print("=" * 68)
print("  BACKTEST REAL 15 DIAS — CÓDIGO IDÊNTICO AO BOT")
print(f"  Amostragem: 1 avaliação a cada {SAMPLE_EVERY} ticks (~30s)")
print("=" * 68)
print("\nCarregando ticks...")
df = pd.read_csv("data/ticks_BOOM1000_max.csv")
df["dt"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
df["hour"] = df["dt"].dt.hour
df["date"] = df["dt"].dt.date
df = df.sort_values("epoch").reset_index(drop=True)
q = df["quote"].values
rets = np.zeros(len(q))
rets[1:] = np.abs(np.diff(q) / q[:-1])
boom = rets > BOOM_THRESH
df["boom"] = boom
df["avg_ret"] = pd.Series(rets).rolling(10).mean().values
print(f"  {len(df):,} ticks | {df['date'].min()} → {df['date'].max()}")
print(f"  Booms: {boom.sum()}\n")

days = sorted(df["date"].unique())
balance = START_BAL
all_results = []
total_calls = 0
t0 = time.time()

print(
    f"{'Data':<12} {'Trades':>7} {'WR':>6} {'P&L':>8} {'Pico%':>7} {'Saldo':>8}  Parada"
)
print("-" * 68)

for day in days:
    day_df = df[df["date"] == day].reset_index(drop=True)
    if len(day_df) < TICK_COUNT + 10:
        continue

    prices = day_df["quote"].values
    hours = day_df["hour"].values
    avgs = day_df["avg_ret"].values
    booms = day_df["boom"].values
    epochs = day_df["epoch"].values

    tick_buf = deque(maxlen=TICK_COUNT)
    for w in range(min(TICK_COUNT, len(day_df))):
        tick_buf.append({"epoch": int(epochs[w]), "quote": float(prices[w])})

    sod = bal = balance
    peak = balance
    trail = False
    sor_step = sor_cd = 0
    sor_prof = 0.0
    wins = losses = 0
    stop_reason = None
    i = TICK_COUNT

    while i < len(day_df) - MAX_HOLD - 1:
        # SEMPRE adiciona tick ao buffer (consecutivo, sem gaps)
        tick_buf.append({"epoch": int(epochs[i]), "quote": float(prices[i])})

        # Filtra horário bloqueado mas CONTINUA adicionando ticks ao buffer
        if hours[i] in BLOCKED_HOURS:
            i += 1
            continue  # avança 1 a 1 para manter buffer consistente

        # Amostragem: só avalia a cada SAMPLE_EVERY ticks
        if (i - TICK_COUNT) % SAMPLE_EVERY != 0:
            i += 1
            continue

        avg = avgs[i]
        if np.isnan(avg) or avg >= CALM_THRESH:
            i += 1
            continue

        # Chama código REAL do bot
        try:
            df_ind = calculate_tick_indicators(list(tick_buf), config=accu_cfg)
            total_calls += 1
        except Exception:
            i += 1
            continue
        if df_ind is None or df_ind.empty:
            i += 1
            continue

        # RESET INDEX — obrigatorio para indicadores funcionarem
        df_ind = df_ind.reset_index(drop=True)

        # Gera sinal EXATAMENTE como o bot faz
        prices_list = [t["quote"] for t in list(tick_buf)]
        try:
            signal, score, p_loss = generate_calm_accu_signal(
                prices_list,
                threshold=CALM_THRESH,
                lookback=10,
                df=df_ind,
                config=accu_cfg,
                ensemble_scorer=None,
            )
        except Exception:
            i += 1
            continue

        if signal != "ACCU":
            i += 1
            continue

        # Quality gate (idêntico ao bot)
        row = df_ind.iloc[-1]
        cusum_v = float(row.get("cusum_score", 0) or 0)
        hurst_v = float(row.get("hurst_exponent", 0.5) or 0.5)
        if cusum_v > CUSUM_MAX or hurst_v < HURST_MIN:
            i += 1
            continue

        # Stake com soros
        if sor_cd > 0:
            stake = STAKE
            sor_cd -= 1
        elif sor_step > 0:
            stake = round(min(STAKE + sor_prof, MAX_STAKE), 2)
        else:
            stake = STAKE
        stake = max(0.35, min(stake, MAX_STAKE, bal))
        if stake < 0.35:
            i += SAMPLE_EVERY
            continue

        # Simula trade com ticks REAIS
        profit = -stake
        is_win = False
        hold = MAX_HOLD
        for j in range(1, MAX_HOLD + 1):
            if i + j >= len(prices):
                break
            if booms[i + j]:
                hold = j
                break
            cv = stake * ((1 + GROWTH_RATE) ** j)
            if cv - stake >= stake * TP_PCT:
                profit = round(stake * TP_PCT, 2)
                is_win = True
                hold = j
                break

        if is_win:
            wins += 1
            sor_cd = 0
            if sor_step < SOROS_STEPS:
                sor_step += 1
                sor_prof = round(sor_prof + profit, 2)
            else:
                sor_step = 0
                sor_prof = 0.0
        else:
            losses += 1
            sor_step = 0
            sor_prof = 0.0
            sor_cd = SOROS_COOLDOWN

        bal = round(bal + profit, 2)
        peak = max(peak, bal)
        pnl = bal - sod
        i += hold + 6  # pula o hold + cooldown

        if pnl >= sod * STOP_GAIN:
            stop_reason = "DOBROU"
            break
        if not trail and pnl >= sod * TRAILING_S:
            trail = True
        if trail and pnl <= sod * TRAILING_L:
            stop_reason = "TRAILING"
            break

    total = wins + losses
    wr = wins / total * 100 if total else 0
    pnl_d = bal - sod
    pk_p = (peak - sod) / sod * 100
    elapsed = time.time() - t0

    if stop_reason == "DOBROU":
        flag = "🎯"
        tag = f"DOBROU! ${bal:.2f}"
    elif stop_reason == "TRAILING":
        flag = "🔒"
        tag = f"+{pnl_d / sod * 100:.0f}% trailing"
    elif pnl_d > 0:
        flag = "✅"
        tag = f"+${pnl_d:.2f}"
    else:
        flag = "❌"
        tag = f"-${abs(pnl_d):.2f}"

    print(
        f"{flag} {day}  {total:>6}  {wr:>5.1f}%  {pnl_d:>+7.2f}  {pk_p:>+6.1f}%  ${bal:>7.2f}  {tag}  [{elapsed:.0f}s]"
    )
    sys.stdout.flush()

    all_results.append(
        {
            "date": day,
            "pnl": pnl_d,
            "balance": bal,
            "peak_pct": pk_p,
            "wr": wr,
            "total": total,
            "wins": wins,
            "losses": losses,
            "doubled": stop_reason == "DOBROU",
            "trailing": stop_reason == "TRAILING",
        }
    )
    balance = bal

n = len(all_results)
if n == 0:
    print("Sem resultados.")
    sys.exit(0)

nd = sum(1 for r in all_results if r["doubled"])
np_ = sum(1 for r in all_results if r["pnl"] > 0)
nt = sum(1 for r in all_results if r["trailing"])
aw = sum(r["wins"] for r in all_results)
al = sum(r["losses"] for r in all_results)
at = aw + al

print("\n" + "=" * 68)
print("  RESULTADO — DADOS REAIS DA DERIV")
print("=" * 68)
print(f"""
  {n} dias | Dobrou: {nd} ({nd / n * 100:.0f}%) | Positivos: {np_} ({np_ / n * 100:.0f}%) | Trailing+: {nt}

  Trades reais: {at} ({aw}W/{al}L) | WR: {aw / at * 100:.1f}%
  $50 → ${balance:.2f} ({(balance - START_BAL) / START_BAL * 100:+.1f}%)
  Melhor dia: ${max(r["pnl"] for r in all_results):+.2f}
  Pior dia:   ${min(r["pnl"] for r in all_results):+.2f}
  Indicador calls: {total_calls:,} | Tempo: {(time.time() - t0) / 60:.1f}min
""")
print(
    "  "
    + (
        "✅ SIM DOBRA"
        if nd / n >= 0.2
        else "⚠️  EM DIAS BONS SIM"
        if nd / n >= 0.1
        else "❌ NAO CONSISTENTE"
    )
)
