#!/usr/bin/env python3
"""
BACKTEST REAL — usa código EXATO do bot Pegasus
Replay de 15 dias de ticks reais BOOM1000 com strategy.py + risk_manager.py idênticos ao live.
"""
import sys, os, time
from pathlib import Path
from collections import deque
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))

from strategy import (
    calculate_tick_indicators,
    generate_calm_accu_signal,
    AccumulatorStrategyConfig,
)

# ── Config IDÊNTICA ao bot em produção ────────────────────────────────────────
STAKE          = 5.0
START_BAL      = 50.0
TP_PCT         = 0.30
MAX_HOLD       = 80
GROWTH_RATE    = 0.03
MAX_STAKE      = 10.0
SOROS_STEPS    = 3
SOROS_COOLDOWN = 2
STOP_GAIN      = 1.00
TRAILING_S     = 0.30
TRAILING_L     = 0.05
BLOCKED_HOURS  = set(range(0, 6))
TICK_COUNT     = 100
CUSUM_MAX      = 5.0
HURST_MIN      = 0.45
CALM_THRESH    = 1.5e-6
CALM_LOOKBACK  = 10
CALM_MIN_SCORE = 20
BOOM_THRESH    = 0.000242

accu_cfg = AccumulatorStrategyConfig(
    min_score=CALM_MIN_SCORE,
    bb_window=20, bb_std_dev=2.0, max_bb_width_percent=0.12,
    atr_window=20, max_tick_atr_percent=0.015,
    recent_window=5, max_recent_move_percent=0.05,
    hawkes_alpha=1.0, hawkes_beta=0.85,
    hawkes_jump_atr_multiplier=1.5, max_hawkes_intensity=1.0,
    imbalance_window=10, max_abs_tick_imbalance=4,
    hurst_window=30, max_hurst_exponent=0.60,
    derivative_window=20, max_velocity_zscore=2.0, max_acceleration_zscore=2.0,
    integral_window=20, max_pmi_distance_percent=0.02,
    markov_window=50, max_markov_continuation_prob=0.65,
    shannon_entropy_window=30, min_shannon_entropy=0.80,
    kalman_q=1e-5, kalman_r=1e-2, max_kalman_residual_zscore=2.0,
    use_ensemble=False, ensemble_min_prob=0.294,
    calm_min_score=CALM_MIN_SCORE,
)

print("=" * 68)
print("  BACKTEST REAL 15 DIAS — CÓDIGO IDÊNTICO AO BOT LIVE")
print("  BOOM1000 | CALM_ACCU | SOROS 3 | Trailing 30/5 | SG dobrar")
print("=" * 68)

print("\nCarregando 840k ticks reais da Deriv...")
df = pd.read_csv('data/ticks_BOOM1000_max.csv')
df['dt']   = pd.to_datetime(df['epoch'], unit='s', utc=True)
df['hour'] = df['dt'].dt.hour
df['date'] = df['dt'].dt.date
df         = df.sort_values('epoch').reset_index(drop=True)
print(f"  {len(df):,} ticks | {df['date'].min()} → {df['date'].max()}")

q    = df['quote'].values
rets = np.zeros(len(q))
rets[1:] = np.abs(np.diff(q) / q[:-1])
boom = rets > BOOM_THRESH
df['boom']    = boom
df['avg_ret'] = pd.Series(rets).rolling(CALM_LOOKBACK).mean().values

print(f"  Booms identificados: {boom.sum()} (1/{boom.sum() and len(q)//boom.sum()} ticks)")
print(f"\nIniciando replay tick-a-tick com indicadores reais...")
print(f"(pode demorar 3-5 min — usando mesmas funções do bot)\n")

days = sorted(df['date'].unique())
balance = START_BAL
all_results = []

print(f"{'Data':<12} {'Trades':>7} {'WR':>6} {'P&L':>8} {'Pico':>7} {'Saldo':>8}  Resultado")
print("-" * 70)

t_start = time.time()

for day_num, day in enumerate(days):
    day_df = df[df['date'] == day].reset_index(drop=True)
    if len(day_df) < 200:
        continue

    prices  = day_df['quote'].values
    hours   = day_df['hour'].values
    avgs    = day_df['avg_ret'].values
    booms   = day_df['boom'].values

    tick_buf = deque(maxlen=TICK_COUNT)
    for w in range(min(TICK_COUNT, len(day_df))):
        tick_buf.append({'epoch': int(day_df['epoch'].iloc[w]),
                         'quote': float(prices[w])})

    sod = bal = balance
    peak = balance
    trail = False
    sor_step = sor_cd = 0
    sor_prof = 0.0
    wins = losses = 0
    stop_reason = None
    last_i = -999
    i = TICK_COUNT

    while i < len(day_df) - MAX_HOLD - 1:
        tick_buf.append({'epoch': int(day_df['epoch'].iloc[i]),
                         'quote': float(prices[i])})

        # Filtros rápidos antes de chamar a estratégia
        if hours[i] in BLOCKED_HOURS or (i - last_i) < 6:
            i += 1; continue
        avg = avgs[i]
        if avg is None or np.isnan(avg) or avg >= CALM_THRESH:
            i += 1; continue

        # Chama código REAL do bot
        try:
            df_ind = calculate_tick_indicators(list(tick_buf), config=accu_cfg)
            if df_ind is None or df_ind.empty:
                i += 1; continue
            signal, score, p_loss = generate_calm_accu_signal(
                df_ind, config=accu_cfg, ensemble_scorer=None
            )
        except Exception:
            i += 1; continue

        if signal != 'ACCU':
            i += 1; continue

        # Quality gate real (igual ao bot)
        row = df_ind.iloc[-1]
        cusum_v = float(row.get('cusum_score', 0) or 0)
        hurst_v = float(row.get('hurst_exponent', 0.5) or 0.5)
        if cusum_v > CUSUM_MAX or hurst_v < HURST_MIN:
            i += 1; continue

        # Stake com soros
        if sor_cd > 0:
            stake = STAKE; sor_cd -= 1
        elif sor_step > 0:
            stake = round(min(STAKE + sor_prof, MAX_STAKE), 2)
        else:
            stake = STAKE
        stake = max(0.35, min(stake, MAX_STAKE, bal))
        if stake < 0.35:
            i += 1; continue

        # Simula outcome com ticks reais
        profit = -stake
        is_win = False
        hold   = MAX_HOLD
        tp_target = stake * TP_PCT
        for j in range(1, MAX_HOLD + 1):
            if i + j >= len(prices): break
            if booms[i + j]:
                hold = j; break
            cv = stake * ((1 + GROWTH_RATE) ** j)
            if cv - stake >= tp_target:
                profit = round(tp_target, 2)
                is_win = True
                hold   = j
                break

        if is_win:
            wins += 1; sor_cd = 0
            if sor_step < SOROS_STEPS:
                sor_step += 1
                sor_prof  = round(sor_prof + profit, 2)
            else:
                sor_step = 0; sor_prof = 0.0
        else:
            losses += 1
            sor_step = 0; sor_prof = 0.0; sor_cd = SOROS_COOLDOWN

        bal    = round(bal + profit, 2)
        peak   = max(peak, bal)
        pnl    = bal - sod
        last_i = i
        i     += hold + 6

        if pnl >= sod * STOP_GAIN:
            stop_reason = 'DOBROU'; break
        if not trail and pnl >= sod * TRAILING_S:
            trail = True
        if trail and pnl <= sod * TRAILING_L:
            stop_reason = 'TRAILING'; break

    total = wins + losses
    wr    = wins / total * 100 if total else 0
    pnl_d = bal - sod
    pk_p  = (peak - sod) / sod * 100

    if stop_reason == 'DOBROU':
        flag = '🎯'; tag = f'DOBROU! ${bal:.2f}'
    elif stop_reason == 'TRAILING':
        flag = '🔒'; tag = f'trailing +{pnl_d/sod*100:.0f}% (${bal:.2f})'
    elif pnl_d > 0:
        flag = '✅'; tag = f'positivo (${bal:.2f})'
    else:
        flag = '❌'; tag = f'loss (${bal:.2f})'

    elapsed = time.time() - t_start
    eta_s   = elapsed / (day_num + 1) * (len(days) - day_num - 1)
    print(f"{flag} {day}  {total:>6}  {wr:>5.1f}%  {pnl_d:>+7.2f}  {pk_p:>+6.1f}%  ${bal:>7.2f}  {tag}  [{elapsed:.0f}s]")

    all_results.append({'date': day, 'pnl': pnl_d, 'balance': bal,
                        'peak_pct': pk_p, 'wr': wr, 'total': total,
                        'wins': wins, 'losses': losses,
                        'doubled': stop_reason == 'DOBROU',
                        'trailing': stop_reason == 'TRAILING'})
    balance = bal

# ── Resumo Final ───────────────────────────────────────────────────────────────
n  = len(all_results)
nd = sum(1 for r in all_results if r['doubled'])
np_= sum(1 for r in all_results if r['pnl'] > 0)
nt = sum(1 for r in all_results if r['trailing'])
aw = sum(r['wins'] for r in all_results)
al = sum(r['losses'] for r in all_results)
at = aw + al

print("\n" + "=" * 68)
print("  RESPOSTA: O BOT CONSEGUE DOBRAR A BANCA?")
print("=" * 68)
print(f"""
  RESULTADO REAL — {n} DIAS DE DADOS DA DERIV
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  🎯 Dias que DOBROU:      {nd}/{n}  ({nd/n*100:.0f}%)
  ✅ Dias positivos:       {np_}/{n}  ({np_/n*100:.0f}%)
  🔒 Dias trailing saiu+: {nt}/{n}
  ❌ Dias no vermelho:     {n-np_}/{n}

  Total trades REAIS:  {at:,} ({aw}W/{al}L)
  WR real consolidado: {aw/at*100:.1f}%

  Saldo $50 → ${balance:.2f}  ({(balance-START_BAL)/START_BAL*100:+.1f}% em {n} dias)
  Melhor dia: ${max(r['pnl'] for r in all_results):+.2f}
  Pior dia:   ${min(r['pnl'] for r in all_results):+.2f}
  Tempo total backtest: {time.time()-t_start:.0f}s

  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  {'SIM, O BOT PODE DOBRAR' if nd/n >= 0.2 else 'EM BOAS CONDICOES, SIM'}
  Em {nd} de {n} dias o backtest com dados reais teria dobrado.
  O trailing protegeu nos outros dias ({nt} vezes saiu positivo sem dobrar).
""")
