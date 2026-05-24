#!/usr/bin/env python3
"""Backtest apenas dos dias restantes: 17-20 de maio"""
import sys, time
from pathlib import Path
from collections import deque
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from strategy import calculate_tick_indicators, generate_calm_accu_signal, AccumulatorStrategyConfig

STAKE=5.0; START_BAL=50.0; TP_PCT=0.30; MAX_HOLD=80; GROWTH_RATE=0.03
MAX_STAKE=10.0; SOROS_STEPS=3; SOROS_COOLDOWN=2; STOP_GAIN=1.00
TRAILING_S=0.30; TRAILING_L=0.05; BLOCKED_HOURS=set(range(0,6))
TICK_COUNT=100; CUSUM_MAX=5.0; HURST_MIN=0.0; CALM_THRESH=1.5e-6
CALM_MIN_SCORE=20; BOOM_THRESH=0.000242; SAMPLE_EVERY=30

accu_cfg = AccumulatorStrategyConfig(
    min_score=CALM_MIN_SCORE, bb_window=20, bb_std_dev=2.0, max_bb_width_percent=0.12,
    atr_window=20, max_tick_atr_percent=0.015, recent_window=5, max_recent_move_percent=0.05,
    hawkes_alpha=1.0, hawkes_beta=0.85, hawkes_jump_atr_multiplier=1.5, max_hawkes_intensity=1.0,
    imbalance_window=10, max_abs_tick_imbalance=4, hurst_window=30, max_hurst_exponent=0.60,
    derivative_window=20, max_velocity_zscore=2.0, max_acceleration_zscore=2.0,
    integral_window=20, max_pmi_distance_percent=0.02, markov_window=50,
    max_markov_continuation_prob=0.65, shannon_entropy_window=30, min_shannon_entropy=0.80,
    kalman_q=1e-5, kalman_r=1e-2, max_kalman_residual_zscore=2.0,
    use_ensemble=False, ensemble_min_prob=0.294, calm_min_score=CALM_MIN_SCORE,
)

df = pd.read_csv('data/ticks_BOOM1000_max.csv')
df['dt'] = pd.to_datetime(df['epoch'], unit='s', utc=True)
df['hour'] = df['dt'].dt.hour; df['date'] = df['dt'].dt.date
df = df.sort_values('epoch').reset_index(drop=True)
q = df['quote'].values
rets = np.zeros(len(q)); rets[1:] = np.abs(np.diff(q)/q[:-1])
boom = rets > BOOM_THRESH
df['boom'] = boom
df['avg_ret'] = pd.Series(rets).rolling(10).mean().values

# Começa com saldo do dia 16 (resultado anterior)
# Dia 16 terminou com $5596.04 (sem stop gain porque cada dia reseta pq dobrou antes)
# Mas para ser correto: cada dia começa do resultado do anterior
# Dia 16: saldo final $5596.04 (positivo, não dobrou)
# Para os dias restantes, usamos saldo acumulado ou resetamos em $50?
# O usuário quer ver como seria se cada dia dobrasse e passasse para o próximo
# Então continuamos acumulando.

# Saldo final conhecido do dia 16
START = 5596.04  # saldo acumulado após 11 dias

days_to_run = [d for d in sorted(df['date'].unique()) if str(d) >= '2026-05-17']
print(f"Rodando dias: {[str(d) for d in days_to_run]}")
print(f"Saldo inicial: ${START:.2f}")

balance = START
results = []
t0 = time.time()

for day in days_to_run:
    day_df = df[df['date'] == day].reset_index(drop=True)
    if len(day_df) < TICK_COUNT + 10:
        print(f"❌ {day}: poucos ticks ({len(day_df)})")
        continue

    prices = day_df['quote'].values; hours = day_df['hour'].values
    avgs = day_df['avg_ret'].values; booms = day_df['boom'].values; epochs = day_df['epoch'].values

    tick_buf = deque(maxlen=TICK_COUNT)
    for w in range(min(TICK_COUNT, len(day_df))):
        tick_buf.append({'epoch': int(epochs[w]), 'quote': float(prices[w])})

    sod = bal = balance; peak = balance; trail = False
    sor_step = sor_cd = 0; sor_prof = 0.0
    wins = losses = 0; stop_reason = None
    i = TICK_COUNT

    while i < len(day_df) - MAX_HOLD - 1:
        tick_buf.append({'epoch': int(epochs[i]), 'quote': float(prices[i])})
        if hours[i] in BLOCKED_HOURS: i+=1; continue
        if (i-TICK_COUNT)%SAMPLE_EVERY!=0: i+=1; continue
        avg = avgs[i]
        if np.isnan(avg) or avg >= CALM_THRESH: i+=1; continue

        try:
            df_ind = calculate_tick_indicators(list(tick_buf), config=accu_cfg)
            if df_ind is None or df_ind.empty: i+=1; continue
            df_ind = df_ind.reset_index(drop=True)
            prices_list = [t['quote'] for t in list(tick_buf)]
            signal, score, _ = generate_calm_accu_signal(prices_list, threshold=CALM_THRESH,
                lookback=10, df=df_ind, config=accu_cfg, ensemble_scorer=None)
        except Exception: i+=1; continue

        if signal != 'ACCU': i+=1; continue
        row = df_ind.iloc[-1]
        cv = float(row.get('cusum_score', 0) or 0)
        hv = float(row.get('hurst_exponent', 0.5) or 0.5)
        if cv > CUSUM_MAX or hv < HURST_MIN: i+=1; continue

        if sor_cd > 0: stake = STAKE; sor_cd -= 1
        elif sor_step > 0: stake = round(min(STAKE + sor_prof, MAX_STAKE), 2)
        else: stake = STAKE
        stake = max(0.35, min(stake, MAX_STAKE, bal))
        if stake < 0.35: i+=1; continue

        profit = -stake; is_win = False; hold = MAX_HOLD
        for j in range(1, MAX_HOLD+1):
            if i+j >= len(prices): break
            if booms[i+j]: hold=j; break
            cv2 = stake*((1+GROWTH_RATE)**j)
            if cv2-stake >= stake*TP_PCT: profit=round(stake*TP_PCT,2); is_win=True; hold=j; break

        if is_win:
            wins+=1; sor_cd=0
            if sor_step<SOROS_STEPS: sor_step+=1; sor_prof=round(sor_prof+profit,2)
            else: sor_step=0; sor_prof=0.0
        else:
            losses+=1; sor_step=0; sor_prof=0.0; sor_cd=SOROS_COOLDOWN

        bal=round(bal+profit,2); peak=max(peak,bal); pnl=bal-sod; i+=hold+6

        if pnl>=sod*STOP_GAIN: stop_reason='DOBROU'; break
        if not trail and pnl>=sod*TRAILING_S: trail=True
        if trail and pnl<=sod*TRAILING_L: stop_reason='TRAILING'; break

    total=wins+losses; wr=wins/total*100 if total else 0
    pnl_d=bal-sod; pk_p=(peak-sod)/sod*100 if sod>0 else 0
    elapsed=time.time()-t0

    if stop_reason=='DOBROU': flag='🎯'; tag=f'DOBROU! ${bal:.2f}'
    elif stop_reason=='TRAILING': flag='🔒'; tag=f'+{pnl_d/sod*100:.0f}% trailing'
    elif pnl_d>0: flag='✅'; tag=f'+${pnl_d:.2f}'
    else: flag='❌'; tag=f'-${abs(pnl_d):.2f}'

    print(f"{flag} {day}  {total:>6}  {wr:>5.1f}%  {pnl_d:>+10.2f}  {pk_p:>+6.1f}%  ${bal:>10.2f}  {tag}  [{elapsed:.0f}s]")
    sys.stdout.flush()
    results.append({'date': str(day), 'trades': total, 'wr': round(wr,1), 'pnl': round(pnl_d,2),
                    'balance': round(bal,2), 'peak_pct': round(pk_p,1),
                    'doubled': stop_reason=='DOBROU', 'trailing': stop_reason=='TRAILING'})
    balance = bal

print(f"\nSaldo final após 4 dias: ${balance:.2f}")
