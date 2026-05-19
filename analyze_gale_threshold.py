"""
Análise de calibração do EnsembleScorer para encontrar o threshold ótimo de G2.

Usa os 504k ticks do shadow_ticks_full.csv (resultado real = WIN/LOSS observado)
para medir: para cada faixa de P(LOSS) prevista, qual é o win rate real?

Também simula a estratégia completa (martingale + soros) com a config atual.
"""
from __future__ import annotations
import sys
import time
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

# ─── carrega modelo ──────────────────────────────────────────────────────────
try:
    from strategy import EnsembleScorer
    scorer = EnsembleScorer()
    print(f"✅ Modelo carregado — {len(scorer.feature_names)} features")
except Exception as e:
    print(f"❌ Erro ao carregar EnsembleScorer: {e}")
    sys.exit(1)

# ─── carrega shadow data ─────────────────────────────────────────────────────
DATA_PATH = "data/legacy_accumulator/shadow_ticks_full.csv"
print(f"\nCarregando {DATA_PATH} …", flush=True)
t0 = time.time()
df = pd.read_csv(DATA_PATH, on_bad_lines="skip")
print(f"  {len(df):,} linhas carregadas em {time.time()-t0:.1f}s")

# Filtra só as linhas com resultado válido (WIN/LOSS)
df = df[df["future_result"].isin(["WIN", "LOSS"])].copy()
df["is_loss"] = (df["future_result"] == "LOSS").astype(int)
print(f"  {len(df):,} linhas com resultado válido")

# ─── score EnsembleScorer em batch ──────────────────────────────────────────
print("\nCalculando P(LOSS) para todos os registros …", flush=True)
t0 = time.time()

import xgboost as xgb
features = scorer.feature_names
# Fill NaN with 0 (same logic as predict_loss_probability)
X = df[features].fillna(0.0).values.astype(float)
dmatrix = xgb.DMatrix(X, feature_names=features)
ploss = scorer.booster.predict(dmatrix)
df["p_loss"] = ploss
print(f"  Scoring completo em {time.time()-t0:.1f}s  — P(LOSS) range: [{ploss.min():.4f}, {ploss.max():.4f}]")

# ─── CALIBRAÇÃO: P(LOSS) vs win rate real ───────────────────────────────────
print("\n" + "="*70)
print("CALIBRAÇÃO: P(LOSS) prevista vs Win Rate real")
print("="*70)

thresholds = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.10, 0.12, 0.15, 0.20]

print(f"\n{'Threshold':<12} {'N signals':<12} {'Win Rate':<12} {'Losses':<10} {'Pass %':<10}")
print("-"*55)
total = len(df)
for thr in thresholds:
    subset = df[df["p_loss"] < thr]
    n = len(subset)
    if n == 0:
        continue
    wins = (subset["is_loss"] == 0).sum()
    losses = subset["is_loss"].sum()
    wr = wins / n * 100
    pass_pct = n / total * 100
    print(f"  < {thr*100:.0f}%      {n:<12,} {wr:<12.4f} {losses:<10,} {pass_pct:<10.2f}%")

# ─── Score 17+ only (same filter as bot) ────────────────────────────────────
print(f"\n{'Threshold':<12} {'N score≥17':<14} {'Win Rate':<12} {'Losses':<10}")
print("-"*50)
high_score = df[df["score"] >= 17]
print(f"  Total score≥17: {len(high_score):,}")
for thr in thresholds:
    subset = high_score[high_score["p_loss"] < thr]
    n = len(subset)
    if n == 0:
        continue
    wins = (subset["is_loss"] == 0).sum()
    losses = subset["is_loss"].sum()
    wr = wins / n * 100
    print(f"  < {thr*100:.0f}%      {n:<14,} {wr:<12.4f} {losses:<10,}")

# ─── Bins de P(LOSS): histograma de win rate por faixa ──────────────────────
print(f"\n{'Faixa P(LOSS)':<20} {'N signals':<12} {'Win Rate':<12} {'Losses'}")
print("-"*55)
bins = [0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.07, 0.10, 0.15, 0.20, 1.0]
for lo, hi in zip(bins[:-1], bins[1:]):
    subset = df[(df["p_loss"] >= lo) & (df["p_loss"] < hi)]
    n = len(subset)
    if n == 0:
        continue
    wins = (subset["is_loss"] == 0).sum()
    losses = subset["is_loss"].sum()
    wr = wins / n * 100
    print(f"  [{lo*100:4.1f}% – {hi*100:4.1f}%)   {n:<12,} {wr:<12.4f} {losses}")

# ─── BACKTEST COMPLETO: simula martingale+soros com config atual ─────────────
print("\n" + "="*70)
print("BACKTEST COMPLETO — Config atual + Martingale (G2) + Soros")
print("="*70)

# Config atual
INITIAL_BALANCE   = 10_000.0
DYNAMIC_STAKE_BASE_PCT = 0.02        # 2% banca
MAX_STAKE         = 1000.0
MARTINGALE_MAX_GALES = 2
MARTINGALE_PAYOUT_RATE = 0.50       # taxa de payout do acumulador (50%)
SOROS_MAX_STEPS   = 3
SOROS_PROFIT_FACTOR = 1.0
MIN_PLOSS_G0      = 0.20             # threshold entrada normal
# Thresholds G2 a testar
G2_THRESHOLDS = [0.01, 0.02, 0.03, 0.04, 0.05, 0.10, None]  # None = sem filtro G2

def stake_multiplier(p_loss: float) -> float:
    if p_loss < 0.05: return 4.0
    if p_loss < 0.10: return 3.0
    if p_loss < 0.15: return 2.0
    if p_loss < 0.20: return 1.5
    if p_loss < 0.25: return 1.25
    return 1.0

def run_sim(data: pd.DataFrame, g2_ploss_thr: float | None) -> dict:
    balance     = INITIAL_BALANCE
    equity      = [balance]
    m_step      = 0
    m_acc_loss  = 0.0
    m_base_stk  = 0.0
    soros_step  = 0
    soros_profit= 0.0
    trades      = 0
    wins        = 0
    losses      = 0
    g2_trades   = 0
    g2_wins     = 0
    g2_losses   = 0
    g2_skipped  = 0
    max_dd      = 0.0
    peak        = balance
    consec_loss = 0
    max_consec  = 0

    for _, row in data.iterrows():
        p_loss = float(row["p_loss"])
        result = row["future_result"]

        # G0 filter: só entra se p_loss < 20%
        if m_step == 0 and p_loss >= MIN_PLOSS_G0:
            continue

        # G2 filter: aguarda threshold mais apertado
        if m_step == MARTINGALE_MAX_GALES:
            if g2_ploss_thr is not None and p_loss >= g2_ploss_thr:
                g2_skipped += 1
                continue  # aguarda sinal melhor

        # Calcula stake
        base = max(balance * DYNAMIC_STAKE_BASE_PCT, 1.0)
        if m_step > 0 and m_base_stk > 0:
            # Gale: recuperação matemática
            raw_stake = m_acc_loss / MARTINGALE_PAYOUT_RATE + m_base_stk
        else:
            raw_stake = base * stake_multiplier(p_loss)
            # Soros
            if soros_step > 0 and soros_profit > 0:
                raw_stake = raw_stake + soros_profit
            # Gale-safe cap para G0
            if MARTINGALE_MAX_GALES > 0:
                gale_factor = (1.0 + 1.0/MARTINGALE_PAYOUT_RATE) ** MARTINGALE_MAX_GALES
                raw_stake = min(raw_stake, MAX_STAKE / gale_factor)

        stake = round(min(raw_stake, MAX_STAKE, balance), 2)
        if stake < 0.35:
            continue

        is_g2 = (m_step == MARTINGALE_MAX_GALES)
        if is_g2:
            g2_trades += 1

        trades += 1
        is_win = (result == "WIN")
        profit = stake * MARTINGALE_PAYOUT_RATE if is_win else -stake  # approximation
        # Use real outcome (held ticks based proxy: hold >= 1 = win, depends on accumulator contract)
        # Actually use future_result directly
        if is_win:
            profit_real = round(stake * MARTINGALE_PAYOUT_RATE * row.get("future_held_ticks", 1), 2)
        else:
            profit_real = -stake

        balance = round(balance + profit_real, 2)
        equity.append(balance)

        if is_win:
            wins += 1
            consec_loss = 0
            if is_g2:
                g2_wins += 1
            was_gale = m_step > 0
            m_step = 0
            m_acc_loss = 0.0
            m_base_stk = 0.0
            if not was_gale:
                if soros_step < SOROS_MAX_STEPS:
                    soros_step += 1
                    soros_profit = round(profit_real * SOROS_PROFIT_FACTOR, 2)
                else:
                    soros_step = 0
                    soros_profit = 0.0
        else:
            losses += 1
            consec_loss += 1
            max_consec = max(max_consec, consec_loss)
            if is_g2:
                g2_losses += 1
            soros_step = 0
            soros_profit = 0.0
            if m_step == 0:
                m_base_stk = stake
            m_acc_loss += stake
            if m_step >= MARTINGALE_MAX_GALES:
                # Gale exausto — reset (novo comportamento)
                m_step = 0
                m_acc_loss = 0.0
                m_base_stk = 0.0
            else:
                m_step += 1

        peak = max(peak, balance)
        dd = (peak - balance) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)

    wr = wins/trades*100 if trades else 0.0
    g2_wr = g2_wins/g2_trades*100 if g2_trades else float('nan')
    return {
        "trades": trades, "wins": wins, "losses": losses,
        "wr": round(wr, 2),
        "g2_trades": g2_trades, "g2_wins": g2_wins, "g2_losses": g2_losses,
        "g2_wr": round(g2_wr, 2) if g2_trades else float('nan'),
        "g2_skipped": g2_skipped,
        "pnl": round(balance - INITIAL_BALANCE, 2),
        "balance": round(balance, 2),
        "max_dd_pct": round(max_dd * 100, 2),
        "max_consec": max_consec,
    }

print(f"\nRodando {len(G2_THRESHOLDS)} cenários de G2 threshold …", flush=True)

print(f"\n{'G2 Thr':<10} {'G2 WR%':<10} {'G2 Trades':<12} {'G2 Loss':<10} {'G2 Skip':<10} {'PnL':>10} {'MaxDD%':>8}")
print("-"*68)
for thr in G2_THRESHOLDS:
    r = run_sim(df, thr)
    thr_str = f"{thr*100:.0f}%" if thr is not None else "None"
    g2wr = f"{r['g2_wr']:.2f}%" if not (r['g2_wr'] != r['g2_wr']) else "N/A"
    print(
        f"  {thr_str:<10} {g2wr:<10} {r['g2_trades']:<12} {r['g2_losses']:<10} "
        f"{r['g2_skipped']:<10} {r['pnl']:>+10.2f} {r['max_dd_pct']:>7.2f}%"
    )

# ─── Win Rate por P(LOSS) no G2 ─────────────────────────────────────────────
print(f"\n{'='*70}")
print("Win Rate REAL por faixa de P(LOSS) — toda a base (500k linhas)")
print(f"{'='*70}")
print(f"\n{'Bucket':<18} {'N':<10} {'Wins':<10} {'Losses':<10} {'Win Rate':<12} {'Cumu < X WR'}")
print("-"*68)
buckets = [(0, 0.01), (0.01, 0.02), (0.02, 0.03), (0.03, 0.04), (0.04, 0.05),
           (0.05, 0.07), (0.07, 0.10), (0.10, 0.15), (0.15, 0.20)]
for lo, hi in buckets:
    sub = df[(df["p_loss"] >= lo) & (df["p_loss"] < hi)]
    n = len(sub)
    if n == 0:
        continue
    w = (sub["is_loss"]==0).sum()
    l = sub["is_loss"].sum()
    wr = w/n*100
    cumul_sub = df[df["p_loss"] < hi]
    cn, cw = len(cumul_sub), (cumul_sub["is_loss"]==0).sum()
    cum_wr = cw/cn*100 if cn else 0
    print(f"  [{lo*100:.1f}%-{hi*100:.1f}%)  {n:<10,} {w:<10,} {l:<10,} {wr:<12.4f} {cum_wr:.4f}%")

# ─── RESPOSTA FINAL ──────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("CONCLUSÃO: Threshold recomendado para G2 (win rate alvo ≥ 99.5%)")
print(f"{'='*70}")
for thr in [0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05]:
    sub = df[df["p_loss"] < thr]
    n = len(sub)
    if n < 10:
        print(f"  < {thr*100:.1f}%: N={n} (muito poucos)")
        continue
    w = (sub["is_loss"]==0).sum()
    wr = w/n*100
    freq = n / len(df) * 100
    print(f"  P(LOSS) < {thr*100:.1f}%:  win rate = {wr:.4f}%  ({n:,} signals = {freq:.2f}% do tempo disponível)")
