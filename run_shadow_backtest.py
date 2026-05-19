"""
Backtest real usando shadow_ticks_full.csv (98k sinais com resultado real).

Simula a estratégia completa com:
- Martingale (G0→G1→G2) com lógica idêntica ao risk_manager.py real
- Soros compounding
- P(LOSS) thresholds por nível de gale
- MARTINGALE_LAST_GALE_MAX_WAIT_TICKS timeout
- Stop loss diário
- Dynamic stake (% da banca)

Imprime: curva de capital, win rates por gale, max drawdown, KPIs.
"""
from __future__ import annotations

import os, sys, time, warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import xgboost as xgb

# ── Importa config e modelo do projeto ───────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))
from strategy import EnsembleScorer

# ── Configuração (cópia exata do .env do servidor) ───────────────────────────
class Cfg:
    INITIAL_BALANCE        = 10_000.0
    DYNAMIC_STAKE_BASE_PCT = 0.02       # 2%
    MAX_STAKE              = 1000.0
    MARTINGALE_MAX_GALES   = 2
    MARTINGALE_PAYOUT_RATE = 0.50
    # Filters
    ENSEMBLE_MIN_PROB      = 0.20       # G0: bloqueia se P(LOSS) >= 20%
    MARTINGALE_LAST_GALE_MAX_PLOSS = 0.05  # G2: aguarda P(LOSS) < 5%
    MARTINGALE_LAST_GALE_MAX_WAIT_TICKS = 0  # 0 = sem timeout
    # Soros
    USE_SOROS              = True
    SOROS_MAX_STEPS        = 3
    SOROS_PROFIT_FACTOR    = 1.0
    # Risk
    MAX_LOSS_DAY_PCT       = 0.10       # stop 10% banca
    MAX_CONSECUTIVE_LOSSES = 4
    MIN_STAKE              = 0.35
    MIN_SCORE              = 17         # score mínimo para entrar

def stake_multiplier(p_loss: float) -> float:
    if p_loss < 0.05: return 4.0
    if p_loss < 0.10: return 3.0
    if p_loss < 0.15: return 2.0
    if p_loss < 0.20: return 1.5
    return 1.0

def compute_g0_stake(balance: float, p_loss: float) -> float:
    """Stake G0 com gale-safe cap."""
    base = balance * Cfg.DYNAMIC_STAKE_BASE_PCT
    raw  = base * stake_multiplier(p_loss)
    # Gale-safe cap: G0 ≤ MAX_STAKE / (1+1/payout)^max_gales
    gale_factor = (1.0 + 1.0 / Cfg.MARTINGALE_PAYOUT_RATE) ** Cfg.MARTINGALE_MAX_GALES
    cap = Cfg.MAX_STAKE / gale_factor
    return round(max(Cfg.MIN_STAKE, min(raw, cap, balance)), 2)

def compute_gale_stake(acc_loss: float, base_stake: float, balance: float) -> float:
    """Stake de recuperação gale."""
    raw = acc_loss / Cfg.MARTINGALE_PAYOUT_RATE + base_stake
    return round(max(Cfg.MIN_STAKE, min(raw, Cfg.MAX_STAKE, balance)), 2)

# ── Carrega modelo ────────────────────────────────────────────────────────────
print("Carregando EnsembleScorer …", flush=True)
scorer = EnsembleScorer()
features = scorer.feature_names

# ── Carrega dados ─────────────────────────────────────────────────────────────
DATA = "data/shadow_ticks_full.csv"
print(f"Carregando {DATA} …", flush=True)
df = pd.read_csv(DATA, on_bad_lines="skip")
df = df[df["future_result"].isin(["WIN", "LOSS"])].copy()
df = df.sort_values("entry_epoch").reset_index(drop=True)
print(f"  {len(df):,} sinais com resultado válido")

# ── Calcula P(LOSS) em batch ──────────────────────────────────────────────────
print("Calculando P(LOSS) …", flush=True)
X = df[features].fillna(0.0).values.astype(float)
dm = xgb.DMatrix(X, feature_names=features)
df["p_loss"] = scorer.booster.predict(dm)

# ── Simulação ─────────────────────────────────────────────────────────────────
print("\nRodando backtest …", flush=True)

balance      = Cfg.INITIAL_BALANCE
equity_curve = [balance]
timestamps   = [df["entry_epoch"].iloc[0]]

# Estado martingale
m_step  = 0
m_acc   = 0.0
m_base  = 0.0
m_wait  = 0      # ticks em wait no último gale

# Estado soros
s_step  = 0
s_profit= 0.0

# Risco diário
day_start_bal = balance
consec = 0

# Estatísticas por nível de gale
stats = {i: {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0} for i in range(3)}
skip_g0 = skip_g2 = timeout_absorb = 0
day_stop_count = 0

for idx, row in df.iterrows():
    p_loss = float(row["p_loss"])
    result = row["future_result"]
    score  = int(row.get("score", 0))
    epoch  = row["entry_epoch"]
    ticks_held = int(row.get("future_held_ticks", 1))

    # ── G0: filtros de entrada ────────────────────────────────────────────────
    if m_step == 0:
        if p_loss >= Cfg.ENSEMBLE_MIN_PROB:
            skip_g0 += 1
            continue
        # Shadow data: score máx = 10 (diferente da escala do bot real 17-20)
        # Usa signal=1 para ticks que passaram o filtro no shadow, OU score>=8 como proxy
        sig = int(row.get("signal", 0))
        if sig == 0 and score < 8:
            continue

    # ── G_last: aguarda threshold mais apertado ───────────────────────────────
    if m_step == Cfg.MARTINGALE_MAX_GALES and Cfg.MARTINGALE_LAST_GALE_MAX_PLOSS > 0:
        if p_loss >= Cfg.MARTINGALE_LAST_GALE_MAX_PLOSS:
            m_wait += 1
            skip_g2 += 1
            # Timeout: absorve perda e reseta
            if Cfg.MARTINGALE_LAST_GALE_MAX_WAIT_TICKS > 0 and m_wait >= Cfg.MARTINGALE_LAST_GALE_MAX_WAIT_TICKS:
                timeout_absorb += 1
                m_step = 0; m_acc = 0.0; m_base = 0.0; m_wait = 0
            continue
        m_wait = 0  # sinal seguro encontrado

    # ── Calcula stake ─────────────────────────────────────────────────────────
    if m_step == 0:
        stake = compute_g0_stake(balance, p_loss)
        # Soros
        if Cfg.USE_SOROS and s_step > 0 and s_profit > 0:
            stake = round(min(stake + s_profit, Cfg.MAX_STAKE, balance), 2)
    else:
        stake = compute_gale_stake(m_acc, m_base, balance)

    if stake < Cfg.MIN_STAKE:
        continue

    # ── Stop loss diário ──────────────────────────────────────────────────────
    day_loss = balance - day_start_bal
    if day_loss <= -(day_start_bal * Cfg.MAX_LOSS_DAY_PCT):
        day_stop_count += 1
        # Resetar estado para próximo dia
        day_start_bal = balance
        m_step = 0; m_acc = 0.0; m_base = 0.0; m_wait = 0
        s_step = 0; s_profit = 0.0; consec = 0
        continue

    if consec >= Cfg.MAX_CONSECUTIVE_LOSSES:
        continue

    # ── Executa trade ─────────────────────────────────────────────────────────
    level = min(m_step, 2)
    is_win = (result == "WIN")

    if is_win:
        # Lucro proporcional aos ticks segurados (acumulador cresce a cada tick)
        raw_profit = stake * Cfg.MARTINGALE_PAYOUT_RATE * (ticks_held / max(ticks_held, 1))
        # Simplificação: profit = stake * take_profit_pct quando win
        raw_profit = stake * 0.50
        balance = round(balance + raw_profit, 2)
        stats[level]["wins"] += 1
        stats[level]["pnl"] += raw_profit
    else:
        balance = round(balance - stake, 2)
        stats[level]["losses"] += 1
        stats[level]["pnl"] -= stake

    stats[level]["trades"] += 1
    equity_curve.append(balance)
    timestamps.append(epoch)

    # ── Atualiza estado ───────────────────────────────────────────────────────
    if is_win:
        consec = 0
        if m_step > 0:
            m_step = 0; m_acc = 0.0; m_base = 0.0; m_wait = 0
            s_step = 0; s_profit = 0.0  # reset soros após gale
        else:
            if Cfg.USE_SOROS and s_step < Cfg.SOROS_MAX_STEPS:
                s_step += 1
                s_profit = round(raw_profit * Cfg.SOROS_PROFIT_FACTOR, 2)
            elif Cfg.USE_SOROS and s_step >= Cfg.SOROS_MAX_STEPS:
                s_step = 0; s_profit = 0.0
    else:
        consec += 1
        s_step = 0; s_profit = 0.0
        if m_step == 0:
            m_base = stake
        m_acc += stake
        if m_step >= Cfg.MARTINGALE_MAX_GALES:
            m_step = 0; m_acc = 0.0; m_base = 0.0; m_wait = 0
        else:
            m_step += 1

# ── KPIs ──────────────────────────────────────────────────────────────────────
eq = np.array(equity_curve)
peak = np.maximum.accumulate(eq)
dd   = (peak - eq) / peak
max_dd = dd.max()

total_trades = sum(s["trades"] for s in stats.values())
total_wins   = sum(s["wins"]   for s in stats.values())
total_losses = sum(s["losses"] for s in stats.values())
total_wr     = total_wins / total_trades * 100 if total_trades else 0.0
pnl          = balance - Cfg.INITIAL_BALANCE
pnl_pct      = pnl / Cfg.INITIAL_BALANCE * 100

print(f"""
{'='*68}
RESULTADO DO BACKTEST (shadow_ticks_full — {len(df):,} sinais)
Config: G2 threshold={Cfg.MARTINGALE_LAST_GALE_MAX_PLOSS*100:.0f}%  G0 filter={Cfg.ENSEMBLE_MIN_PROB*100:.0f}%
{'='*68}

  Saldo final:   ${balance:>10,.2f}  (inicial: ${Cfg.INITIAL_BALANCE:,.2f})
  PnL total:     ${pnl:>+10,.2f}  ({pnl_pct:+.2f}%)
  Total trades:  {total_trades:>8,}
  Win rate geral:{total_wr:>8.2f}%
  Max Drawdown:  {max_dd*100:>8.2f}%
  Stop dias:     {day_stop_count:>8}
  G2 timeouts:   {timeout_absorb:>8}

Por nível de GALE:
{'Nível':<8} {'Trades':>8} {'Wins':>8} {'Losses':>8} {'Win Rate':>10} {'PnL':>12}
{'-'*58}""")
for lvl, s in sorted(stats.items()):
    if s["trades"] == 0: continue
    wr = s["wins"]/s["trades"]*100
    print(f"  G{lvl}      {s['trades']:>8,} {s['wins']:>8,} {s['losses']:>8,} {wr:>9.2f}% {s['pnl']:>+12,.2f}")
print(f"""
  Ticks G0 filtrados (P≥20%):  {skip_g0:,}
  Ticks G2 aguardados (P≥5%):  {skip_g2:,}
{'='*68}
""")

# ── Salva curva de equity ──────────────────────────────────────────────────────
OUT = "logs/backtest_equity.csv"
pd.DataFrame({"epoch": timestamps, "balance": equity_curve}).to_csv(OUT, index=False)
print(f"Curva de capital salva em {OUT}")
