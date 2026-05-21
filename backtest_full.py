#!/usr/bin/env python3
"""
BACKTEST COMPLETO — JD50 Jump Rise/Fall
========================================
Testa TODAS as estratégias de recovery contra:
  1. Dados reais do DB (259 trades, sequência exata de WIN/LOSS)
  2. Monte Carlo (10K sims × 1000 trades) em WR 45/47/48/50/52/55%
  3. Stress tests: all-loss streak, 25% WR pattern, cascatas repetidas
  4. Worst-case: quantas cascatas até bust para cada estratégia

Objetivo: encontrar a estratégia que NUNCA zera $50 de banca.
"""
import csv
import random
import statistics
import sys
from dataclasses import dataclass, field
from typing import Callable, Optional

# ═══════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ═══════════════════════════════════════════════════════════════════
BASE_STAKE = 0.35
PAYOUT_RATE = 0.953          # lucro = stake × 0.953
STARTING_BALANCE = 50.00
MIN_BALANCE = 0.35           # abaixo = BUST (não dá nem pra apostar)
NUM_MC_SIMS = 1_000
MC_TRADES = 300              # trades por sim MC
MC_WIN_RATES = [0.45, 0.47, 0.48, 0.50, 0.52, 0.55]
FIB = [1, 1, 2, 3, 5, 8, 13, 21]

# ═══════════════════════════════════════════════════════════════════
# STRATEGY FUNCTIONS — cada uma retorna o stake para dado gale_step
# Retorna None para "não apostar" (skip gale)
# ═══════════════════════════════════════════════════════════════════

def flat(step, base, bal, maxg):
    """Sem gale: só aposta base no G0."""
    return base if step == 0 else None

def fib(step, base, bal, maxg):
    """Fibonacci: base × FIB[step]."""
    return base * FIB[min(step, len(FIB)-1)]

def fib_capped(step, base, bal, maxg):
    """Fibonacci com cap: max stake = 5% do saldo."""
    raw = base * FIB[min(step, len(FIB)-1)]
    cap = bal * 0.05
    return min(raw, max(base, cap))

def fib_conservative(step, base, bal, maxg):
    """Fibonacci conservador: [1,1,2,3] — nunca sobe agressivo."""
    CFIB = [1, 1, 2, 3]
    return base * CFIB[min(step, len(CFIB)-1)]

def martingale(step, base, bal, maxg):
    """Martingale clássico: dobra a cada loss."""
    return base * (2 ** step)

def martingale_capped(step, base, bal, maxg):
    """Martingale com cap: max stake = 10% do saldo."""
    raw = base * (2 ** step)
    cap = bal * 0.10
    return min(raw, max(base, cap))

def soft_martingale(step, base, bal, maxg):
    """Soft martingale: ×1.5 ao invés de ×2."""
    return base * (1.5 ** step)

def linear(step, base, bal, maxg):
    """Linear: base × (1 + step)."""
    return base * (1 + step)

def dalembert(step, base, bal, maxg):
    """D'Alembert: base + base×step."""
    return base + (base * step)

def pct_balance(step, base, bal, maxg):
    """1% do saldo por trade. Gales = 1%×(step+1)."""
    return max(base, bal * 0.01 * (step + 1))

def pct_half(step, base, bal, maxg):
    """0.5% do saldo. Ultra conservador."""
    return max(base, bal * 0.005 * (step + 1))

def anti_martingale(step, base, bal, maxg):
    """Anti-martingale: sem gale algum."""
    return base if step == 0 else None

def fib_pct_hybrid(step, base, bal, maxg):
    """Fibonacci mas limitado a 3% do saldo."""
    raw = base * FIB[min(step, len(FIB)-1)]
    cap = bal * 0.03
    return min(raw, max(base, cap))

def breakeven_recovery(step, base, bal, maxg):
    """Calcula stake pra recuperar EXATAMENTE o perdido."""
    # Soma perdida nos gales anteriores
    lost = sum(base * FIB[min(i, len(FIB)-1)] for i in range(step))
    # Precisa recuperar: lost / PAYOUT_RATE
    needed = (lost + base) / PAYOUT_RATE
    return min(needed, bal * 0.10)  # cap 10% do saldo

# ═══════════════════════════════════════════════════════════════════
# ADAPTIVE FIB — nível persiste entre trades, step-back-2 on WIN
# ═══════════════════════════════════════════════════════════════════

def simulate_adaptive(signals, max_gales, name, start_bal=STARTING_BALANCE):
    """Simula Adaptive Fib: cada sinal = 1 trade.
    WIN  → level = max(0, level - 2)  (de-escalation gradual)
    LOSS → level = min(max_gales, level + 1)
    Stake = BASE_STAKE × FIB[level]
    """
    r = Result(name=name)
    bal = start_bal
    r.peak_bal = bal
    level = 0

    for sig in signals:
        if bal < MIN_BALANCE:
            r.busted = True
            r.bust_trade = r.trades
            break

        stake = BASE_STAKE * FIB[min(level, len(FIB) - 1)]
        stake = min(stake, bal)
        if stake < 0.01:
            break

        r.trades += 1
        r.total_staked += stake
        if level == 0:
            r.g0_entries += 1

        if sig == "WIN":
            bal += stake * PAYOUT_RATE
            r.wins += 1
            level = max(0, level - 2)
        else:
            bal -= stake
            r.losses += 1
            old_level = level
            level = min(max_gales, level + 1)
            if level >= max_gales and old_level >= max_gales:
                r.full_cascades += 1

        r.min_bal = min(r.min_bal, bal)
        r.max_bal = max(r.max_bal, bal)
        if bal > r.peak_bal:
            r.peak_bal = bal
        dd = (r.peak_bal - bal) / r.peak_bal * 100 if r.peak_bal > 0 else 0
        r.max_dd_pct = max(r.max_dd_pct, dd)

        if bal < MIN_BALANCE:
            r.busted = True
            r.bust_trade = r.trades
            break

    r.final_bal = bal
    if r.min_bal == float("inf"):
        r.min_bal = bal
    return r

# Flag: strategies marked True use simulate_adaptive instead of simulate
# Tuple: (name, fn_or_None, max_gales, is_adaptive)
ADAPTIVE_NAMES = set()

# ═══════════════════════════════════════════════════════════════════
# TODAS AS ESTRATÉGIAS
# ═══════════════════════════════════════════════════════════════════
STRATEGIES = [
    # (nome, função, max_gales)
    ("Flat (sem gale)",            flat,               0),
    ("Anti-Martingale",            anti_martingale,    0),
    ("Fib 2 gales",                fib,                2),
    ("Fib 3 gales",                fib,                3),
    ("Fib 4 gales",                fib,                4),
    ("Fib 5 gales",                fib,                5),
    ("Fib 7 gales (ATUAL)",       fib,                7),
    ("Fib Capped 3g (5%bal)",     fib_capped,         3),
    ("Fib Capped 5g (5%bal)",     fib_capped,         5),
    ("Fib Conserv 3g [1,1,2,3]",  fib_conservative,   3),
    ("Fib+%Bal Hybrid 3g (3%)",   fib_pct_hybrid,     3),
    ("Fib+%Bal Hybrid 5g (3%)",   fib_pct_hybrid,     5),
    ("Breakeven Recovery 3g",     breakeven_recovery,  3),
    ("Breakeven Recovery 5g",     breakeven_recovery,  5),
    ("Martingale 2 gales",         martingale,         2),
    ("Martingale 3 gales",         martingale,         3),
    ("Martingale 4 gales",         martingale,         4),
    ("Mart Capped 3g (10%bal)",   martingale_capped,   3),
    ("Soft Mart 3 gales",          soft_martingale,    3),
    ("Soft Mart 4 gales",          soft_martingale,    4),
    ("Linear 3 gales",             linear,             3),
    ("Linear 4 gales",             linear,             4),
    ("D'Alembert 3 gales",         dalembert,          3),
    ("D'Alembert 4 gales",         dalembert,          4),
    ("1% Balance 3g",              pct_balance,        3),
    ("0.5% Balance 3g",            pct_half,           3),
    # Adaptive Fib — level persists, WIN=step-back-2, LOSS=step+1
    ("AdaptFib 3g (step-back-2)",  None,               3),
    ("AdaptFib 5g (step-back-2)",  None,               5),
    ("AdaptFib 7g (step-back-2)",  None,               7),
]

ADAPTIVE_NAMES = {
    "AdaptFib 3g (step-back-2)",
    "AdaptFib 5g (step-back-2)",
    "AdaptFib 7g (step-back-2)",
}

# ═══════════════════════════════════════════════════════════════════
# SIMULAÇÃO
# ═══════════════════════════════════════════════════════════════════

@dataclass
class Result:
    name: str
    final_bal: float = 0.0
    min_bal: float = float("inf")
    max_bal: float = 0.0
    trades: int = 0
    wins: int = 0
    losses: int = 0
    busted: bool = False
    bust_trade: int = 0
    max_dd_pct: float = 0.0
    peak_bal: float = 0.0
    full_cascades: int = 0
    total_staked: float = 0.0
    g0_entries: int = 0

def simulate(signals: list[str], strat_fn: Callable, max_gales: int,
             name: str, start_bal: float = STARTING_BALANCE) -> Result:
    """
    Simula estratégia sobre sequência de sinais.
    Cada sinal = resultado de mercado (WIN/LOSS).
    G0 consume 1 sinal. Se LOSS, gales consomem sinais subsequentes.
    """
    r = Result(name=name)
    bal = start_bal
    r.peak_bal = bal
    i = 0
    n = len(signals)

    while i < n:
        if bal < MIN_BALANCE:
            r.busted = True
            r.bust_trade = r.trades
            break

        # G0 entry
        step = 0
        stake = strat_fn(step, BASE_STAKE, bal, max_gales)
        if stake is None or stake <= 0:
            i += 1
            continue

        stake = min(stake, bal)
        r.g0_entries += 1
        r.trades += 1
        r.total_staked += stake

        if signals[i] == "WIN":
            bal += stake * PAYOUT_RATE
            r.wins += 1
            i += 1
        else:
            bal -= stake
            r.losses += 1
            i += 1

            # Gales
            while step < max_gales and i < n:
                step += 1
                stake = strat_fn(step, BASE_STAKE, bal, max_gales)
                if stake is None or stake <= 0:
                    break
                stake = min(stake, bal)
                if bal < MIN_BALANCE or stake < 0.01:
                    break

                r.trades += 1
                r.total_staked += stake

                if signals[i] == "WIN":
                    bal += stake * PAYOUT_RATE
                    r.wins += 1
                    i += 1
                    break
                else:
                    bal -= stake
                    r.losses += 1
                    i += 1
                    if step >= max_gales:
                        r.full_cascades += 1

        # Track
        r.min_bal = min(r.min_bal, bal)
        r.max_bal = max(r.max_bal, bal)
        if bal > r.peak_bal:
            r.peak_bal = bal
        dd = (r.peak_bal - bal) / r.peak_bal * 100 if r.peak_bal > 0 else 0
        r.max_dd_pct = max(r.max_dd_pct, dd)

        if bal < MIN_BALANCE:
            r.busted = True
            r.bust_trade = r.trades
            break

    r.final_bal = bal
    if r.min_bal == float("inf"):
        r.min_bal = bal
    return r

# ═══════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════

def load_signals_csv(path: str = "data/trades_JD50_history.csv") -> list[str]:
    """Carrega TODOS os resultados (WIN/LOSS) em ordem."""
    signals = []
    with open(path) as f:
        for row in csv.DictReader(f):
            signals.append(row["result"])
    return signals

def load_g0_only(path: str = "data/trades_JD50_history.csv") -> list[str]:
    """Carrega apenas sinais G0."""
    signals = []
    with open(path) as f:
        for row in csv.DictReader(f):
            if int(row["gale_step"]) == 0:
                signals.append(row["result"])
    return signals

# ═══════════════════════════════════════════════════════════════════
# MONTE CARLO
# ═══════════════════════════════════════════════════════════════════

def mc_signals(n: int, wr: float) -> list[str]:
    return ["WIN" if random.random() < wr else "LOSS" for _ in range(n)]

def run_sim(name, strat_fn, max_gales, sigs):
    """Dispatch to correct simulator."""
    if name in ADAPTIVE_NAMES:
        return simulate_adaptive(sigs, max_gales, name)
    return simulate(sigs, strat_fn, max_gales, name)

def monte_carlo(strat_fn, max_gales, name, wr, n_sims=NUM_MC_SIMS, n_trades=MC_TRADES):
    busts = 0
    finals = []
    mins = []
    dds = []
    cascades = []

    for _ in range(n_sims):
        sigs = mc_signals(n_trades, wr)
        r = run_sim(name, strat_fn, max_gales, sigs)
        if r.busted:
            busts += 1
        finals.append(r.final_bal)
        mins.append(r.min_bal)
        dds.append(r.max_dd_pct)
        cascades.append(r.full_cascades)

    finals.sort()
    return {
        "name": name,
        "wr": wr,
        "bust_pct": busts / n_sims * 100,
        "busts": busts,
        "avg": statistics.mean(finals),
        "median": statistics.median(finals),
        "p5": finals[int(n_sims * 0.05)],
        "p95": finals[int(n_sims * 0.95)],
        "worst": finals[0],
        "best": finals[-1],
        "avg_min": statistics.mean(mins),
        "worst_min": min(mins),
        "avg_dd": statistics.mean(dds),
        "worst_dd": max(dds),
        "avg_cascades": statistics.mean(cascades),
    }

# ═══════════════════════════════════════════════════════════════════
# STRESS TESTS
# ═══════════════════════════════════════════════════════════════════

def all_loss_test(strat_fn, max_gales, name):
    """Quantos trades de pura loss até bust."""
    sigs = ["LOSS"] * 500
    r = run_sim(name, strat_fn, max_gales, sigs)
    return r.bust_trade if r.busted else -1, r.final_bal

def pattern_25pct(strat_fn, max_gales, name):
    """W L L L repetido = 25% WR."""
    sigs = (["WIN"] + ["LOSS"] * 3) * 200  # 800 trades
    return run_sim(name, strat_fn, max_gales, sigs)

def pattern_33pct(strat_fn, max_gales, name):
    """W L L repetido = 33% WR."""
    sigs = (["WIN"] + ["LOSS"] * 2) * 300  # 900 trades
    return run_sim(name, strat_fn, max_gales, sigs)

def cascade_burst_test(strat_fn, max_gales, name):
    """Padrão real observado: 5 wins seguidos, depois cascata completa.
    Simula ciclos de sorte→azar repetidos."""
    pattern = []
    for _ in range(50):  # 50 ciclos
        pattern.extend(["WIN"] * 3)                    # 3 wins
        pattern.extend(["LOSS"] * (max_gales + 1))     # cascata completa
    return run_sim(name, strat_fn, max_gales, pattern)

def worst_realistic_test(strat_fn, max_gales, name):
    """Padrão baseado nos dados reais: WR ~47% com clusters de losses."""
    pattern = []
    random.seed(42)
    pattern.extend(mc_signals(50, 0.60))
    pattern.extend(mc_signals(100, 0.45))
    pattern.extend(mc_signals(50, 0.40))
    pattern.extend(mc_signals(100, 0.50))
    return run_sim(name, strat_fn, max_gales, pattern)

# ═══════════════════════════════════════════════════════════════════
# CASCADE COST
# ═══════════════════════════════════════════════════════════════════

def cascade_cost(strat_fn, max_gales, name="", bal=50.0):
    """Custo de perder TODOS os gales (worst case de 1 cascata)."""
    if name in ADAPTIVE_NAMES:
        # For adaptive: cost of hitting max level from 0 (max_gales+1 losses in a row)
        total = 0.0
        for g in range(max_gales + 1):
            total += BASE_STAKE * FIB[min(g, len(FIB) - 1)]
        return total
    total = 0.0
    for g in range(max_gales + 1):
        s = strat_fn(g, BASE_STAKE, bal, max_gales)
        if s is None:
            break
        total += s
    return total

# ═══════════════════════════════════════════════════════════════════
# OUTPUT
# ═══════════════════════════════════════════════════════════════════

W = 110  # largura do separador

def header(title):
    print(f"\n{'═'*W}")
    print(f"  {title}")
    print(f"{'═'*W}")

def fmt_result(r: Result) -> str:
    wr = r.wins / r.trades * 100 if r.trades > 0 else 0
    pnl = r.final_bal - STARTING_BALANCE
    status = "💀BUST" if r.busted else "✅VIVO"
    return (f"  {r.name:<30s} | ${r.final_bal:>7.2f} | PnL ${pnl:>+7.2f} | "
            f"DD {r.max_dd_pct:>5.1f}% | Min ${r.min_bal:>6.2f} | "
            f"T:{r.trades:>3d} WR:{wr:>4.1f}% | Casc:{r.full_cascades:>2d} | {status}")

# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    random.seed(2026)

    header("PEGASUS BACKTEST COMPLETO — JD50 JUMP RISE/FALL")
    print(f"  Saldo Inicial: ${STARTING_BALANCE:.2f}")
    print(f"  Stake Base: ${BASE_STAKE:.2f}")
    print(f"  Payout: {PAYOUT_RATE*100:.1f}%")
    print(f"  Bust Floor: ${MIN_BALANCE:.2f}")

    # ── 0. Load Data ───────────────────────────────────────────────
    all_sigs = load_signals_csv()
    g0_sigs = load_g0_only()
    g0_wins = sum(1 for s in g0_sigs if s == "WIN")
    all_wins = sum(1 for s in all_sigs if s == "WIN")

    # Max consecutive losses
    max_streak = 0
    streak = 0
    for s in all_sigs:
        if s == "LOSS":
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0

    header("0. DADOS REAIS DO DB")
    print(f"  Total trades: {len(all_sigs)}")
    print(f"  G0 entries: {len(g0_sigs)} | G0 WR: {g0_wins}/{len(g0_sigs)} = {100*g0_wins/len(g0_sigs):.1f}%")
    print(f"  All trades WR: {all_wins}/{len(all_sigs)} = {100*all_wins/len(all_sigs):.1f}%")
    print(f"  Max consecutive losses: {max_streak}")
    print(f"  Sequência: {''.join('W' if s=='WIN' else 'L' for s in all_sigs[:80])}...")

    # ── 1. Cascade Cost ────────────────────────────────────────────
    header("1. CUSTO DE 1 CASCATA COMPLETA (todos gales perdem)")
    print(f"  {'Estratégia':<30s} | {'Custo':>8s} | {'Cascatas→Bust':>14s} | {'% da Banca':>10s}")
    print(f"  {'-'*30}-+-{'-'*8}-+-{'-'*14}-+-{'-'*10}")

    costs = []
    for name, fn, mg in STRATEGIES:
        c = cascade_cost(fn, mg, name)
        n_bust = int(STARTING_BALANCE / c) if c > 0 else 9999
        pct = c / STARTING_BALANCE * 100
        costs.append((name, c, n_bust, pct))
        print(f"  {name:<30s} | ${c:>6.2f} | {n_bust:>14d} | {pct:>9.1f}%")

    # ── 2. Real Data Backtest ──────────────────────────────────────
    header("2. BACKTEST DADOS REAIS (259 trades — sequência exata do dia)")
    print(f"  {'Estratégia':<30s} | {'Saldo':>8s} | {'PnL':>9s} | {'DD%':>6s} | {'Min$':>7s} | {'Trades':>6s} | {'WR%':>5s} | {'Casc':>4s} | Status")
    print(f"  {'-'*30}-+-{'-'*8}-+-{'-'*9}-+-{'-'*6}-+-{'-'*7}-+-{'-'*6}-+-{'-'*5}-+-{'-'*4}-+-{'-'*6}")

    real_results = []
    for name, fn, mg in STRATEGIES:
        r = run_sim(name, fn, mg, all_sigs)
        real_results.append(r)
        print(fmt_result(r))

    # ── 3. All-Loss Stress Test ────────────────────────────────────
    header("3. STRESS TEST: LOSSES CONSECUTIVAS ATÉ BUST")
    print(f"  {'Estratégia':<30s} | {'Trades→Bust':>12s}")
    print(f"  {'-'*30}-+-{'-'*12}")
    for name, fn, mg in STRATEGIES:
        tb, fb = all_loss_test(fn, mg, name)
        s = str(tb) if tb > 0 else "NUNCA (500L+)"
        print(f"  {name:<30s} | {s:>12s}")

    # ── 4. Pattern Tests ───────────────────────────────────────────
    header("4. STRESS TEST: PADRÕES ADVERSOS")

    print(f"\n  ─── Padrão W-L-L-L (25% WR, 800 trades) ───")
    for name, fn, mg in STRATEGIES:
        r = pattern_25pct(fn, mg, name)
        print(fmt_result(r))

    print(f"\n  ─── Padrão W-L-L (33% WR, 900 trades) ───")
    for name, fn, mg in STRATEGIES:
        r = pattern_33pct(fn, mg, name)
        print(fmt_result(r))

    print(f"\n  ─── Cascade Burst (3W + full cascade × 50 ciclos) ───")
    for name, fn, mg in STRATEGIES:
        r = cascade_burst_test(fn, mg, name)
        print(fmt_result(r))

    print(f"\n  ─── Worst Realistic (60%→45%→40%→50% WR, 300 trades) ───")
    for name, fn, mg in STRATEGIES:
        r = worst_realistic_test(fn, mg, name)
        print(fmt_result(r))

    # ── 5. Monte Carlo ─────────────────────────────────────────────
    n_strats = len(STRATEGIES)
    n_wrs = len(MC_WIN_RATES)
    total_mc = n_strats * n_wrs
    header(f"5. MONTE CARLO — {NUM_MC_SIMS:,} SIMULAÇÕES × {MC_TRADES} TRADES ({total_mc} combos)")

    # Collect MC results for final ranking
    mc_all = {}
    combo = 0
    for wr in MC_WIN_RATES:
        print(f"\n  ─── Win Rate: {wr*100:.0f}% ──────────────────────────────────────────────")
        print(f"  {'Estratégia':<30s} | {'Bust%':>6s} | {'Média$':>8s} | {'Mediana':>8s} | "
              f"{'P5':>8s} | {'P95':>8s} | {'Pior':>8s} | {'MaxDD%':>7s}")
        print(f"  {'-'*30}-+-{'-'*6}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*7}")

        for name, fn, mg in STRATEGIES:
            combo += 1
            mc = monte_carlo(fn, mg, name, wr)
            key = name
            if key not in mc_all:
                mc_all[key] = {}
            mc_all[key][wr] = mc

            print(f"  {name:<30s} | {mc['bust_pct']:>5.1f}% | ${mc['avg']:>7.2f} | "
                  f"${mc['median']:>7.2f} | ${mc['p5']:>7.2f} | ${mc['p95']:>7.2f} | "
                  f"${mc['worst']:>7.2f} | {mc['worst_dd']:>6.1f}%")

    # ── 6. RANKING FINAL ──────────────────────────────────────────
    header("6. RANKING FINAL — QUAL ESTRATÉGIA NUNCA BUSTA?")

    print(f"\n  Critérios (em ordem de importância):")
    print(f"    1. Bust Rate = 0% em MC com WR 47% (pior caso realista)")
    print(f"    2. Bust Rate = 0% em MC com WR 45% (stress)")
    print(f"    3. Sobrevive dados reais (259 trades)")
    print(f"    4. Menor max drawdown em MC 48%")
    print(f"    5. Maior saldo médio em MC 50%")

    # Score each strategy
    scores = []
    for name, fn, mg in STRATEGIES:
        mc47 = mc_all.get(name, {}).get(0.47, {})
        mc45 = mc_all.get(name, {}).get(0.45, {})
        mc48 = mc_all.get(name, {}).get(0.48, {})
        mc50 = mc_all.get(name, {}).get(0.50, {})

        real = next((r for r in real_results if r.name == name), None)
        survived_real = not real.busted if real else False

        bust47 = mc47.get("bust_pct", 100)
        bust45 = mc45.get("bust_pct", 100)
        bust48 = mc48.get("bust_pct", 100)
        avg50 = mc50.get("avg", 0)
        dd48 = mc48.get("worst_dd", 100)
        avg_min48 = mc48.get("avg_min", 0)

        # Score: lower bust = better, lower DD = better, higher avg = better
        score = 0
        if bust47 == 0: score += 100
        elif bust47 < 1: score += 80
        elif bust47 < 5: score += 50
        elif bust47 < 20: score += 20

        if bust45 == 0: score += 50
        elif bust45 < 5: score += 30
        elif bust45 < 20: score += 10

        if survived_real: score += 30
        if bust48 == 0: score += 20

        # Bonus for profitability at 50% WR
        if avg50 > STARTING_BALANCE:
            score += min(30, int((avg50 - STARTING_BALANCE) / 5))

        # Penalty for high DD
        if dd48 > 90: score -= 20
        elif dd48 > 70: score -= 10

        cost = cascade_cost(fn, mg, name)
        n_bust_cascades = int(STARTING_BALANCE / cost) if cost > 0 else 9999

        scores.append({
            "name": name,
            "score": score,
            "bust47": bust47,
            "bust45": bust45,
            "bust48": bust48,
            "avg50": avg50,
            "dd48": dd48,
            "survived_real": survived_real,
            "cascade_cost": cost,
            "cascades_to_bust": n_bust_cascades,
        })

    scores.sort(key=lambda x: x["score"], reverse=True)

    print(f"\n  {'#':>2s} {'Estratégia':<30s} | {'Score':>5s} | {'B47%':>5s} | {'B45%':>5s} | "
          f"{'B48%':>5s} | {'Avg@50':>8s} | {'DD48':>5s} | {'CascCost':>8s} | {'C→Bust':>6s} | {'Real':>5s}")
    print(f"  {'─'*2}─{'─'*30}─┼─{'─'*5}─┼─{'─'*5}─┼─{'─'*5}─┼─{'─'*5}─┼─{'─'*8}─┼─{'─'*5}─┼─{'─'*8}─┼─{'─'*6}─┼─{'─'*5}")

    for i, s in enumerate(scores):
        real_str = "✅" if s["survived_real"] else "💀"
        print(f"  {i+1:>2d} {s['name']:<30s} | {s['score']:>5d} | {s['bust47']:>4.1f}% | {s['bust45']:>4.1f}% | "
              f"{s['bust48']:>4.1f}% | ${s['avg50']:>7.2f} | {s['dd48']:>4.1f}% | ${s['cascade_cost']:>7.2f} | "
              f"{s['cascades_to_bust']:>6d} | {real_str}")

    # Winner
    winner = scores[0]
    header("7. RECOMENDAÇÃO FINAL")
    print(f"\n  🏆 MELHOR ESTRATÉGIA: {winner['name']}")
    print(f"     Score: {winner['score']}")
    print(f"     Bust Rate @47% WR: {winner['bust47']:.1f}%")
    print(f"     Bust Rate @45% WR: {winner['bust45']:.1f}%")
    print(f"     Avg Balance @50% WR: ${winner['avg50']:.2f}")
    print(f"     Cascade Cost: ${winner['cascade_cost']:.2f}")
    print(f"     Cascatas até bust: {winner['cascades_to_bust']}")
    print(f"     Sobreviveu dados reais: {'SIM ✅' if winner['survived_real'] else 'NÃO 💀'}")

    # Top 3
    print(f"\n  TOP 3:")
    for i, s in enumerate(scores[:3]):
        print(f"    {i+1}. {s['name']} (score={s['score']}, bust@47={s['bust47']:.1f}%, avg@50=${s['avg50']:.2f})")

    # Danger zone
    print(f"\n  ⚠️  ESTRATÉGIAS PERIGOSAS (bust >10% @47% WR):")
    for s in scores:
        if s["bust47"] > 10:
            print(f"    ❌ {s['name']} (bust@47={s['bust47']:.1f}%)")

    # Current strategy comparison
    current = next((s for s in scores if "ATUAL" in s["name"]), None)
    if current:
        rank = next(i+1 for i, s in enumerate(scores) if s["name"] == current["name"])
        print(f"\n  📍 ESTRATÉGIA ATUAL (Fib 7 gales):")
        print(f"     Ranking: #{rank}/{len(scores)}")
        print(f"     Bust Rate @47%: {current['bust47']:.1f}%")
        print(f"     Bust Rate @45%: {current['bust45']:.1f}%")
        print(f"     Cascade Cost: ${current['cascade_cost']:.2f}")
        print(f"     Sobreviveu dados reais: {'SIM ✅' if current['survived_real'] else 'NÃO 💀'}")


if __name__ == "__main__":
    main()
