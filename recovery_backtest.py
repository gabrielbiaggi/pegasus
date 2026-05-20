#!/usr/bin/env python3
"""
Pegasus Recovery Strategy Backtest
===================================
Simula 7 estratégias de recuperação usando:
1. Replay dos 58 trades reais (apenas sequência WIN/LOSS)
2. Monte Carlo: 10.000 sessões de 200 trades com WR real

Estratégias testadas:
  A) Pure Martingale (atual, G15 max)
  B) Capped Martingale G4 (absorve perda)
  C) Soft Recovery (G4 cap + pool distribuído)
  D) Fibonacci Recovery (escala Fibonacci, não 2x)
  E) D'Alembert (incremento linear)
  F) Anti-Martingale Hybrid (G3 + flat recovery chunks)
  G) Percentage Recovery (% do pool por trade)
"""

import csv
import random
import statistics
from dataclasses import dataclass, field
from typing import List, Tuple

# ── Constantes ──
PAYOUT = 0.953       # 95.3% payout da Deriv
BASE_STAKE = 0.35
INITIAL_BALANCE = 50.0
WIN_RATE = 0.515     # WR observado em sessões longas (~51.5%)
MC_SESSIONS = 10_000
MC_TRADES_PER_SESSION = 200

# ── Carregar trades reais ──
def load_real_trades(path: str) -> List[str]:
    """Retorna lista de 'WIN'/'LOSS' na ordem real."""
    results = []
    with open(path) as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 6 and row[5].strip() in ('WIN', 'LOSS'):
                results.append(row[5].strip())
    return results


# ── Gerar sequência Monte Carlo ──
def generate_mc_sequence(n: int, wr: float) -> List[str]:
    return ['WIN' if random.random() < wr else 'LOSS' for _ in range(n)]


# ══════════════════════════════════════════════════
# ESTRATÉGIAS
# ══════════════════════════════════════════════════

@dataclass
class Result:
    name: str
    balance: float
    trades: int
    wins: int
    losses: int
    max_drawdown: float
    peak_balance: float
    recovery_pool: float = 0.0
    busted: bool = False  # saldo < base_stake


def strategy_a_pure_martingale(outcomes: List[str], balance: float = INITIAL_BALANCE, max_gales: int = 15) -> Result:
    """Martingale puro com multiplicador 2.05x, até max_gales."""
    bal = balance
    peak = bal
    max_dd = 0.0
    wins = losses = 0
    gale_step = 0
    acc_loss = 0.0
    mult = 2.0 / PAYOUT  # ~2.098

    for outcome in outcomes:
        if bal < BASE_STAKE:
            return Result("A) Martingale Puro G15", bal, wins + losses, wins, losses, max_dd, peak, busted=True)

        if gale_step == 0:
            stake = BASE_STAKE
        else:
            stake = round((acc_loss + BASE_STAKE) * mult ** (gale_step - 1) * (1 / PAYOUT), 2)
            # Mas fórmula real: stake = (acc_loss + base) / payout
            # Simplificação: dobra ajustada
            raw = acc_loss / PAYOUT + BASE_STAKE
            stake = min(raw, bal)

        stake = min(stake, bal)
        stake = max(stake, BASE_STAKE)

        if outcome == 'WIN':
            profit = round(stake * PAYOUT, 2)
            bal += profit
            wins += 1
            gale_step = 0
            acc_loss = 0.0
        else:
            bal -= stake
            losses += 1
            acc_loss += stake
            gale_step += 1
            if gale_step > max_gales:
                gale_step = 0
                acc_loss = 0.0

        peak = max(peak, bal)
        dd = peak - bal
        max_dd = max(max_dd, dd)

    return Result("A) Martingale Puro G15", bal, wins + losses, wins, losses, max_dd, peak)


def strategy_b_capped_g4(outcomes: List[str], balance: float = INITIAL_BALANCE) -> Result:
    """Martingale capped em G4, absorve perda depois."""
    bal = balance
    peak = bal
    max_dd = 0.0
    wins = losses = 0
    gale_step = 0
    acc_loss = 0.0
    absorbed_total = 0.0

    for outcome in outcomes:
        if bal < BASE_STAKE:
            return Result("B) Martingale G4 (absorve)", bal, wins + losses, wins, losses, max_dd, peak, busted=True)

        if gale_step == 0:
            stake = BASE_STAKE
        else:
            raw = acc_loss / PAYOUT + BASE_STAKE
            stake = min(raw, bal)
            stake = max(stake, BASE_STAKE)

        stake = min(stake, bal)

        if outcome == 'WIN':
            profit = round(stake * PAYOUT, 2)
            bal += profit
            wins += 1
            gale_step = 0
            acc_loss = 0.0
        else:
            bal -= stake
            losses += 1
            acc_loss += stake
            gale_step += 1
            if gale_step > 4:
                absorbed_total += acc_loss
                gale_step = 0
                acc_loss = 0.0

        peak = max(peak, bal)
        dd = peak - bal
        max_dd = max(max_dd, dd)

    return Result("B) Martingale G4 (absorve)", bal, wins + losses, wins, losses, max_dd, peak)


def strategy_c_soft_recovery(outcomes: List[str], balance: float = INITIAL_BALANCE,
                              surcharge_pct: float = 0.30) -> Result:
    """G4 cap + recovery pool. Cada WIN base paga surcharge_pct do pool."""
    bal = balance
    peak = bal
    max_dd = 0.0
    wins = losses = 0
    gale_step = 0
    acc_loss = 0.0
    recovery_pool = 0.0

    for outcome in outcomes:
        if bal < BASE_STAKE:
            return Result("C) Soft Recovery 30%", bal, wins + losses, wins, losses, max_dd, peak, recovery_pool, busted=True)

        if gale_step == 0:
            # Stake base + surcharge do pool
            surcharge = round(recovery_pool * surcharge_pct, 2) if recovery_pool > 0.01 else 0.0
            surcharge = min(surcharge, bal - BASE_STAKE)  # não exceder saldo
            surcharge = max(surcharge, 0)
            stake = BASE_STAKE + surcharge
        else:
            raw = acc_loss / PAYOUT + BASE_STAKE
            stake = min(raw, bal)
            stake = max(stake, BASE_STAKE)

        stake = min(stake, bal)

        if outcome == 'WIN':
            profit = round(stake * PAYOUT, 2)
            bal += profit
            wins += 1
            if gale_step == 0 and recovery_pool > 0:
                # O surcharge que adicionamos já foi "investido", o lucro extra vai diminuir o pool
                recovered = round(surcharge * PAYOUT, 2) if surcharge > 0 else 0
                recovery_pool = max(0, recovery_pool - recovered)
            gale_step = 0
            acc_loss = 0.0
        else:
            bal -= stake
            losses += 1
            acc_loss += stake
            gale_step += 1
            if gale_step > 4:
                recovery_pool += acc_loss
                gale_step = 0
                acc_loss = 0.0

        peak = max(peak, bal)
        dd = peak - bal
        max_dd = max(max_dd, dd)

    return Result("C) Soft Recovery 30%", bal, wins + losses, wins, losses, max_dd, peak, recovery_pool)


def strategy_d_fibonacci(outcomes: List[str], balance: float = INITIAL_BALANCE) -> Result:
    """Fibonacci sequence para stakes: 0.35, 0.35, 0.72, 1.07, 1.79, 2.86..."""
    fibs = [1, 1, 2, 3, 5, 8, 13, 21]  # multiplicadores
    bal = balance
    peak = bal
    max_dd = 0.0
    wins = losses = 0
    fib_idx = 0

    for outcome in outcomes:
        if bal < BASE_STAKE:
            return Result("D) Fibonacci", bal, wins + losses, wins, losses, max_dd, peak, busted=True)

        stake = round(BASE_STAKE * fibs[min(fib_idx, len(fibs) - 1)], 2)
        stake = min(stake, bal)

        if outcome == 'WIN':
            profit = round(stake * PAYOUT, 2)
            bal += profit
            wins += 1
            fib_idx = max(0, fib_idx - 2)  # volta 2 posições
        else:
            bal -= stake
            losses += 1
            fib_idx += 1
            if fib_idx >= len(fibs):
                fib_idx = 0  # reset se esgotou

        peak = max(peak, bal)
        dd = peak - bal
        max_dd = max(max_dd, dd)

    return Result("D) Fibonacci", bal, wins + losses, wins, losses, max_dd, peak)


def strategy_e_dalembert(outcomes: List[str], balance: float = INITIAL_BALANCE,
                          increment: float = 0.35) -> Result:
    """D'Alembert: +increment por loss, -increment por win. Floor = BASE_STAKE."""
    bal = balance
    peak = bal
    max_dd = 0.0
    wins = losses = 0
    stake = BASE_STAKE

    for outcome in outcomes:
        if bal < BASE_STAKE:
            return Result("E) D'Alembert", bal, wins + losses, wins, losses, max_dd, peak, busted=True)

        stake = min(stake, bal)
        stake = max(stake, BASE_STAKE)

        if outcome == 'WIN':
            profit = round(stake * PAYOUT, 2)
            bal += profit
            wins += 1
            stake = max(BASE_STAKE, round(stake - increment, 2))
        else:
            bal -= stake
            losses += 1
            stake = round(stake + increment, 2)

        peak = max(peak, bal)
        dd = peak - bal
        max_dd = max(max_dd, dd)

    return Result("E) D'Alembert", bal, wins + losses, wins, losses, max_dd, peak)


def strategy_f_anti_hybrid(outcomes: List[str], balance: float = INITIAL_BALANCE) -> Result:
    """G3 martingale + flat recovery: divide perda em 5 chunks fixos."""
    bal = balance
    peak = bal
    max_dd = 0.0
    wins = losses = 0
    gale_step = 0
    acc_loss = 0.0
    recovery_chunks: List[float] = []  # lista de chunks pendentes

    for outcome in outcomes:
        if bal < BASE_STAKE:
            return Result("F) Anti-Hybrid G3+chunks", bal, wins + losses, wins, losses, max_dd, peak,
                          sum(recovery_chunks), busted=True)

        if gale_step == 0:
            # Stake base + próximo chunk de recuperação
            chunk = recovery_chunks[0] if recovery_chunks else 0.0
            stake = BASE_STAKE + chunk
            stake = min(stake, bal)
        else:
            raw = acc_loss / PAYOUT + BASE_STAKE
            stake = min(raw, bal)
            stake = max(stake, BASE_STAKE)

        stake = min(stake, bal)

        if outcome == 'WIN':
            profit = round(stake * PAYOUT, 2)
            bal += profit
            wins += 1
            if gale_step == 0 and recovery_chunks:
                recovery_chunks.pop(0)  # chunk recuperado
            gale_step = 0
            acc_loss = 0.0
        else:
            bal -= stake
            losses += 1
            acc_loss += stake
            gale_step += 1
            if gale_step > 3:
                # Divide a perda acumulada em 5 chunks
                chunk_size = round(acc_loss / (5 * PAYOUT), 2)
                recovery_chunks.extend([chunk_size] * 5)
                gale_step = 0
                acc_loss = 0.0

        peak = max(peak, bal)
        dd = peak - bal
        max_dd = max(max_dd, dd)

    return Result("F) Anti-Hybrid G3+chunks", bal, wins + losses, wins, losses, max_dd, peak, sum(recovery_chunks))


def strategy_g_pct_recovery(outcomes: List[str], balance: float = INITIAL_BALANCE,
                             pct_of_balance: float = 0.02) -> Result:
    """G4 cap + recovery via % do saldo. Stake = base + pool_remaining * pct_of_balance."""
    bal = balance
    peak = bal
    max_dd = 0.0
    wins = losses = 0
    gale_step = 0
    acc_loss = 0.0
    recovery_pool = 0.0

    for outcome in outcomes:
        if bal < BASE_STAKE:
            return Result("G) % Recovery (2%bal)", bal, wins + losses, wins, losses, max_dd, peak, recovery_pool, busted=True)

        if gale_step == 0:
            surcharge = round(bal * pct_of_balance, 2) if recovery_pool > 0.01 else 0.0
            surcharge = min(surcharge, recovery_pool)  # nunca mais que o pool
            stake = BASE_STAKE + surcharge
        else:
            raw = acc_loss / PAYOUT + BASE_STAKE
            stake = min(raw, bal)
            stake = max(stake, BASE_STAKE)

        stake = min(stake, bal)

        if outcome == 'WIN':
            profit = round(stake * PAYOUT, 2)
            bal += profit
            wins += 1
            if gale_step == 0 and recovery_pool > 0:
                recovered = round(surcharge * PAYOUT, 2)
                recovery_pool = max(0, recovery_pool - recovered)
            gale_step = 0
            acc_loss = 0.0
        else:
            bal -= stake
            losses += 1
            acc_loss += stake
            gale_step += 1
            if gale_step > 4:
                recovery_pool += acc_loss
                gale_step = 0
                acc_loss = 0.0

        peak = max(peak, bal)
        dd = peak - bal
        max_dd = max(max_dd, dd)

    return Result("G) % Recovery (2%bal)", bal, wins + losses, wins, losses, max_dd, peak, recovery_pool)


# ══════════════════════════════════════════════════
# RUNNER
# ══════════════════════════════════════════════════

STRATEGIES = [
    ("A) Martingale Puro G15", strategy_a_pure_martingale),
    ("B) Martingale G4 (absorve)", strategy_b_capped_g4),
    ("C) Soft Recovery 30%", strategy_c_soft_recovery),
    ("D) Fibonacci", strategy_d_fibonacci),
    ("E) D'Alembert", strategy_e_dalembert),
    ("F) Anti-Hybrid G3+chunks", strategy_f_anti_hybrid),
    ("G) % Recovery (2%bal)", strategy_g_pct_recovery),
]


def run_real_data(trades_path: str):
    """Replay com dados reais."""
    outcomes = load_real_trades(trades_path)
    print(f"\n{'='*80}")
    print(f"REPLAY COM DADOS REAIS — {len(outcomes)} trades")
    print(f"{'='*80}")
    print(f"WR real: {sum(1 for o in outcomes if o == 'WIN')}/{len(outcomes)} = "
          f"{100*sum(1 for o in outcomes if o == 'WIN')/len(outcomes):.1f}%")
    print(f"Saldo inicial: ${INITIAL_BALANCE:.2f}")
    print()

    results = []
    for name, fn in STRATEGIES:
        r = fn(outcomes)
        results.append(r)

    _print_table(results)
    return outcomes


def run_monte_carlo():
    """Monte Carlo: 10.000 sessões de 200 trades."""
    print(f"\n{'='*80}")
    print(f"MONTE CARLO — {MC_SESSIONS:,} sessões × {MC_TRADES_PER_SESSION} trades | WR={WIN_RATE*100:.1f}%")
    print(f"{'='*80}\n")

    all_results = {name: [] for name, _ in STRATEGIES}

    random.seed(42)
    for _ in range(MC_SESSIONS):
        outcomes = generate_mc_sequence(MC_TRADES_PER_SESSION, WIN_RATE)
        for name, fn in STRATEGIES:
            r = fn(outcomes)
            all_results[name].append(r)

    # Estatísticas
    print(f"{'Estratégia':<30} {'Saldo Med':>10} {'Saldo P10':>10} {'Saldo P90':>10} "
          f"{'DD Med':>8} {'DD Max':>8} {'Bust%':>7} {'Pool Med':>10}")
    print("-" * 115)

    summary = []
    for name, _ in STRATEGIES:
        rs = all_results[name]
        balances = [r.balance for r in rs]
        dds = [r.max_drawdown for r in rs]
        pools = [r.recovery_pool for r in rs]
        busts = sum(1 for r in rs if r.busted)

        bal_med = statistics.median(balances)
        bal_p10 = sorted(balances)[int(len(balances) * 0.10)]
        bal_p90 = sorted(balances)[int(len(balances) * 0.90)]
        dd_med = statistics.median(dds)
        dd_max = max(dds)
        pool_med = statistics.median(pools) if any(p > 0 for p in pools) else 0

        print(f"{name:<30} ${bal_med:>8.2f} ${bal_p10:>8.2f} ${bal_p90:>8.2f} "
              f"${dd_med:>6.2f} ${dd_max:>6.2f} {100*busts/len(rs):>5.1f}% ${pool_med:>8.2f}")

        summary.append({
            'name': name, 'bal_med': bal_med, 'bal_p10': bal_p10, 'bal_p90': bal_p90,
            'dd_med': dd_med, 'dd_max': dd_max, 'bust_pct': 100*busts/len(rs),
            'pool_med': pool_med,
        })

    # Ranking
    print(f"\n{'─'*80}")
    print("RANKING (por saldo mediano × (1 - bust%) × (1/DD))")
    print(f"{'─'*80}")
    for i, s in enumerate(sorted(summary, key=lambda x: x['bal_med'] * (1 - x['bust_pct']/100) / max(x['dd_med'], 0.01), reverse=True)):
        score = s['bal_med'] * (1 - s['bust_pct']/100) / max(s['dd_med'], 0.01)
        marker = " ← MELHOR" if i == 0 else ""
        print(f"  {i+1}. {s['name']:<30} score={score:.2f}{marker}")

    return summary


def run_worst_case():
    """Testa cenários extremos: 8-12 losses consecutivos no início."""
    print(f"\n{'='*80}")
    print("STRESS TEST — Worst Case: 8 losses seguidos no início + 192 trades WR=51.5%")
    print(f"{'='*80}\n")

    random.seed(123)
    # 8 losses + 192 trades normais
    worst_start = ['LOSS'] * 8
    normal_rest = generate_mc_sequence(192, WIN_RATE)
    outcomes = worst_start + normal_rest

    results = []
    for name, fn in STRATEGIES:
        r = fn(outcomes)
        results.append(r)

    _print_table(results)

    # 12 losses seguidos
    print(f"\n{'─'*80}")
    print("EXTREME: 12 losses seguidos no início + 188 trades WR=51.5%")
    print(f"{'─'*80}\n")

    worst_start_12 = ['LOSS'] * 12
    normal_rest_12 = generate_mc_sequence(188, WIN_RATE)
    outcomes_12 = worst_start_12 + normal_rest_12

    results_12 = []
    for name, fn in STRATEGIES:
        r = fn(outcomes_12)
        results_12.append(r)

    _print_table(results_12)


def _print_table(results: List[Result]):
    print(f"{'Estratégia':<30} {'Saldo':>10} {'Trades':>7} {'W':>4} {'L':>4} "
          f"{'WR%':>6} {'DD Max':>8} {'Pool':>8} {'Bust':>6}")
    print("-" * 100)
    for r in results:
        wr = 100 * r.wins / max(r.trades, 1)
        bust = "💀 SIM" if r.busted else "   NÃO"
        print(f"{r.name:<30} ${r.balance:>8.2f} {r.trades:>7} {r.wins:>4} {r.losses:>4} "
              f"{wr:>5.1f}% ${r.max_drawdown:>6.2f} ${r.recovery_pool:>6.2f} {bust}")


if __name__ == '__main__':
    # 1. Replay real
    outcomes = run_real_data('/tmp/pegasus_trades.csv')

    # 2. Worst-case stress test
    run_worst_case()

    # 3. Monte Carlo massivo
    summary = run_monte_carlo()

    print(f"\n{'='*80}")
    print("CONCLUSÃO")
    print(f"{'='*80}")
    best = min(summary, key=lambda x: x['bust_pct'])
    safest = min(summary, key=lambda x: x['dd_med'])
    richest = max(summary, key=lambda x: x['bal_med'])
    print(f"  Menor bust rate:  {best['name']} ({best['bust_pct']:.1f}%)")
    print(f"  Menor drawdown:   {safest['name']} (DD med ${safest['dd_med']:.2f})")
    print(f"  Maior saldo med:  {richest['name']} (${richest['bal_med']:.2f})")
    print()
