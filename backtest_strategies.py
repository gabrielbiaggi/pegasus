#!/usr/bin/env python3
"""
Backtest completo: compara estratégias de recovery no JD50 jump_rise_fall.

Usa:
  1. Dados reais (259 trades JD50 do dia 2026-05-20)
  2. Monte Carlo (10.000 simulações com WR variável)

Objetivo: encontrar a estratégia que NUNCA zera a banca ($50).
"""
from __future__ import annotations

import csv
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable


# ── Configuração base ─────────────────────────────────────────────
BASE_STAKE = 0.35
PAYOUT_RATE = 0.953        # lucro = stake * payout_rate
STARTING_BALANCE = 50.00
MIN_BALANCE_FLOOR = 1.00   # abaixo disso = BUST
NUM_MONTE_CARLO = 10_000
MC_TRADES_PER_SIM = 500    # trades por simulação MC
MC_WIN_RATES = [0.45, 0.48, 0.50, 0.52, 0.55]

FIB_SEQUENCE = [1, 1, 2, 3, 5, 8, 13, 21]


# ── Estratégias ───────────────────────────────────────────────────

@dataclass
class StrategyResult:
    name: str
    final_balance: float = 0.0
    min_balance: float = float("inf")
    max_balance: float = 0.0
    total_trades: int = 0
    total_wins: int = 0
    total_losses: int = 0
    busted: bool = False
    bust_trade: int = 0
    max_drawdown_pct: float = 0.0
    peak_balance: float = 0.0
    cascades_full: int = 0       # quantas vezes atingiu max gale
    total_staked: float = 0.0
    equity_curve: list[float] = field(default_factory=list)


def simulate_strategy(
    signals: list[str],       # sequência de "WIN"/"LOSS" para G0
    strategy_fn: Callable,    # (gale_step, base_stake, balance) -> stake | None
    max_gales: int,
    name: str,
    starting_balance: float = STARTING_BALANCE,
) -> StrategyResult:
    """
    Simula uma estratégia de recovery sobre uma sequência de sinais G0.

    Para cada sinal G0:
      - Se WIN: lucro = stake * PAYOUT_RATE, reset gale
      - Se LOSS: avança gale. Se gale > max_gales, aceita perda total e reset.

    O trick aqui é que o signal sequence nos dá o resultado do G0.
    Para gales (G1+), assumimos que cada gale TEM A MESMA probabilidade
    de WIN/LOSS que o G0 (baseado no WR geral dos dados).

    Na verdade, nos dados reais, gales G1+ mostram WR levemente melhor (~55-60%).
    Mas para stress test, vamos assumir WR igual ao G0.
    """
    result = StrategyResult(name=name)
    balance = starting_balance
    result.peak_balance = balance
    result.equity_curve.append(balance)

    # Calcula WR do dataset para uso nos gales
    total_w = sum(1 for s in signals if s == "WIN")
    wr = total_w / len(signals) if signals else 0.5

    i = 0
    while i < len(signals):
        if balance < MIN_BALANCE_FLOOR:
            result.busted = True
            result.bust_trade = result.total_trades
            break

        # Entrada G0
        gale_step = 0
        stake = strategy_fn(gale_step, BASE_STAKE, balance, max_gales)
        if stake is None or stake <= 0:
            i += 1
            continue

        # Garante que não aposta mais do que tem
        stake = min(stake, balance)

        outcome = signals[i]
        result.total_trades += 1
        result.total_staked += stake

        if outcome == "WIN":
            balance += stake * PAYOUT_RATE
            result.total_wins += 1
            i += 1
        else:
            balance -= stake
            result.total_losses += 1
            i += 1

            # Tenta gales
            while gale_step < max_gales and i < len(signals):
                gale_step += 1
                stake = strategy_fn(gale_step, BASE_STAKE, balance, max_gales)
                if stake is None or stake <= 0:
                    break
                stake = min(stake, balance)

                if balance < MIN_BALANCE_FLOOR or stake < 0.35:
                    break

                # Usa próximo sinal como resultado do gale
                outcome = signals[i]
                result.total_trades += 1
                result.total_staked += stake

                if outcome == "WIN":
                    balance += stake * PAYOUT_RATE
                    result.total_wins += 1
                    i += 1
                    break  # Recuperou, reset
                else:
                    balance -= stake
                    result.total_losses += 1
                    i += 1

                    if gale_step >= max_gales:
                        result.cascades_full += 1

        # Track equity
        result.equity_curve.append(balance)
        result.min_balance = min(result.min_balance, balance)
        result.max_balance = max(result.max_balance, balance)
        if balance > result.peak_balance:
            result.peak_balance = balance
        dd = (result.peak_balance - balance) / result.peak_balance * 100 if result.peak_balance > 0 else 0
        result.max_drawdown_pct = max(result.max_drawdown_pct, dd)

        if balance < MIN_BALANCE_FLOOR:
            result.busted = True
            result.bust_trade = result.total_trades
            break

    result.final_balance = balance
    return result


# ── Strategy Functions ─────────────────────────────────────────────

def flat_strategy(gale_step: int, base: float, balance: float, max_gales: int) -> float | None:
    """Flat betting — sem recovery, sempre aposta base."""
    if gale_step > 0:
        return None
    return base


def martingale_strategy(gale_step: int, base: float, balance: float, max_gales: int) -> float:
    """Martingale clássico: dobra a cada loss."""
    return base * (2 ** gale_step)


def fibonacci_strategy(gale_step: int, base: float, balance: float, max_gales: int) -> float:
    """Fibonacci recovery: multiplica por FIB_SEQUENCE."""
    idx = min(gale_step, len(FIB_SEQUENCE) - 1)
    return base * FIB_SEQUENCE[idx]


def conservative_fib_strategy(gale_step: int, base: float, balance: float, max_gales: int) -> float:
    """Fibonacci conservador: FIB limitado = [1, 1, 2, 3]."""
    CONS_FIB = [1, 1, 2, 3]
    idx = min(gale_step, len(CONS_FIB) - 1)
    return base * CONS_FIB[idx]


def linear_strategy(gale_step: int, base: float, balance: float, max_gales: int) -> float:
    """Linear recovery: incrementa base a cada loss."""
    return base * (1 + gale_step)


def percent_balance_strategy(gale_step: int, base: float, balance: float, max_gales: int) -> float:
    """Aposta 1% do saldo (Kelly-like)."""
    stake = max(0.35, balance * 0.01)
    if gale_step > 0:
        stake = max(0.35, balance * 0.01 * (gale_step + 1))
    return stake


def anti_martingale_strategy(gale_step: int, base: float, balance: float, max_gales: int) -> float | None:
    """Anti-martingale: sem gale, só reseta."""
    if gale_step > 0:
        return None
    return base


def soft_martingale_strategy(gale_step: int, base: float, balance: float, max_gales: int) -> float:
    """Soft martingale: multiplica por 1.5 ao invés de 2."""
    return base * (1.5 ** gale_step)


def dalembert_strategy(gale_step: int, base: float, balance: float, max_gales: int) -> float:
    """D'Alembert: soma base a cada loss."""
    return base + (base * gale_step)


# ── Cascade Cost Calculator ───────────────────────────────────────

def calc_cascade_cost(strategy_fn, max_gales: int, balance: float = 50.0) -> float:
    """Calcula custo total se perder TODOS os gales (worst case)."""
    total = 0.0
    for g in range(max_gales + 1):
        stake = strategy_fn(g, BASE_STAKE, balance, max_gales)
        if stake is None:
            break
        total += min(stake, balance - total)
    return total


# ── Load real data ─────────────────────────────────────────────────

def load_real_signals(path: str = "data/trades_JD50_history.csv") -> list[str]:
    """Carrega resultados reais dos trades JD50."""
    signals = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            signals.append(row["result"])
    return signals


def load_g0_signals(path: str = "data/trades_JD50_history.csv") -> list[str]:
    """Carrega APENAS sinais G0 (entradas puras)."""
    signals = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if int(row["gale_step"]) == 0:
                signals.append(row["result"])
    return signals


# ── Monte Carlo ────────────────────────────────────────────────────

def generate_mc_signals(num_trades: int, win_rate: float) -> list[str]:
    """Gera sequência aleatória com dado WR."""
    return ["WIN" if random.random() < win_rate else "LOSS" for _ in range(num_trades)]


def monte_carlo_test(
    strategy_fn: Callable,
    max_gales: int,
    name: str,
    win_rate: float,
    num_sims: int = NUM_MONTE_CARLO,
    trades_per_sim: int = MC_TRADES_PER_SIM,
) -> dict:
    """Roda N simulações Monte Carlo."""
    busts = 0
    final_balances = []
    min_balances = []
    max_drawdowns = []

    for _ in range(num_sims):
        signals = generate_mc_signals(trades_per_sim, win_rate)
        result = simulate_strategy(signals, strategy_fn, max_gales, name)
        if result.busted:
            busts += 1
        final_balances.append(result.final_balance)
        min_balances.append(result.min_balance)
        max_drawdowns.append(result.max_drawdown_pct)

    final_balances.sort()
    return {
        "name": name,
        "win_rate": win_rate,
        "bust_rate": busts / num_sims * 100,
        "busts": busts,
        "avg_final": sum(final_balances) / len(final_balances),
        "median_final": final_balances[len(final_balances) // 2],
        "p5_final": final_balances[int(len(final_balances) * 0.05)],
        "p95_final": final_balances[int(len(final_balances) * 0.95)],
        "worst_final": final_balances[0],
        "best_final": final_balances[-1],
        "avg_min_bal": sum(min_balances) / len(min_balances),
        "worst_min_bal": min(min_balances),
        "avg_max_dd": sum(max_drawdowns) / len(max_drawdowns),
        "worst_max_dd": max(max_drawdowns),
    }


# ── Stress Tests ───────────────────────────────────────────────────

def worst_case_streak_test(strategy_fn, max_gales: int, name: str) -> dict:
    """Testa o pior cenário: quantas losses consecutivas até bust."""
    balance = STARTING_BALANCE
    trades = 0
    all_loss_signals = ["LOSS"] * 200  # 200 losses seguidas
    result = simulate_strategy(all_loss_signals, strategy_fn, max_gales, name)
    return {
        "name": name,
        "trades_to_bust": result.bust_trade if result.busted else -1,
        "busted": result.busted,
        "final_balance": result.final_balance,
    }


def alternating_loss_test(strategy_fn, max_gales: int, name: str) -> StrategyResult:
    """W L L L W L L L ... (25% WR em pattern)."""
    pattern = ["WIN", "LOSS", "LOSS", "LOSS"] * 100
    return simulate_strategy(pattern, strategy_fn, max_gales, name)


# ── Main ──────────────────────────────────────────────────────────

STRATEGIES = [
    ("Flat (sem gale)",       flat_strategy,           0),
    ("Fib 2 gales",           fibonacci_strategy,      2),
    ("Fib 3 gales",           fibonacci_strategy,      3),
    ("Fib 4 gales",           fibonacci_strategy,      4),
    ("Fib 5 gales",           fibonacci_strategy,      5),
    ("Fib 7 gales (atual)",   fibonacci_strategy,      7),
    ("Martingale 2 gales",    martingale_strategy,     2),
    ("Martingale 3 gales",    martingale_strategy,     3),
    ("Martingale 4 gales",    martingale_strategy,     4),
    ("Soft Mart 3 gales",     soft_martingale_strategy, 3),
    ("Soft Mart 4 gales",     soft_martingale_strategy, 4),
    ("Linear 3 gales",        linear_strategy,         3),
    ("Linear 4 gales",        linear_strategy,         4),
    ("Cons Fib 3 gales",      conservative_fib_strategy, 3),
    ("D'Alembert 3 gales",    dalembert_strategy,      3),
    ("D'Alembert 4 gales",    dalembert_strategy,      4),
    ("1% Balance",            percent_balance_strategy, 3),
    ("Anti-Mart (flat+stop)", anti_martingale_strategy, 0),
]


def print_header(title: str) -> None:
    print(f"\n{'='*90}")
    print(f"  {title}")
    print(f"{'='*90}")


def print_strategy_result(r: StrategyResult) -> None:
    wr = r.total_wins / r.total_trades * 100 if r.total_trades > 0 else 0
    pnl = r.final_balance - STARTING_BALANCE
    roi = pnl / STARTING_BALANCE * 100
    print(f"  {r.name:<25s} | Bal: ${r.final_balance:>7.2f} | PnL: ${pnl:>+7.2f} | "
          f"ROI: {roi:>+6.1f}% | DD: {r.max_drawdown_pct:>5.1f}% | "
          f"Min: ${r.min_balance:>6.2f} | Trades: {r.total_trades:>3d} | "
          f"WR: {wr:>4.1f}% | Cascades: {r.cascades_full:>2d} | "
          f"{'💀 BUST' if r.busted else '✅ VIVO'}")


def main():
    print_header("PEGASUS BACKTEST COMPLETO — JD50 Jump Rise/Fall")
    print(f"  Starting Balance: ${STARTING_BALANCE:.2f}")
    print(f"  Base Stake: ${BASE_STAKE:.2f}")
    print(f"  Payout Rate: {PAYOUT_RATE*100:.1f}%")
    print(f"  Min Balance Floor: ${MIN_BALANCE_FLOOR:.2f}")

    # ── 1. Custo de cascata worst-case ─────────────────────────────
    print_header("1. CUSTO DE CASCATA COMPLETA (all gales lose)")
    print(f"  {'Estratégia':<25s} | {'Custo Total':>12s} | {'Cascatas até bust':>18s}")
    print(f"  {'-'*25}-+-{'-'*12}-+-{'-'*18}")
    for name, fn, mg in STRATEGIES:
        if mg == 0 and fn != percent_balance_strategy:
            cost = BASE_STAKE
        else:
            cost = calc_cascade_cost(fn, mg)
        cascades_to_bust = int(STARTING_BALANCE / cost) if cost > 0 else float("inf")
        print(f"  {name:<25s} | ${cost:>10.2f} | {cascades_to_bust:>18d}")

    # ── 2. Backtest com dados reais ────────────────────────────────
    print_header("2. BACKTEST COM DADOS REAIS (259 trades JD50)")

    all_signals = load_real_signals()
    g0_signals = load_g0_signals()

    print(f"  Total sinais: {len(all_signals)} (todos) / {len(g0_signals)} (G0 only)")
    g0_wins = sum(1 for s in g0_signals if s == "WIN")
    print(f"  G0 Win Rate: {g0_wins}/{len(g0_signals)} = {100*g0_wins/len(g0_signals):.1f}%")

    # Conta max consecutive losses
    max_streak = 0
    streak = 0
    for s in all_signals:
        if s == "LOSS":
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    print(f"  Max consecutive losses: {max_streak}")
    print()

    real_results = []
    for name, fn, mg in STRATEGIES:
        r = simulate_strategy(all_signals, fn, mg, name)
        real_results.append(r)
        print_strategy_result(r)

    # ── 3. Worst case: all losses ──────────────────────────────────
    print_header("3. STRESS TEST: TODAS LOSSES CONSECUTIVAS")
    print(f"  {'Estratégia':<25s} | {'Trades até bust':>16s} | {'Busted':>7s}")
    print(f"  {'-'*25}-+-{'-'*16}-+-{'-'*7}")
    for name, fn, mg in STRATEGIES:
        wc = worst_case_streak_test(fn, mg, name)
        trades_str = str(wc["trades_to_bust"]) if wc["busted"] else "NEVER"
        print(f"  {name:<25s} | {trades_str:>16s} | {'💀 SIM' if wc['busted'] else '✅ NÃO':>7s}")

    # ── 4. Pattern test: 25% WR ───────────────────────────────────
    print_header("4. STRESS TEST: PADRÃO W-L-L-L (25% WR)")
    for name, fn, mg in STRATEGIES:
        r = alternating_loss_test(fn, mg, name)
        print_strategy_result(r)

    # ── 5. Monte Carlo ─────────────────────────────────────────────
    print_header("5. MONTE CARLO — 10.000 SIMULAÇÕES × 500 TRADES")

    for wr in MC_WIN_RATES:
        print(f"\n  ─── Win Rate: {wr*100:.0f}% ─────────────────────────────────────")
        print(f"  {'Estratégia':<25s} | {'Bust%':>6s} | {'Avg Final':>10s} | "
              f"{'Mediana':>10s} | {'P5':>10s} | {'P95':>10s} | {'Pior':>10s} | {'Melhor':>10s}")
        print(f"  {'-'*25}-+-{'-'*6}-+-{'-'*10}-+-{'-'*10}-+-{'-'*10}-+-{'-'*10}-+-{'-'*10}-+-{'-'*10}")

        mc_results = []
        for name, fn, mg in STRATEGIES:
            mc = monte_carlo_test(fn, mg, name, wr)
            mc_results.append(mc)
            bust_str = f"{mc['bust_rate']:>5.1f}%"
            print(f"  {name:<25s} | {bust_str:>6s} | ${mc['avg_final']:>9.2f} | "
                  f"${mc['median_final']:>9.2f} | ${mc['p5_final']:>9.2f} | "
                  f"${mc['p95_final']:>9.2f} | ${mc['worst_final']:>9.2f} | ${mc['best_final']:>9.2f}")

    # ── 6. CONCLUSÃO ──────────────────────────────────────────────
    print_header("6. CONCLUSÃO E RECOMENDAÇÃO")

    # Encontra estratégias que NÃO bustaram nos dados reais
    safe_real = [r for r in real_results if not r.busted]
    busted_real = [r for r in real_results if r.busted]

    if busted_real:
        print(f"\n  💀 ESTRATÉGIAS QUE BUSTARAM COM DADOS REAIS:")
        for r in busted_real:
            print(f"     - {r.name} (bust no trade #{r.bust_trade})")

    if safe_real:
        print(f"\n  ✅ ESTRATÉGIAS SEGURAS COM DADOS REAIS:")
        # Ordena por final balance
        safe_real.sort(key=lambda r: r.final_balance, reverse=True)
        for r in safe_real:
            pnl = r.final_balance - STARTING_BALANCE
            print(f"     - {r.name}: ${r.final_balance:.2f} (PnL: ${pnl:+.2f}, MaxDD: {r.max_drawdown_pct:.1f}%)")

    # Recomendação baseada em MC 50% WR com 0% bust
    print(f"\n  📊 CRITÉRIO DE SELEÇÃO: Bust Rate = 0% em MC com WR 48%")
    print(f"     (Se não sobrevive com 48% WR em 10K simulações, está FORA)")


if __name__ == "__main__":
    main()
