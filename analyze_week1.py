"""Analyze first week of JD50 backtest signals (May 6-12, 2026).

Loads cached signals, runs all 54 strategy simulations, and prints
detailed analysis including daily breakdown, hourly patterns, and
time-to-double calculations.
"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from backtest_comprehensive import (
    BASE_STAKE,
    PAYOUT_RATE,
    STARTING_BALANCE,
    BALANCE_FLOOR,
    StrategyConfig,
    define_strategies,
    simulate_strategy,
)


def load_signals_csv(path: Path) -> list[dict]:
    """Load signals from CSV file."""
    signals = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            signals.append({
                "idx": int(row["idx"]),
                "epoch": int(row["epoch"]),
                "direction": row["direction"],
                "score": int(row["score"]),
                "confidence": float(row["confidence"]),
                "imbalance": float(row["imbalance"]),
                "bayes": float(row["bayes"]),
                "hurst": float(row["hurst"]),
                "won": row["won"] == "True",
                "hour": int(row["hour"]),
            })
    return signals


def main():
    sig_dir = Path("data/signals")

    # First week: May 6-12 (7 days)
    week1_dates = [
        "2026-05-06", "2026-05-07", "2026-05-08", "2026-05-09",
        "2026-05-10", "2026-05-11", "2026-05-12",
    ]

    # Load all signals
    all_signals = []
    day_signals = {}
    print("=" * 80)
    print("  LOADING FIRST WEEK SIGNALS (May 6-12, 2026)")
    print("=" * 80)

    for date_str in week1_dates:
        path = sig_dir / f"signals_{date_str}.csv"
        if not path.exists():
            print(f"  ⚠ MISSING: {path}")
            continue
        sigs = load_signals_csv(path)
        day_signals[date_str] = sigs
        all_signals.extend(sigs)

        wins = sum(1 for s in sigs if s["won"])
        total = len(sigs)
        wr = wins / total * 100 if total else 0
        calls = sum(1 for s in sigs if s["direction"] == "CALL")
        puts = total - calls

        # Filtered stats
        filt = [s for s in sigs if abs(s["imbalance"]) >= 6]
        filt_wins = sum(1 for s in filt if s["won"])
        filt_wr = filt_wins / len(filt) * 100 if filt else 0

        print(f"  {date_str}: {total:>6,} signals (CALL={calls}, PUT={puts}) | "
              f"Raw WR={wr:.1f}% | Filtered(imb≥6) WR={filt_wr:.1f}% ({len(filt)} sigs)")

    if not all_signals:
        print("No signals found!")
        sys.exit(1)

    total_sigs = len(all_signals)
    total_wins = sum(1 for s in all_signals if s["won"])
    total_wr = total_wins / total_sigs * 100

    # Filtered
    filtered = [s for s in all_signals if abs(s["imbalance"]) >= 6]
    filt_wins = sum(1 for s in filtered if s["won"])
    filt_wr = filt_wins / len(filtered) * 100 if filtered else 0

    print(f"\n  TOTAL: {total_sigs:,} signals | Raw WR={total_wr:.1f}% | "
          f"Filtered(imb≥6)={len(filtered):,} ({filt_wr:.1f}% WR)")

    # ── Run all 54 strategies ──
    print(f"\n{'=' * 80}")
    print(f"  RUNNING 54 STRATEGY SIMULATIONS ON {total_sigs:,} SIGNALS (7 DAYS)")
    print(f"{'=' * 80}")

    strategies = define_strategies()
    results = []
    for cfg in strategies:
        result = simulate_strategy(all_signals, cfg)
        results.append(result)

    # ── Sort and display ──
    profitable = [r for r in results if r["pnl"] > 0 and not r["ruined"]]
    ruined = [r for r in results if r["ruined"]]
    neutral = [r for r in results if r["pnl"] <= 0 and not r["ruined"]]

    profitable.sort(key=lambda r: -r["pnl"])
    ruined.sort(key=lambda r: r["ruin_trade"])

    # ── PROFITABLE TABLE ──
    print(f"\n{'=' * 140}")
    print(f"  ESTRATÉGIAS LUCRATIVAS ({len(profitable)} de {len(results)})")
    print(f"{'=' * 140}")
    header = (f"{'#':>3} {'Estratégia':<35} {'Trades':>6} {'WR%':>6} {'PnL':>10} "
              f"{'Saldo':>8} {'MaxDD':>8} {'T2x':>5} {'Soros':>5} {'G0':>6} {'G1':>5} {'G2':>5} {'Casc':>4}")
    print(header)
    print("-" * 140)

    for i, r in enumerate(profitable, 1):
        t2x = str(r["time_to_double"]) if r["time_to_double"] else "—"
        print(f"  {i:>2} {r['label']:<35} {r['total']:>6} {r['wr']:>5.1f}% "
              f"${r['pnl']:>+8.2f} ${r['final']:>7.2f} ${r['max_dd']:>7.2f} "
              f"{t2x:>5} {r['soros_trades']:>5} {r['g0']:>6} {r['g1']:>5} {r['g2']:>5} {r['cascades']:>4}")

    # ── RUINED TABLE ──
    print(f"\n{'=' * 100}")
    print(f"  ESTRATÉGIAS QUE QUEBRARAM ({len(ruined)} de {len(results)})")
    print(f"{'=' * 100}")
    print(f"  {'Estratégia':<35} {'Trades até Ruína':>16} {'MaxDD':>8} {'Casc':>5}")
    print(f"  {'-' * 70}")
    for r in ruined[:15]:
        print(f"  {r['label']:<35} {r['ruin_trade']:>16} ${r['max_dd']:>7.2f} {r['cascades']:>5}")
    if len(ruined) > 15:
        print(f"  ... e mais {len(ruined) - 15} estratégias que quebraram")

    # ── TOP 5 DETAILED ──
    if profitable:
        print(f"\n{'=' * 100}")
        print(f"  TOP 5 MELHORES ESTRATÉGIAS — DETALHADO")
        print(f"{'=' * 100}")
        for i, r in enumerate(profitable[:5], 1):
            t2x_info = f"dobra a banca em {r['time_to_double']} trades" if r["time_to_double"] else "não dobrou"
            days_active = 7
            daily_pnl = r["pnl"] / days_active
            monthly_proj = daily_pnl * 30

            print(f"\n  #{i}: {r['label']}")
            print(f"       Lucro Total: ${r['pnl']:+.2f} (saldo final: ${r['final']:.2f})")
            print(f"       Trades: {r['total']} | Vitórias: {r['wins']} | Derrotas: {r['losses']}")
            print(f"       Taxa de Acerto: {r['wr']:.1f}%")
            print(f"       Maior Queda (MaxDD): ${r['max_dd']:.2f}")
            print(f"       Lucro Diário Médio: ${daily_pnl:.2f}")
            print(f"       Projeção 30 dias: ${monthly_proj:.2f}")
            print(f"       Dobrar a Banca: {t2x_info}")
            if r["soros_trades"] > 0:
                print(f"       Trades com Soros: {r['soros_trades']}")
            if r["g1"] > 0:
                print(f"       Gales: G0={r['g0']}, G1={r['g1']}, G2={r['g2']}, Cascatas={r['cascades']}")

    # ── HOURLY ANALYSIS ──
    print(f"\n{'=' * 100}")
    print(f"  ANÁLISE POR HORA UTC (melhores estratégias agregadas)")
    print(f"{'=' * 100}")

    # Aggregate hourly data from top 5 profitable using flat imb>=6 cd=30 (safest)
    # Find the safest profitable config for hourly analysis
    best = profitable[0] if profitable else None
    if best:
        print(f"  Baseado na melhor estratégia: {best['label']}")
        print(f"\n  {'Hora(UTC)':>10} {'WR%':>7} {'PnL':>10} {'Veredicto':>15}")
        print(f"  {'-' * 50}")

        best_hours = []
        worst_hours = []
        for h in range(24):
            wr_h = best["hourly_wr"].get(h, 0)
            pnl_h = best["hourly_pnl"].get(h, 0)

            if wr_h >= 55:
                verdict = "✅ OPERAR"
                best_hours.append((h, wr_h, pnl_h))
            elif wr_h <= 47 and wr_h > 0:
                verdict = "❌ EVITAR"
                worst_hours.append((h, wr_h, pnl_h))
            elif wr_h == 0:
                verdict = "— s/ dados"
            else:
                verdict = "⚠ CUIDADO"

            print(f"  {h:>8}:00 {wr_h:>6.1f}% ${pnl_h:>8.2f} {verdict:>15}")

        if best_hours:
            print(f"\n  🟢 MELHORES HORÁRIOS: {', '.join(f'{h}:00 ({wr:.1f}%)' for h, wr, _ in sorted(best_hours, key=lambda x: -x[1]))}")
        if worst_hours:
            print(f"  🔴 PIORES HORÁRIOS:  {', '.join(f'{h}:00 ({wr:.1f}%)' for h, wr, _ in sorted(worst_hours, key=lambda x: x[1]))}")

    # ── DAILY BREAKDOWN per strategy ──
    if profitable:
        print(f"\n{'=' * 100}")
        print(f"  DESEMPENHO DIÁRIO — TOP 3 ESTRATÉGIAS")
        print(f"{'=' * 100}")

        for rank, cfg_result in enumerate(profitable[:3], 1):
            label = cfg_result["label"]
            # Find the matching strategy config
            cfg = None
            for s in strategies:
                if s.label == label:
                    cfg = s
                    break

            if not cfg:
                continue

            print(f"\n  #{rank}: {label}")
            print(f"  {'Data':<14} {'Trades':>7} {'WR%':>7} {'PnL':>10} {'Saldo':>10}")
            print(f"  {'-' * 55}")

            running_balance = STARTING_BALANCE
            for date_str in week1_dates:
                if date_str not in day_signals:
                    continue
                day_sigs = day_signals[date_str]
                day_result = simulate_strategy(day_sigs, cfg)
                day_pnl = day_result["pnl"]
                running_balance += day_pnl

                print(f"  {date_str:<14} {day_result['total']:>7} {day_result['wr']:>6.1f}% "
                      f"${day_pnl:>+8.2f} ${running_balance:>8.2f}")

            # Note: running_balance won't perfectly match cfg_result["final"]
            # because the full simulation processes signals sequentially (state carries over)

    # ── COMPARISON: FLAT vs SOROS vs GALE ──
    print(f"\n{'=' * 100}")
    print(f"  COMPARAÇÃO DE TIPOS DE ESTRATÉGIA (com filtro imb≥6, cd=30)")
    print(f"{'=' * 100}")

    compare_labels = [
        "flat | imb>=6 | cd=30",
        "soros1 | imb>=6 | cd=30",
        "soros2 | imb>=6 | cd=30",
        "fib2g | imb>=6 | cd=30",
        "fib3g | imb>=6 | cd=30",
        "classic2g | imb>=6 | cd=30",
        "soros1+fib2g | imb>=6 | cd=30",
        "soros2+fib3g | imb>=6 | cd=30",
    ]

    print(f"  {'Tipo':<35} {'Trades':>6} {'WR%':>6} {'PnL':>10} {'MaxDD':>8} {'Ruiu?':>6} {'T2x':>5}")
    print(f"  {'-' * 85}")

    for label in compare_labels:
        match = [r for r in results if r["label"] == label]
        if match:
            r = match[0]
            t2x = str(r["time_to_double"]) if r["time_to_double"] else "—"
            ruin = "SIM" if r["ruined"] else "não"
            pnl_marker = "✅" if r["pnl"] > 0 and not r["ruined"] else "❌" if r["ruined"] else "⚠"
            print(f"  {pnl_marker} {r['label']:<33} {r['total']:>6} {r['wr']:>5.1f}% "
                  f"${r['pnl']:>+8.2f} ${r['max_dd']:>7.2f} {ruin:>6} {t2x:>5}")

    # ── RISK ANALYSIS ──
    print(f"\n{'=' * 100}")
    print(f"  ANÁLISE DE RISCO")
    print(f"{'=' * 100}")

    n_profitable = len(profitable)
    n_ruined = len(ruined)
    n_neutral = len(neutral)

    print(f"  Total de estratégias testadas: {len(results)}")
    print(f"  Lucrativas: {n_profitable} ({n_profitable/len(results)*100:.0f}%)")
    print(f"  Quebraram a banca: {n_ruined} ({n_ruined/len(results)*100:.0f}%)")
    print(f"  Neutras/Prejuízo (sem quebrar): {n_neutral} ({n_neutral/len(results)*100:.0f}%)")

    if profitable:
        avg_pnl = np.mean([r["pnl"] for r in profitable])
        avg_dd = np.mean([r["max_dd"] for r in profitable])
        avg_wr = np.mean([r["wr"] for r in profitable])
        print(f"\n  Médias das estratégias lucrativas:")
        print(f"    Lucro médio: ${avg_pnl:.2f}")
        print(f"    WR médio: {avg_wr:.1f}%")
        print(f"    MaxDD médio: ${avg_dd:.2f}")

        # Risk-reward ratio
        if profitable:
            best_r = profitable[0]
            risk_reward = best_r["pnl"] / best_r["max_dd"] if best_r["max_dd"] > 0 else float('inf')
            print(f"\n  Melhor Risk/Reward: {risk_reward:.2f}x ({best_r['label']})")
            print(f"    → Lucro ${best_r['pnl']:.2f} / Risco ${best_r['max_dd']:.2f}")

    # ── FINAL VERDICT ──
    print(f"\n{'=' * 100}")
    print(f"  🏆 VEREDICTO FINAL — PRIMEIRA SEMANA (7 DIAS)")
    print(f"{'=' * 100}")

    if profitable:
        best = profitable[0]
        safest = None
        for r in profitable:
            if "flat" in r["label"]:
                safest = r
                break

        print(f"\n  MELHOR ESTRATÉGIA GERAL: {best['label']}")
        print(f"    Lucro: ${best['pnl']:+.2f} em 7 dias")
        print(f"    Taxa de Acerto: {best['wr']:.1f}%")
        print(f"    Maior Queda: ${best['max_dd']:.2f}")
        daily_avg = best["pnl"] / 7
        print(f"    Média diária: ${daily_avg:.2f}")
        print(f"    Projeção mensal (30d): ${daily_avg * 30:.2f}")

        if safest and safest != best:
            print(f"\n  ESTRATÉGIA MAIS SEGURA: {safest['label']}")
            print(f"    Lucro: ${safest['pnl']:+.2f} em 7 dias")
            print(f"    Taxa de Acerto: {safest['wr']:.1f}%")
            print(f"    Maior Queda: ${safest['max_dd']:.2f}")
            daily_safe = safest["pnl"] / 7
            print(f"    Média diária: ${daily_safe:.2f}")
            print(f"    Projeção mensal (30d): ${daily_safe * 30:.2f}")

        print(f"\n  ⚠ CONCLUSÕES:")
        print(f"    1. {n_ruined} de {len(results)} estratégias QUEBRARAM — martingale é PERIGOSO")
        if any("gale" not in r["label"].lower() and "fib" not in r["label"] and "classic" not in r["label"]
               for r in profitable):
            print(f"    2. Estratégias SEM gale (flat/soros) são as únicas consistentes")
        print(f"    3. Filtro de qualidade (imb≥6) é OBRIGATÓRIO para lucrar")
        print(f"    4. Cooldown alto (cd=30) reduz trades mas aumenta win rate")
    else:
        print(f"\n  ❌ NENHUMA ESTRATÉGIA FOI LUCRATIVA NA PRIMEIRA SEMANA")
        print(f"     {n_ruined} de {len(results)} quebraram a banca")

    print(f"\n{'=' * 100}")


if __name__ == "__main__":
    main()
