"""Deep analysis of week 1 failure — day-by-day breakdown and comparison with 24h data."""
from __future__ import annotations

import csv
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


def analyze_flat_daily(day_signals: dict, dates: list[str]):
    """Analyze flat strategy day by day (no carry-over state)."""
    # Use flat | imb>=6 | cd=30 (safest config)
    cfg = StrategyConfig(
        name="flat", cooldown=30, quality_gate=True, qg_imbalance=6.0,
    )

    print(f"\n{'='*100}")
    print(f"  ANÁLISE DIÁRIA — flat | imb≥6 | cd=30 (aposta fixa $0.35)")
    print(f"  (cada dia simulado INDEPENDENTE, com $50 iniciais)")
    print(f"{'='*100}")
    print(f"  {'Data':<14} {'Sinais':>7} {'Filtrad':>7} {'Trades':>7} {'WR%':>7} "
          f"{'PnL':>10} {'MaxDD':>8} {'Ruiu?':>6}")
    print(f"  {'-'*75}")

    total_pnl = 0
    for date_str in dates:
        if date_str not in day_signals:
            continue
        sigs = day_signals[date_str]
        filt = [s for s in sigs if abs(s["imbalance"]) >= 6]
        result = simulate_strategy(sigs, cfg)
        total_pnl += result["pnl"]
        ruin = "SIM" if result["ruined"] else "não"
        print(f"  {date_str:<14} {len(sigs):>7,} {len(filt):>7,} {result['total']:>7} {result['wr']:>6.1f}% "
              f"${result['pnl']:>+8.2f} ${result['max_dd']:>7.2f} {ruin:>6}")

    print(f"  {'-'*75}")
    print(f"  {'SOMA':<14} {'':>7} {'':>7} {'':>7} {'':>7} ${total_pnl:>+8.2f}")


def analyze_soros2_daily(day_signals: dict, dates: list[str]):
    """Analyze soros2 strategy day by day."""
    cfg = StrategyConfig(
        name="soros2", use_soros=True, soros_max_steps=2,
        cooldown=30, quality_gate=True, qg_imbalance=6.0,
    )

    print(f"\n{'='*100}")
    print(f"  ANÁLISE DIÁRIA — soros2 | imb≥6 | cd=30 (melhor config do teste 24h)")
    print(f"  (cada dia simulado INDEPENDENTE, com $50 iniciais)")
    print(f"{'='*100}")
    print(f"  {'Data':<14} {'Sinais':>7} {'Trades':>7} {'WR%':>7} "
          f"{'PnL':>10} {'MaxDD':>8} {'Ruiu?':>6} {'Soros':>6}")
    print(f"  {'-'*75}")

    total_pnl = 0
    for date_str in dates:
        if date_str not in day_signals:
            continue
        sigs = day_signals[date_str]
        result = simulate_strategy(sigs, cfg)
        total_pnl += result["pnl"]
        ruin = "SIM" if result["ruined"] else "não"
        print(f"  {date_str:<14} {len(sigs):>7,} {result['total']:>7} {result['wr']:>6.1f}% "
              f"${result['pnl']:>+8.2f} ${result['max_dd']:>7.2f} {ruin:>6} {result['soros_trades']:>6}")

    print(f"  {'-'*75}")
    print(f"  {'SOMA':<14} {'':>7} {'':>7} {'':>7} ${total_pnl:>+8.2f}")


def breakeven_analysis(all_signals: list[dict]):
    """Calculate theoretical break-even and actual edge."""
    total = len(all_signals)
    wins = sum(1 for s in all_signals if s["won"])
    wr = wins / total * 100

    # Filtered
    filtered = [s for s in all_signals if abs(s["imbalance"]) >= 6]
    filt_wins = sum(1 for s in filtered if s["won"])
    filt_wr = filt_wins / len(filtered) * 100 if filtered else 0

    # Break-even WR for payout 95.3%
    # Win: +stake * 0.953, Loss: -stake
    # E[profit] = WR * 0.953 - (1-WR) = 0
    # 0.953*WR - 1 + WR = 0
    # WR * (1.953) = 1
    # WR = 1/1.953 = 0.51203...
    be_wr = 1 / (1 + PAYOUT_RATE) * 100

    # Expected profit per $1 staked
    edge_raw = wr/100 * PAYOUT_RATE - (1 - wr/100)
    edge_filt = filt_wr/100 * PAYOUT_RATE - (1 - filt_wr/100) if filtered else 0

    print(f"\n{'='*100}")
    print(f"  ANÁLISE MATEMÁTICA — PONTO DE EQUILÍBRIO")
    print(f"{'='*100}")
    print(f"  Payout da Deriv: {PAYOUT_RATE*100:.1f}%")
    print(f"  WR necessário para empatar: {be_wr:.2f}%")
    print(f"")
    print(f"  WR bruto (todos sinais): {wr:.2f}% ({wins}/{total})")
    print(f"    → Edge por $1 apostado: ${edge_raw:.4f} ({'POSITIVO ✅' if edge_raw > 0 else 'NEGATIVO ❌'})")
    print(f"    → Perda esperada por trade ($0.35): ${edge_raw * BASE_STAKE:.4f}")
    print(f"")
    print(f"  WR filtrado (imb≥6): {filt_wr:.2f}% ({filt_wins}/{len(filtered)})")
    print(f"    → Edge por $1 apostado: ${edge_filt:.4f} ({'POSITIVO ✅' if edge_filt > 0 else 'NEGATIVO ❌'})")
    print(f"    → Perda esperada por trade ($0.35): ${edge_filt * BASE_STAKE:.4f}")

    if edge_raw < 0:
        trades_to_ruin = abs(STARTING_BALANCE / (edge_raw * BASE_STAKE))
        print(f"\n  ⚠ Com edge negativo, a banca de ${STARTING_BALANCE:.2f} dura ~{trades_to_ruin:.0f} trades")

    # Per-day WR
    print(f"\n  Win Rate por dia (sinais filtrados imb≥6):")
    return be_wr


def hourly_deep_analysis(all_signals: list[dict], be_wr: float):
    """Detailed hourly win rate analysis."""
    filtered = [s for s in all_signals if abs(s["imbalance"]) >= 6]

    hourly = {}
    for h in range(24):
        h_sigs = [s for s in filtered if s["hour"] == h]
        wins = sum(1 for s in h_sigs if s["won"])
        total = len(h_sigs)
        wr = wins / total * 100 if total else 0
        hourly[h] = {"wins": wins, "total": total, "wr": wr}

    print(f"\n{'='*100}")
    print(f"  WIN RATE POR HORA UTC — SINAIS FILTRADOS (imb≥6)")
    print(f"  Break-even WR = {be_wr:.1f}%")
    print(f"{'='*100}")
    print(f"  {'Hora':>6} {'Total':>7} {'Wins':>6} {'WR%':>7} {'Edge/trade':>12} {'Status':>15}")
    print(f"  {'-'*65}")

    best_hours = []
    worst_hours = []

    for h in range(24):
        d = hourly[h]
        if d["total"] == 0:
            continue
        edge = d["wr"]/100 * PAYOUT_RATE - (1 - d["wr"]/100)
        edge_per_trade = edge * BASE_STAKE

        if d["wr"] >= be_wr + 2:  # > 53.2%
            status = "🟢 LUCRATIVO"
            best_hours.append((h, d["wr"], d["total"]))
        elif d["wr"] >= be_wr:  # 51.2-53.2%
            status = "🟡 MARGINAL"
        elif d["wr"] >= be_wr - 2:  # 49.2-51.2%
            status = "🟠 PERDENDO"
        else:
            status = "🔴 PERIGOSO"
            worst_hours.append((h, d["wr"], d["total"]))

        print(f"  {h:>4}:00 {d['total']:>7} {d['wins']:>6} {d['wr']:>6.1f}% "
              f"${edge_per_trade:>+10.4f} {status:>15}")

    print(f"\n  🟢 Horários lucrativos (WR > {be_wr+2:.0f}%): ", end="")
    if best_hours:
        print(", ".join(f"{h}:00 ({wr:.1f}%)" for h, wr, _ in sorted(best_hours, key=lambda x: -x[1])))
    else:
        print("NENHUM")

    print(f"  🔴 Horários perigosos (WR < {be_wr-2:.0f}%): ", end="")
    if worst_hours:
        print(", ".join(f"{h}:00 ({wr:.1f}%)" for h, wr, _ in sorted(worst_hours, key=lambda x: x[1])))
    else:
        print("NENHUM")

    return best_hours, worst_hours


def score_distribution(all_signals: list[dict], be_wr: float):
    """Check if higher scores = higher WR."""
    filtered = [s for s in all_signals if abs(s["imbalance"]) >= 6]

    print(f"\n{'='*100}")
    print(f"  WIN RATE POR SCORE (nível de confiança do sinal)")
    print(f"{'='*100}")

    # Group by score ranges
    ranges = [(7, 9), (10, 12), (13, 15), (16, 18), (19, 21)]
    print(f"  {'Score':>10} {'Total':>7} {'WR%':>7} {'Edge?':>10}")
    print(f"  {'-'*40}")

    for lo, hi in ranges:
        sigs = [s for s in filtered if lo <= s["score"] <= hi]
        if not sigs:
            continue
        wins = sum(1 for s in sigs if s["won"])
        wr = wins / len(sigs) * 100
        edge = "✅" if wr > be_wr else "❌"
        print(f"  {lo:>4}-{hi:<4} {len(sigs):>7} {wr:>6.1f}% {edge:>10}")

    # Confidence ranges
    print(f"\n  {'Confidence':>12} {'Total':>7} {'WR%':>7} {'Edge?':>10}")
    print(f"  {'-'*40}")
    conf_ranges = [(0.60, 0.65), (0.65, 0.70), (0.70, 0.80), (0.80, 0.90), (0.90, 1.01)]
    for lo, hi in conf_ranges:
        sigs = [s for s in filtered if lo <= s["confidence"] < hi]
        if not sigs:
            continue
        wins = sum(1 for s in sigs if s["won"])
        wr = wins / len(sigs) * 100
        edge = "✅" if wr > be_wr else "❌"
        label = f"{lo:.0%}-{hi:.0%}"
        print(f"  {label:>12} {len(sigs):>7} {wr:>6.1f}% {edge:>10}")


def compare_with_24h(day_signals: dict):
    """Compare week 1 performance with 24h validation data (May 19-20)."""
    print(f"\n{'='*100}")
    print(f"  COMPARAÇÃO: SEMANA 1 vs DADOS 24H (Mai 19-20)")
    print(f"{'='*100}")

    # Week 1 dates
    w1 = ["2026-05-06", "2026-05-07", "2026-05-08", "2026-05-09",
          "2026-05-10", "2026-05-11", "2026-05-12"]

    # 24h dates
    h24 = ["2026-05-19", "2026-05-20"]

    for label, dates in [("Semana 1", w1), ("24h (Mai 19-20)", h24)]:
        sigs = []
        for d in dates:
            if d in day_signals:
                sigs.extend(day_signals[d])

        if not sigs:
            continue

        total = len(sigs)
        wins = sum(1 for s in sigs if s["won"])
        wr = wins / total * 100

        filt = [s for s in sigs if abs(s["imbalance"]) >= 6]
        filt_wins = sum(1 for s in filt if s["won"])
        filt_wr = filt_wins / len(filt) * 100 if filt else 0

        # Simulate best configs
        cfg_flat = StrategyConfig(name="flat", cooldown=30, quality_gate=True, qg_imbalance=6.0)
        cfg_soros2 = StrategyConfig(name="soros2", use_soros=True, soros_max_steps=2,
                                     cooldown=30, quality_gate=True, qg_imbalance=6.0)

        r_flat = simulate_strategy(sigs, cfg_flat)
        r_soros = simulate_strategy(sigs, cfg_soros2)

        print(f"\n  {label}:")
        print(f"    Sinais: {total:,} | Raw WR: {wr:.1f}% | Filtrado WR: {filt_wr:.1f}%")
        print(f"    flat imb≥6 cd=30:   PnL=${r_flat['pnl']:>+8.2f} | WR={r_flat['wr']:.1f}% | {'RUIU' if r_flat['ruined'] else 'OK'}")
        print(f"    soros2 imb≥6 cd=30: PnL=${r_soros['pnl']:>+8.2f} | WR={r_soros['wr']:.1f}% | {'RUIU' if r_soros['ruined'] else 'OK'}")


def main():
    sig_dir = Path("data/signals")

    # Load ALL available signal files
    all_dates = []
    day_signals = {}

    for f in sorted(sig_dir.glob("signals_*.csv")):
        date_str = f.stem.replace("signals_", "")
        sigs = load_signals_csv(f)
        day_signals[date_str] = sigs
        all_dates.append(date_str)

    print(f"Loaded {len(all_dates)} days: {', '.join(all_dates)}")

    # Week 1 dates
    week1 = ["2026-05-06", "2026-05-07", "2026-05-08", "2026-05-09",
             "2026-05-10", "2026-05-11", "2026-05-12"]

    week1_sigs = []
    for d in week1:
        if d in day_signals:
            week1_sigs.extend(day_signals[d])

    # 1. Break-even analysis
    be_wr = breakeven_analysis(week1_sigs)

    # Per-day WR
    for d in week1:
        if d in day_signals:
            sigs = day_signals[d]
            filt = [s for s in sigs if abs(s["imbalance"]) >= 6]
            wins_raw = sum(1 for s in sigs if s["won"])
            wr_raw = wins_raw / len(sigs) * 100
            wins_filt = sum(1 for s in filt if s["won"])
            wr_filt = wins_filt / len(filt) * 100 if filt else 0
            marker = "✅" if wr_filt > be_wr else "❌"
            print(f"    {d}: Raw={wr_raw:.1f}% | Filtrado={wr_filt:.1f}% {marker}")

    # 2. Daily P&L
    analyze_flat_daily(day_signals, week1)
    analyze_soros2_daily(day_signals, week1)

    # 3. Hourly analysis
    best_hours, worst_hours = hourly_deep_analysis(week1_sigs, be_wr)

    # 4. Score distribution
    score_distribution(week1_sigs, be_wr)

    # 5. Compare with 24h data
    compare_with_24h(day_signals)

    # 6. Final conclusions
    print(f"\n{'='*100}")
    print(f"  🔬 DIAGNÓSTICO FINAL")
    print(f"{'='*100}")

    filt = [s for s in week1_sigs if abs(s["imbalance"]) >= 6]
    filt_wr = sum(1 for s in filt if s["won"]) / len(filt) * 100 if filt else 0
    edge = filt_wr/100 * PAYOUT_RATE - (1 - filt_wr/100)

    print(f"""
  PROBLEMA: O sistema de sinais NÃO tem edge suficiente para o JD50.

  NÚMEROS:
  - Payout da Deriv: {PAYOUT_RATE*100:.1f}%
  - WR mínimo para empatar: {be_wr:.1f}%
  - WR real (filtrado, 7 dias): {filt_wr:.1f}%
  - GAP: {filt_wr - be_wr:+.1f}% ({'acima' if filt_wr > be_wr else 'ABAIXO'} do break-even)

  O QUE ISSO SIGNIFICA (para um leigo):
  - Imagine um jogo de cara ou coroa onde você ganha 95 centavos quando acerta
    mas perde $1 quando erra. Para empatar, você precisa acertar pelo menos
    {be_wr:.0f}% das vezes (mais da metade).
  - Nosso bot está acertando {filt_wr:.1f}% das vezes — {'bom' if filt_wr > be_wr else 'NÃO O SUFICIENTE'}.
  - A cada 1000 trades de $0.35, {'ganhamos' if edge > 0 else 'perdemos'} ~${abs(edge * BASE_STAKE * 1000):.2f}

  POR QUE O TESTE DE 24H MOSTROU LUCRO?
  - Os dados de 24h (Mai 19-20) podem ter sido um período "sorteado"
  - Com amostra pequena (1-2 dias), a sorte domina sobre a estatística
  - 7 dias dão uma visão mais realista da performance verdadeira

  PRÓXIMOS PASSOS RECOMENDADOS:
  1. Verificar se horários específicos têm edge real (operar só nesses)
  2. Aumentar o threshold de score/confiança dos sinais
  3. Testar outros instrumentos (Volatility 75, Crash 300, etc.)
  4. Considerar outro tipo de contrato (não Rise/Fall)
  5. Esperar os dados dos 14 dias completos para confirmar
""")


if __name__ == "__main__":
    main()
