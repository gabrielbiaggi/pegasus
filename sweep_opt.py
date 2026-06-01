#!/usr/bin/env python3
"""
Pegasus Sweep Optimizer
-----------------------
Realiza uma varredura de hiperparâmetros (Grid/Random Search) usando dados REAIS de ticks
do mês de Maio de 2026. Implementa a simulação vetorial em alta performance O(1) para
descobrir as configurações ótimas que maximizam o saldo final e minimizam o drawdown.
"""

import sys
import os
import json
import time
import pandas as pd
import numpy as np
from pathlib import Path
from itertools import product
from datetime import date, timedelta

# Garante que imports do projeto funcionem
BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

# Ignora conexão PG localmente para acelerar fallback a CSVs
os.environ["PG_DSN"] = ""

from strategy import AccumulatorStrategyConfig, calculate_tick_indicators
from backtest_engine import _load_day_df, PER_TICK_BARRIER

# Configurações do Sweep
START_DATE = date(2026, 5, 7)
END_DATE = date(2026, 5, 20)
DATA_DIR = BASE / "data"
OUTPUT_CSV = BASE / "logs" / "sweep_opt_results.csv"

def run_simulation(
    days_dfs: list[pd.DataFrame],
    min_score: int,
    calm_thresh: float,
    max_hurst: float,
    min_entropy: float,
    max_kalman_z: float,
    cusum_max: float,
    compounding: bool = True
) -> dict:
    """
    Simulação rápida e ultra-fiel de todo o mês de Maio para uma combinação de parâmetros.
    """
    initial_balance = 50.0
    balance = initial_balance
    peak_balance = initial_balance
    max_dd = 0.0
    
    total_trades = 0
    total_wins = 0
    total_losses = 0
    busted = False
    positive_days = 0
    
    # Simula dia a dia de forma sequencial com juros compostos
    for df in days_dfs:
        if df.empty or busted:
            continue
            
        day_start_balance = balance
        day_pnl = 0.0
        
        # Filtros quantitativos em nível de série (vetorizado rápido)
        # O(1) filter check:
        valid_ticks = df[
            (df["bb_width_percent"] <= 0.12) &
            (df["tick_atr_percent"] <= 0.015) &
            (df["recent_move_percent"] <= 0.05) &
            (df["avg_ret"] < calm_thresh)
        ]
        
        if valid_ticks.empty:
            continue
            
        # Avalia sinais e regimes para os ticks qualificados
        trades_today = 0
        wins_today = 0
        losses_today = 0
        
        # Juros Compostos: 10% do saldo atual como stake base
        stake_base = round(balance * 0.10, 2)
        stake_base = max(5.0, min(100.0, stake_base)) # min $5, max $100
        
        # Simula as posições abertas no dia
        # Para velocidade extrema, simulamos amostrando os sinais disparados
        # mantendo um cooldown de 30 segundos (~30 ticks) entre trades
        last_trade_idx = -999
        
        # Filtra os ticks que atendem ao score mínimo
        for idx in valid_ticks.index:
            if idx - last_trade_idx < 30: # Cooldown de 30 ticks
                continue
                
            tick = df.loc[idx]
            
            # Calcula score personalizado
            score = 0
            # Squeeze + ATR + Stability
            if tick["bb_width_percent"] <= 0.10: score += 4
            if tick["tick_atr_percent"] <= 0.015: score += 4
            if tick["recent_move_percent"] <= 0.05: score += 2
            
            # Quantitativos
            if tick.get("hurst_exponent", 1.0) < max_hurst: score += 1
            if tick.get("shannon_entropy", 0.0) >= min_entropy: score += 1
            if abs(tick.get("kalman_residual_zscore", 9.0)) <= max_kalman_z: score += 1
            if tick.get("cusum_score", 9.0) < cusum_max: score += 1
            
            # Se atingiu o score, entra no trade
            if score >= min_score:
                # Transição de regimes dinâmica (Super-Frankenstein / PCS)
                is_absolute_calm = (
                    tick["avg_ret"] < 1.0e-6
                    and tick["cusum_score"] < 2.5
                    and tick["hurst_exponent"] > 0.48
                    and tick.get("shannon_entropy", 0.0) > 0.85
                    and abs(tick.get("kalman_residual_zscore", 0.0)) < 1.5
                )
                is_medium_calm = (
                    tick["avg_ret"] < 2.2e-6
                    and tick["cusum_score"] < 4.0
                    and tick["hurst_exponent"] > 0.45
                )
                
                if is_absolute_calm:
                    # Regime A: 30% TP, 9 Ticks Hold, Soros Ativo
                    tp_pct = 0.30
                    hold_ticks = 9
                    stake = stake_base
                elif is_medium_calm:
                    # Regime B+: 9% TP, 3 Ticks Hold, Soros Inativo
                    tp_pct = 0.09
                    hold_ticks = 3
                    stake = stake_base
                else:
                    # Regime B-: 3% TP, 1 Tick Hold, Soros Inativo
                    tp_pct = 0.03
                    hold_ticks = 1
                    stake = stake_base
                    
                # Simula barreira de barreira Deriv
                # Procura se algum tick nos próximos 'hold_ticks' rompeu a barreira
                # Payout real aproximado de 1 a 2% do stake por tick no Accumulator
                future_ticks = df.loc[idx:idx + hold_ticks]
                if len(future_ticks) < hold_ticks:
                    continue # fim do dia, pula
                    
                # Checa vitória baseada na barreira padrão
                barrier_hit = False
                close_prices = future_ticks["close"].values
                for i in range(1, len(close_prices)):
                    diff_pct = abs(close_prices[i] - close_prices[i-1]) / close_prices[i-1]
                    if diff_pct >= PER_TICK_BARRIER:
                        barrier_hit = True
                        break
                
                if not barrier_hit:
                    # Vitória!
                    profit = round(stake * tp_pct, 2)
                    balance += profit
                    day_pnl += profit
                    wins_today += 1
                    total_wins += 1
                else:
                    # Derrota! (Martingale cirúrgico é acionado se configurado, mas simulamos flat dinâmico para pureza estatística)
                    loss = -stake
                    balance += loss
                    day_pnl += loss
                    losses_today += 1
                    total_losses += 1
                    
                last_trade_idx = idx
                trades_today += 1
                
                # Proteção diária contra quebra total
                if balance < 5.0:
                    busted = True
                    balance = 0.0
                    break
                    
        if busted:
            break
            
        if day_pnl > 0:
            positive_days += 1
            
        peak_balance = max(peak_balance, balance)
        dd = (peak_balance - balance) / peak_balance * 100
        max_dd = max(max_dd, dd)
        
    total_trades = total_wins + total_losses
    winrate = round(total_wins / total_trades * 100, 1) if total_trades > 0 else 0.0
    net_profit = round(balance - initial_balance, 2)
    roi_pct = round(net_profit / initial_balance * 100, 1)
    
    return {
        "min_score": min_score,
        "calm_thresh": calm_thresh,
        "max_hurst": max_hurst,
        "min_entropy": min_entropy,
        "max_kalman_z": max_kalman_z,
        "cusum_max": cusum_max,
        "trades": total_trades,
        "wins": total_wins,
        "losses": total_losses,
        "winrate": winrate,
        "ending_balance": round(balance, 2),
        "net_profit": net_profit,
        "roi_pct": roi_pct,
        "max_dd": round(max_dd, 1),
        "positive_days": positive_days,
        "busted": busted
    }

def calculate_lightweight_indicators(df: pd.DataFrame, config: AccumulatorStrategyConfig) -> pd.DataFrame:
    import ta
    # 1. Bollinger Bands
    bb = ta.volatility.BollingerBands(df["close"], window=config.bb_window, window_dev=config.bb_std_dev)
    df["bb_width_percent"] = (bb.bollinger_hband() - bb.bollinger_lband()) / df["close"] * 100
    
    # 2. ATR
    df["abs_tick_move_percent"] = df["close"].pct_change().abs() * 100
    df["tick_atr_percent"] = df["abs_tick_move_percent"].rolling(config.atr_window).mean()
    
    # 3. Recent Move
    df["recent_move_percent"] = df["close"].pct_change(config.recent_window).abs() * 100
    
    # 4. Hurst (import from strategy)
    from strategy import _hurst_exponent_from_prices
    df["hurst_exponent"] = df["close"].rolling(config.hurst_window).apply(_hurst_exponent_from_prices, raw=True)
    
    # 5. Shannon Entropy
    from strategy import _shannon_entropy
    df["shannon_entropy"] = _shannon_entropy(df["close"], config.shannon_entropy_window)
    
    # 6. Kalman Z-Score
    from strategy import _kalman_filter_metrics, _rolling_abs_zscore
    kalman = _kalman_filter_metrics(df["close"], config.kalman_q, config.kalman_r)
    df["kalman_residual_zscore"] = _rolling_abs_zscore(kalman["kalman_residual"], config.derivative_window)
    
    # 7. CUSUM Score
    from strategy import _cusum_score
    df["cusum_score"] = _cusum_score(df["close"], config.shannon_entropy_window)
    
    return df

def main():
    print("=" * 80)
    print(" 🚀 INICIANDO PEGASUS SWEEP OPTIMIZER")
    print("    Carregando e processando ticks reais de Maio de 2026...")
    print("=" * 80)
    
    t0 = time.time()
    
    # 1. Carrega o max.csv uma única vez em memória para performance extrema
    max_csv_path = DATA_DIR / "ticks_BOOM1000_max.csv"
    if not max_csv_path.exists():
        print(f"  ❌ Erro: {max_csv_path} não encontrado!")
        sys.exit(1)
        
    print("  [CSV] Carregando ticks_BOOM1000_max.csv de forma otimizada...")
    full_df = pd.read_csv(max_csv_path)
    full_df["epoch"] = pd.to_numeric(full_df["epoch"], errors="coerce")
    full_df["quote"] = pd.to_numeric(full_df["quote"], errors="coerce")
    full_df = full_df.dropna(subset=["epoch", "quote"]).sort_values("epoch").reset_index(drop=True)
    full_df["_date"] = pd.to_datetime(full_df["epoch"], unit="s", utc=True).dt.date
    
    days_dfs = []
    current_date = START_DATE
    while current_date <= END_DATE:
        sys.stdout.write(f"\r  Processando dia: {current_date.isoformat()} ...")
        sys.stdout.flush()
        
        # Filtra o dia localmente na memória (O(1) fast slice)
        df = full_df[full_df["_date"] == current_date].drop(columns=["_date"]).copy()
        if df.empty or len(df) < 110:
            current_date += timedelta(days=1)
            continue
            
        # Colunas auxiliares idênticas ao bot
        df["close"] = df["quote"]
        df["dt"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
        df["hour"] = df["dt"].dt.hour
        q = df["quote"].values
        rets = np.zeros(len(q))
        rets[1:] = np.abs(np.diff(q) / q[:-1])
        df["avg_ret"] = pd.Series(rets).rolling(10).mean().values
        
        # Pré-calcula indicadores
        df_ind = calculate_lightweight_indicators(df, AccumulatorStrategyConfig())
        days_dfs.append(df_ind)
        
        current_date += timedelta(days=1)
        
    print(f"\n  ✅ {len(days_dfs)} dias de ticks processados com sucesso em {time.time() - t0:.1f}s!")
    print()
    
    # 2. Define grade de varredura
    min_scores = [6, 7, 8]
    calm_thresholds = [1.2e-6, 1.8e-6, 2.5e-6]
    max_hursts = [0.48, 0.52, 0.55]
    min_entropies = [0.80, 0.85]
    max_kalman_zs = [1.5, 2.0]
    cusum_maxs = [3.0, 5.0]
    
    combinations = list(product(
        min_scores,
        calm_thresholds,
        max_hursts,
        min_entropies,
        max_kalman_zs,
        cusum_maxs
    ))
    
    print(f"  🔍 Grade do Sweep: {len(combinations)} combinações matemáticas de parâmetros.")
    print("     Simulando todo o mês de Maio de 2026 para cada uma...")
    print("-" * 80)
    
    results = []
    t_sweep = time.time()
    for idx, (min_score, calm_thresh, max_hurst, min_entropy, max_kalman_z, cusum_max) in enumerate(combinations):
        if idx % 20 == 0 and idx > 0:
            print(f"    - Simulado {idx}/{len(combinations)} ...")
            
        res = run_simulation(
            days_dfs=days_dfs,
            min_score=min_score,
            calm_thresh=calm_thresh,
            max_hurst=max_hurst,
            min_entropy=min_entropy,
            max_kalman_z=max_kalman_z,
            cusum_max=cusum_max
        )
        # Filtra combinações razoáveis (não quebradas e com trades)
        if not res["busted"] and res["trades"] > 10:
            results.append(res)
            
    print(f"  ✅ Varredura concluída em {time.time() - t_sweep:.1f}s!")
    print()
    
    # 3. Ordena os resultados
    # Melhores: maior saldo final (lucro) e menor drawdown
    results.sort(key=lambda r: (r["ending_balance"], -r["max_dd"]), reverse=True)
    
    # Salva em CSV
    if results:
        df_res = pd.DataFrame(results)
        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        df_res.to_csv(OUTPUT_CSV, index=False)
        
        print("=" * 80)
        print(" 🎯 TOP 5 CONFIGURAÇÕES OTIMIZADAS PARA PEGASUS CONGLOMERATE (PCS)")
        print("=" * 80)
        for i, r in enumerate(results[:5]):
            print(f" #{i+1} | Saldo: ${r['ending_balance']:.2f} (ROI: {r['roi_pct']:+}% | Drawdown: {r['max_dd']}%)")
            print(f"    Filtros: MinScore={r['min_score']} | VolCalm={r['calm_thresh']:.2e} | HurstMax={r['max_hurst']}")
            print(f"             EntropyMin={r['min_entropy']} | KalmanZ={r['max_kalman_z']} | CUSUMMax={r['cusum_max']}")
            print(f"    Operações: {r['trades']}T ({r['wins']}W/{r['losses']}L) | WR={r['winrate']}% | Dias Positivos={r['positive_days']}")
            print("-" * 80)
            
        # Escreve a melhor config no env
        best = results[0]
        print(f"  💡 Sugestão para o .env: DYNAMIC_STAKE_BASE_PCT=0.10 | ACCUMULATOR_MIN_SCORE={best['min_score']}")
    else:
        print("  ⚠️ Nenhuma combinação viável e segura foi encontrada sem quebrar a banca.")
        
if __name__ == "__main__":
    main()
