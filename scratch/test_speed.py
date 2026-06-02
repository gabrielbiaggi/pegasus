import time
import os
import sys
from pathlib import Path

# Adiciona o diretório principal ao sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

import backtest_engine

env_vars = {
    "CALM_ACCU_MIN_SCORE": "10",
    "ENSEMBLE_MIN_PROB": "0.30",
    "CALM_ACCU_MAX_ENTRY_CUSUM": "5.5",
    "ACCUMULATOR_MIN_HURST_EXPONENT": "0.40",
    "CALM_ACCU_THRESHOLD": "3.99e-06",
    "PCS_XGB_BYPASS_LIMIT": "0.23",
    "PCS_REGIME_B_PLUS_TP": "24.0",
    "PCS_REGIME_B_MINUS_TP": "9.5",
    "STAKE": "7.0",
    "PEGASUS_OPTIMIZER_RUN": "true",
}

print("Iniciando backtest de teste...")
t0 = time.time()
res1 = backtest_engine.run_backtest_direct(
    start_date_str="2026-05-01",
    end_date_str="2026-05-31",
    start_balance=50.0,
    env_overrides=env_vars,
)
elapsed1 = time.time() - t0
print(f"Primeira execução (fase de carregamento de cache): {elapsed1:.2f}s")
if res1:
    print(f"Resultado: score={res1['score']:.2f} avg_day=${res1['avg_daily_profit']:.2f}")
else:
    print("Falha na primeira execução")

print("\nSegunda execução (com cache de RAM completo)...")
t0 = time.time()
res2 = backtest_engine.run_backtest_direct(
    start_date_str="2026-05-01",
    end_date_str="2026-05-31",
    start_balance=50.0,
    env_overrides=env_vars,
)
elapsed2 = time.time() - t0
print(f"Segunda execução: {elapsed2:.4f}s")
if res2:
    print(f"Resultado: score={res2['score']:.2f} avg_day=${res2['avg_daily_profit']:.2f}")
else:
    print("Falha na segunda execução")
