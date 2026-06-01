import sys
import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date, timedelta

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

# Force CSV load
os.environ["PG_DSN"] = ""

from strategy import AccumulatorStrategyConfig
from sweep_opt import calculate_lightweight_indicators, run_simulation

DATA_DIR = BASE / "data"
max_csv_path = DATA_DIR / "ticks_BOOM1000_max.csv"

print("Carregando CSV...")
full_df = pd.read_csv(max_csv_path)
full_df["epoch"] = pd.to_numeric(full_df["epoch"], errors="coerce")
full_df["quote"] = pd.to_numeric(full_df["quote"], errors="coerce")
full_df = full_df.dropna(subset=["epoch", "quote"]).sort_values("epoch").reset_index(drop=True)
full_df["_date"] = pd.to_datetime(full_df["epoch"], unit="s", utc=True).dt.date

days_dfs = []
# Pega apenas os primeiros 5 dias
current_date = date(2026, 5, 7)
for i in range(5):
    print(f"Processando dia: {current_date}")
    df = full_df[full_df["_date"] == current_date].drop(columns=["_date"]).copy()
    if not df.empty and len(df) >= 110:
        df["close"] = df["quote"]
        df["dt"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
        df["hour"] = df["dt"].dt.hour
        q = df["quote"].values
        rets = np.zeros(len(q))
        rets[1:] = np.abs(np.diff(q) / q[:-1])
        df["avg_ret"] = pd.Series(rets).rolling(10).mean().values
        
        df_ind = calculate_lightweight_indicators(df, AccumulatorStrategyConfig())
        days_dfs.append(df_ind)
    current_date += timedelta(days=1)

print("\nExecutando simulação diagnóstica...")
res = run_simulation(
    days_dfs=days_dfs,
    min_score=6,
    calm_thresh=1.8e-6,
    max_hurst=0.55,
    min_entropy=0.80,
    max_kalman_z=2.0,
    cusum_max=5.0
)

print("\nResultado Diagnóstico:")
for k, v in res.items():
    print(f"  {k}: {v}")
