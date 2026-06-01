import os
import sys
import time
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date, timedelta
from multiprocessing import Pool

BASE = Path("/home/bill/dev/pegasus")
sys.path.insert(0, str(BASE))

from strategy import AccumulatorStrategyConfig, calculate_tick_indicators, EnsembleScorer

# Configurações do cache
data_dir = BASE / "data"
cache_dir = data_dir / "cache"
cache_dir.mkdir(parents=True, exist_ok=True)

# Parâmetros fixos do Accumulator
accu_cfg = AccumulatorStrategyConfig()
TICK_COUNT = 100
SAMPLE_EVERY = 60
BLOCKED_HOURS = {5, 6, 7, 8, 9} # UTC blocked hours
MAX_CALM_THRESH = 2.5e-6

# Scorer global para o worker (inicializado de forma preguiçosa no filho)
_ensemble_scorer = None

def _local_load_day_df(day: date, data_dir: Path) -> pd.DataFrame | None:
    """Carrega ticks de um arquivo diário sem depender de backtest_engine."""
    daily_path = data_dir / f"ticks_BOOM1000_{day.isoformat()}.csv"
    if not daily_path.exists() or daily_path.stat().st_size < 1000:
        return None
        
    df = pd.read_csv(daily_path)
    if df.empty or len(df) < TICK_COUNT + 10:
        return None

    df["epoch"] = pd.to_numeric(df["epoch"], errors="coerce")
    df["quote"] = pd.to_numeric(df["quote"], errors="coerce")
    df = (
        df.dropna(subset=["epoch", "quote"]).sort_values("epoch").reset_index(drop=True)
    )

    # Colunas auxiliares
    df["close"] = df["quote"]
    df["dt"] = pd.to_datetime(df["epoch"], unit="s", utc=True)
    df["hour"] = df["dt"].dt.hour
    q = df["quote"].values
    rets = np.zeros(len(q))
    rets[1:] = np.abs(np.diff(q) / q[:-1])
    df["avg_ret"] = pd.Series(rets).rolling(10).mean().values

    return df

def process_day(day: date) -> str:
    global _ensemble_scorer
    cache_path = cache_dir / f"indicators_BOOM1000_{day.isoformat()}.csv.gz"
    if cache_path.exists():
        return f"  {day}: cache already exists, skipping."
        
    t_start = time.time()
    
    # Inicialização tardia do XGBoost no worker pós-fork (evita deadlock)
    if _ensemble_scorer is None:
        try:
            _model_path = BASE / "models" / "pegasus_xgb_v3_pertick.json"
            _feat_path = BASE / "models" / "pegasus_features_v3_pertick.json"
            if _model_path.exists() and _feat_path.exists():
                _ensemble_scorer = EnsembleScorer(
                    model_path=str(_model_path),
                    features_path=str(_feat_path),
                )
        except Exception as e:
            print(f"XGBoost failed to load in child for {day}: {e}", flush=True)

    # Carrega df do dia
    day_df = _local_load_day_df(day, data_dir)
    if day_df is None:
        return f"  {day}: no tick data available."
        
    prices = day_df["quote"].values
    hours = day_df["hour"].values
    avgs = day_df["avg_ret"].values
    epochs = day_df["epoch"].values
    
    # Determina o superconjunto de indices de amostragem
    super_indices = []
    for w in range(TICK_COUNT, len(day_df)):
        if hours[w] in BLOCKED_HOURS:
            continue
        if (w - TICK_COUNT) % SAMPLE_EVERY != 0:
            continue
        avg = avgs[w]
        if np.isnan(avg) or avg >= MAX_CALM_THRESH:
            continue
        super_indices.append(w)
        
    day_ticks = [{"epoch": int(epochs[w]), "quote": float(prices[w])} for w in range(len(day_df))]
    try:
        df_ind = calculate_tick_indicators(day_ticks, config=accu_cfg, sample_indices=super_indices)
        df_ind = df_ind.reset_index(drop=True)
        
        # Pre-calcula as probabilidades de perda do XGBoost
        if _ensemble_scorer is not None:
            df_ind["p_loss"] = _ensemble_scorer.predict_loss_probability_batch(df_ind)
        else:
            df_ind["p_loss"] = None
            
        df_ind.to_csv(cache_path, index=False, compression="gzip")
        elapsed = time.time() - t_start
        return f"  {day}: cache built successfully with {len(super_indices)} sample points in {elapsed:.2f}s."
    except Exception as e:
        return f"  {day}: failed to build cache: {e}"

def main():
    print("=" * 80)
    print(" 🌀 PEGASUS PARALLEL INDICATOR CACHE BUILDER (LAZY XGB)")
    print("=" * 80)
    
    # Lista de dias de maio
    start_date = date(2026, 5, 1)
    end_date = date(2026, 5, 31)
    days = []
    cur = start_date
    while cur <= end_date:
        days.append(cur)
        cur += timedelta(days=1)
        
    cores = os.cpu_count() or 4
    print(f"Using {cores} parallel processes to build cache for {len(days)} days.")
    
    t0 = time.time()
    with Pool(processes=cores) as pool:
        results = pool.map(process_day, days)
        
    for r in results:
        print(r)
        
    print("=" * 80)
    print(f"Finished building cache in {time.time() - t0:.2f}s.")
    print("=" * 80)

if __name__ == "__main__":
    main()
