import sys
import os
from pathlib import Path
import traceback

sys.path.insert(0, str(Path(__file__).parent.parent))
from backtest_engine import _load_day_df, _ensure_day_ticks, accu_cfg, get_max_calm_thresh, SYMBOL
from strategy import calculate_tick_indicators
import pandas as pd
import numpy as np

def main():
    day = pd.to_datetime("2026-05-18").date()
    data_dir = Path(__file__).parent.parent / "data"
    
    print("Ensuring ticks exist...")
    _ensure_day_ticks(day, data_dir)
    
    print("Loading day df...")
    day_df = _load_day_df(day, data_dir)
    if day_df is None:
        print("Failed to load day_df")
        return
        
    print(f"Loaded day_df, size: {len(day_df)}")
    
    epochs = day_df["epoch"].values
    prices = day_df["quote"].values
    hours = day_df["hour"].values
    avgs = day_df["avg_ret"].values
    
    max_calm_thresh = get_max_calm_thresh(SYMBOL, avgs)
    super_indices = []
    TICK_COUNT = 100
    SAMPLE_EVERY = 60
    BLOCKED_HOURS = {5, 6, 7, 8, 9}
    
    for w in range(TICK_COUNT, len(day_df)):
        if hours[w] in BLOCKED_HOURS:
            continue
        if (w - TICK_COUNT) % SAMPLE_EVERY != 0:
            continue
        avg = avgs[w]
        if np.isnan(avg) or avg >= max_calm_thresh:
            continue
        super_indices.append(w)
        
    print(f"super_indices size: {len(super_indices)}")
    if super_indices:
        print(f"Min index: {min(super_indices)}, Max index: {max(super_indices)}")
        
    day_ticks = [{"epoch": int(epochs[w]), "quote": float(prices[w])} for w in range(len(day_df))]
    
    try:
        df = calculate_tick_indicators(day_ticks, config=accu_cfg, sample_indices=super_indices)
        print("Success!")
    except Exception as e:
        print("Error encountered:")
        traceback.print_exc()

if __name__ == "__main__":
    main()
