import time
import pandas as pd
from pathlib import Path
import sys

BASE = Path("/home/bill/dev/pegasus")
sys.path.insert(0, str(BASE))

from strategy import AccumulatorStrategyConfig, calculate_tick_indicators

data_dir = BASE / "data"
day_file = data_dir / "ticks_BOOM1000_2026-05-01.csv"

print(f"Loading {day_file}...")
df = pd.read_csv(day_file)
df["close"] = df["quote"]
day_ticks = df[["epoch", "quote"]].to_dict(orient="records")

print(f"Loaded {len(day_ticks)} ticks. Running calculate_tick_indicators with sample_indices=None...")
t0 = time.time()
res = calculate_tick_indicators(day_ticks, config=AccumulatorStrategyConfig(), sample_indices=None)
print(f"Done in {time.time() - t0:.2f} seconds. Result shape: {res.shape}")
