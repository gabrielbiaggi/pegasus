import pandas as pd
import numpy as np

def analyze():
    for name, path in [("1HZ100V", "data/ticks_1HZ100V_max.csv"), ("BOOM1000", "data/ticks_BOOM1000_max.csv")]:
        try:
            df = pd.read_csv(path)
            print(f"\n==================== {name} ====================")
            print(f"Total ticks: {len(df)}")
            df['quote'] = pd.to_numeric(df['quote'], errors='coerce')
            df = df.dropna().reset_index(drop=True)
            
            # Calculate returns
            returns = df['quote'].pct_change().dropna()
            abs_returns = returns.abs()
            
            # Calculate rolling 10-tick average absolute returns
            rolling_vol = abs_returns.rolling(10).mean().dropna()
            
            print(f"\nRolling 10-tick average absolute returns statistics:")
            print(rolling_vol.describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.95, 0.99]))
        except Exception as e:
            print(f"Error loading {name}: {e}")

if __name__ == "__main__":
    analyze()
