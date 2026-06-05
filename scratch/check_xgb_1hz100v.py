import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path

# Insert project directory
project_dir = Path("/home/bill/dev/pegasus")
sys.path.insert(0, str(project_dir))

from strategy import EnsembleScorer, AccumulatorStrategyConfig, calculate_tick_indicators

def check():
    # Load model
    model_path = project_dir / "models" / "pegasus_xgb_v3_pertick.json"
    feat_path = project_dir / "models" / "pegasus_features_v3_pertick.json"
    
    if not model_path.exists():
        print("Model file not found!")
        return
        
    scorer = EnsembleScorer(str(model_path), str(feat_path))
    print("XGBoost Model loaded successfully.")
    
    # Load some 1HZ100V ticks
    df_ticks = pd.read_csv("data/ticks_1HZ100V_max.csv", nrows=10000)
    ticks = [{"epoch": int(r['epoch']), "quote": float(r['quote'])} for _, r in df_ticks.iterrows()]
    
    # Calculate indicators
    cfg = AccumulatorStrategyConfig()
    df_ind = calculate_tick_indicators(ticks, config=cfg)
    print(f"Calculated indicators for {len(df_ind)} ticks.")
    
    # Drop rows that don't have all indicators
    df_valid = df_ind.dropna(subset=scorer.feature_names).copy()
    print(f"Valid ticks with all features: {len(df_valid)}")
    
    if len(df_valid) == 0:
        return
        
    # Get a sample of predictions
    df_valid['p_loss'] = scorer.predict_loss_probability_batch(df_valid)
    
    print("\nPredicted P(LOSS) distribution on 1HZ100V:")
    print(df_valid['p_loss'].describe(percentiles=[0.25, 0.5, 0.75, 0.9, 0.95, 0.99]))
    
    # Check how many would pass threshold 0.294
    passed = (df_valid['p_loss'] < 0.294).sum()
    print(f"\nTicks passing threshold (P(LOSS) < 0.294): {passed} out of {len(df_valid)} ({passed/len(df_valid)*100:.2f}%)")

if __name__ == "__main__":
    check()
