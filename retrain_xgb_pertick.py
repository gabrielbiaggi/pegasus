#!/usr/bin/env python3
"""Retrain XGBoost usando labels com barreira PER-TICK real.

Usa os ticks brutos do CSV (não da shadow_ticks que tem labels errados).
Para cada sinal coletado na shadow_ticks, busca os ticks subsequentes no CSV
e computa WIN/LOSS com a barreira per-tick de 0.0000025.
"""

import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import xgboost as xgb
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split

sys.path.insert(0, str(Path(__file__).parent))

PG_DSN = "postgresql://pegasus:pegasus@localhost/pegasus_db"
PER_TICK_BARRIER = 0.0000025
GROWTH_RATE = 0.03

FEATURES = [
    "bb_width_percent",
    "tick_atr_percent",
    "recent_move_percent",
    "hurst_exponent",
    "tick_imbalance",
    "hawkes_intensity",
    "velocity_zscore",
    "acceleration_zscore",
    "pmi_distance_percent",
    "shannon_entropy",
    "kalman_residual_zscore",
    "markov_p_up_given_up",
    "markov_p_down_given_down",
]


def calc_win_ticks(tp_pct: float) -> int:
    v = 1.0
    for j in range(1, 200):
        v *= 1 + GROWTH_RATE
        if v - 1.0 >= tp_pct:
            return j
    return 80


def compute_pertick_label(
    tick_prices: np.ndarray, entry_idx: int, win_ticks: int
) -> int | None:
    """Returns 1=LOSS, 0=WIN using per-tick barrier check."""
    if entry_idx + win_ticks >= len(tick_prices):
        return None
    for j in range(1, win_ticks + 1):
        prev = tick_prices[entry_idx + j - 1]
        curr = tick_prices[entry_idx + j]
        if abs(curr - prev) / prev >= PER_TICK_BARRIER:
            return 1  # LOSS
    return 0  # WIN


def main():
    t0 = time.time()
    win_ticks_50 = calc_win_ticks(0.50)
    print(f"WIN_TICKS (TP50): {win_ticks_50}")

    # Load tick data
    data_dir = Path(__file__).parent / "data"
    print("Loading tick CSVs...")
    dfs = []
    for f in sorted(data_dir.glob("ticks_BOOM1000*.csv")):
        df = pd.read_csv(f, usecols=["epoch", "quote"])
        dfs.append(df)
    ticks = (
        pd.concat(dfs)
        .sort_values("epoch")
        .drop_duplicates("epoch")
        .reset_index(drop=True)
    )
    print(f"  Total ticks: {len(ticks):,}")
    tick_epochs = ticks["epoch"].values
    tick_prices = ticks["quote"].values

    # Build epoch→index lookup
    epoch_to_idx = {}
    for i, ep in enumerate(tick_epochs):
        epoch_to_idx[int(ep)] = i

    # Load shadow signals from DB
    print("Loading shadow_ticks from DB...")
    conn = psycopg2.connect(PG_DSN)
    signals = pd.read_sql(
        f"SELECT entry_epoch, {', '.join(FEATURES)} FROM shadow_ticks WHERE score >= 15",
        conn,
    )
    conn.close()
    print(f"  Signals: {len(signals)}")

    # Clean features
    for f in FEATURES:
        signals[f] = pd.to_numeric(signals[f], errors="coerce")
    signals = signals.dropna(subset=FEATURES)
    print(f"  After cleaning: {len(signals)}")

    # Compute per-tick labels
    print("Computing per-tick labels...")
    labels = []
    matched = 0
    for _, row in signals.iterrows():
        ep = int(row["entry_epoch"])
        idx = epoch_to_idx.get(ep)
        if idx is None:
            labels.append(None)
            continue
        label = compute_pertick_label(tick_prices, idx, win_ticks_50)
        labels.append(label)
        if label is not None:
            matched += 1

    signals["label"] = labels
    signals = signals.dropna(subset=["label"])
    signals["label"] = signals["label"].astype(int)

    n_win = (signals["label"] == 0).sum()
    n_loss = (signals["label"] == 1).sum()
    wr = n_win / len(signals) * 100
    print(f"  Matched: {matched}, Valid: {len(signals)}")
    print(f"  WIN: {n_win}, LOSS: {n_loss}, WR: {wr:.1f}%")

    if len(signals) < 100:
        print("NOT ENOUGH DATA — need more tick overlap with shadow_ticks")
        return

    # Train XGBoost
    X = signals[FEATURES].values
    y = signals["label"].values
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    print(f"\nTrain: {len(X_train)}, Test: {len(X_test)}")

    dtrain = xgb.DMatrix(X_train, label=y_train, feature_names=FEATURES)
    dtest = xgb.DMatrix(X_test, label=y_test, feature_names=FEATURES)

    params = {
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 5,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 5,
        "scale_pos_weight": n_win / max(n_loss, 1),
        "seed": 42,
    }

    print("\nTraining XGBoost com labels PER-TICK...")
    model = xgb.train(
        params,
        dtrain,
        num_boost_round=500,
        evals=[(dtest, "test")],
        verbose_eval=100,
        early_stopping_rounds=50,
    )

    # Evaluate
    preds = model.predict(dtest)
    y_pred = (preds > 0.5).astype(int)
    acc = accuracy_score(y_test, y_pred)
    print(f"\nAccuracy: {acc:.3f}")
    print(classification_report(y_test, y_pred, target_names=["WIN", "LOSS"]))

    # Feature importance
    importance = model.get_score(importance_type="gain")
    print("Feature importance (gain):")
    for feat, score in sorted(importance.items(), key=lambda x: x[1], reverse=True)[:8]:
        print(f"  {feat}: {score:.1f}")

    # Threshold analysis
    print("\nThreshold analysis:")
    for thresh in [0.50, 0.40, 0.30, 0.20, 0.15, 0.10, 0.05]:
        filt = preds < thresh
        if filt.sum() > 5:
            wr_f = 1 - y_test[filt].mean()
            print(
                f"  P(LOSS)<{thresh:.2f}: kept {filt.sum():>4}/{len(preds)} ({filt.sum() / len(preds) * 100:>5.1f}%)  WR={wr_f * 100:.1f}%"
            )

    # Save
    model_path = Path(__file__).parent / "models" / "pegasus_xgb_v3_pertick.json"
    model.save_model(str(model_path))
    features_path = (
        Path(__file__).parent / "models" / "pegasus_features_v3_pertick.json"
    )
    features_path.write_text(json.dumps(FEATURES))
    print(f"\nModel: {model_path} ({model_path.stat().st_size // 1024}KB)")
    print(f"Done in {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
