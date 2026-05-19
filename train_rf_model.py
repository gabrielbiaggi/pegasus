#!/usr/bin/env python3
"""Train Rise/Fall XGBoost direction model from shadow data.

Uses future_rf_direction_5t as the training target (UP=1 / DOWN=0).
Features include SIGNED velocity and imbalance — key for direction prediction.

Usage:
    python train_rf_model.py [--data data/shadow_ticks_full.csv]
                              [--out models/pegasus_rf_v1.json]
                              [--target future_rf_direction_5t]
                              [--min-rows 2000]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import xgboost as xgb
    from sklearn.model_selection import StratifiedKFold
    from sklearn.metrics import roc_auc_score, accuracy_score
except ImportError:
    print("Instale: pip install xgboost scikit-learn")
    sys.exit(1)

# Features for Rise/Fall direction prediction.
# Critically uses SIGNED features (price_velocity, tick_imbalance) not available
# in the accumulator model which uses only absolute z-scores.
RF_FEATURES = [
    "price_velocity",            # signed: positive = going up (raw diff)
    "tick_imbalance",            # signed: +N = more up-ticks in window
    "markov_p_up_given_up",      # P(up|up) — momentum persistence
    "markov_p_down_given_down",  # P(down|down) — momentum persistence
    "hurst_exponent",            # 0.5 = random, <0.5 = mean-reverting, >0.5 = trending
    "hawkes_intensity",          # tick event intensity
    "velocity_zscore",           # abs z-score (amplitude of velocity)
    "acceleration_zscore",       # abs z-score (rate of velocity change)
    "bb_width_percent",          # volatility proxy
    "tick_atr_percent",          # volatility proxy
    "recent_move_percent",       # recent absolute move size
    "shannon_entropy",           # randomness of tick sequence
    "kalman_residual_zscore",    # deviation from Kalman estimate
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Treina modelo de direcao Rise/Fall.")
    parser.add_argument("--data", default="data/legacy_accumulator/shadow_ticks_full.csv")
    parser.add_argument("--out", default="models/pegasus_rf_v1.json")
    parser.add_argument("--features-out", default="models/pegasus_rf_features_v1.json")
    parser.add_argument("--target", default="future_rf_direction_5t",
                        help="Coluna alvo: future_rf_direction_1t | 3t | 5t")
    parser.add_argument("--min-rows", type=int, default=2000)
    args = parser.parse_args()

    print(f"Carregando dados: {args.data}")
    df = pd.read_csv(args.data, on_bad_lines="skip")
    print(f"  Total rows: {len(df):,}")

    if args.target not in df.columns:
        print(f"\n⚠️  Coluna '{args.target}' nao encontrada no CSV.")
        print("    Rode shadow_collect.py para coletar novos dados com direction labels.")
        print(f"    Colunas disponiveis: {list(df.columns)}")
        sys.exit(1)

    # Filter: only UP/DOWN (exclude TIE)
    df = df[df[args.target].isin(["UP", "DOWN"])].copy()
    print(f"  Rows UP/DOWN: {len(df):,}")

    if len(df) < args.min_rows:
        print(f"\n⚠️  Poucos dados ({len(df)} < {args.min_rows} rows minimos).")
        print("    Continue coletando com shadow_collect.py e re-rode este script.")
        sys.exit(1)

    df["label"] = (df[args.target] == "UP").astype(int)
    up_pct = df["label"].mean() * 100
    print(f"  Label balance: UP={up_pct:.1f}% DOWN={100-up_pct:.1f}%")

    # Convert features to numeric, drop NaN
    available = [f for f in RF_FEATURES if f in df.columns]
    missing = [f for f in RF_FEATURES if f not in df.columns]
    if missing:
        print(f"\n  Features ausentes no CSV (precisam de calculo): {missing}")
        print("  Compute-as re-rodando shadow_collect.py ou verifique strategy.py.")
        if len(available) < 5:
            print("  Poucos features disponiveis — abortando.")
            sys.exit(1)
        print(f"  Continuando com {len(available)} features.")

    for feat in available:
        df[feat] = pd.to_numeric(df[feat], errors="coerce")

    df_clean = df[available + ["label"]].dropna()
    print(f"  Rows apos dropna: {len(df_clean):,}")

    if len(df_clean) < args.min_rows:
        print(f"⚠️  Apos limpeza: {len(df_clean)} rows < {args.min_rows} minimo.")
        sys.exit(1)

    X = df_clean[available].values.astype(float)
    y = df_clean["label"].values.astype(int)

    print(f"\nTreinando XGBoost com {len(available)} features, {len(X):,} amostras...")
    print(f"Features: {available}")

    # Cross-validation
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    aucs, accs = [], []
    for fold, (train_idx, val_idx) in enumerate(cv.split(X, y)):
        dtrain = xgb.DMatrix(X[train_idx], label=y[train_idx], feature_names=available)
        dval = xgb.DMatrix(X[val_idx], label=y[val_idx], feature_names=available)
        params = {
            "objective": "binary:logistic",
            "eval_metric": "auc",
            "eta": 0.05,
            "max_depth": 4,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "min_child_weight": 20,
            "scale_pos_weight": (y == 0).sum() / max((y == 1).sum(), 1),
            "seed": 42,
            "verbosity": 0,
        }
        booster = xgb.train(
            params,
            dtrain,
            num_boost_round=200,
            evals=[(dval, "val")],
            early_stopping_rounds=20,
            verbose_eval=False,
        )
        preds = booster.predict(dval)
        auc = roc_auc_score(y[val_idx], preds)
        acc = accuracy_score(y[val_idx], (preds >= 0.5).astype(int))
        aucs.append(auc)
        accs.append(acc)
        print(f"  Fold {fold+1}: AUC={auc:.4f}  ACC={acc:.4f}")

    mean_auc = np.mean(aucs)
    mean_acc = np.mean(accs)
    print(f"\n  CV mean AUC = {mean_auc:.4f}  ACC = {mean_acc:.4f}")

    if mean_auc < 0.51:
        print("\n⚠️  AUC ≈ 0.50 — features nao tem poder preditivo de direcao ainda.")
        print("    Isso e esperado com pouco dados ou instrumento puramente IID.")
        print("    Continue coletando dados. O modelo sera salvo mas nao ative RISE_FALL_USE_ENSEMBLE=true")
        print("    ate ter AUC >= 0.53.")
    else:
        print(f"\n✅ AUC {mean_auc:.4f} — modelo tem edge! Pode ativar USE_ENSEMBLE=true após validacao.")

    # Train final model on all data
    print("\nTreinando modelo final em todos os dados...")
    dtrain_full = xgb.DMatrix(X, label=y, feature_names=available)
    params["verbosity"] = 0
    final_booster = xgb.train(params, dtrain_full, num_boost_round=200, verbose_eval=False)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    final_booster.save_model(args.out)
    print(f"Modelo salvo: {args.out}")

    with open(args.features_out, "w") as f:
        json.dump(available, f)
    print(f"Features salvas: {args.features_out}")

    # Feature importance
    importance = final_booster.get_score(importance_type="gain")
    if importance:
        print("\nFeature importance (gain):")
        for feat, score in sorted(importance.items(), key=lambda x: -x[1])[:10]:
            print(f"  {feat:35s} {score:.1f}")

    # Direction correlation check (sanity)
    print("\nCorrelacao features → label (UP=1):")
    for feat in available:
        corr = np.corrcoef(df_clean[feat].values, df_clean["label"].values)[0, 1]
        star = " ★" if abs(corr) > 0.01 else ""
        print(f"  {feat:35s} {corr:+.4f}{star}")


if __name__ == "__main__":
    main()
