"""
Retreina o XGBoost com labels CORRETOS baseados no barrier real do Deriv.

PROBLEMA RAIZ DESCOBERTO (2026-05-19):
- Shadow simulation usava barrier = ATR × 5 ≈ 0.07% → WR=99.94% (irreal)
- Barrier REAL do Deriv (1HZ25V, 5% growth) ≈ 0.013% → WR=54% (match com live)
- Modelo v1 treinado em labels errados: "alta ATR = baixo P(LOSS)" (INVERSO!)

SOLUÇÃO:
- Novo target: future_max_move_percent < REAL_BARRIER_PCT = WIN
- Modelo aprende o QUE REALMENTE importa: volatilidade baixa → sobrevive barrier
"""
from __future__ import annotations

import json, os, sys, warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.metrics import roc_auc_score, classification_report, brier_score_loss
from sklearn.calibration import calibration_curve

# ─── Config ─────────────────────────────────────────────────────────────────
DATA_PATH       = "data/legacy_accumulator/shadow_ticks_full.csv"
FEATURES_PATH   = "models/pegasus_features_v1.json"
OUT_MODEL       = "models/pegasus_xgb_v2.json"
OUT_FEATURES    = "models/pegasus_features_v2.json"

# Barrier real do Deriv para 1HZ25V com 5% growth (calibrado na live data)
# P(max_move_9_ticks < 0.013%) ≈ 54% = WR observado em live trading
REAL_BARRIER_PCT = 0.013  # %

# Para retreinar buscando WR > break-even (67.7%), usar threshold mais apertado
# Isso faz o modelo aprender a selecionar apenas os melhores momentos
TARGET_WIN_BARRIER = REAL_BARRIER_PCT  # Igual ao barrier real (base)

print("=" * 70)
print(f"RETREINAMENTO XGBoost com barrier REAL: {TARGET_WIN_BARRIER:.3f}%")
print("=" * 70)

# ─── Carrega dados ───────────────────────────────────────────────────────────
print(f"\nCarregando {DATA_PATH} ...")
df = pd.read_csv(DATA_PATH, on_bad_lines="skip")
df = df[df.future_result.isin(["WIN", "LOSS"])].copy()
df = df.sort_values("entry_epoch").reset_index(drop=True)
print(f"  {len(df):,} linhas válidas")

# ─── Features ────────────────────────────────────────────────────────────────
with open(FEATURES_PATH) as f:
    FEATURES = json.load(f)

print(f"  Features: {FEATURES}")

# ─── Novo target ─────────────────────────────────────────────────────────────
df["max_move"] = pd.to_numeric(df.future_max_move_percent, errors="coerce")
df = df.dropna(subset=["max_move"] + FEATURES)

# WIN = max_move_9_ticks < real_barrier (sobrevive o barrier do Deriv)
# LOSS = max_move_9_ticks >= real_barrier (seria knocked out)
df["y_correct"] = (df.max_move < TARGET_WIN_BARRIER).astype(int)
# 1 = WIN (sobrevive), 0 = LOSS (knocked out)
# NOTA: XGBoost prevê P(WIN) mas precisamos de P(LOSS)
# Então invertemos: target = LOSS = 1, WIN = 0 (para manter compatibilidade)
df["y"] = 1 - df["y_correct"]  # 1 = LOSS (max_move >= barrier)

wr_base = df["y_correct"].mean()
loss_rate = df["y"].mean()
print(f"\nNovos labels:")
print(f"  WR base (max_move < {TARGET_WIN_BARRIER:.3f}%): {wr_base*100:.1f}%")
print(f"  Loss rate: {loss_rate*100:.1f}%")
print(f"  Comparar: WR live observado = 54-55%  ✓")

# ─── Prepara X, y ────────────────────────────────────────────────────────────
X = df[FEATURES].astype(float).values
y = df["y"].values  # 1 = LOSS

print(f"\nDataset: {X.shape[0]:,} amostras, {X.shape[1]} features")

# ─── Validação cross-val ─────────────────────────────────────────────────────
print("\nCross-validando modelo (5 folds)...")
params_cv = {
    "objective":       "binary:logistic",
    "eval_metric":     "auc",
    "n_estimators":    300,
    "learning_rate":   0.05,
    "max_depth":       4,
    "subsample":       0.8,
    "colsample_bytree":0.8,
    "min_child_weight":20,
    "scale_pos_weight":wr_base / loss_rate,  # balanceia classes
    "seed":            42,
    "tree_method":     "hist",
    "verbosity":       0,
}

from xgboost import XGBClassifier
clf_cv = XGBClassifier(**params_cv)
cv = StratifiedKFold(n_splits=5, shuffle=False)  # sem shuffle p/ respeit. tempo
auc_scores = cross_val_score(clf_cv, X, y, cv=cv, scoring="roc_auc")
print(f"  AUC por fold: {[f'{s:.4f}' for s in auc_scores]}")
print(f"  AUC médio:    {auc_scores.mean():.4f} ± {auc_scores.std():.4f}")

# AUC > 0.55 = modelo tem alguma capacidade preditiva
# AUC > 0.65 = bom; AUC > 0.70 = excelente
if auc_scores.mean() > 0.55:
    print("  ✅ Modelo tem discriminação útil")
else:
    print("  ⚠️  AUC baixo — features podem não prever max_move bem")

# ─── Treino final no dataset completo ────────────────────────────────────────
print("\nTreinando modelo final...")
# Split temporal: 80% treino, 20% validação final
split = int(len(X) * 0.80)
X_tr, y_tr = X[:split], y[:split]
X_val, y_val = X[split:], y[split:]

dtrain = xgb.DMatrix(X_tr, label=y_tr, feature_names=FEATURES)
dval   = xgb.DMatrix(X_val, label=y_val, feature_names=FEATURES)

params_train = {
    "objective":        "binary:logistic",
    "eval_metric":      ["logloss", "auc"],
    "learning_rate":    0.03,
    "max_depth":        4,
    "subsample":        0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 20,
    "scale_pos_weight": wr_base / loss_rate,
    "seed":             42,
    "tree_method":      "hist",
    "verbosity":        0,
}

evals = [(dtrain, "train"), (dval, "val")]
model = xgb.train(
    params_train,
    dtrain,
    num_boost_round=500,
    evals=evals,
    early_stopping_rounds=30,
    verbose_eval=False,
)
print(f"  Best iteration: {model.best_iteration} rounds")

# ─── Avaliação no conjunto de validação ──────────────────────────────────────
preds_val = model.predict(dval)
auc_val = roc_auc_score(y_val, preds_val)
brier = brier_score_loss(y_val, preds_val)
print(f"  AUC validação: {auc_val:.4f}")
print(f"  Brier score:   {brier:.4f} (menor = melhor)")

print("\n=== WIN RATE POR FAIXA DE P(LOSS) PREDITO (novo modelo) ===")
val_df = pd.DataFrame({"p_loss": preds_val, "y": y_val})
for low, high in [(0, 0.10), (0.10, 0.20), (0.20, 0.30), (0.30, 0.40), (0.40, 0.50), (0.50, 1.0)]:
    sub = val_df[(val_df.p_loss >= low) & (val_df.p_loss < high)]
    if len(sub) == 0: continue
    wr = 1 - sub.y.mean()
    print(f"  P(LOSS)={low*100:.0f}-{high*100:.0f}%: n={len(sub):5d} | WR_simulado={wr*100:.1f}%  (target: {high*100:.0f}%)")

# ─── Feature importance ──────────────────────────────────────────────────────
print("\n=== FEATURE IMPORTANCE (gain) ===")
fi = model.get_score(importance_type="gain")
fi_sorted = sorted(fi.items(), key=lambda x: x[1], reverse=True)
for feat, score in fi_sorted:
    print(f"  {feat:<35} {score:>10.2f}")

# ─── Salva modelo ────────────────────────────────────────────────────────────
model.save_model(OUT_MODEL)
with open(OUT_FEATURES, "w") as f:
    json.dump(FEATURES, f)
print(f"\n✅ Modelo salvo: {OUT_MODEL}")
print(f"✅ Features salvas: {OUT_FEATURES}")

# ─── Comparação v1 vs v2 ─────────────────────────────────────────────────────
print("\n=== COMPARAÇÃO v1 (errado) vs v2 (correto) ===")
scorer_old = xgb.Booster()
scorer_old.load_model("models/pegasus_xgb_v1.json")
with open("models/pegasus_features_v1.json") as f:
    feat_old = json.load(f)

dval_old = xgb.DMatrix(X_val, feature_names=feat_old)
preds_old = scorer_old.predict(dval_old)

auc_old = roc_auc_score(y_val, preds_old)
auc_new = roc_auc_score(y_val, preds_val)
print(f"  Modelo v1 (ATR×5 barrier, errado): AUC={auc_old:.4f}")
print(f"  Modelo v2 (0.013% barrier, correto): AUC={auc_new:.4f}")
print()

# Análise crítica: quais entradas o v1 "aprovava" vs o v2?
mask_v1_enter = preds_old < 0.20  # v1: entra quando P(LOSS) < 20%
mask_v2_enter = preds_val < 0.20  # v2: entra quando P(LOSS) < 20%

wr_v1_entries = 1 - y_val[mask_v1_enter].mean() if mask_v1_enter.sum() > 0 else 0
wr_v2_entries = 1 - y_val[mask_v2_enter].mean() if mask_v2_enter.sum() > 0 else 0
print(f"  WR das entradas aprovadas pelo v1 (P<20%): {wr_v1_entries*100:.1f}% (n={mask_v1_enter.sum()})")
print(f"  WR das entradas aprovadas pelo v2 (P<20%): {wr_v2_entries*100:.1f}% (n={mask_v2_enter.sum()})")

print("\n=== THRESHOLDS RECOMENDADOS PARA LUCRO ===")
print("Break-even WR = 67.7% (payout 47.75%)")
print()
for threshold in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]:
    mask = preds_val < threshold
    if mask.sum() == 0: continue
    wr = 1 - y_val[mask].mean()
    pct_entries = mask.mean() * 100
    print(f"  P(LOSS) < {threshold*100:.0f}%: WR={wr*100:.1f}% | {pct_entries:.1f}% das entradas | break-even={'✅' if wr >= 0.677 else '❌'}")

print("\n" + "="*70)
print("PRÓXIMOS PASSOS:")
print("1. Se AUC > 0.60: deploy v2, teste com stake pequeno")
print("2. Ajustar ENSEMBLE_MIN_PROB para o threshold que dá WR > 68%")
print("3. Corrigir shadow_collect barrier para coletar dados melhores")
print("="*70)
