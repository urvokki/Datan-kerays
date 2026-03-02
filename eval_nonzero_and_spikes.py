# scripts/eval_nonzero_and_spikes.py
"""
Evaluate model vs persistence baseline focusing on non-zero events and spike detection.
Outputs:
 - MAE overall (repeat)
 - MAE on samples where y>0
 - Precision/Recall/F1 for detecting spikes (y>=1) using:
    * baseline (lag_1)
    * model (prediction threshold >= 1.0)
 - Show small sample of errors.
"""
import os, joblib
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, precision_score, recall_score, f1_score, confusion_matrix

CSV = "resampled_5min_fix.csv"
MODEL_PATH = "models/lgbm_5min.pkl"

if not os.path.exists(CSV):
    raise SystemExit("CSV puuttuu: " + CSV)
if not os.path.exists(MODEL_PATH):
    raise SystemExit("Mallia ei löydy: " + MODEL_PATH)

# load
df = pd.read_csv(CSV, index_col=0, parse_dates=True)
series = df["person_count"].astype(float)

# features same as train
horizon = 1
max_lag = 12
X = pd.DataFrame(index=series.index)
X["y"] = series
for lag in range(1, max_lag+1):
    X[f"lag_{lag}"] = X["y"].shift(lag)
X["rmean_3"] = X["y"].rolling(window=3).mean().shift(1)
X["rmean_12"] = X["y"].rolling(window=12).mean().shift(1)
X["hour"] = X.index.hour
X["dow"] = X.index.dayofweek
X["target"] = X["y"].shift(-horizon)
X = X.dropna().copy()

y = X["target"].values
X_feat = X.drop(columns=["y", "target"])

n_total = len(X_feat)
split = int(n_total * 0.8)
X_test = X_feat.iloc[split:]
y_test = y[split:]
idx_test = X_test.index

print("Samples total:", n_total, "Test size:", len(X_test))

# baseline predictions (persistence: lag_1)
baseline_pred = X_test["lag_1"].values

# load model
model = joblib.load(MODEL_PATH)
model_pred = model.predict(X_test)

# overall MAE (repeat)
mae_overall_baseline = mean_absolute_error(y_test, baseline_pred)
mae_overall_model = mean_absolute_error(y_test, model_pred)
print(f"Overall MAE - baseline: {mae_overall_baseline:.4f}, model: {mae_overall_model:.4f}")

# MAE on non-zero true samples
mask_nonzero = (y_test > 0)
if mask_nonzero.sum() > 0:
    mae_nonzero_baseline = mean_absolute_error(y_test[mask_nonzero], baseline_pred[mask_nonzero])
    mae_nonzero_model = mean_absolute_error(y_test[mask_nonzero], model_pred[mask_nonzero])
else:
    mae_nonzero_baseline = mae_nonzero_model = float("nan")

print(f"MAE on non-zero true samples (count={mask_nonzero.sum()}): baseline={mae_nonzero_baseline:.4f}, model={mae_nonzero_model:.4f}")

# Spike detection (binary): define true_spike = y_test >= 1
y_true_bin = (y_test >= 1).astype(int)

# baseline binary prediction: lag_1 >= 1
y_base_bin = (baseline_pred >= 1).astype(int)
# model binary prediction: choose threshold 1.0 (pred >= 1 -> predict spike)
y_model_bin = (model_pred >= 1.0).astype(int)

def prf(y_true, y_pred, label):
    p = precision_score(y_true, y_pred, zero_division=0)
    r = recall_score(y_true, y_pred, zero_division=0)
    f = f1_score(y_true, y_pred, zero_division=0)
    print(f"{label}: Precision={p:.4f}, Recall={r:.4f}, F1={f:.4f} (positives={y_pred.sum()})")
    cm = confusion_matrix(y_true, y_pred)
    print(f" Confusion matrix:\n{cm}")

print("\nSpike detection (threshold >=1):")
prf(y_true_bin, y_base_bin, "Baseline (lag_1)")
prf(y_true_bin, y_model_bin, "Model (pred>=1.0)")

# Show examples where model predicted spike but baseline didn't and vice versa
cmp = pd.DataFrame({
    "y_true": y_test,
    "baseline": baseline_pred,
    "model": model_pred,
    "base_bin": y_base_bin,
    "model_bin": y_model_bin
}, index=idx_test)

print("\nExamples: model caught spike but baseline missed (upto 10):")
print(cmp[(cmp["model_bin"]==1) & (cmp["base_bin"]==0) & (cmp["y_true"]>=1)].head(10).to_string())

print("\nExamples: baseline caught spike but model missed (upto 10):")
print(cmp[(cmp["base_bin"]==1) & (cmp["model_bin"]==0) & (cmp["y_true"]>=1)].head(10).to_string())

# feature importances
try:
    fi = pd.Series(model.feature_importances_, index=X_feat.columns).sort_values(ascending=False)
    print("\nTop feature importances:")
    print(fi.head(10).to_string())
except Exception as e:
    print("Feature importances error:", e)
