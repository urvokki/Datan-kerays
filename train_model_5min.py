# scripts/train_model_5min.py
"""
Train LightGBM on 5-minute aggregated data (resampled_5min_fix.csv).
Produces: models/lgbm_5min.pkl and results/metrics.json
Simplified to avoid early_stopping compatibility issues.
"""
import os, json
import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit
import lightgbm as lgb

os.makedirs("models", exist_ok=True)
os.makedirs("results", exist_ok=True)

CSV = "resampled_5min_fix.csv"   # produced by eda_resample_fix.py
MODEL_OUT = "models/lgbm_5min.pkl"
METRICS_OUT = "results/metrics.json"

# --- load data ---
df = pd.read_csv(CSV, index_col=0, parse_dates=True)
# ensure column name
if "person_count" not in df.columns:
    raise SystemExit("CSV missing 'person_count' column: " + CSV)

series = df["person_count"].astype(float)

# --- feature engineering ---
# target: next-step (horizon=1) prediction (next 5-min bucket)
horizon = 1
max_lag = 12  # lags of up to 1 hour (12 * 5min)
X = pd.DataFrame(index=series.index)
X["y"] = series
for lag in range(1, max_lag+1):
    X[f"lag_{lag}"] = X["y"].shift(lag)
# rolling features
X["rmean_3"] = X["y"].rolling(window=3).mean().shift(1)
X["rmean_12"] = X["y"].rolling(window=12).mean().shift(1)
# time features
X["hour"] = X.index.hour
X["dow"] = X.index.dayofweek

# target column
X["target"] = X["y"].shift(-horizon)
X = X.dropna().copy()

y = X["target"]
X_feat = X.drop(columns=["y","target"])

print("Total samples after feature engineering:", len(X_feat))

# --- time series CV ---
tscv = TimeSeriesSplit(n_splits=5)
fold_maes = []
fold = 0
models = []
for train_idx, val_idx in tscv.split(X_feat):
    fold += 1
    X_train, X_val = X_feat.iloc[train_idx], X_feat.iloc[val_idx]
    y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

    # simplified model call: no early_stopping_rounds for compatibility
    model = lgb.LGBMRegressor(n_estimators=300, learning_rate=0.05, random_state=42)
    model.fit(X_train, y_train)  # simple fit

    pred = model.predict(X_val)
    mae = mean_absolute_error(y_val, pred)
    print(f"Fold {fold} MAE: {mae:.4f}  (train size {len(X_train)}, val size {len(X_val)})")
    fold_maes.append(mae)
    models.append(model)

# summary
mean_mae = float(np.mean(fold_maes))
print("Mean MAE:", mean_mae)
metrics = {"fold_maes": [float(x) for x in fold_maes], "mean_mae": mean_mae, "n_samples": len(X_feat)}

# --- train final model on all data ---
final_model = lgb.LGBMRegressor(n_estimators=300, learning_rate=0.05, random_state=42)
final_model.fit(X_feat, y)

joblib.dump(final_model, MODEL_OUT)
with open(METRICS_OUT, "w", encoding="utf-8") as f:
    json.dump(metrics, f, ensure_ascii=False, indent=2)

print("Saved final model to", MODEL_OUT)
print("Saved metrics to", METRICS_OUT)
