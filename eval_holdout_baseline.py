# scripts/eval_holdout_baseline.py
"""
Evaluate saved model vs persistence baseline on a holdout split (last 20%).
Prints MAE for baseline and model and shows a small sample of predictions.
"""
import os
import joblib
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error

MODEL_PATH = "models/lgbm_5min.pkl"
CSV = "resampled_5min_fix.csv"

if not os.path.exists(CSV):
    raise SystemExit("CSV puuttuu: " + CSV)
if not os.path.exists(MODEL_PATH):
    raise SystemExit("Mallia ei löydy: " + MODEL_PATH)

# load data
df = pd.read_csv(CSV, index_col=0, parse_dates=True)
if "person_count" not in df.columns:
    raise SystemExit("person_count puuttuu CSV:stä")

series = df["person_count"].astype(float)

# feature engineering (same kuin training)
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

y = X["target"]
X_feat = X.drop(columns=["y","target"])

# holdout split: viimeiset 20% näytteistä
n = len(X_feat)
if n < 50:
    print("Varoitus: liian vähän näytteitä holdout-testiin:", n)
split = int(n * 0.8)
X_train, X_test = X_feat.iloc[:split], X_feat.iloc[split:]
y_train, y_test = y.iloc[:split], y.iloc[split:]

print(f"Total samples: {n}, train: {len(X_train)}, test: {len(X_test)}")

# baseline: persistence (lag_1)
if "lag_1" not in X_test.columns:
    raise SystemExit("lag_1 ei löytynyt featureista")
baseline_pred = X_test["lag_1"].values  # ennustaa viimeisen 5-min arvon
baseline_mae = mean_absolute_error(y_test, baseline_pred)
print(f"Baseline (persistence) MAE on holdout: {baseline_mae:.4f}")

# load model and evaluate
model = joblib.load(MODEL_PATH)
model_pred = model.predict(X_test)
model_mae = mean_absolute_error(y_test, model_pred)
print(f"Model MAE on holdout: {model_mae:.4f}")

# print small sample of comparisons
cmp = pd.DataFrame({
    "y_true": y_test,
    "baseline": baseline_pred,
    "model": model_pred
}, index=y_test.index)
print("\nSample comparisons (last 20 rows):")
print(cmp.tail(20).to_string())

# optional: feature importances
try:
    importances = getattr(model, "feature_importances_", None)
    if importances is not None:
        featimp = pd.Series(importances, index=X_feat.columns).sort_values(ascending=False)
        print("\nTop feature importances:")
        print(featimp.head(10).to_string())
except Exception as e:
    print("Feature importance failed:", e)
