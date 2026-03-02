# scripts/predict_simple.py
# Yksinkertainen ennustepalvelu: baseline + mallipohjainen piikkitarkistus
from dotenv import load_dotenv
load_dotenv()

import os
import joblib
import certifi
import pandas as pd
from pymongo import MongoClient
from datetime import timezone

# Konfiguraatio (voi muokata .env:ssä)
MONGO_URI = os.getenv("MONGO_URI")
COL = os.getenv("PRED_COL", "p_count")   # kokoelma
MODEL_PATH = os.getenv("MODEL_PATH", "models/lgbm_5min.pkl")

if not MONGO_URI:
    raise SystemExit("MONGO_URI puuttuu .env:stä")

# Käytetään certifi:n CA-bundlea TLS-varmennukseen (auttaa TLS handshake -ongelmissa)
client = MongoClient(MONGO_URI, tls=True, tlsCAFile=certifi.where())
db = client["data_ml"]
coll = db[COL]

# Hae data (kaikki dokumentit tai rajattu määrä)
rows = list(coll.find({}, projection=["person_count","count","_sensor_datetime_utc","_received_at"]).sort([("_sensor_datetime_utc",1)]))
if not rows:
    rows = list(coll.find({}, projection=["person_count","count","_received_at"]).sort([("_received_at",1)]))

if not rows:
    raise SystemExit("Ei dataa kokoelmassa " + COL)

df = pd.DataFrame(rows)

# Normalisoi aika: käytä _sensor_datetime_utc jos saatavilla, muuten _received_at
# -> parse kaikki ajat UTC:ksi (utc=True muuntaa tz-naive -> UTC ja tz-aware -> UTC)
if "_sensor_datetime_utc" in df.columns and df["_sensor_datetime_utc"].notna().any():
    df["_sensor_datetime_utc"] = pd.to_datetime(df["_sensor_datetime_utc"], errors="coerce", utc=True)
    # jos edelleen kaikki NaT, fallback
    if df["_sensor_datetime_utc"].notna().sum() == 0 and "_received_at" in df.columns:
        df["_received_at"] = pd.to_datetime(df["_received_at"], errors="coerce", utc=True)
        df = df.set_index("_received_at").sort_index()
    else:
        df = df.set_index("_sensor_datetime_utc").sort_index()
else:
    df["_received_at"] = pd.to_datetime(df["_received_at"], errors="coerce", utc=True)
    df = df.set_index("_received_at").sort_index()

# Varmista että indeksi on timezone-aware (UTC)
if getattr(df.index, "tz", None) is None:
    # localisoidaan UTC, mutta tämä tapaus tulee harvoin koska pd.to_datetime(..., utc=True) teki sen
    df.index = df.index.tz_localize(timezone.utc)

# Varmista person_count-numero
if "person_count" not in df.columns:
    if "count" in df.columns:
        df["person_count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    else:
        df["person_count"] = 0

# Resample 5T kuten koulutuksessa
series = df["person_count"].resample("5T").sum().fillna(0)

# Jos sarja on liian lyhyt, käytä viimeistä arvoa baselineksi
if len(series) < 2:
    last = int(series.iloc[-1]) if len(series) >= 1 else 0
    print({"baseline": int(last), "model_pred": None, "is_spike": False})
    raise SystemExit(0)

# Rakennetaan featuret (samat kuin koulutuksessa)
horizon = 1
max_lag = 12
X = pd.DataFrame({'y': series})
for lag in range(1, max_lag+1):
    X[f"lag_{lag}"] = X['y'].shift(lag)
X["rmean_3"] = X["y"].rolling(window=3).mean().shift(1)
X["rmean_12"] = X["y"].rolling(window=12).mean().shift(1)
X["hour"] = X.index.hour
X["dow"] = X.index.dayofweek

X = X.dropna()
if X.empty:
    last = int(series.iloc[-1])
    print({"baseline": int(last), "model_pred": None, "is_spike": False})
    raise SystemExit(0)

# Ota viimeinen rivi featuresiksi
x_latest = X.iloc[[-1]].drop(columns=["y"])  # dataframe shape (1, n_features)
baseline = int(x_latest["lag_1"].iloc[0])  # baseline = viimeisin 5-min arvo

# Lataa malli (jos olemassa)
model = None
if os.path.exists(MODEL_PATH):
    try:
        model = joblib.load(MODEL_PATH)
    except Exception as e:
        print("Mallin lataus epäonnistui:", e)
        model = None

# Malliennuste (float); jos malli puuttuu, jätä None
model_pred = None
is_spike = False
if model is not None:
    pred = model.predict(x_latest)[0]
    model_pred = float(pred)
    # yksinkertainen sääntö: piikki jos mallin arvo >= 1.0
    is_spike = bool(model_pred >= 1.0)

# Tulostus (JSON-tyylinen)
out = {
    "baseline": baseline,
    "model_pred": model_pred,
    "is_spike": is_spike,
    "timestamp_for_prediction": x_latest.index[0].isoformat()
}
print(out)

