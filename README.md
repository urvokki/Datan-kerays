# Datan keräys: RoboLab ja AIoT labran sensoridata

## Kuvaus

Projektin tarkoitus on:

- Kerätä luokkahuoneiden läsnäolodata MQTT-brokerista
- Tallentaa data MongoDB Atlas -pilveen
- Tehdä yksinkertainen EDA (aikasarjan resamplaus ja visualisointi)
- Rakentaa kevyt ennuste- ja piikkitunnistusratkaisu

Toteutus on harjoitusluonteinen, mutta toimiva ja loogisesti perusteltu kokonaisuus.  
Ratkaisuna käytetään yksinkertaista baseline-ennustetta sekä kevyttä LightGBM-pohjaista piikkimallia.

---

## Repository-rakenne

- `mqtt_to_mongo.py` — MQTT-consumer (MQTT → MongoDB)
- `eda_resample_fix.py` — EDA ja resamplaus (1T, 5T, 15T)
- `resampled_5min_fix.csv` — 5 min aggregoitu data
- `scripts/train_model_5min.py` — mallin koulutus
- `scripts/eval_holdout_baseline.py` — baseline vs malli
- `scripts/eval_nonzero_and_spikes.py` — ei-nollat + piikkitunnistus
- `scripts/predict_simple.py` — ennustaja (baseline + model)
- `models/lgbm_5min.pkl` — tallennettu malli (valinnainen)
- `results/metrics.json` — mallin mittarit
- `plots/` — generoituja kuvia
- `.env` — konfiguraatiot (ei versionhallintaan)
- `.env.example` — esimerkkimuuttujat
- `requirements.txt` — riippuvuudet

---

## Asennus

### 1. Luo virtuaaliympäristö

~~~powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
~~~

### 2. Asenna riippuvuudet

~~~powershell
pip install -r requirements.txt
~~~

Jos requirements.txt puuttuu:

~~~powershell
pip install paho-mqtt pymongo python-dateutil pandas matplotlib certifi scikit-learn lightgbm joblib
~~~

---

## .env tiedosto

Luo projektin juureen `.env` (ei salaisia osia tänne):

~~~
MQTT_HOST=automaatio.cloud.shiftr.io
MQTT_PORT=1883
MQTT_USER=automaatio
MQTT_PASS=CHANGEME
MQTT_TOPIC=aiotgarage/+/+/presence

MONGO_URI=mongodb+srv://.....
MONGO_DB=data_ml
MONGO_COLLECTION=p_count
~~~

---

## Käyttö

### 1. Käynnistä keruu

~~~powershell
python mqtt_to_mongo.py
~~~

### 2. EDA ja resamplaus

~~~powershell
python eda_resample_fix.py
~~~

### 3. Mallin koulutus

~~~powershell
python scripts/train_model_5min.py
~~~

### 4. Evaluointi

~~~powershell
python scripts/eval_holdout_baseline.py
python scripts/eval_nonzero_and_spikes.py
~~~

### 5. Ennusteen demo

~~~powershell
python scripts/predict_simple.py
~~~

---

## Tulokset

- Data kerätty MQTT:ltä MongoDB Atlas -pilveen
- Resamplattu 5 minuutin välein
- Baseline toimii vertailukohtana
- LightGBM parantaa piikkien tunnistusta
- Lopullinen ratkaisu: baseline + piikkimalli
