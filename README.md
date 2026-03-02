Datan keräys: RoboLab ja AIoT labran sensoridata
Kuvaus

Projektin tarkoitus on:

Kerätä luokkahuoneiden läsnäolodata MQTT-brokerista

Tallentaa data MongoDB Atlas -pilveen

Tehdä yksinkertainen EDA (aikasarjan resamplaus ja visualisointi)

Rakentaa kevyt ennuste- ja piikkitunnistusratkaisu

Toteutus on harjoitusluonteinen, mutta toimiva ja loogisesti perusteltu kokonaisuus.
Ratkaisuna käytetään yksinkertaista baseline-ennustetta sekä kevyttä LightGBM-pohjaista piikkimallia.

Repository-rakenne (olennaiset tiedostot)

mqtt_to_mongo.py — MQTT-consumer (MQTT → MongoDB)

eda_resample_fix.py — EDA ja resamplaus (1T, 5T, 15T)

resampled_5min_fix.csv — 5 min aggregoitu data

scripts/train_model_5min.py — mallin koulutus

scripts/eval_holdout_baseline.py — baseline vs malli (holdout)

scripts/eval_nonzero_and_spikes.py — ei-nollat + piikkitunnistus

scripts/predict_simple.py — ennustaja (baseline + model)

models/lgbm_5min.pkl — tallennettu malli (valinnainen)

results/metrics.json — mallin mittarit

plots/ — generoituja kuvia

.env — konfiguraatiot (ei versionhallintaan)

.env.example — esimerkkimuuttujat ilman salasanoja

requirements.txt — riippuvuudet

Asennus
1. Luo virtuaaliympäristö
python -m venv .venv
.\.venv\Scripts\Activate.ps1
2. Asenna riippuvuudet
pip install -r requirements.txt

Jos requirements.txt puuttuu:

pip install paho-mqtt pymongo python-dateutil pandas matplotlib certifi scikit-learn lightgbm joblib
Ympäristömuuttujat (.env)

Luo projektin juureen .env-tiedosto (älä lisää GitHubiin):

MQTT_HOST=automaatio.cloud.shiftr.io
MQTT_PORT=1883
MQTT_USER=automaatio
MQTT_PASS=CHANGEME
MQTT_TOPIC=aiotgarage/+/+/presence

MONGO_URI=mongodb+srv://USER:PASS@cluster.mongodb.net/?retryWrites=true&w=majority
MONGO_DB=data_ml
MONGO_COLLECTION=p_count
Käyttö
1. Käynnistä keruu
python mqtt_to_mongo.py
2. EDA ja resamplaus
python eda_resample_fix.py
3. Mallin koulutus
python scripts/train_model_5min.py
4. Evaluointi
python scripts/eval_holdout_baseline.py
python scripts/eval_nonzero_and_spikes.py
5. Ennusteen demo
python scripts/predict_simple.py
