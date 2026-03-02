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

scripts/predict_simple.py — yksinkertainen ennustaja (baseline + model)

models/lgbm_5min.pkl — tallennettu malli (valinnainen)

results/metrics.json — mallin mittarit

plots/ — generoituja kuvia

.env — konfiguraatiot (EI versionhallintaan)

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

Luo projektin juureen .env tiedosto (älä lisää GitHubiin):

MQTT_HOST=automaatio.cloud.shiftr.io
MQTT_PORT=1883
MQTT_USER=automaatio
MQTT_PASS=CHANGEME
MQTT_TOPIC=aiotgarage/+/+/presence

MONGO_URI=mongodb+srv://USER:PASS@cluster.mongodb.net/?retryWrites=true&w=majority
MONGO_DB=data_ml
MONGO_COLLECTION=p_count
Käyttö
1. Käynnistä keruu (MQTT → MongoDB)
python mqtt_to_mongo.py

Lokissa pitäisi näkyä onnistunut MongoDB-yhteys ja MQTT-loopin käynnistyminen.

2. EDA ja resamplaus
python eda_resample_fix.py

Tuottaa mm.:

resampled_5min_fix.csv

resampled_15min_fix.csv

visualisointeja plots/-kansioon

3. Mallin koulutus
python scripts/train_model_5min.py

Tuottaa:

models/lgbm_5min.pkl

results/metrics.json

4. Evaluointi
python scripts/eval_holdout_baseline.py
python scripts/eval_nonzero_and_spikes.py

Näillä verrataan baselinea ja mallia (MAE + piikkimittarit).

5. Ennusteen demo
python scripts/predict_simple.py

Tulostaa JSON-tyylisen ennusteen:

baseline

model_pred

is_spike

timestamp

Visualisoinnit

Projektissa tuotetut kuvat löytyvät plots/-kansiosta, esimerkiksi:

Aikasarjakuva (resampled 5 min)

Zoom 2.–3.2.2026

Boxplot tunneittain (08–21, Helsinki-aika)

Nämä kuvat esittävät datan jakautumisen ja ajallisen rakenteen.

Tulokset (tiivistelmä)

Data onnistuneesti kerätty MQTT:ltä MongoDB Atlas -pilveen

Resamplattu 5 min aikaintervalliin

Baseline-ennuste toimi vahvana vertailukohtana

LightGBM-malli paransi ei-nollien ja piikkien tunnistusta

Lopullinen ratkaisu: baseline yleiseen ennusteeseen + malli piikkien tunnistukseen
