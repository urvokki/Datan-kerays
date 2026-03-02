# Datan keräys: RoboLab ja AIoT labran sensoridata

## Kuvaus

Projektin tarkoitus: kerätä luokkahuoneiden läsnäolodata MQTT-brokereista, tallentaa se MongoDB Atlas -pilveen, tehdä yksinkertainen EDA (aikasarjan resamplaus) ja rakentaa kevyt ennuste-/piikkitunnistusratkaisu. Tämä repo sisältää keruuputken, analyysiskriptit ja yksinkertaisen ennustajan.

Tavoite: harjoitusluonteinen, toimiva ja helposti toistettava kokonaisuus — riittää yksinkertainen baseline + kevyt piikkimalli.

---

## Repository-rakenne (olennaiset tiedostot)

* `mqtt_to_mongo.py` — MQTT-consumer: ottaa viestit MQTT:ltä ja tallentaa MongoDB:hen
* `.env` — konfiguraatiot (EI versionhallintaan: MONGO_URI, MQTT credentials)
* `eda_resample_fix.py` — EDA ja resample (1T, 5T, 15T); tuottaa `resampled_5min_fix.csv` jne.
* `resampled_5min_fix.csv` (tuotos)
* `scripts/train_model_5min.py` — koulutusskripti (LightGBM)
* `models/lgbm_5min.pkl` — (valinnainen) tallennettu malli
* `results/metrics.json` — koulutuksen tulokset
* `scripts/eval_holdout_baseline.py` — holdout-eval baseline vs model
* `scripts/eval_nonzero_and_spikes.py` — ei-nollat / piikkitarkistus
* `scripts/predict_simple.py` — valmis ennustaja: baseline + model spike flag
* `scripts/*.py` — muut apu- ja plot-skriptit (plot_zoom_..., boxplot_hours_8_21.py)
* `plots/` — suosittelemme lisätä tähän generoidut PNG-kuvat
* `failed_queue.jsonl` — puskuritiedosto epäonnistuneille tallennuksille (consumer)

---

## Vaadittavat ympäristö- ja asennusvaiheet

1. Luo virtuaaliympäristö (Windows PowerShell -esimerkki):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Asenna riippuvuudet (projekti käyttää mm. `paho-mqtt`, `pymongo`, `python-dateutil`, `pandas`, `matplotlib`, `lightgbm`):

```powershell
pip install -r requirements.txt
# Jos requirements.txt puuttuu, voit asentaa minimipaketit:
# pip install paho-mqtt pymongo python-dateutil pandas matplotlib certifi scikit-learn lightgbm joblib
```

3. Lisää `.env` tiedosto juureen (osa sisällöstä salattua):

```
# Esimerkki (korvaa omilla arvoilla)
MQTT_HOST=automaatio.cloud.shiftr.io
MQTT_PORT=1883
MQTT_USER=automaatio
MQTT_PASS=CHANGEME
MQTT_TOPIC=aiotgarage/+/+/presence

MONGO_URI=mongodb+srv://USER:PASS@cluster0.example.mongodb.net/?retryWrites=true&w=majority
MONGO_DB=data_ml
MONGO_COLLECTION=p_count
```

> HUOM: lisää `.env` ja muita salaisuuksia `.gitignore`-tiedostoon ennen pushia.

---

## Käyttöohjeet — päävaiheet

### 1) Käynnistä consumer (MQTT → MongoDB)

```powershell
.\.venv\Scripts\Activate.ps1
python mqtt_to_mongo.py
```

Tarkista loki, että `Yhdistetty MongoDB:hen (connection OK)` ja `MQTT loop started` näkyvät.

### 2) Testaa publish (testi-viesti)

```powershell
python publish_test.py
```

Tarkista, että testiviesti näkyy MongoDB:ssä. Käytä `check_db.py` tai MongoDB Atlas UI:ta.

### 3) EDA ja resample

```powershell
python eda_resample_fix.py
```

Skripti tuottaa CSV- ja PNG-tiedostoja (`resampled_5min_fix.csv`, `resampled_minute_fix.csv`, `resampled_15min_fix.csv` ja vastaavat kuvat). Tarkista kansio `plots/` tai repo-juuri.

### 4) Koulutus

```powershell
python scripts\train_model_5min.py
```

Tämä luo `models/lgbm_5min.pkl` ja `results/metrics.json`.

### 5) Evaluointi

```powershell
python scripts\eval_holdout_baseline.py
python scripts\eval_nonzero_and_spikes.py
```

Näillä saat MAE:t ja piikkitarkkuudet. Dokumentoi tulokset `results/`-kansioon.

### 6) Ennuste / demo

```powershell
python scripts\predict_simple.py
```

Tulostaa JSON-tyylisen ennusteen: baseline, model_pred, is_spike ja käytetyn timestampin.

---

## Plots ja visualisointi

* `scripts/plot_timeseries_mongo.py` tai `scripts/plot_timeseries_csv.py` luo yleiskuvan.
* `scripts/plot_zoom_2026_02_02_03.py` tekee zoomin 2.–3.2.2026 (valmis skripti).
* `scripts/boxplot_hours_8_21.py` tuottaa boxplotin tunneittain (08–21 Helsinki-aika).

Lisää kuvat `plots/` ja viittaa niihin README:ssä raporttiosiossa.

---

## Hyväksyttävä lopputulos (mitä kurssiarvioija odottaa)

* Automatisoitu pipeline keruuseen (MQTT → MongoDB) ja toimiva demo (publish_test).
* EDA: resamplaus ja kuvakaappaukset (CSV + kuva(t)).
* Yksinkertainen ennustemalli tai baseline: jos baseline valitaan, dokumentoi syy (baseline parempi MAE:ssa); jos malli käytössä, liitä `models/` ja `results/metrics.json`.
* Koodi ja ohjeet GitHubissa; riittävä README joka kertoo miten toistaa tulokset.

---

## Git-työskentely — ehdotetut commitit

1. `git add mqtt_to_mongo.py eda_resample_fix.py scripts/*.py .env.example README.md requirements.txt`
2. `git commit -m "Initial pipeline: MQTT->Mongo, EDA, model training scripts and predict_simple"`
3. `git push origin main`

Lisää `.env.example` joka ei sisällä salasanoja mutta näyttää vaaditut muuttujat.

---

## Lopuksi / checklist (lopullinen tarkistus ennen palautusta)

* [ ] README.md selkeä ja sisältää käyttöohjeet
* [ ] requirements.txt tai instructions for dependencies
* [ ] .env.example mukana, .env pois versionhallinnasta
* [ ] Consumer toimii ja testiviesti näkyy Atlasissa
* [ ] Resampled CSV ja muut EDA-kuvat lisätty `plots/`
* [ ] models/ ja results/ (tai ainakin metrics.json) lisätty tai ohje niiden lataamisesta
* [ ] Lyhyt teksti (README) miksi valittiin baseline tai piikkimalli (perustelu)

---

Jos haluat, teen nyt yhden seuraavista yhdellä komennolla:

* A: lisään valmiin `.gitignore` ja `.env.example` tiedoston repoosi; tai
* B: luon `requirements.txt` luettelon nykyisistä riippuvuuksista; tai
* C: annan valmiin commit-viestin ja git-komennot, joita suoritat paikallisesti.

Valitse A/B/C.
