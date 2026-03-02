Yhteenveto

Tämä repo sisältää yksinkertaisen, toistettavan pipeline:n anturidatan keruuseen (MQTT → MongoDB), pienen EDA-vaiheen (resample, visualisoinnit) ja kevyen ennuste/piikkitunnistus-skriptin. Lopputulokset (kuvat ja mittarit) on tallennettu plots/ ja results/ kansioihin.

Sisältö (olennaiset tiedostot)

mqtt_to_mongo.py — MQTT → MongoDB consumer (reaaliaikainen)

scripts/boxplot_hours_8_21.py — boxplot (08–21, Helsinki)

scripts/plot_zoom_2026_02_02_03.py — zoom 2.–3.2.2026

scripts/predict_simple.py — yksinkertainen ennustaja (baseline + model)

resampled_5min_fix.csv (valinnainen, näyte)

plots/ — generoituja kuvia (esim. boxplot_hours_8_21.png)

results/metrics.json (valinnainen)

requirements.txt

.env.example (esimerkkiympäristömuuttujat; EI salasanoja)

Lyhyet suoritusohjeet (minimi)

Luo ja aktivoi virtuaaliympäristö:

python -m venv .venv
.\.venv\Scripts\Activate.ps1

Asenna riippuvuudet:

pip install -r requirements.txt

Kopioi .env.example → .env ja täytä omilla arvoillasi (ÄLÄ commitoi .env).

(Live) Käynnistä consumer:

python mqtt_to_mongo.py

Generoi kuvat (offlinella: skriptit lukevat resampled_5min_fix.csv):

python scripts/boxplot_hours_8_21.py
python scripts/plot_zoom_2026_02_02_03.py

(Valinnainen) Ennuste:

python scripts/predict_simple.py
Mitä arvioijan kannattaa katsoa

mqtt_to_mongo.py — dokumentoitu consumer ja .env.example (keruun toteutus)

plots/ — tuottamasi visualisoinnit (aikasarja, boxplot)

results/metrics.json — mallin mittarit (jos mukana)

README ja helppo testi: aja kaksi komentoa kohdasta 5.

Huomioitavaa

Älä commitoi .env-tiedostoa tai virtuaaliympäristöä (.venv/).

Mallit (models/) ja suuret CSV:t voi jättää pois; lisää tarvittaessa latausohjeet README.md-kohtaan.

Lyhyt tulossanasto (repoon liitettynä)

Resampled 5-min data ja plotit löytyvät resampled_5min_fix.csv ja plots/.

Perustulokset ja arviointi tallennettu results/metrics.json (esim. MAE, piikkitunnistus-mittarit).
