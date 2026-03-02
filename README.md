# Datan keräys — RoboLab / AIoT labra

## Yhteenveto
Tämä repo sisältää minimipipeline:n anturidatan keruuseen (MQTT → MongoDB), yksinkertaisen EDA-vaiheen (resample, visualisoinnit) ja kevyen ennuste/piikkitunnistus-skriptin. Lopputulokset (kuvat ja mittarit) on tallennettu `plots/` ja `results/` -kansioihin.

## Sisältö (olennaiset tiedostot)
- `README.md` — tämä tiedosto  
- `mqtt_to_mongo.py` — MQTT → MongoDB consumer (reaaliaikainen)  
- `scripts/boxplot_hours_8_21.py` — boxplot (08–21, Helsinki)  
- `scripts/plot_zoom_2026_02_02_03.py` — zoom 2.–3.2.2026 (valmis skripti)  
- `scripts/predict_simple.py` — yksinkertainen ennustaja (baseline + model)  
- `resampled_5min_fix.csv` (valinnainen, näyte)  
- `plots/` — generoituja kuvia (esim. `boxplot_hours_8_21.png`)  
- `results/metrics.json` (valinnainen)  
- `requirements.txt`  
- `.env.example` (esimerkkiympäristömuuttujat; EI salasanoja)

> Huom. ÄLÄ commitoi `.env` tai `.venv`-kansiota.

---

## Nopein tapa toistaa (minimi)
1. Luo ja aktivoi virtuaaliympäristö:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
