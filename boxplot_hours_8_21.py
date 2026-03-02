# scripts/boxplot_hours_8_21.py
import os
import pandas as pd
import matplotlib.pyplot as plt

CSV = "resampled_5min_fix.csv"
OUT = "boxplot_hours_8_21.png"

if not os.path.exists(CSV):
    raise SystemExit(f"CSV missing: {CSV}")

# Lue data
df = pd.read_csv(CSV, index_col=0, parse_dates=True)

if "person_count" not in df.columns:
    raise SystemExit("CSV missing 'person_count' column")

series = df["person_count"].astype(float)

# Varmista timezone: oletetaan että CSV indeksi on UTC tai naive UTC.
if series.index.tz is None:
    series.index = series.index.tz_localize("UTC")

# TÄSSÄ MUUNNETAAN HELSINGIN AIKAAN
series = series.tz_convert("Europe/Helsinki")

# Ota vain tunnit 8–21 (paikallinen Helsinki-aika nyt)
df_hours = pd.DataFrame({"person_count": series})
df_hours["hour"] = df_hours.index.hour
df_hours = df_hours[(df_hours["hour"] >= 8) & (df_hours["hour"] <= 21)]

if df_hours.empty:
    raise SystemExit("No data between 08–21 (Helsinki time).")

# Ryhmittele tunneittain (8..21)
grouped = [df_hours[df_hours["hour"] == h]["person_count"].values
           for h in range(8, 22)]

# Piirrä boxplot
plt.figure(figsize=(12,5))
plt.boxplot(grouped, labels=[str(h) for h in range(8,22)])
plt.title("Kävijämäärien jakauma tunneittain (08–21, Helsinki)")
plt.xlabel("Tunti (EET/EEST)")
plt.ylabel("Person count (5-min aggregaatti)")
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(OUT, dpi=200)

print("Saved:", OUT)
