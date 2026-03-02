# scripts/plot_timeseries_csv.py
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import os

CSV = "resampled_5min_fix.csv"   # tai "resampled_minute_fix.csv"
OUT = "timeseries_visitors.png"

if not os.path.exists(CSV):
    raise SystemExit(f"CSV-tiedosto puuttuu: {CSV}")

# lue CSV, oletetaan, että indeksi on aikaleima (ensimmäinen sarake)
df = pd.read_csv(CSV, index_col=0, parse_dates=True)
if "person_count" not in df.columns:
    raise SystemExit("CSV ei sisällä saraketta 'person_count'")

series = df["person_count"].astype(float)

# piirtäminen
plt.figure(figsize=(14,5))
plt.plot(series.index, series.values, linewidth=1)
plt.fill_between(series.index, series.values, alpha=0.2)
plt.title("Kävijämäärät ajan funktiona (5-min aggregaatti)")
plt.ylabel("Person count")
plt.xlabel("Aika (UTC)")
# x-akselin muotoilu
ax = plt.gca()
ax.xaxis.set_major_formatter(DateFormatter("%Y-%m-%d\n%H:%M"))
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.grid(alpha=0.3)
plt.savefig(OUT, dpi=150)
print("Tallennettu:", OUT)
