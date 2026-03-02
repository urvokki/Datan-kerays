# scripts/plot_zoom_2026_02_02_03.py
"""
Zoom plot for 2-3 Feb 2026 (5-min aggregated series).
Reads resampled_5min_fix.csv (index = datetime), produces PNG.
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

CSV = "resampled_5min_fix.csv"
OUT = "timeseries_zoom_2026-2.png"

if not os.path.exists(CSV):
    raise SystemExit(f"CSV missing: {CSV}")

# read csv, ensure parsed datetimes and UTC timezone
df = pd.read_csv(CSV, index_col=0, parse_dates=True)
if "person_count" not in df.columns:
    raise SystemExit("CSV missing 'person_count' column")

series = df["person_count"].astype(float)

# ensure index is timezone-aware UTC (if naive, assume UTC)
if series.index.tz is None:
    series.index = series.index.tz_localize("UTC")

sub = sub.tz_convert("Europe/Helsinki")
ax.xaxis.set_major_formatter(DateFormatter("%Y-%m-%d\n%H:%M"))

# define zoom window in UTC (start inclusive, end inclusive)
start = pd.Timestamp("2026-02-04 08:00:00", tz="UTC")
end   = pd.Timestamp("2026-02-04 21:00:00", tz="UTC")

sub = series.loc[start:end]
if sub.empty:
    raise SystemExit(f"No data in window {start} - {end}. Check CSV timezone/index.")

# plot
plt.figure(figsize=(12,4))
plt.plot(sub.index, sub.values, linewidth=1)
plt.fill_between(sub.index, sub.values, alpha=0.2)
plt.title("Kävijämäärät (zoom) 2.–3.2.2026 — 5-min aggregaatti")
plt.ylabel("Person count")
plt.xlabel("Aika (UTC)")
ax = plt.gca()
ax.xaxis.set_major_formatter(DateFormatter("%Y-%m-%d\n%H:%M"))
plt.xticks(rotation=30, ha="right")
# set y limit with small margin to make small peaks visible
ymax = max(sub.max(), 1)
plt.ylim(0, ymax + 1)
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig(OUT, dpi=200)
print("Saved", OUT, " — rows plotted:", len(sub))
