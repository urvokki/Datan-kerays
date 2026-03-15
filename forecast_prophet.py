import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt

df = pd.read_csv("resampled_5min_fix.csv")

# parse datetime
df["_sensor_dt_norm"] = pd.to_datetime(df["_sensor_dt_norm"])

# poista timezone (Prophet vaatii tämän)
df["_sensor_dt_norm"] = df["_sensor_dt_norm"].dt.tz_localize(None)

# Prophet sarakenimet
df = df.rename(columns={
    "_sensor_dt_norm": "ds",
    "person_count": "y"
})

model = Prophet(
    daily_seasonality=True,
    weekly_seasonality=True
)

model.fit(df)

future = model.make_future_dataframe(
    periods=288,
    freq="5min"
)

forecast = model.predict(future)

model.plot(forecast)
plt.title("Visitor forecast")
plt.show()