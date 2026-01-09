import pandas as pd

# -----------------------------
# Read raw Windographer file
# -----------------------------
df = pd.read_csv(
    "wind_data_18_19.txt",
    sep="\t",
    skiprows=15,
    encoding="latin1"
)

# -----------------------------
# Rename columns
# -----------------------------
df = df.rename(columns={
    "Date/Time": "measurement_datetime",
    "100m_N Avg [m/s]": "wind_100m_n_avg",
    "100m_S Avg [m/s]": "wind_100m_s_avg",
    "80m Avg [m/s]": "wind_80m_avg",
    "50m Avg [m/s]": "wind_50m_avg",
    "20m Avg [m/s]": "wind_20m_avg",
    "10m Avg [m/s]": "wind_10m_avg",
    "Pressure 5m [mbar]": "pressure_mbar",
    "98m WV [°]": "wind_direction_98m",
    "78m WV [°]": "wind_direction_78m",
    "48m WV [°]": "wind_direction_48m",
    "Temp 5m [°C]": "temperature_5m",
    "Hum 5m": "humidity_5m"
})
df = df.drop(columns=["Unnamed: 13"], errors="ignore")
# -----------------------------
# Fix datetime format
# -----------------------------
df["measurement_datetime"] = pd.to_datetime(
    df["measurement_datetime"],
    format="%d-%m-%Y %H:%M"
)

# -----------------------------
# Add metadata and quality flags
# -----------------------------
df["data_source"] = "2018_2019"
df["data_quality"] = "GOOD"
df["quality_notes"] = ""

df.loc[df["wind_80m_avg"].isnull(), "data_quality"] = "MISSING"
df.loc[df["wind_80m_avg"].isnull(), "quality_notes"] = "Missing 80m wind speed"

# -----------------------------
# Save cleaned data
# -----------------------------
df.to_csv("wind_data_cleaned_18_19.csv", index=False)

print("2018–19 data cleaned and saved successfully.")

